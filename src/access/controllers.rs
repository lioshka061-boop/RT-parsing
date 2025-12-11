use crate::control::{render_template, see_other, Record, Response};
use crate::subscription::controllers::UserSubscription;
use crate::subscription::payment::{self, service::PaymentService, Payment};
use actix::Addr;
use actix_session::Session;
use actix_web::{
    get, post,
    web::{Data, Form, Query},
};
use anyhow::Context as AnyhowContext;
use askama::Template;
use rt_types::access::service::{UserCredentialsDto, UserCredentialsService};
use rt_types::access::{self, Login, RegistrationToken, UserCredentials};
use rt_types::shop::{
    self,
    service::{CreateShopPermission, ShopService},
    Shop,
};
use rt_types::subscription::service::SubscriptionService;
use rt_types::subscription::{self, Subscription};
use serde::Deserialize;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use time::OffsetDateTime;
use typesafe_repository::{GetIdentity, IdentityOf};

#[derive(Deserialize)]
pub struct LoginDto {
    pub login: Login,
    pub password: String,
}

#[post("/login")]
async fn log_in(
    form: Form<LoginDto>,
    session: Session,
    service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let creds = service
        .send(access::service::Get(form.login.clone()))
        .await
        .context("Unable to send message to UserCredentialsService")??;
    let creds = match creds {
        Some(c) => c,
        None => {
            log::info!("Creds not found");
            return Ok(see_other("/login?invalid"));
        }
    };
    if creds
        .password
        .check(&form.password)
        .context("Unable to verify password")?
    {
        session
            .insert("login", creds.login)
            .context("Unable to insert login into session")?;
        Ok(see_other("/shops"))
    } else {
        Ok(see_other("/login?invalid"))
    }
}

#[get("/logout")]
async fn log_out(session: Session) -> Response {
    session.clear();
    Ok(see_other("/login"))
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage {
    err: bool,
    user: Option<UserCredentials>,
}

#[derive(Deserialize)]
struct LoginQuery {
    invalid: Option<String>,
}

#[get("/login")]
async fn login_page(q: Query<LoginQuery>) -> Response {
    render_template(LoginPage {
        err: q.invalid.is_some(),
        user: None,
    })
}

#[derive(Template)]
#[template(path = "shops.html")]
struct ShopsPage {
    shops: Vec<Shop>,
    user: UserCredentials,
    create_permission: Option<CreateShopPermission>,
}

#[get("/shops")]
async fn shops(
    shop_service: Data<Addr<ShopService>>,
    user: Record<UserCredentials>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let user = user.t;
    let mut shops = BTreeSet::new();
    for id in user.available_shops() {
        let shop = shop_service
            .send(shop::service::Get(id))
            .await
            .context("Unable to send message to ShopService")??;
        match shop {
            Some(s) => {
                shops.insert(s);
            }
            None => {
                log::warn!("Shop not found: {id}");
            }
        }
    }
    let owned_shops = shop_service
        .send(shop::service::ListBy(user.id()))
        .await??;
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let create_permission = CreateShopPermission::acquire(&user, &owned_shops, &subscription);
    let mut owned_shops = owned_shops.into_inner().into_iter().collect();
    shops.append(&mut owned_shops);
    let mut shops = shops.into_iter().collect::<Vec<_>>();
    shops.sort_by(|a, b| a.name.cmp(&b.name));
    render_template(ShopsPage {
        shops,
        user,
        create_permission,
    })
}

#[derive(Deserialize)]
pub struct RegisterDto {
    #[serde(flatten)]
    dto: UserCredentialsDto,
    token: RegistrationToken,
}

#[post("/register")]
async fn register(
    Form(RegisterDto { dto, token }): Form<RegisterDto>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    user_credentials_service
        .send(access::service::Register(dto, token))
        .await??;
    Ok(see_other("/login"))
}

#[derive(Template)]
#[template(path = "register.html")]
pub struct RegisterPage {
    user: Option<UserCredentials>,
}

#[get("/register")]
async fn register_page(user: Option<Record<UserCredentials>>) -> Response {
    render_template(RegisterPage {
        user: user.map(|u| u.t),
    })
}

#[derive(Template)]
#[template(path = "me.html")]
pub struct MePage {
    user: UserCredentials,
}

#[get("/me")]
async fn me_page(user: Record<UserCredentials>) -> Response {
    render_template(MePage { user: user.t })
}

#[derive(Template)]
#[template(path = "me/subscription.html")]
pub struct MeSubscriptionPage {
    user: UserCredentials,
    subscription: Option<(Subscription, Option<Payment>)>,
}

#[get("/me/subscription")]
async fn me_subscription_page(
    user: Record<UserCredentials>,
    UserSubscription(subscription): UserSubscription,
    payment_service: Data<Addr<PaymentService>>,
) -> Response {
    let user = user.t;
    let subscription = match subscription {
        Some(sub) => Some((
            sub,
            payment_service
                .send(payment::service::HasValidPayment(user.login.clone()))
                .await??,
        )),
        None => None,
    };
    render_template(MeSubscriptionPage { user, subscription })
}

#[derive(Template)]
#[template(path = "me/subscriptions.html")]
pub struct MeSubscriptionsPage {
    user: UserCredentials,
    subscriptions: Vec<Subscription>,
}

#[get("/me/subscriptions")]
async fn me_subscriptions_page(
    user: Record<UserCredentials>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let subscriptions = subscription_service
        .send(subscription::service::ListLatest)
        .await??;
    render_template(MeSubscriptionsPage {
        user: user.t,
        subscriptions,
    })
}

#[derive(Deserialize)]
pub struct ApplySubscriptionDto {
    pub subscription_id: IdentityOf<Subscription>,
}

#[post("/me/subscription/apply")]
async fn apply_subscription(
    user: Record<UserCredentials>,
    form: Form<ApplySubscriptionDto>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let ApplySubscriptionDto { subscription_id } = form.into_inner();
    let subscription = subscription_service
        .send(subscription::service::Get(subscription_id))
        .await??
        .ok_or(anyhow::anyhow!("Subscription not found"))?;
    user.map(|user| {
        user.subscription = Some((subscription.id, subscription.version));
    })
    .await?;
    Ok(see_other("/me/subscription"))
}
