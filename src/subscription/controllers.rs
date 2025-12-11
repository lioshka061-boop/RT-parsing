use crate::control::{
    render_template, see_other, ControlPanelAccess, ControllerError, Record, Response,
};
use crate::format_duration;
use crate::invoice::{self, service::InvoiceService};
use crate::invoice::{AcceptPaymentBuilder, Currency, InvoiceConfirmation};
use crate::subscription::payment::{self, service::PaymentService, Payment, PaymentStatus};
use actix::Addr;
use actix_web::{dev::Payload, FromRequest, HttpRequest, HttpResponse};
use actix_web::{
    get, post,
    web::{Data, Form, Json, Path},
};
use anyhow::Context;
use askama::Template;
use hmac::Mac;
use itertools::Itertools;
use md5::Md5;
use rt_types::access::{self, service::UserCredentialsService, UserCredentials};
use rt_types::shop::ShopLimits;
use rt_types::subscription::{
    self, service::SubscriptionService, Subscription, SubscriptionVersion,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::num::NonZero;
use std::num::NonZeroU32;
use time::{Duration, OffsetDateTime};
use typesafe_repository::IdentityOf;
use uuid::Uuid;

#[derive(Template)]
#[template(path = "control_panel/subscriptions.html")]
pub struct SubscriptionsPage {
    pub user: UserCredentials,
    pub subscriptions: BTreeMap<IdentityOf<Subscription>, Vec<Subscription>>,
}

#[get("/control_panel/subscriptions")]
async fn subscriptions_page(
    ControlPanelAccess { user, .. }: ControlPanelAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let subscriptions = subscription_service
        .send(subscription::service::List)
        .await??;
    let subscriptions = subscriptions.into_iter().into_group_map_by(|s| s.id);
    let subscriptions = subscriptions
        .into_iter()
        .map(|(k, mut v)| {
            v.sort_by_key(|s| s.version);
            v.reverse();
            (k, v)
        })
        .collect();
    render_template(SubscriptionsPage {
        user,
        subscriptions,
    })
}

#[derive(Template)]
#[template(path = "control_panel/subscription_versions.html")]
pub struct SubscriptionVersionsPage {
    user: UserCredentials,
    subscriptions: Vec<Subscription>,
    id: IdentityOf<Subscription>,
}

#[get("/control_panel/subscriptions/{id}/versions")]
async fn subscription_versions_page(
    ControlPanelAccess { user, .. }: ControlPanelAccess,
    path: Path<IdentityOf<Subscription>>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let id = path.into_inner();
    let subscriptions = subscription_service
        .send(subscription::service::ListVersions(id))
        .await??;
    render_template(SubscriptionVersionsPage {
        user,
        id,
        subscriptions,
    })
}

#[derive(Template)]
#[template(path = "control_panel/add_subscription.html")]
pub struct AddSubscriptionPage {
    pub user: Option<UserCredentials>,
}

#[get("/control_panel/subscriptions/add")]
async fn add_subscription_page(ControlPanelAccess { user, .. }: ControlPanelAccess) -> Response {
    render_template(AddSubscriptionPage { user: Some(user) })
}

#[derive(Template)]
#[template(path = "control_panel/edit_subscription.html")]
pub struct EditSubscriptionPage {
    pub user: Option<UserCredentials>,
    pub subscription: Subscription,
}

#[get("/control_panel/subscriptions/{subscription}/{version}/edit")]
async fn edit_subscription_page(
    ControlPanelAccess { user, .. }: ControlPanelAccess,
    path: Path<(IdentityOf<Subscription>, SubscriptionVersion)>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let (id, version) = path.into_inner();
    let subscription = subscription_service
        .send(subscription::service::GetVersion(id, version))
        .await??
        .ok_or(anyhow::anyhow!("Subscription not found"))?;
    render_template(EditSubscriptionPage {
        user: Some(user),
        subscription,
    })
}

#[derive(Deserialize)]
pub struct AddSubscriptionDto {
    name: String,
    maximum_shops: NonZeroU32,
    price: Decimal,
    #[serde(flatten)]
    limits: ShopLimits,
}

impl Into<Subscription> for AddSubscriptionDto {
    fn into(self) -> Subscription {
        let Self {
            name,
            maximum_shops,
            price,
            limits,
        } = self;
        Subscription {
            id: Uuid::new_v4(),
            limits,
            maximum_shops,
            price,
            name,
            version: 0,
            yanked: false,
        }
    }
}

#[post("/control_panel/subscriptions/add")]
async fn add_subscription(
    ControlPanelAccess { .. }: ControlPanelAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
    subscription: Form<AddSubscriptionDto>,
) -> Response {
    let subscription = subscription.into_inner().into();
    subscription_service
        .send(subscription::service::Add(subscription))
        .await??;
    Ok(see_other("/control_panel/subscriptions"))
}

#[post("/control_panel/subscriptions/{id}/{version}/copy")]
async fn copy_subscription(
    path: Path<(IdentityOf<Subscription>, SubscriptionVersion)>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let (id, version) = path.into_inner();
    let mut sub = subscription_service
        .send(subscription::service::GetVersion(id, version))
        .await??
        .ok_or(ControllerError::NotFound)?;
    sub.name = format!("{} (копия)", sub.name);
    sub.version = 0;
    let id = Uuid::new_v4();
    sub.id = id;
    subscription_service
        .send(subscription::service::Add(sub))
        .await??;
    Ok(see_other(&format!(
        "/control_panel/subscriptions/{id}/versions"
    )))
}

#[derive(Deserialize)]
pub struct EditSubscriptionDto {
    id: Uuid,
    name: String,
    maximum_shops: NonZeroU32,
    price: Decimal,
    #[serde(flatten)]
    limits: ShopLimits,
}

impl Into<Subscription> for EditSubscriptionDto {
    fn into(self) -> Subscription {
        Subscription {
            id: self.id,
            limits: self.limits,
            maximum_shops: self.maximum_shops,
            price: self.price,
            name: self.name,
            version: 0,
            yanked: false,
        }
    }
}

#[post("/control_panel/subscriptions/{subscription}/{version}/edit")]
async fn edit_subscription(
    ControlPanelAccess { .. }: ControlPanelAccess,
    path: Path<(IdentityOf<Subscription>, SubscriptionVersion)>,
    subscription: Form<EditSubscriptionDto>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let (subscription_id, _) = path.into_inner();
    let subscription = subscription.into_inner().into();
    let version = subscription_service
        .send(subscription::service::Update(subscription))
        .await??;
    Ok(see_other(&format!(
        "/control_panel/subscriptions/{subscription_id}/{version}/edit",
    )))
}

#[post("/control_panel/subscriptions/{subscription}/{subscription_version}/remove")]
async fn remove_subscription(
    ControlPanelAccess { .. }: ControlPanelAccess,
    path: Path<(IdentityOf<Subscription>, SubscriptionVersion)>,
    subscription_service: Data<Addr<SubscriptionService>>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let (id, version) = path.into_inner();
    let guard = user_credentials_service
        .send(access::service::SubscriptionUsed(id, version))
        .await??;
    if let Some(guard) = guard {
        subscription_service
            .send(subscription::service::Remove(id, version, guard))
            .await??;
        Ok(see_other("/control_panel/subscriptions"))
    } else {
        Err(ControllerError::Forbidden)
    }
}

#[post("/invoice/confirm")]
async fn confirm_invoice(
    confirmation: Json<InvoiceConfirmation>,
    invoice_service: Data<Addr<InvoiceService>>,
) -> Response {
    log::info!("Got invoice confirmation");
    log::info!("{confirmation:?}");
    let res = invoice_service
        .send(invoice::service::ConfirmInvoice(confirmation.into_inner()))
        .await??;
    Ok(HttpResponse::Ok().json(&res))
}

#[derive(Deserialize)]
pub struct PayQuery {
    pub days: u16,
}

#[post("/me/subscription/pay")]
async fn pay(
    invoice_service: Data<Addr<InvoiceService>>,
    payment_service: Data<Addr<PaymentService>>,
    user: Record<UserCredentials>,
    user_subscription: UserSubscription,
    query: Form<PayQuery>,
) -> Response {
    let self_addr: String = envmnt::get_parse("SELF_ADDR").context("SELF_ADDR not set")?;
    let sub = match user_subscription.0 {
        Some(sub) => sub,
        None => return Err(ControllerError::NotFound),
    };
    let query = query.into_inner();
    let days = query.days.max(30);
    let merchant_secret_key: String =
        envmnt::get_parse("WAYFORPAY_SECRET_KEY").context("WAYFORPAY_SECRET_KEY not set")?;
    let merchant_account: String =
        envmnt::get_parse("WAYFORPAY_MERCHANT_ACCOUNT").context("WAYFORPAY_SECRET_KEY not set")?;
    let merchant_domain_name = "46.254.107.103".to_string();
    let order_reference = Uuid::new_v4();
    let (user, _) = user.into_inner();
    let order_date = time::OffsetDateTime::now_utc().unix_timestamp();
    let amount = sub.price / Decimal::from(30) * Decimal::from(query.days);
    let currency = "USD";
    let product_name = format!(
        "Продление подписки на llink-import на {}. План '{}'",
        format_duration(&std::time::Duration::from_millis(
            days as u64 * 24 * 60 * 60 * 1000
        )),
        sub.name
    );
    let product_count = 1;
    let product_price = sub.price;
    let mut hasher = hmac::Hmac::<Md5>::new_from_slice(merchant_secret_key.as_bytes())
        .context("Unable to init hasher")?;
    let hash_input = format!(
        "{merchant_account};{merchant_domain_name}\
        ;{order_reference};{order_date}\
        ;{amount};{currency}\
        ;{product_name};{product_count}\
        ;{product_price}"
    );
    hasher.update(&hash_input.as_bytes());
    let payment = AcceptPaymentBuilder::default()
        .merchant_account(merchant_account)
        .merchant_domain_name(merchant_domain_name)
        .amount(amount)
        .currency(Currency::Usd)
        .order_reference(order_reference.to_string())
        .order_date(order_date)
        .merchant_signature(format!("{:x}", hasher.finalize().into_bytes()))
        .product_name(vec![product_name])
        .product_count(vec![product_count])
        .product_price(vec![product_price])
        .service_url(Some("http://46.254.107.103/invoice/confirm".to_string()))
        .return_url(Some(format!(
            "http://{self_addr}/invoice/completed?payment={order_reference}"
        )))
        .regular_mode(invoice::RegularMode::Monthly)
        .build()
        .context("Unable to build CreateInvoice struct")?;
    if let Some(sub) = user.subscription {
        let url = invoice_service
            .send(invoice::service::AcceptPayment(payment))
            .await??;
        payment_service
            .send(payment::service::Add(Payment {
                id: order_reference,
                user: user.login,
                subscription: sub,
                paid_days: days,
                amount,
                currency: currency.to_string(),
                status: PaymentStatus::Pending {
                    due: OffsetDateTime::now_utc() + Duration::seconds(1 * 60 * 60),
                },
            }))
            .await??;
        Ok(see_other(&url))
    } else {
        Err(ControllerError::NotFound)
    }
}

pub struct UserSubscription(pub Option<Subscription>);

impl FromRequest for UserSubscription {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let user = Record::<UserCredentials>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract UserCredentials from request"))?
                .t;
            let subscription_service = Data::<Addr<SubscriptionService>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract SubscriptionService"))?;
            let subscription = match user.subscription {
                Some((id, ver)) => {
                    subscription_service
                        .send(subscription::service::GetVersion(id, ver))
                        .await??
                }
                None => None,
            };
            Ok(UserSubscription(subscription))
        })
    }
}
