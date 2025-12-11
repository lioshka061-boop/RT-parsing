use crate::control::{render_template, Record, Response};
use actix::Addr;
use actix_web::get;
use actix_web::web::Data;
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::subscription::{self, service::SubscriptionService, Subscription};

#[derive(Template)]
#[template(path = "landing/index.html")]
pub struct IndexPage {
    user: Option<UserCredentials>,
    subscriptions: Vec<Subscription>,
}

#[get("/")]
pub async fn index(
    user: Option<Record<UserCredentials>>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let subscriptions = subscription_service
        .send(subscription::service::List)
        .await??;
    render_template(IndexPage {
        user: user.map(|u| u.t),
        subscriptions,
    })
}
