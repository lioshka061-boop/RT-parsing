use crate::control::{render_template, Record, Response};
use crate::dt;
use actix::Addr;
use actix_web::get;
use actix_web::web::Data;
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::category::{self, By, CategoryRepository, TopLevel};
use rt_types::shop::{self, service::ShopService};
use rt_types::subscription::{self, service::SubscriptionService, Subscription};
use std::sync::Arc;

#[derive(Template)]
#[template(path = "landing/index.html")]
pub struct IndexPage {
    user: Option<UserCredentials>,
    subscriptions: Vec<Subscription>,
    categories: Vec<category::Category>,
    products: Vec<dt::product::Product>,
}

#[get("/")]
pub async fn index(
    user: Option<Record<UserCredentials>>,
    subscription_service: Data<Addr<SubscriptionService>>,
    shop_service: Data<Addr<ShopService>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository>>,
) -> Response {
    let subscriptions = subscription_service
        .send(subscription::service::List)
        .await??;

    // Вибираємо перший магазин, щоб підхопити його категорії
    let shop = shop_service
        .send(shop::service::List)
        .await??
        .into_iter()
        .next();
    let categories = if let Some(shop) = shop {
        category_repo
            .select(&TopLevel(By(shop.id)))
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    // Беремо доступні товари, сортуємо за датою і обмежуємо топ-8
    let mut products = dt_repo
        .select(&dt::product::AvailableSelector)
        .await
        .unwrap_or_default();
    products.sort_by_key(|p| p.last_visited);
    products.reverse();
    products.truncate(8);

    render_template(IndexPage {
        user: user.map(|u| u.t),
        subscriptions,
        categories,
        products,
    })
}
