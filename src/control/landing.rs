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
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
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
    let (categories, published_vendors, allow_dt) = if let Some(shop) = shop {
        let published_vendors: Vec<String> = shop
            .export_entries
            .iter()
            .flat_map(|e| e.links.as_ref().into_iter().flatten())
            .filter(|l| l.publish || l.options.as_ref().map(|o| o.publish).unwrap_or(true))
            .map(|l| l.vendor_name())
            .filter(|v| !v.is_empty())
            .collect();
        let allow_dt = shop.export_entries.iter().any(|e| {
            e.dt_parsing
                .as_ref()
                .map(|o| o.options.publish)
                .unwrap_or(false)
                || e.maxton_parsing
                    .as_ref()
                    .map(|o| o.options.publish)
                    .unwrap_or(false)
                || e.jgd_parsing
                    .as_ref()
                    .map(|o| o.options.publish)
                    .unwrap_or(false)
                || e.pl_parsing
                    .as_ref()
                    .map(|o| o.options.publish)
                    .unwrap_or(false)
                || e.skm_parsing
                    .as_ref()
                    .map(|o| o.options.publish)
                    .unwrap_or(false)
                || e.dt_tt_parsing
                    .as_ref()
                    .map(|o| o.options.publish)
                    .unwrap_or(false)
        });
        category_repo
            .select(&TopLevel(By(shop.id)))
            .await
            .map(|cats| (cats, published_vendors, allow_dt))
            .unwrap_or_default()
    } else {
        (vec![], vec![], true)
    };

    // Беремо доступні товари, сортуємо за датою і обмежуємо топ-8
    let mut products = if allow_dt {
        let base = dt_repo
            .select(&dt::product::AvailableSelector)
            .await
            .unwrap_or_default();
        if !published_vendors.is_empty() {
            let filtered = base
                .into_iter()
                .filter(|p| {
                    let title = p.title.to_lowercase();
                    let brand = p.brand.to_lowercase();
                    let model = p.model.0.to_lowercase();
                    published_vendors.iter().any(|v| {
                        let v = v.to_lowercase();
                        brand.contains(&v) || title.contains(&v) || model.contains(&v)
                    })
                })
                .collect::<Vec<_>>();
            if filtered.is_empty() {
                dt_repo
                    .select(&dt::product::AvailableSelector)
                    .await
                    .unwrap_or_default()
            } else {
                filtered
            }
        } else {
            base
        }
    } else {
        vec![]
    };
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
