use crate::control::{render_template, Record, Response};
use crate::dt;
use actix_web::get;
use actix_web::web::{Data, Query};
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::category::{By, CategoryRepository, TopLevel};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone)]
pub struct UiCategory {
    pub id: String,
    pub name: String,
    pub selected: bool,
}

#[derive(Template)]
#[template(path = "search.html")]
pub struct SearchPage {
    user: Option<UserCredentials>,
    categories: Vec<UiCategory>,
    products: Vec<dt::product::Product>,
    query: String,
    has_query: bool,
    selected_cat: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    pub cat: Option<String>,
}

#[get("/search")]
pub async fn search(
    user: Option<Record<UserCredentials>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<SearchParams>,
) -> Response {
    let selected_cat = params.cat.clone();

    let shop = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .next();

    // Категорії з першого магазину
    let categories: Vec<UiCategory> = if let Some(shop) = &shop {
        category_repo
            .select(&TopLevel(By(shop.id)))
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| UiCategory {
                id: c.id.to_string(),
                name: c.name,
                selected: selected_cat
                    .as_ref()
                    .map(|v| v == &c.id.to_string())
                    .unwrap_or(false),
            })
            .collect()
    } else {
        vec![]
    };

    // Публікаційні обмеження
    let (allow_dt, published_vendors) = shop
        .as_ref()
        .map(|shop| {
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
            (allow_dt, published_vendors)
        })
        .unwrap_or((true, vec![]));

    let mut products = if allow_dt {
        dt_repo
            .select(&dt::product::AvailableSelector)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    // Фільтр за опублікованими постачальниками
    if !published_vendors.is_empty() {
        products = products
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
            .collect();
    }

    // Пошук за текстом і категорією
    let q = params.q.as_ref().map(|s| s.to_lowercase());
    let cat = params.cat.clone();
    let allowed_cats: HashSet<String> = categories.iter().map(|c| c.id.clone()).collect();

    products = products
        .into_iter()
        .filter(|p| {
            let text_ok = if let Some(q) = &q {
                p.title.to_lowercase().contains(q)
                    || p.brand.to_lowercase().contains(q)
                    || p.model.0.to_lowercase().contains(q)
            } else {
                true
            };
            let cat_ok = if let Some(ref c) = cat {
                // якщо категорія існує в списку — дивимось співпадіння рядка
                if !allowed_cats.is_empty() && !allowed_cats.contains(c) {
                    true
                } else {
                    p.category
                        .as_ref()
                        .map(|pc| pc.contains(c))
                        .unwrap_or(false)
                }
            } else {
                true
            };
            text_ok && cat_ok
        })
        .collect();

    products.sort_by_key(|p| p.last_visited);
    products.reverse();

    render_template(SearchPage {
        user: user.map(|u| u.t),
        categories,
        products,
        query: params.q.clone().unwrap_or_default(),
        has_query: params.q.is_some(),
        selected_cat,
    })
}
