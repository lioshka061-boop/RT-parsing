use crate::control::{render_template, Record, Response};
use crate::dt;
use actix_web::get;
use actix_web::web::{Data, Path};
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::shop::{self, service::ShopService};
use std::sync::Arc;

#[derive(Template)]
#[template(path = "product.html")]
pub struct ProductPage {
    user: Option<UserCredentials>,
    product: dt::product::Product,
}

#[get("/product/{slug:.*}")]
pub async fn view(
    slug: Path<String>,
    user: Option<Record<UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_service: Data<actix::Addr<ShopService>>,
) -> Response {
    let slug = slug.into_inner();
    let shop = shop_service
        .send(shop::service::List)
        .await??
        .into_iter()
        .next();
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

    if !allow_dt {
        return Err(crate::control::ControllerError::NotFound.into());
    }

    let mut products = dt_repo
        .select(&dt::product::AvailableSelector)
        .await
        .unwrap_or_default();

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

    let slug_lower = slug.to_lowercase();
    let product = products
        .into_iter()
        .find(|p| {
            let p_slug = p.slug().to_lowercase();
            let url = p.url.0.to_lowercase();
            p_slug == slug_lower
                || p.article.to_lowercase() == slug_lower
                || url.trim_matches('/').ends_with(&slug_lower)
        })
        .ok_or(crate::control::ControllerError::NotFound)?;

    render_template(ProductPage {
        user: user.map(|u| u.t),
        product,
    })
}
