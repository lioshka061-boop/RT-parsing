use crate::control::{Record, Response};
use crate::dt;
use actix_web::get;
use actix_web::web::{Data, Path, Query};
use actix_web::HttpRequest;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ProductsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Serialize)]
pub struct ProductDto {
    pub article: String,
    pub title: String,
    pub model: String,
    pub brand: String,
    pub price: Option<usize>,
    pub available: rt_types::Availability,
    pub url: String,
    pub images: Vec<String>,
    pub category: Option<String>,
}

fn ensure_api_key(req: &HttpRequest) -> Result<(), crate::control::ControllerError> {
    if let Ok(expected) = std::env::var("SITE_API_KEY") {
        if expected.trim().is_empty() {
            return Ok(());
        }
        let provided = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        if Some(expected) != provided {
            return Err(crate::control::ControllerError::Forbidden);
        }
    }
    Ok(())
}

#[get("/api/site/products")]
pub async fn list_products(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<ProductsQuery>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;

    // Визначаємо, чи потрібно показувати товари (publish)
    let shop = shop_service
        .send(rt_types::shop::service::List)
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
        return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new()));
    }

    // Базові товари
    let mut products = dt_repo
        .select(&dt::product::AvailableSelector)
        .await
        .unwrap_or_default();

    // Якщо задані конкретні постачальники для публікації — фільтруємо
    if !published_vendors.is_empty() {
        let filtered: Vec<_> = products
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
        products = filtered;
    }

    products.sort_by_key(|p| p.last_visited);
    products.reverse();

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let slice = products
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|p| ProductDto {
            article: p.article.clone(),
            title: p.title.clone(),
            model: p.format_model().unwrap_or(p.model.0.clone()),
            brand: p.brand.clone(),
            price: p.price,
            available: p.available,
            url: p.url.0.clone(),
            images: p.images.clone(),
            category: p.category.clone(),
        })
        .collect::<Vec<_>>();

    Ok(actix_web::HttpResponse::Ok().json(slice))
}

#[get("/api/site/products/{article}")]
pub async fn get_product(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    article: Path<String>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    let shop = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .next();
    let published_vendors: Vec<String> = shop
        .as_ref()
        .map(|shop| {
            shop.export_entries
                .iter()
                .flat_map(|e| e.links.as_ref().into_iter().flatten())
                .filter(|l| l.publish || l.options.as_ref().map(|o| o.publish).unwrap_or(true))
                .map(|l| l.vendor_name())
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let article = article.into_inner();
    let product = dt_repo
        .select(&dt::product::AvailableSelector)
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|p| {
            p.article.eq_ignore_ascii_case(&article)
                && (published_vendors.is_empty()
                    || published_vendors.iter().any(|v| {
                        let v = v.to_lowercase();
                        p.brand.to_lowercase().contains(&v)
                            || p.title.to_lowercase().contains(&v)
                            || p.model.0.to_lowercase().contains(&v)
                    }))
        })
        .ok_or(crate::control::ControllerError::NotFound)?;

    Ok(actix_web::HttpResponse::Ok().json(ProductDto {
        article: product.article.clone(),
        title: product.title.clone(),
        model: product.format_model().unwrap_or(product.model.0.clone()),
        brand: product.brand.clone(),
        price: product.price,
        available: product.available,
        url: product.url.0.clone(),
        images: product.images.clone(),
        category: product.category.clone(),
    }))
}
