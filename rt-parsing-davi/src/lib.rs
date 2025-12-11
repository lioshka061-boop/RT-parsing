use actix::prelude::*;
use anyhow::Context as AnyhowContext;
use async_trait::async_trait;
use derive_more::Constructor;
use futures::StreamExt;
use itertools::{Either, Itertools};
use log_error::LogError;
use rt_types::category::Category;
use rt_types::product::AvailableSelector;
use rt_types::shop::Shop;
use rt_types::{Availability, Url};
use rust_decimal::Decimal;
use scraper::Html;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::pin::pin;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_postgres::Row;
use typesafe_repository::async_ops::{Get, GetBy, List, Save, Select};
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::SelectBy;
use uuid::Uuid;

#[derive(Clone)]
pub struct ParsingOptions {
    pub client: reqwest_middleware::ClientWithMiddleware,
    pub repo: Arc<dyn ProductRepository>,
}

#[derive(Id, Clone, Debug)]
#[Id(ref_id, get_id)]
pub struct Product {
    pub title: String,
    pub description: Option<String>,
    pub price: u32,
    #[id]
    pub article: String,
    pub available: Availability,
    #[id_by]
    pub url: Url,
    pub last_visited: OffsetDateTime,
    pub images: Vec<String>,
    pub properties: HashMap<String, String>,
    pub categories: Vec<String>,
}

impl Into<rt_types::product::Product> for Product {
    fn into(self) -> rt_types::product::Product {
        let vendor = "davi";
        rt_types::product::Product {
            id: rt_types::product::generate_id(&self.article, vendor, &None),
            title: self.title,
            ua_translation: None,
            description: self.description,
            price: Decimal::from(self.price),
            article: self.article,
            in_stock: None,
            currency: "UAH".to_string(),
            keywords: None,
            params: self.properties,
            brand: String::new(),
            model: String::new(),
            category: None,
            available: self.available,
            vendor: vendor.to_string(),
            images: self
                .images
                .into_iter()
                .map(|i| format!("https://davi.com.ua{i}"))
                .collect(),
        }
    }
}

pub fn get_categories<H: BuildHasher>(
    products: &Vec<Product>,
    mut categories: HashSet<Category, H>,
    shop_id: IdentityOf<Shop>,
) -> HashSet<Category, H> {
    for p in products {
        let mut parent_id = None;
        for name in p.categories.iter() {
            parent_id = match categories.iter().find(|ca| &ca.name == name) {
                Some(c) => Some(c.id),
                None => {
                    let id = Uuid::new_v4();
                    categories.insert(Category {
                        id,
                        parent_id,
                        regex: name
                            .as_str()
                            .try_into()
                            .log_error("Unable to parse category name as regex"),
                        name: name.to_string(),
                        shop_id,
                    });
                    Some(id)
                }
            };
        }
    }
    categories
}

impl Product {
    pub fn enrich<'a, C: Iterator<Item = &'a Category>>(
        self,
        mut categories: C,
    ) -> EnrichedProductRef {
        EnrichedProductRef {
            category: self
                .categories
                .last()
                .and_then(|name| categories.find(|c| c.name == *name))
                .cloned(),
            product: self,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnrichedProductRef {
    product: Product,
    category: Option<Category>,
}

impl Into<rt_types::product::Product> for EnrichedProductRef {
    fn into(self) -> rt_types::product::Product {
        let vendor = "davi";
        rt_types::product::Product {
            category: self.category.map(|c| c.id),
            id: rt_types::product::generate_id(&self.product.article, vendor, &None),
            title: self.product.title,
            ua_translation: None,
            description: self.product.description,
            price: Decimal::from(self.product.price),
            article: self.product.article,
            in_stock: None,
            currency: "UAH".to_string(),
            keywords: None,
            params: self.product.properties,
            brand: String::new(),
            model: String::new(),
            available: self.product.available,
            vendor: vendor.to_string(),
            images: self
                .product
                .images
                .into_iter()
                .map(|i| format!("https://davi.com.ua{i}"))
                .collect(),
        }
    }
}

pub mod selectors {
    #![allow(clippy::unwrap_used)]
    use once_cell::sync::Lazy;
    use scraper::Selector;

    pub mod product {
        use super::*;

        pub static TITLE: Lazy<Selector> = Lazy::new(|| Selector::parse(".product-title").unwrap());
        pub static AVAILABILITY: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".product-header__availability").unwrap());
        pub static ARTICLE: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".product-header__code").unwrap());
        pub static PRICE: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".product-price__item").unwrap());
        pub static CATEGORIES: Lazy<Selector> =
            Lazy::new(|| Selector::parse("nav.breadcrumbs span:not(:has(span))").unwrap());
        pub static DESCRIPTION: Lazy<Selector> = Lazy::new(|| {
            Selector::parse(".product-description .text:not(:has(table:only-child))").unwrap()
        });
        pub static PROPERTIES: Lazy<Selector> = Lazy::new(|| {
            Selector::parse(".product-description table tr:has(td:nth-child(2))").unwrap()
        });
        pub static PROPERTIES_NAME: Lazy<Selector> =
            Lazy::new(|| Selector::parse("td:first-child").unwrap());
        pub static PROPERTIES_VALUE: Lazy<Selector> = Lazy::new(|| {
            Selector::parse("td:nth-child(2) a, td:nth-child(2):not(:has(a))").unwrap()
        });
        pub static IMAGES: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".gallery__photo-img").unwrap());
    }

    pub mod list {
        use super::*;

        pub static PRODUCT: Lazy<Selector> =
            Lazy::new(|| Selector::parse("li.catalog-grid__item").unwrap());
        pub static PRODUCT_LINK: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".catalogCard-title a").unwrap());
        pub static PRODUCT_PRICE: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".catalogCard-price").unwrap());
        pub static LAST_PAGE: Lazy<Selector> =
            Lazy::new(|| Selector::parse(".pager__item.j-catalog-pagination-btn span").unwrap());
    }

    pub static CATEGORIES: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".products-menu__title-link").unwrap());
}

pub fn parse_product_page(document: &Html, url: &Url) -> Result<Product, anyhow::Error> {
    let article = document
        .select(&selectors::product::ARTICLE)
        .map(|e| e.inner_html().replace("Артикул:", "").trim().to_string())
        .next()
        .ok_or(anyhow::anyhow!("Article not found"))?;
    let categories = document
        .select(&selectors::product::CATEGORIES)
        .map(|e| e.inner_html())
        .collect::<Vec<_>>();
    let categories = categories.into_iter().rev().skip(1).rev().collect();
    Ok(Product {
        title: document
            .select(&selectors::product::TITLE)
            .map(|e| e.inner_html())
            .next()
            .ok_or(anyhow::anyhow!("Title not found"))?,
        available: document
            .select(&selectors::product::AVAILABILITY)
            .map(|e| match e.inner_html().trim().to_lowercase().as_str() {
                "в наявності" | "в наличии" => Availability::Available,
                "немає в наявності" | "нет в наличии" | "очікується" | "ожидается" => {
                    Availability::NotAvailable
                }
                e => {
                    log::warn!("Invalid availability variant for product {article}: {e}");
                    Availability::NotAvailable
                }
            })
            .next()
            .ok_or(anyhow::anyhow!("Availability not found"))?,
        categories,
        description: document
            .select(&selectors::product::DESCRIPTION)
            .map(|e| e.inner_html())
            .next(),
        images: document
            .select(&selectors::product::IMAGES)
            .filter_map(|e| match e.attr("src") {
                Some(src) => Some(src.to_string()),
                None => {
                    log::warn!("Image without src for product {article}");
                    None
                }
            })
            .collect(),
        price: document
            .select(&selectors::product::PRICE)
            .filter_map(|e| {
                Some(
                    e.inner_html()
                        .lines()
                        .last()?
                        .replace("грн", "")
                        .replace(" ", ""),
                )
            })
            .filter(|e| e != "Цінууточнюйте" && e != "Ценууточняйте")
            .map(|e| e.parse().context(format!("Unable to parse price {e}")))
            .next()
            .ok_or(anyhow::anyhow!("Price not found"))??,
        url: url.clone(),
        last_visited: OffsetDateTime::now_utc(),
        properties: document
            .select(&selectors::product::PROPERTIES)
            .filter_map(|e| {
                Some((
                    e.select(&selectors::product::PROPERTIES_NAME)
                        .next()
                        .map(|e| e.inner_html())
                        .ok_or(anyhow::anyhow!("Unable to find property name"))
                        .log_error(&format!("Unable to parse product {article} properties"))?,
                    e.select(&selectors::product::PROPERTIES_VALUE)
                        .next()
                        .map(|e| e.inner_html())
                        .ok_or(anyhow::anyhow!("Unable to find property value"))
                        .log_error(&format!("Unable to parse product {article} properties"))?,
                ))
            })
            .collect(),
        article,
    })
}

#[derive(Debug)]
pub struct ProductEntry {
    pub url: Url,
    pub price: Option<u32>,
}

pub async fn parse_product_list(document: &Html) -> Result<Vec<ProductEntry>, anyhow::Error> {
    let products = document
        .select(&selectors::list::PRODUCT)
        .map(|e| {
            let price = e
                .select(&selectors::list::PRODUCT_PRICE)
                .next()
                .and_then(|e| {
                    e.inner_html()
                        .lines()
                        .last()
                        .filter(|e| !e.contains("Ціну уточнюйте") && !e.contains("Цену уточняйте"))
                        .map(|l| l.replace("грн", "").replace(" ", "").to_string())
                });
            Ok(ProductEntry {
                url: e
                    .select(&selectors::list::PRODUCT_LINK)
                    .next()
                    .and_then(|e| e.attr("href"))
                    .map(|e| Url(e.to_string()))
                    .ok_or(anyhow::anyhow!("Product entry without url"))?,
                price: price
                    .as_ref()
                    .map(|e| e.parse())
                    .transpose()
                    .log_error(&format!("Unable to parse product entry price: {price:?}"))
                    .flatten(),
            })
        })
        .collect::<Result<_, anyhow::Error>>()?;
    Ok(products)
}

pub async fn parse_list_pages_count(document: &Html) -> Result<Option<u32>, anyhow::Error> {
    document
        .select(&selectors::list::LAST_PAGE)
        .rev()
        .skip(1)
        .next()
        .map(|e| e.inner_html().parse())
        .transpose()
        .context("Unable to parse last page index")
}

pub fn parse_categories(document: &Html) -> Vec<String> {
    document
        .select(&selectors::CATEGORIES)
        .filter_map(|e| {
            e.attr("href")
                .log_error("Category without href")
                .map(ToString::to_string)
        })
        .collect()
}

async fn work_cycle(opts: ParsingOptions) -> Result<(), anyhow::Error> {
    let body: String = opts
        .client
        .get("https://davi.com.ua/odyag-ta-vzuttya")
        .send()
        .await?
        .text()
        .await?;
    let document = Html::parse_document(&body);
    let res = parse_categories(&document);
    let client = opts.client;
    futures::stream::iter(res.iter())
        .map(|c| {
            let cl = client.clone();
            async move {
                let url = format!("https://davi.com.ua/{c}");
                let body = cl.get(&url).send().await?.text().await.unwrap();
                let document = Html::parse_document(&body);
                let res = parse_list_pages_count(&document)
                    .await
                    .context(format!("Unable to parse pages count: {c}"))
                    .unwrap();
                Ok::<_, anyhow::Error>(futures::stream::once(async { url }).chain(
                    futures::stream::iter(res.into_iter().flat_map(|res| 1..=res).map(move |p| {
                        let suffix = if p > 1 {
                            format!("/filter/page={p}/")
                        } else {
                            String::new()
                        };
                        format!("https://davi.com.ua/{c}{suffix}")
                    })),
                ))
            }
        })
        .buffer_unordered(64)
        .filter_map(|x| async { x.log_error("Unable to parse product list pages count") })
        .flatten()
        .map(|url| {
            let cl = client.clone();
            async move {
                let cl = cl.clone();
                let body: String = cl.get(url).send().await?.text().await?;
                let document = Html::parse_document(&body);
                Ok::<_, anyhow::Error>(futures::stream::iter(
                    parse_product_list(&document).await?.into_iter(),
                ))
            }
        })
        .buffer_unordered(128)
        .filter_map(|x| async { x.log_error("Unable to parse product list pages count") })
        .flatten()
        .map(|entry| {
            let cl = client.clone();
            async move {
                let cl = cl.clone();
                let url = &entry.url;
                let body: String = cl
                    .get(&format!("https://davi.com.ua/{}", url.0))
                    .send()
                    .await?
                    .text()
                    .await?;
                let document = Html::parse_document(&body);
                parse_product_page(&document, url).context(format!("Unable to parse {}", url.0))
            }
        })
        .buffer_unordered(64)
        .filter_map(|x| async { x.log_error("Unable to parse product") })
        .for_each(|p| {
            let repo = opts.repo.clone();
            async move {
                repo.save(p).await.log_error("Unable to save product");
            }
        })
        .await;
    Ok(())
}

#[derive(Constructor)]
pub struct ParserService {
    opts: ParsingOptions,
}

impl Actor for ParserService {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        log::info!("Davi parser started");
        let opts = self.opts.clone();
        tokio::task::spawn_local(async move {
            loop {
                let res = work_cycle(opts.clone()).await;
                if let Err(err) = res {
                    log::error!("Unable to parse davi products: {err}");
                }
            }
        });
    }
}

impl SelectBy<AvailableSelector> for Product {}

pub trait ProductRepository:
    Repository<Product, Error = anyhow::Error>
    + Get<Product>
    + Save<Product>
    + List<Product>
    + GetBy<Product, Url>
    + Repository<rt_types::product::Product, Error = anyhow::Error>
    + List<rt_types::product::Product>
    + Select<Product, AvailableSelector>
    + Select<rt_types::product::Product, AvailableSelector>
    + Send
    + Sync
{
}

#[derive(Constructor)]
pub struct PostgresProductRepository {
    client: Arc<tokio_postgres::Client>,
}

impl Repository<Product> for PostgresProductRepository {
    type Error = anyhow::Error;
}

impl Repository<rt_types::product::Product> for PostgresProductRepository {
    type Error = anyhow::Error;
}

impl TryFrom<Row> for Product {
    type Error = anyhow::Error;

    fn try_from(r: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            title: r.try_get("title")?,
            description: r.try_get("description")?,
            price: r.try_get::<_, i64>("price")? as u32,
            article: r.try_get("article")?,
            available: Availability::from(r.try_get::<_, i32>("available")? as u8),
            url: Url(r.try_get("url")?),
            last_visited: r.try_get("last_visited")?,
            images: r.try_get("images")?,
            properties: r
                .try_get::<_, HashMap<String, Option<String>>>("properties")?
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
            categories: r.try_get("categories")?,
        })
    }
}

#[async_trait]
impl Save<Product> for PostgresProductRepository {
    async fn save(&self, product: Product) -> Result<(), anyhow::Error> {
        self.client
            .execute(
                "INSERT INTO davi_product \
                (title, description, price, article, available, url, last_visited, \
                 images, properties, categories)\
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                ON CONFLICT (article) DO UPDATE \
                SET title = $1, description = $2, price = $3, article = $4,\
                available = $5, url = $6, last_visited = $7, images = $8,\
                properties = $9, categories = $10",
                &[
                    &product.title,
                    &product.description,
                    &(product.price as i64),
                    &product.article,
                    &(product.available as i32),
                    &product.url.0,
                    &product.last_visited,
                    &product.images,
                    &product
                        .properties
                        .into_iter()
                        .map(|(k, v)| (k, Some(v)))
                        .collect::<HashMap<_, _>>(),
                    &product.categories,
                ],
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Get<Product> for PostgresProductRepository {
    async fn get_one(&self, id: &IdentityOf<Product>) -> Result<Option<Product>, anyhow::Error> {
        let mut res = pin!(
            self.client
                .query_raw("SELECT * FROM davi_product WHERE article = $1", &[&id])
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(Product::try_from)
            .transpose()?)
    }
}

#[async_trait]
impl GetBy<Product, Url> for PostgresProductRepository {
    async fn get_by(&self, url: &Url) -> Result<Option<Product>, anyhow::Error> {
        let mut res = pin!(
            self.client
                .query_raw("SELECT * FROM davi_product WHERE url = $1", &[&url.0])
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(Product::try_from)
            .transpose()?)
    }
}

#[async_trait]
impl List<Product> for PostgresProductRepository {
    async fn list(&self) -> Result<Vec<Product>, anyhow::Error> {
        let res = self.client.query("SELECT * FROM davi_product", &[]).await?;
        res.into_iter().map(Product::try_from).collect()
    }
}

#[async_trait]
impl List<rt_types::product::Product> for PostgresProductRepository {
    async fn list(&self) -> Result<Vec<rt_types::product::Product>, anyhow::Error> {
        Ok(self.list().await?.into_iter().map(Product::into).collect())
    }
}

#[async_trait]
impl Select<Product, AvailableSelector> for PostgresProductRepository {
    async fn select(&self, _: &AvailableSelector) -> Result<Vec<Product>, anyhow::Error> {
        self.client
            .query(
                &format!(
                    "SELECT * FROM davi_product WHERE available = {} OR available = {}",
                    Availability::Available as u8,
                    Availability::OnOrder as u8
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(Product::try_from)
            .collect()
    }
}

#[async_trait]
impl Select<rt_types::product::Product, AvailableSelector> for PostgresProductRepository {
    async fn select(
        &self,
        s: &AvailableSelector,
    ) -> Result<Vec<rt_types::product::Product>, anyhow::Error> {
        Ok(self
            .select(s)
            .await?
            .into_iter()
            .map(Product::into)
            .collect())
    }
}

impl ProductRepository for PostgresProductRepository {}
