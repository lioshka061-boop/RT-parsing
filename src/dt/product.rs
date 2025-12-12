#![allow(clippy::let_and_return)]

use crate::{Model, Url};
use async_trait::async_trait;
use rt_types::Availability;
use rusqlite::{params, Transaction, TransactionBehavior};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::*;
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{SelectBy, Selector};

#[derive(Id, Clone, Debug)]
#[Id(ref_id, get_id)]
pub struct Product {
    pub title: String,
    pub description: Option<String>,
    pub price: Option<usize>,
    #[id]
    pub article: String,
    pub brand: String,
    #[id_by]
    pub model: Model,
    pub category: Option<String>,
    pub available: Availability,
    #[id_by]
    pub url: Url,
    pub last_visited: OffsetDateTime,
    pub images: Vec<String>,
}

impl Product {
    fn img_as_str(&self) -> String {
        itertools::intersperse(self.images.iter().cloned(), ",".to_string()).collect()
    }
    fn img_from_str<T: AsRef<str>>(s: T) -> Vec<String> {
        s.as_ref().split(',').map(ToString::to_string).collect()
    }
    pub fn is_outdated(&self) -> bool {
        let now = OffsetDateTime::now_utc();
        self.last_visited
            .checked_add(Duration::hours(24))
            .map(|x| x < now)
            .unwrap_or(true)
    }
    pub fn format_model(&self) -> Option<String> {
        let Model(model) = &self.model;
        let brand = &self.brand;
        if model.contains(brand) {
            Some(model.clone())
        } else if model.contains(&brand.to_uppercase()) {
            Some(model.replace(&brand.to_uppercase(), brand))
        } else if model.contains(&brand.to_lowercase()) {
            Some(model.replace(&brand.to_lowercase(), brand))
        } else {
            Some(format!("{brand} {model}"))
        }
    }
}

pub struct FromDateAvailableSelector(pub OffsetDateTime);
pub struct AvailableSelector;

impl Selector for FromDateAvailableSelector {}
impl Selector for AvailableSelector {}

impl SelectBy<FromDateAvailableSelector> for Product {}
impl SelectBy<AvailableSelector> for Product {}

#[async_trait]
pub trait ProductRepository:
    Repository<Product, Error = anyhow::Error>
    + Save<Product>
    + Get<Product>
    + List<Product>
    + GetBy<Product, Url>
    + ListBy<Product, Model>
    + Select<Product, FromDateAvailableSelector>
    + Select<Product, AvailableSelector>
    + Send
    + Sync
{
}

pub struct SqliteProductRepository {
    conn: Connection,
}

impl SqliteProductRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            let conn = Transaction::new(conn, TransactionBehavior::Deferred)?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS product (
                    article TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    price INTEGER,
                    model TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    category TEXT,
                    available INTEGER,
                    url TEXT,
                    last_visited INTEGER,
                    images TEXT
                )",
                [],
            )?;
            conn.commit()?;
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

impl Repository<Product> for SqliteProductRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Save<Product> for SqliteProductRepository {
    async fn save(&self, p: Product) -> Result<(), Self::Error> {
        self.conn
            .call(move |conn| {
                let img = p.img_as_str();
                conn.execute(
                    "INSERT INTO product 
                    (title, description, price, article, model, category, available, url, last_visited, brand, images) 
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                    ON CONFLICT(article)
                    DO UPDATE SET title=?1, description=?2, price=?3, model=?5, category=?6, available=?7, url=?8, last_visited=?9, brand=?10, images = ?11",
                    params![
                        p.title,
                        p.description,
                        p.price,
                        p.article,
                        p.model.0,
                        p.category,
                        p.available as u8,
                        p.url.0,
                        p.last_visited,
                        p.brand,
                        img,
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Get<Product> for SqliteProductRepository {
    async fn get_one(&self, id: &IdentityOf<Product>) -> Result<Option<Product>, Self::Error> {
        let id = id.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let p = {
                    let mut stmt = conn.prepare(
                        "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images
                        FROM product WHERE article = ?1",
                    )?;
                    let p = stmt
                        .query_map([&id], |row| {
                            Ok(Product {
                                title: row.get(0)?,
                                description: row.get(1)?,
                                price: row.get(2)?,
                                article: row.get(3)?,
                                model: Model(row.get(4)?),
                                category: row.get(5)?,
                                available: row.get::<_, u8>(6)?.into(),
                                url: Url(row.get(7)?),
                                last_visited: row.get(8)?,
                                brand: row.get(9)?,
                                images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                            })
                        })?
                        .collect::<Result<Vec<_>, _>>();
                        p
                };
                Ok(p?.pop())
            })
            .await?)
    }
}

#[async_trait]
impl GetBy<Product, Url> for SqliteProductRepository {
    async fn get_by(&self, url: &Url) -> Result<Option<Product>, Self::Error> {
        let url = url.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images
                    FROM product WHERE url = ?1",
                )?;
                let mut p = stmt
                    .query_map([url.0], |row| {
                        Ok(Product {
                            title: row.get(0)?,
                            description: row.get(1)?,
                            price: row.get(2)?,
                            article: row.get(3)?,
                            model: Model(row.get(4)?),
                            category: row.get(5)?,
                            available: row.get::<_, u8>(6)?.into(),
                            url: Url(row.get(7)?),
                            last_visited: row.get(8)?,
                            brand: row.get(9)?,
                            images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p.pop())
            })
            .await?)
    }
}

#[async_trait]
impl List<Product> for SqliteProductRepository {
    async fn list(&self) -> Result<Vec<Product>, Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let p = {
                    let mut stmt = conn.prepare(
                        "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images
                        FROM product",
                    )?;
                    let p = stmt
                        .query_map([], |row| {
                            Ok(Product {
                                title: row.get(0)?,
                                description: row.get(1)?,
                                price: row.get(2)?,
                                article: row.get(3)?,
                                model: Model(row.get(4)?),
                                category: row.get(5)?,
                                available: row.get::<_, u8>(6)?.into(),
                                url: Url(row.get(7)?),
                                last_visited: row.get(8)?,
                                brand: row.get(9)?,
                                images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                            })
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    p
                };
                Ok(p)
            })
            .await?)
    }
}

#[async_trait]
impl ListBy<Product, Model> for SqliteProductRepository {
    async fn list_by(&self, model: &Model) -> Result<Vec<Product>, Self::Error> {
        let Model(model) = model.clone();
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images
                    FROM product WHERE model = ?1",
                )?;
                let p = stmt
                    .query_map([model], |row| {
                        Ok(Product {
                            title: row.get(0)?,
                            description: row.get(1)?,
                            price: row.get(2)?,
                            article: row.get(3)?,
                            model: Model(row.get(4)?),
                            category: row.get(5)?,
                            available: row.get::<_, u8>(6)?.into(),
                            url: Url(row.get(7)?),
                            last_visited: row.get(8)?,
                            brand: row.get(9)?,
                            images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                    p
            };
            Ok(p)
        }).await?
        )
    }
}

#[async_trait]
impl Select<Product, AvailableSelector> for SqliteProductRepository {
    async fn select(&self, _: &AvailableSelector) -> Result<Vec<Product>, Self::Error> {
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images 
                    FROM product WHERE available > 0",
                )?;
                let p = stmt
                    .query_map([], |row| {
                        Ok(Product {
                            title: row.get(0)?,
                            description: row.get(1)?,
                            price: row.get(2)?,
                            article: row.get(3)?,
                            model: Model(row.get(4)?),
                            category: row.get(5)?,
                            available: row.get::<_, u8>(6)?.into(),
                            url: Url(row.get(7)?),
                            last_visited: row.get(8)?,
                            brand: row.get(9)?,
                            images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                p
            };
            Ok(p)
        }).await?)
    }
}

#[async_trait]
impl Select<Product, FromDateAvailableSelector> for SqliteProductRepository {
    async fn select(
        &self,
        FromDateAvailableSelector(date): &FromDateAvailableSelector,
    ) -> Result<Vec<Product>, Self::Error> {
        let date = *date;
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, price, article, model, category, available, url, last_visited, brand, images 
                    FROM product WHERE last_visited >= ?1 AND available > 0",
                )?;
                let p = stmt
                    .query_map([date], |row| {
                        Ok(Product {
                            title: row.get(0)?,
                            description: row.get(1)?,
                            price: row.get(2)?,
                            article: row.get(3)?,
                            model: Model(row.get(4)?),
                            category: row.get(5)?,
                            available: row.get::<_, u8>(6)?.into(),
                            url: Url(row.get(7)?),
                            last_visited: row.get(8)?,
                            brand: row.get(9)?,
                            images: row.get::<_, Option<String>>(10)?.map(Product::img_from_str).unwrap_or_default(),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                p
            };
            Ok(p)
        }).await?)
    }
}

impl ProductRepository for SqliteProductRepository {}

impl TryInto<rt_types::product::Product> for Product {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<rt_types::product::Product, Self::Error> {
        let vendor = "design-tuning";
        let images = self
            .images
            .iter()
            .map(|i| {
                if i.starts_with('/') {
                    format!("https://design-tuning.com{}", i.replace("mini_", ""))
                } else {
                    i.clone()
                }
            })
            .collect();

        let keywords = match &self.category {
            Some(category) => format!("{}, {}, {}", self.brand, self.model.0, category),
            None => format!("{}, {}", self.brand, self.model.0),
        };
        let keywords = Some(keywords);
        let mut params = HashMap::new();
        params.insert("Марка".to_string(), self.brand.clone());
        params.insert("Модель".to_string(), self.model.clone().0);
        Ok(rt_types::product::Product {
            id: rt_types::product::generate_id(&self.article, &vendor, &keywords),
            title: self.title,
            params,
            ua_translation: None,
            description: self.description,
            price: self
                .price
                .map(Into::into)
                .ok_or(anyhow::anyhow!("self must contain price"))?,
            in_stock: None,
            currency: "UAH".to_string(),
            article: self.article,
            brand: self.brand,
            model: self.model.0,
            category: None,
            available: self.available,
            vendor: vendor.into(),
            keywords,
            images,
        })
    }
}

pub struct MaxtonProduct(pub Product);

impl Identity for MaxtonProduct {
    type Id = <Product as Identity>::Id;
}

impl RefIdentity for MaxtonProduct {
    fn id_ref(&self) -> &Self::Id {
        self.0.id_ref()
    }
}

impl TryInto<rt_types::product::Product> for MaxtonProduct {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<rt_types::product::Product, Self::Error> {
        let vendor = "maxton";
        let images = self
            .0
            .images
            .iter()
            .map(|i| {
                if i.starts_with('/') {
                    format!("https://design-tuning.com{}", i.replace("mini_", ""))
                } else {
                    i.clone()
                }
            })
            .collect();

        let keywords = match &self.0.category {
            Some(category) => format!("{}, {}, {}", self.0.brand, self.0.model.0, category),
            None => format!("{}, {}", self.0.brand, self.0.model.0),
        };
        let keywords = Some(keywords);
        let mut params = HashMap::new();
        params.insert("Марка".to_string(), self.0.brand.clone());
        params.insert("Модель".to_string(), self.0.model.clone().0);
        Ok(rt_types::product::Product {
            id: rt_types::product::generate_id(&self.0.article, &vendor, &keywords),
            title: self.0.title,
            params,
            ua_translation: None,
            description: self.0.description,
            price: self
                .0
                .price
                .map(Into::into)
                .ok_or(anyhow::anyhow!("self.0 must contain price"))?,
            in_stock: None,
            currency: "UAH".to_string(),
            article: self.0.article,
            brand: self.0.brand,
            model: self.0.model.0,
            category: None,
            available: self.0.available,
            vendor: vendor.into(),
            keywords,
            images,
        })
    }
}
