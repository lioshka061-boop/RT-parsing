use crate::tt::selectors;
use async_trait::async_trait;
use lazy_regex::regex;
use rt_types::{Availability, Model, Url};
use rusqlite::{params, ToSql};
use rust_decimal::Decimal;
use scraper::Html;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::OffsetDateTime;
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::*;
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{SelectBy, Selector};

#[derive(Id, Clone, Debug)]
#[Id(get_id, ref_id)]
pub struct Product {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub price: Decimal,
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
    pub fn format_description(&self) -> Option<String> {
        let document = Html::parse_document(self.description.as_ref()?);
        let desc: String = document
            .select(&selectors::DESCRIPTION_PL)
            .skip(1)
            .map(|e| e.inner_html())
            .collect();
        if desc.is_empty() {
            self.description.clone()
        } else {
            Some(desc)
        }
    }
    pub fn format_model(&self) -> String {
        let regex = regex!(r"(.*\d\d-)(\d\d)?.*");
        let mut res = regex.replace(&self.model.0, "$1$2").to_string();

        static REPLACE: &[(&str, &str)] = &[("klasa", "class"), ("seria", "series")];

        static REPLACE_SUFFIX: &[(&str, &str)] = &[
            ("seda", "sedan"),
            ("touri", "touring"),
            ("vari", "variant"),
            ("hatc", "hatchback"),
            ("sedan typ", "sedan"),
            ("sportbac", "sportback"),
            ("komb", "combi"),
        ];

        for (from, to) in REPLACE {
            let upper = from.to_uppercase();
            let lower = from.to_lowercase();
            if res.contains(&lower) {
                res = res.replace(&lower, &to.to_lowercase())
            } else if res.contains(&upper) {
                res = res.replace(&upper, &to.to_uppercase())
            } else {
                res = res.replace(from, to)
            }
        }

        for (from, to) in REPLACE_SUFFIX {
            let upper = from.to_uppercase();
            let lower = from.to_lowercase();
            if res.ends_with(&lower) {
                res = res.replace(&lower, &to.to_lowercase())
            } else if res.ends_with(&upper) {
                res = res.replace(&upper, &to.to_uppercase())
            } else if res.ends_with(from) {
                res = res.replace(from, to)
            }
        }
        res
    }
    pub fn format_title(&self, model: Option<&String>) -> String {
        const EXCEPTIONS: &[&str] = &["LED", "TRUE DRL", "TRU DRL"];
        const REPLACE: &[(&str, &str)] = &[
            ("SEQ", ""),
            ("SQL", ""),
            ("LED BAR", "LED lighting panel"),
            ("SMOKE", "darkened"),
            ("LICENSE", "license plate lighting"),
            ("SIDE DIRECTION", "side direction lighting"),
            ("DRL", "daytime running lamp"),
        ];
        let mut title = self
            .title
            .to_lowercase()
            .replace(&self.brand.to_lowercase(), &self.brand);
        let mut s = String::new();
        let split = model
            .unwrap_or(&self.model.0)
            .split(' ')
            .map(str::to_lowercase);
        for e in split {
            s.push_str(&e);
            s.push(' ');
            let replacement = format!("{} {}", self.brand, s.to_uppercase());
            let lowercase = format!("{} {}", self.brand, s.to_lowercase());
            title = title.replace(&lowercase, &replacement);
        }
        let mut title = self
            .category
            .as_ref()
            .map(|c| {
                itertools::intersperse(
                    c.split(' ')
                        .filter(|c| !title.to_lowercase().contains(&c.to_lowercase())),
                    " ",
                )
                .collect::<String>()
            })
            .map(|c| c.to_lowercase().replace('_', " "))
            .map(|c| {
                if c.ends_with("ies") {
                    format!("{}y", &c[..(c.len() - 3)])
                } else if c.ends_with('s') {
                    c[..(c.len() - 1)].to_string()
                } else {
                    c
                }
            })
            .map(|c| format!("{c} {title}"))
            .unwrap_or(title);
        for e in EXCEPTIONS {
            title = title.replace(&e.to_lowercase(), e);
        }
        for (a, b) in REPLACE {
            title = title.replace(&a.to_lowercase(), b);
        }
        title = title.replace("  ", " ").trim().to_string();
        title
    }
}

pub struct FromDateAvailableSelector(pub OffsetDateTime);
pub struct AvailableSelector;
pub struct TranslatedSelector;

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
    + Select<Product, AvailableSelector>
    + Send
    + Sync
{
    async fn update_category_where(
        &self,
        ids: Vec<IdentityOf<Product>>,
        category: String,
    ) -> Result<(), Self::Error>;

    async fn update_model_where(
        &self,
        ids: Vec<IdentityOf<Product>>,
        model: String,
    ) -> Result<(), Self::Error>;
    async fn count(&self) -> Result<usize, Self::Error>;
}

pub struct SqliteProductRepository {
    conn: Connection,
}

impl SqliteProductRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS product (
                id TEXT,
                article TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                price TEXT NOT NULL,
                model TEXT NOT NULL,
                brand TEXT NOT NULL,
                category TEXT,
                available INTEGER,
                url TEXT,
                img TEXT,
                last_visited INTEGER
            )",
                [],
            )?;
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
                    (id, title, description, price, article, model, category, available, url, last_visited, img, brand)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                    ON CONFLICT(article)
                    DO UPDATE SET id=?1, title=?2, description=?3, price=?4, article=?5, model=?6, category=?7, available=?8, url=?9, last_visited=?10, img=?11, brand=?12",
                    params![
                        p.id,
                        p.title,
                        p.description,
                        p.price.to_string(),
                        p.article,
                        p.model.0,
                        p.category,
                        p.available as u8,
                        p.url.0,
                        p.last_visited,
                        img,
                        p.brand,
                    ]
                )?;
                Ok(())
            }).await?;
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
                let mut stmt = conn.prepare(
                    "SELECT id, title, description, price, article, model, category, available, url, last_visited, img, brand 
                    FROM product WHERE article = ?1",
                )?;
                let p = stmt
                    .query_map([&id], |row| {
                        Ok(Product {
                            id: row.get(0)?,
                            title: row.get(1)?,
                            description: row.get(2)?,
                            price: Decimal::from_str_exact(&row.get::<_, String>(3)?).map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(err)
                                )
                            })?,
                            article: row.get(4)?,
                            model: Model(row.get(5)?),
                            category: row.get(6)?,
                            available: row.get::<_, u8>(6)?.into(),
                            url: Url(row.get(8)?),
                            last_visited: row.get(9)?,
                            brand: row.get(10)?,
                            images: Product::img_from_str(row.get::<_, String>(11)?),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>();
                    Ok(p?.pop())
            }).await?)
    }
}

#[async_trait]
impl List<Product> for SqliteProductRepository {
    async fn list(&self) -> Result<Vec<Product>, Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, title, description, price, article, model, category, available, url, last_visited, img, brand 
                    FROM product",
                )?;
                let p = stmt
                    .query_map([], |row| {
                        Ok(Product {
                            id: row.get(0)?,
                            title: row.get(1)?,
                            description: row.get(2)?,
                            price: Decimal::from_str_exact(&row.get::<_, String>(3)?).map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(err)
                                )
                            })?,
                            article: row.get(4)?,
                            model: Model(row.get(5)?),
                            category: row.get(6)?,
                            available: row.get::<_, u8>(7)?.into(),
                            url: Url(row.get(8)?),
                            last_visited: row.get(9)?,
                            images: Product::img_from_str(row.get::<_, String>(10)?),
                            brand: row.get(11)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p)
            }).await?)
    }
}

#[async_trait]
impl Select<Product, AvailableSelector> for SqliteProductRepository {
    async fn select(&self, _: &AvailableSelector) -> Result<Vec<Product>, Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, title, description, price, article, model, category, available, url, last_visited, img, brand 
                    FROM product WHERE available > 0",
                )?;
                let p = stmt
                    .query_map([], |row| {
                        Ok(Product {
                            id: row.get(0)?,
                            title: row.get(1)?,
                            description: row.get(2)?,
                            price: Decimal::from_str_exact(&row.get::<_, String>(3)?).map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(err)
                                )
                            })?,
                            article: row.get(4)?,
                            model: Model(row.get(5)?),
                            category: row.get(6)?,
                            available: row.get::<_, u8>(7)?.into(),
                            url: Url(row.get(8)?),
                            last_visited: row.get(9)?,
                            images: Product::img_from_str(row.get::<_, String>(10)?),
                            brand: row.get(11)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p)
            }).await?)
    }
}

#[async_trait]
impl ProductRepository for SqliteProductRepository {
    async fn update_category_where(
        &self,
        ids: Vec<IdentityOf<Product>>,
        category: String,
    ) -> Result<(), Self::Error> {
        self.conn
            .call(move |conn| {
                let ids = ids.iter().map(|s| s as &dyn ToSql).collect::<Vec<_>>();
                let list: &[&dyn ToSql] = &[&category];
                let list = [list, ids.as_slice()].concat();
                let list: &[&dyn ToSql] = list.as_slice();
                let mut query = "UPDATE product SET category = ?1 WHERE article IN (".to_string();
                let q = itertools::intersperse(
                    ids.iter()
                        .enumerate()
                        .map(|(i, _)| i + 2)
                        .map(|i| format!("?{i}")),
                    ",".to_string(),
                )
                .collect::<String>();
                query.push_str(&q);
                query.push_str(");");
                conn.execute(&query, list)?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn update_model_where(
        &self,
        ids: Vec<IdentityOf<Product>>,
        model: String,
    ) -> Result<(), Self::Error> {
        self.conn
            .call(move |conn| {
                let ids = ids.iter().map(|s| s as &dyn ToSql).collect::<Vec<_>>();
                let list: &[&dyn ToSql] = &[&model];
                let list = [list, ids.as_slice()].concat();
                let list: &[&dyn ToSql] = list.as_slice();
                let mut query =
                    "UPDATE product SET model = ?1 WHERE model NOT LIKE '%/%' AND article IN ("
                        .to_string();
                let q = itertools::intersperse(
                    ids.iter()
                        .enumerate()
                        .map(|(i, _)| i + 2)
                        .map(|i| format!("?{i}")),
                    ",".to_string(),
                )
                .collect::<String>();
                query.push_str(&q);
                query.push_str(");");
                conn.execute(&query, list)?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn count(&self) -> Result<usize, Self::Error> {
        let res = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT COUNT(*) FROM product")?;
                let res = stmt.query_row((), |r| r.get::<_, usize>(0))?;
                Ok(res)
            })
            .await?;
        Ok(res)
    }
}

#[derive(Id, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[Id(ref_id, get_id)]
pub struct Translation {
    pub id: IdentityOf<Product>,
    pub article: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
}

impl From<Product> for Translation {
    fn from(p: Product) -> Self {
        let model = p.format_model();
        Self {
            title: p.format_title(Some(&model)),
            description: p.format_description(),
            id: p.id,
            article: p.article,
        }
    }
}

impl Translation {
    pub fn apply_to(self, p: &mut Product) {
        p.title = self.title;
        p.description = self.description;
    }
}

#[async_trait]
pub trait TranslationRepository:
    Repository<Translation, Error = anyhow::Error> + Save<Translation> + Get<Translation> + Send + Sync
{
    async fn exists(&self, id: &IdentityOf<Translation>) -> Result<bool, Self::Error>;
}

pub struct FileSystemTranslationRepository {
    dir: String,
}

impl FileSystemTranslationRepository {
    pub fn new(dir: String) -> Self {
        match std::fs::create_dir(&dir) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(err) => {
                log::error!("Unable to create dir for FileSystemTranslationRepository: {err}")
            }
        }
        let dir = if dir.ends_with('/') {
            dir[..dir.len() - 1].to_string()
        } else {
            dir
        };
        Self { dir }
    }
}

impl Repository<Translation> for FileSystemTranslationRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Save<Translation> for FileSystemTranslationRepository {
    async fn save(&self, t: Translation) -> Result<(), Self::Error> {
        let file = std::fs::File::create(format!("{}/{}", self.dir, t.id_ref()))?;
        serde_yaml::to_writer(file, &t)?;
        Ok(())
    }
}

#[async_trait]
impl Get<Translation> for FileSystemTranslationRepository {
    async fn get_one(
        &self,
        id: &IdentityOf<Translation>,
    ) -> Result<Option<Translation>, Self::Error> {
        let res = std::fs::File::open(format!("{}/{}", self.dir, id));
        let file = match res {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let tr = serde_yaml::from_reader(file)?;
        Ok(Some(tr))
    }
}

#[async_trait]
impl TranslationRepository for FileSystemTranslationRepository {
    async fn exists(&self, id: &IdentityOf<Translation>) -> Result<bool, Self::Error> {
        Ok(tokio::fs::try_exists(format!("{}/{}", self.dir, id)).await?)
    }
}

impl TryInto<rt_types::product::Product> for Product {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<rt_types::product::Product, Self::Error> {
        let vendor = "tuning-tec";
        let images = self
            .images
            .iter()
            .take(10)
            .map(|i| {
                if i.starts_with('/') {
                    format!(
                        "https://tuning-tec.com{}",
                        i.replace("_view3", "").replace("/main", "")
                    )
                } else if i.contains("tuning-tec") {
                    i.replace("_view3", "").replace("/main", "")
                } else {
                    i.clone()
                }
            })
            .collect();
        let model = self.format_model();
        let title = self.format_title(Some(&model));

        let mut params = HashMap::new();
        params.insert("Марка".to_string(), self.brand.clone());
        params.insert("Модель".to_string(), self.model.clone().0);

        Ok(rt_types::product::Product {
            id: rt_types::product::generate_id(&self.article, &vendor, &None),
            params,
            title,
            ua_translation: None,
            description: self.format_description(),
            price: self.price,
            in_stock: None,
            currency: "PLN".to_string(),
            article: self.article,
            brand: self.brand,
            model,
            category: None,
            available: self.available,
            vendor: vendor.into(),
            keywords: None,
            images,
        })
    }
}
