use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, DeflateOption, ZipEntryBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::Itertools;
use log_error::LogError;
use rt_types::category::Category;
use rt_types::category::CategoryRepository;
use rt_types::product::Product;
use rt_types::shop::ExportOptions;
use rt_types::Availability;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::hash::BuildHasher;
use std::sync::Arc;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use typesafe_repository::IdentityOf;

#[derive(Serialize, Deserialize)]
pub struct CsvEntry {
    #[serde(rename = "Артикул")]
    pub article: String,
    #[serde(rename = "Название")]
    pub title: String,
    #[serde(rename = "Описание товара")]
    pub description: String,
    #[serde(rename = "Раздел")]
    pub category: String,
    #[serde(rename = "Цена")]
    pub price: Option<Decimal>,
    #[serde(rename = "Иконки")]
    pub icons: Option<String>,
    #[serde(rename = "Количество")]
    pub count: Option<u64>,
    #[serde(rename = "Наличие")]
    pub available: Available,
    #[serde(rename = "Отображать")]
    pub display: Display,
    #[serde(rename = "Фото", with = "photos_serde")]
    pub photo: Vec<String>,
}

impl CsvEntry {
    pub fn apply_opts(self, opts: &ExportOptions) -> Self {
        Self {
            title: crate::xlsx::build_title(opts, &self.title, false),
            price: self
                .price
                .map(|p| p * opts.adjust_price.unwrap_or(Decimal::ONE)),
            ..self
        }
    }
}

#[derive(Serialize)]
pub struct CsvEntryRef<'a> {
    #[serde(rename = "Артикул")]
    pub article: &'a str,
    #[serde(rename = "Название")]
    pub title: &'a str,
    #[serde(rename = "Описание товара")]
    pub description: String,
    #[serde(rename = "Раздел")]
    pub category: String,
    #[serde(rename = "Цена")]
    pub price: Option<Decimal>,
    #[serde(rename = "Иконки")]
    pub icons: Option<String>,
    #[serde(rename = "Количество")]
    pub count: Option<u64>,
    #[serde(rename = "Наличие")]
    pub available: Available,
    #[serde(rename = "Отображать")]
    pub display: Display,
    #[serde(rename = "Фото", with = "photos_serde")]
    pub photo: &'a Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Available {
    #[serde(rename = "В наличии")]
    Available,
    #[serde(rename = "Не в наличии")]
    NotAvailable,
}

impl From<rt_types::Availability> for Available {
    fn from(availability: rt_types::Availability) -> Self {
        match availability {
            rt_types::Availability::Available => Self::Available,
            rt_types::Availability::OnOrder => Self::NotAvailable,
            rt_types::Availability::NotAvailable => Self::NotAvailable,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Display {
    #[serde(rename = "да")]
    Display,
    #[serde(rename = "нет")]
    Hide,
}

impl From<(Product, CategoryChain)> for CsvEntry {
    fn from((p, c): (Product, CategoryChain)) -> Self {
        let available = p.available.into();
        Self {
            article: p.article,
            title: p.title,
            price: Some(p.price),
            category: c.to_string(),
            icons: match available {
                Available::Available => Some("".to_string()),
                Available::NotAvailable => None,
            },
            available,
            description: p.description.unwrap_or_default(),
            count: p.in_stock.map(|x| x as u64),
            display: Display::Display,
            photo: p.images,
        }
    }
}

impl<'a> From<(&'a Product, CategoryChain)> for CsvEntryRef<'a> {
    fn from((p, c): (&'a Product, CategoryChain)) -> Self {
        let available = p.available.clone().into();
        Self {
            article: &p.article,
            title: &p.title,
            price: Some(p.price),
            description: p.description.clone().unwrap_or_default(),
            category: c.to_string(),
            icons: match available {
                Available::Available => Some("".to_string()),
                Available::NotAvailable => None,
            },
            available,
            count: p.in_stock.map(|x| x as u64),
            display: Display::Display,
            photo: &p.images,
        }
    }
}

impl<'a> From<(&'a Product, String)> for CsvEntryRef<'a> {
    fn from((p, category): (&'a Product, String)) -> Self {
        let available = p.available.clone().into();
        Self {
            article: &p.article,
            title: &p.title,
            description: p.description.clone().unwrap_or_default(),
            price: Some(p.price),
            category,
            icons: match p.available.clone() {
                Availability::Available => Some("Відправка вже сьогодні".to_string()),
                Availability::OnOrder => Some("Під замовлення".to_string()),
                Availability::NotAvailable => None,
            },
            available,
            count: p.in_stock.map(|x| x as u64),
            display: Display::Display,
            photo: &p.images,
        }
    }
}

pub struct CategoryChain {
    categories: Vec<Category>,
}

impl CategoryChain {
    pub fn new<F: Fn(IdentityOf<Category>) -> Option<Category>>(
        root_category: Category,
        find: F,
    ) -> Option<Self> {
        let mut categories = vec![root_category];
        while let Some(id) = categories.last().and_then(|c| c.parent_id) {
            categories.push(find(id)?);
        }
        categories.reverse();
        Some(Self { categories })
    }
    pub async fn new_async<
        F: Fn(IdentityOf<Category>) -> RF,
        RF: Future<Output = Result<Option<Category>, anyhow::Error>>,
    >(
        root_category: Category,
        find: F,
    ) -> Result<Option<Self>, anyhow::Error> {
        let mut categories = vec![root_category];
        while let Some(id) = categories.last().and_then(|c| c.parent_id) {
            match find(id).await? {
                Some(c) => categories.push(c),
                None => return Ok(Some(Self { categories })),
            }
        }
        categories.reverse();
        Ok(Some(Self { categories }))
    }
    pub fn inner(&self) -> &Vec<Category> {
        &self.categories
    }
}

impl ToString for CategoryChain {
    fn to_string(&self) -> String {
        itertools::intersperse(self.categories.iter().map(|x| x.name.as_str()), "/").collect()
    }
}

pub async fn export_csv(
    path: String,
    products: &HashMap<ExportOptions, Vec<Product>, impl BuildHasher>,
    category_repo: Arc<dyn CategoryRepository>,
) -> Result<(), anyhow::Error> {
    let mut res_file = tokio::fs::File::create(&path).await?;
    let mut w = ZipFileWriter::with_tokio(&mut res_file);
    let builder = ZipEntryBuilder::new(
        std::path::Path::new(&path)
            .file_name()
            .and_then(|f| f.to_str())
            .map(|f| f.replace(".zip", ""))
            .ok_or_else(|| anyhow::anyhow!("No filename for path {path:?}"))?
            .into(),
        Compression::Deflate,
    )
    .deflate_option(DeflateOption::Fast)
    .unix_permissions(0o777);
    let mut zip_writer = w.write_entry_stream(builder).await?.compat_write();
    let mut ser = csv_async::AsyncWriterBuilder::new()
        .quote_style(csv_async::QuoteStyle::NonNumeric)
        .create_serializer(&mut zip_writer);
    for product in products.values().flatten() {
        let entry = match product.category {
            Some(category) => {
                let category = category_repo
                    .get_one(&category)
                    .await?
                    .log_error("Category not found");
                let category = match category {
                    Some(c) => c,
                    None => continue,
                };
                let category_chain = CategoryChain::new_async(category, |id| {
                    let category_repo = category_repo.clone();
                    async move { category_repo.get_one(&id).await }
                })
                .await?
                .log_error("Category not found");
                match category_chain {
                    Some(category_chain) => CsvEntryRef::from((product, category_chain)),
                    None => continue,
                }
            }
            None => CsvEntryRef::from((product, "Главная".to_string())),
        };
        ser.serialize(entry).await?;
    }
    ser.flush().await?;
    drop(ser);
    zip_writer.into_inner().close().await?;
    w.close().await?;
    Ok(())
}

pub async fn export_csv_categories(
    path: String,
    products: &HashMap<ExportOptions, Vec<Product>, impl BuildHasher>,
    category_repo: Arc<dyn CategoryRepository>,
) -> Result<(), anyhow::Error> {
    let mut res_file = tokio::fs::File::create(&path).await?;
    let mut w = ZipFileWriter::with_tokio(&mut res_file);
    let builder = ZipEntryBuilder::new(
        std::path::Path::new(&path)
            .file_name()
            .and_then(|f| f.to_str())
            .map(|f| f.replace(".zip", ""))
            .ok_or_else(|| anyhow::anyhow!("No filename for path {path:?}"))?
            .into(),
        Compression::Deflate,
    )
    .deflate_option(DeflateOption::Fast)
    .unix_permissions(0o777);
    let mut zip_writer = w.write_entry_stream(builder).await?.compat_write();
    let mut ser = csv_async::AsyncWriterBuilder::new()
        .quote_style(csv_async::QuoteStyle::NonNumeric)
        .create_serializer(&mut zip_writer);
    let categories = stream::iter(
        products
            .values()
            .flatten()
            .filter_map(|product| product.category),
    )
    .map(|category| {
        let category_repo = category_repo.clone();
        async move {
            let category = category_repo
                .get_one(&category)
                .await?
                .ok_or(anyhow::anyhow!("Category not found"))?;
            let category_chain = CategoryChain::new_async(category, |id| {
                let category_repo = category_repo.clone();
                async move { category_repo.get_one(&id).await }
            })
            .await?
            .ok_or(anyhow::anyhow!("Category not found"));
            category_chain
        }
    })
    .buffered(64)
    .try_collect::<Vec<_>>()
    .await?;
    let max = categories
        .iter()
        .map(|c| c.inner().len())
        .max()
        .unwrap_or(1);
    let headers = std::iter::once("Категория".to_string())
        .chain((1..max).map(|x| format!("Подкатегория {}", x + 1)))
        .collect::<Vec<_>>();
    ser.serialize(headers).await?;
    for chain in categories {
        ser.serialize(
            chain
                .inner()
                .iter()
                .map(|c| c.name.as_str())
                .pad_using(max, |_| "")
                .collect::<Vec<_>>(),
        )
        .await?;
    }
    ser.flush().await?;
    drop(ser);
    zip_writer.into_inner().close().await?;
    w.close().await?;
    Ok(())
}

pub mod photos_serde {
    use serde::{de::Visitor, Deserializer, Serializer};

    pub fn serialize<S>(vec: &Vec<String>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let st: String = itertools::intersperse(vec.iter().map(|x| x.as_str()), ";").collect();
        s.serialize_str(&st)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Vec<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PhotosDeserialize;

        impl<'de> Visitor<'de> for PhotosDeserialize {
            type Value = Vec<String>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Vec<String>, E>
            where
                E: serde::de::Error,
            {
                Ok(value.split(';').map(ToString::to_string).collect())
            }
        }
        d.deserialize_str(PhotosDeserialize)
    }
}
