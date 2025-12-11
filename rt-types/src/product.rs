use crate::category::Category;
use crate::Availability;
use rust_decimal::Decimal;
use std::collections::HashMap;
use typesafe_repository::macros::Id;
use typesafe_repository::{Identity, IdentityOf, RefIdentity, SelectBy, Selector};
use xxhash_rust::xxh64::xxh64;

pub struct AvailableSelector;

impl Selector for AvailableSelector {}

impl SelectBy<AvailableSelector> for Product {}

#[derive(Debug, Clone)]
pub struct UaTranslation {
    pub title: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Id)]
pub struct Product {
    pub id: String,
    pub title: String,
    pub ua_translation: Option<UaTranslation>,
    pub description: Option<String>,
    pub price: Decimal,
    pub article: String,
    pub in_stock: Option<usize>,
    pub currency: String,
    pub keywords: Option<String>,
    pub params: HashMap<String, String>,
    pub brand: String,
    pub model: String,
    pub category: Option<IdentityOf<Category>>,
    pub available: Availability,
    pub vendor: String,
    pub images: Vec<String>,
}

pub fn convert_with_vendor<T, S>(v: Vec<T>, vendor: S) -> impl Iterator<Item = Product>
where
    T: Identity + RefIdentity,
    <T as Identity>::Id: std::fmt::Debug + Clone,
    (S, T): TryInto<Product>,
    <(S, T) as TryInto<Product>>::Error: std::fmt::Display,
    S: Clone,
{
    v.into_iter().filter_map(move |t| {
        let id = t.id_ref().clone();
        match TryInto::<Product>::try_into((vendor.clone(), t)) {
            Ok(p) => Some(p),
            Err(err) => {
                log::warn!(
                    "Unable to convert {} with id {id:?} into Product: {err}",
                    std::any::type_name::<T>()
                );
                None
            }
        }
    })
}

pub fn convert<T>(v: impl Iterator<Item = T>) -> impl Iterator<Item = Product>
where
    T: Identity + RefIdentity,
    <T as Identity>::Id: std::fmt::Debug + Clone,
    T: TryInto<Product>,
    <T as TryInto<Product>>::Error: std::fmt::Display,
{
    v.filter_map(move |t| {
        let id = t.id_ref().clone();
        match TryInto::<Product>::try_into(t) {
            Ok(p) => Some(p),
            Err(err) => {
                log::warn!(
                    "Unable to convert {} with id {id:?} into Product: {err}",
                    std::any::type_name::<T>()
                );
                None
            }
        }
    })
}

pub fn generate_id(article: &str, vendor: &str, keywords: &Option<String>) -> String {
    let keywords = keywords.clone().unwrap_or_default();
    format!(
        "xxh64{:x}",
        xxh64(
            format!("{article}{vendor}{keywords}").as_bytes(),
            article.len() as u64
        )
    )
}
