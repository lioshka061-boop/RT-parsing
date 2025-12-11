use anyhow::anyhow;
use anyhow::Context;
use lazy_regex::regex;
use rt_types::product::{generate_id, Product, UaTranslation};
use rt_types::Availability;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use typesafe_repository::macros::Id;
use typesafe_repository::{Identity, RefIdentity};

pub fn parse_duration(duration: &str) -> Result<Duration, anyhow::Error> {
    let minutes = |x| 60 * x;
    let hours = |x| minutes(60) * x;
    let days = |x| hours(24) * x;
    let weeks = |x| days(7) * x;
    let years = |x| days(365) * x;

    if !duration.is_ascii() {
        return Err(anyhow::anyhow!("Duration is not ASCII"));
    };
    let re = regex!(r"[smhdwy]");
    let duration = duration.to_lowercase();
    if re.is_match(&duration) {
        let mut res_dur = 0;
        let mut buf = String::from("");
        for char in duration.chars() {
            match (buf.parse(), char) {
                (parsed, 'y') => res_dur += years(parsed?),
                (parsed, 'w') => res_dur += weeks(parsed?),
                (parsed, 'd') => res_dur += days(parsed?),
                (parsed, 'h') => res_dur += hours(parsed?),
                (parsed, 'm') => res_dur += minutes(parsed?),
                (parsed, 's') => res_dur += parsed?,
                _ => {
                    char.to_digit(10)
                        .ok_or(anyhow::anyhow!("Wrong time format"))?;
                    buf.push(char);
                }
            }
        }
        Ok(Duration::from_secs(res_dur))
    } else {
        Ok(Duration::from_secs(duration.parse()?))
    }
}

#[derive(Debug, Deserialize)]
pub struct YmlCatalog {
    pub shop: YmlCatalogShop,
}

#[derive(Debug, Deserialize)]
pub struct YmlCatalogShop {
    pub offers: Option<Offers>,
}

#[derive(Debug, Deserialize)]
pub struct Shop {
    pub items: Option<Items>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Offers {
    #[serde(rename = "$value", default)]
    pub offers: Vec<Offer>,
}

#[derive(Id, Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[Id(ref_id)]
pub struct Offer {
    pub name: Option<String>,
    #[serde(default, rename = "@available")]
    pub available: Option<String>,
    #[serde(default, rename = "@id")]
    pub id: Option<String>,
    pub description: Option<String>,
    pub name_ua: Option<String>,
    pub description_ua: Option<String>,
    pub price: Option<String>,
    pub keywords: Option<String>,
    #[serde(default, rename = "picture")]
    pub pictures: Vec<Picture>,
    pub vendor: Option<String>,
    #[serde(rename = "currencyId")]
    pub currency: Option<String>,
    pub quantity_in_stock: Option<String>,
    #[serde(rename = "vendorCode")]
    #[id]
    pub vendor_code: Option<String>,
    #[serde(default, rename = "param")]
    pub params: Vec<Param>,
    pub url: Option<String>,
}

impl Offer {
    pub fn is_available(&self) -> bool {
        self.available.as_ref().is_some_and(|a| a == "true")
    }
    pub fn params(&self) -> HashMap<String, String> {
        self.params
            .iter()
            .cloned()
            .map(|Param { name, val }| (name, val))
            .collect()
    }
}
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct Picture {
    #[serde(rename = "$value", default)]
    pub link: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct Items {
    #[serde(rename = "$value", default)]
    pub items: Vec<Item>,
}

#[derive(Id, Clone, Debug, Deserialize)]
#[Id(ref_id)]
pub struct Item {
    pub name: Option<String>,
    pub name_ua: Option<String>,
    #[serde(alias = "price")]
    pub priceuah: Option<usize>,
    #[serde(rename = "currencyId")]
    pub currency: Option<String>,
    pub country: Option<String>,
    pub vendor: Option<String>,
    pub description: Option<String>,
    pub description_ua: Option<String>,
    pub url: Option<String>,
    #[serde(default, rename = "@id")]
    pub id: Option<String>,
    #[id]
    pub barcode: Option<String>,
    pub keywords: Option<String>,
    #[serde(default, rename = "@available")]
    pub available: Option<bool>,
    #[serde(default, rename = "param")]
    pub params: Vec<Param>,
    #[serde(default)]
    #[serde(rename = "image")]
    pub images: Vec<String>,
}

impl Item {
    pub fn is_available(&self) -> bool {
        self.available.unwrap_or(false)
    }
    pub fn params(&self) -> HashMap<String, String> {
        self.params
            .iter()
            .cloned()
            .map(|Param { name, val }| (name, val))
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Param {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "$value", default)]
    pub val: String,
}

pub struct Vendored<T>(pub String, pub T);

impl<T: Identity> Identity for Vendored<T> {
    type Id = T::Id;
}

impl<T: RefIdentity> RefIdentity for Vendored<T> {
    fn id_ref(&self) -> &Self::Id {
        self.1.id_ref()
    }
}

impl<T> Vendored<T> {
    pub fn with_vendor(s: String) -> impl Fn(T) -> Self {
        move |t| Self(s.clone(), t)
    }
}

impl TryInto<Product> for Vendored<Offer> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Product, Self::Error> {
        let Vendored(vendor, offer) = self;
        let available = match offer.is_available() {
            true => Availability::Available,
            false => Availability::NotAvailable,
        };
        let params = offer.params();
        let article = offer
            .vendor_code
            .or(offer.id)
            .ok_or(anyhow!("Offer must contain vendor code or id"))?;
        Ok(Product {
            id: generate_id(&article, &vendor, &offer.keywords),
            available,
            params,
            title: offer.name.ok_or(anyhow!("Offer must contain name"))?,
            ua_translation: offer.name_ua.map(|title| UaTranslation {
                title,
                description: offer.description_ua,
            }),
            description: offer.description,
            price: offer
                .price
                .map(|p| p.parse().context("Unable to parse offer price"))
                .ok_or(anyhow!("Offer must contain price"))??,
            currency: offer.currency.unwrap_or("UAH".to_string()),
            article,
            brand: offer.vendor.unwrap_or_default(),
            keywords: offer.keywords.map(|k| k.replace('|', ",")),
            in_stock: offer
                .quantity_in_stock
                .filter(|p| !p.is_empty())
                .and_then(|p| {
                    p.parse()
                        .map_err(|err| log::warn!("Unable to parse offer quantity in stock: {err}"))
                        .ok()
                }),
            model: String::new(),
            category: None,
            vendor: vendor.into(),
            images: offer.pictures.into_iter().map(|p| p.link).collect(),
        })
    }
}

impl TryInto<Product> for Vendored<Item> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Product, Self::Error> {
        let Vendored(vendor, item) = self;
        let available = match item.is_available() {
            true => Availability::Available,
            false => Availability::NotAvailable,
        };
        let params = item.params();
        let article = item.barcode.ok_or(anyhow!("Item must contain barcode"))?;
        Ok(Product {
            id: generate_id(&article, &vendor, &item.keywords),
            available,
            params,
            title: item.name.ok_or(anyhow!("Item must contain name"))?,
            ua_translation: item.name_ua.map(|title| UaTranslation {
                title,
                description: item.description_ua,
            }),
            description: item.description,
            in_stock: None,
            price: item
                .priceuah
                .map(Into::into)
                .ok_or(anyhow!("Item must contain price"))?,
            currency: item.currency.unwrap_or("UAH".to_string()),
            article,
            brand: item.vendor.unwrap_or_default(),
            model: String::new(),
            category: None,
            vendor: vendor.into(),
            keywords: item.keywords,
            images: item.images,
        })
    }
}
