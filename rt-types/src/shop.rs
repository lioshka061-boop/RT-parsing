use crate::access::UserCredentials;
use crate::watermark::WatermarkOptions;
use crate::{Availability, DescriptionOptions};
use actix::prelude::*;
use derive_more::Display;
use lazy_regex::regex;
use rust_decimal::Decimal;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_aux::field_attributes::{
    deserialize_number_from_string, deserialize_option_number_from_string,
};
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::time::Duration;
use time::OffsetDateTime;
use typesafe_repository::async_ops::{Get, List, Remove, Save};
use typesafe_repository::macros::Id;
use typesafe_repository::{GetIdentity, Identity, IdentityOf, RefIdentity, Repository};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

pub mod service;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ConfigurationChanged(pub Shop);

pub trait ShopRepository:
    Repository<Shop, Error = anyhow::Error>
    + Get<Shop>
    + List<Shop>
    + Remove<Shop>
    + Save<Shop>
    + Send
    + Sync
{
}

#[derive(Id, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[Id(ref_id, get_id)]
pub struct Shop {
    pub id: Uuid,
    #[serde(default)]
    pub is_suspended: bool,
    pub name: String,
    pub owner: IdentityOf<UserCredentials>,
    pub export_entries: Vec<ExportEntry>,
    pub limits: Option<ShopLimits>,
    pub default_custom_options: Option<CustomOptions>,
    #[serde(default)]
    pub image_proxy: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CustomOptions {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShopLimits {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub maximum_exports: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub links_per_export: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub unique_links: u32,
    #[serde(deserialize_with = "deserialize_option_number_from_string")]
    pub descriptions: Option<NonZeroU32>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub maximum_description_size: u32,
    #[serde(deserialize_with = "deserialize_option_number_from_string")]
    pub categories: Option<NonZeroU32>,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    pub minimum_update_rate: Duration,
}

impl Default for ShopLimits {
    fn default() -> Self {
        Self {
            maximum_exports: 2,
            links_per_export: 2,
            unique_links: 4,
            descriptions: NonZeroU32::new(2),
            maximum_description_size: 10 * 1024,
            categories: NonZeroU32::new(10),
            minimum_update_rate: Duration::from_millis(1000 * 60 * 60 * 6),
        }
    }
}

impl Shop {
    pub fn conforms_limits(&self) -> bool {
        let limits = match &self.limits {
            Some(l) => l,
            None => return true,
        };
        self.export_entries.len() <= limits.maximum_exports as usize
            && self.export_entries.iter().all(|e| {
                e.links
                    .as_ref()
                    .map(|l| l.len() <= limits.links_per_export as usize)
                    .unwrap_or(true)
                    && e.tt_parsing.is_none()
                    && e.dt_parsing.is_none()
                    && e.jgd_parsing.is_none()
                    && e.pl_parsing.is_none()
                    && e.update_rate >= limits.minimum_update_rate
                    && e.links
                        .as_ref()
                        .map(|l| {
                            l.iter().all(|l| {
                                limits.categories.is_some()
                                    || !l.options.as_ref().is_some_and(|o| o.categories)
                            })
                        })
                        .unwrap_or(true)
            })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExportEntry {
    #[serde(default = "default_time")]
    pub created_time: OffsetDateTime,
    #[serde(default = "default_time")]
    pub edited_time: OffsetDateTime,
    pub file_name: Option<String>,
    pub links: Option<Vec<ExportEntryLink>>,
    pub tt_parsing: Option<TtParsingOptions>,
    pub dt_parsing: Option<DtParsingOptions>,
    pub jgd_parsing: Option<DtParsingOptions>,
    pub pl_parsing: Option<DtParsingOptions>,
    pub maxton_parsing: Option<DtParsingOptions>,
    pub davi_parsing: Option<ExportOptions>,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    #[serde(default = "default_update_rate")]
    pub update_rate: Duration,
}

impl ExportEntry {
    pub fn uses_watermark(&self, watermark: &str) -> bool {
        self.tt_parsing
            .as_ref()
            .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .dt_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .maxton_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .jgd_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .pl_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self.links.iter().flatten().any(|l| {
                l.options
                    .as_ref()
                    .is_some_and(|opts| opts.watermarks.iter().any(|(w, _)| w == watermark))
            })
    }

    pub fn generate_hash(&self) -> u64 {
        let mut hasher =
            Xxh3::with_seed(self.links.as_ref().map(Vec::len).unwrap_or_default() as u64);
        self.hash(&mut hasher);
        hasher.digest()
    }
}

impl Hash for ExportEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.links.hash(state);
        self.file_name.hash(state);
        self.created_time.hash(state);
        self.edited_time.hash(state);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct DtParsingOptions {
    #[serde(flatten)]
    pub options: ExportOptions,
}

impl DtParsingOptions {
    pub fn options(&self) -> &ExportOptions {
        &self.options
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct TtParsingOptions {
    #[serde(flatten)]
    pub options: ExportOptions,
    pub append_categories: Option<ParsingCategoriesAction>,
}

impl TtParsingOptions {
    pub fn options(&self) -> &ExportOptions {
        &self.options
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParsingCategoriesAction {
    BeforeTitle { separator: String },
    AfterTitle { separator: String },
}

impl Default for ExportEntry {
    fn default() -> Self {
        Self {
            created_time: default_time(),
            edited_time: default_time(),
            file_name: None,
            links: None,
            tt_parsing: None,
            dt_parsing: None,
            davi_parsing: None,
            jgd_parsing: None,
            pl_parsing: None,
            maxton_parsing: None,
            update_rate: default_update_rate(),
        }
    }
}

fn default_time() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

impl ExportEntry {
    pub fn get_link_by_hash_mut(&mut self, hash: String) -> Option<&mut ExportEntryLink> {
        self.links.as_mut().and_then(|l| {
            l.iter_mut()
                .enumerate()
                .find(|(i, l)| l.hash_with_index(*i).to_string() == hash)
                .map(|(_, l)| l)
        })
    }
    pub fn remove_link_by_hash(&mut self, hash: String) -> Option<ExportEntryLink> {
        self.links
            .as_mut()
            .and_then(|l| {
                l.iter()
                    .enumerate()
                    .find(|(i, l)| l.hash_with_index(*i).to_string() == hash)
                    .unzip()
                    .0
                    .map(|i| (l, i))
            })
            .map(|(l, i)| l.remove(i))
    }
    pub fn file_name<T: Into<Option<FileFormat>>>(&self, file_format: T) -> String {
        if let Some(file_name) = self.file_name.as_ref() {
            // actix-web не дозволяє сегменти шляху, що починаються з '.', тому прибираємо її
            // і замінюємо розділювачі, щоб уникнути 400 при скачуванні.
            let mut file_name = file_name.trim().to_string();
            file_name = file_name.trim_start_matches('.').to_string();
            file_name = file_name.replace(['/', '\\'], "_");
            if file_name.is_empty() {
                file_name = "export".to_string();
            }
            return match file_format.into() {
                Some(FileFormat::Xlsx) => format!("{file_name}.{}", FileFormat::Xlsx.extension()),
                Some(FileFormat::HoroshopCsv) => {
                    format!("{file_name}_hs.{}.zip", FileFormat::HoroshopCsv.extension())
                }
                Some(FileFormat::HoroshopCategories) => {
                    format!(
                        "{file_name}_hs_categories.{}.zip",
                        FileFormat::HoroshopCategories.extension()
                    )
                }
                Some(format) => format!("{file_name}.{}.zip", format.extension()),
                None => file_name,
            };
        }
        let mut file_name = match &self.links {
            Some(links) => itertools::intersperse(
                links
                    .iter()
                    .map(|l| &l.link)
                    .filter_map(parse_vendor_from_link),
                "_".to_string(),
            )
            .collect::<String>(),
            None => "".to_string(),
        };
        if self.dt_parsing.is_some() {
            if !file_name.is_empty() {
                file_name = itertools::intersperse([file_name, "dt".to_string()], "_".to_string())
                    .collect();
            } else {
                file_name = "dt".to_string();
            }
        }
        if self.tt_parsing.is_some() {
            if !file_name.is_empty() {
                file_name = itertools::intersperse([file_name, "tt".to_string()], "_".to_string())
                    .collect();
            } else {
                file_name = "tt".to_string();
            }
        }
        let file_format = file_format.into();
        if let Some(file_format) = &file_format {
            file_name.push_str(&format!(".{}", file_format.extension()));
        }
        if let Some(FileFormat::Csv | FileFormat::Xml) = file_format {
            file_name.push_str(".zip");
        }
        file_name
    }
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Display)]
pub enum FileFormat {
    #[display("xlsx")]
    Xlsx,
    #[display("csv")]
    Csv,
    #[display("xml")]
    Xml,
    #[display("horoshop csv")]
    HoroshopCsv,
    #[display("horoshop categories")]
    HoroshopCategories,
}

impl FileFormat {
    pub fn extension(&self) -> &str {
        match self {
            Self::Xlsx => "xlsx",
            Self::Csv => "csv",
            Self::Xml => "xml",
            Self::HoroshopCsv => "csv",
            Self::HoroshopCategories => "csv",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExportEntryLink {
    pub vendor_name: Option<String>,
    pub link: String,
    #[serde(flatten)]
    pub options: Option<ExportOptions>,
}

impl Hash for ExportEntryLink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.link.hash(state)
    }
}

impl ExportEntryLink {
    pub fn hash_with_index(&self, i: usize) -> u64 {
        let mut hasher = Xxh3::with_seed(i as u64);
        self.hash(&mut hasher);
        hasher.digest()
    }
}

fn parse_vendor_from_link<S: AsRef<str>>(l: S) -> Option<String> {
    let regex = regex!(r"(?U)https?:\/\/(([^\.]+)\.)*(com|net|ua|org|one)(\.[^\.]+)*");
    regex
        .captures(l.as_ref())
        .and_then(|c| c.get(2))
        .map(|m| m.as_str().to_string())
}

impl ExportEntryLink {
    pub fn vendor_name(&self) -> String {
        self.vendor_name
            .as_ref()
            .filter(|n| !n.is_empty())
            .cloned()
            .unwrap_or_else(|| parse_vendor_from_link(&self.link).unwrap_or_default())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ExportOptions {
    pub title_prefix: Option<String>,
    pub title_prefix_ua: Option<String>,
    pub title_suffix: Option<String>,
    pub title_suffix_ua: Option<String>,
    #[serde(default)]
    pub title_replacements: Option<Vec<(String, String)>>,
    #[serde(default = "bool_false")]
    pub only_available: bool,
    pub discount: Option<Discount>,
    #[serde(default = "bool_false")]
    pub format_years: bool,
    #[serde(default = "bool_false")]
    pub add_vendor: bool,
    #[serde(default)]
    pub description: Option<DescriptionOptions>,
    #[serde(default)]
    pub description_ua: Option<DescriptionOptions>,
    pub delivery_time: Option<usize>,
    pub adjust_price: Option<Decimal>,
    #[serde(default = "bool_false")]
    pub categories: bool,
    #[serde(default = "bool_false")]
    pub convert_to_uah: bool,
    pub custom_options: Option<CustomOptions>,
    pub watermarks: Option<(String, Option<WatermarkOptions>)>,
    pub set_availability: Option<Availability>,
}

impl ExportOptions {
    pub fn has_watermark<'a>(&'a self, watermark: impl Into<&'a str>) -> bool {
        match &self.watermarks {
            Some((x, _)) => x == watermark.into(),
            None => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Discount {
    pub percent: usize,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    pub duration: Duration,
}

fn bool_false() -> bool {
    false
}

fn default_update_rate() -> Duration {
    Duration::from_secs(6 * 60 * 60)
}

pub fn deserialize_duration_from_string<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.replace(" ", "");
    crate::parse_duration(&s).map_err(serde::de::Error::custom)
}

pub fn serialize_duration_into_string<S: Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{}s", duration.as_secs()))
}
