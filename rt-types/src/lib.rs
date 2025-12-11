use actix::prelude::*;
use derive_more::Display;
use serde::{Deserialize, Serialize};

pub mod access;
pub mod category;
pub mod product;
pub mod shop;
pub mod subscription;
pub mod watermark;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Pause;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Resume;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DescriptionOptions {
    Replace(String),
    Append(String),
}

impl DescriptionOptions {
    pub fn value(&self) -> &String {
        match self {
            Self::Replace(p) => p,
            Self::Append(p) => p,
        }
    }
    pub fn try_from<S: AsRef<str>>(s: S, p: String) -> Option<Self> {
        match s.as_ref().to_lowercase().as_str() {
            "replace" => Some(Self::Replace(p)),
            "append" => Some(Self::Append(p)),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
#[repr(u8)]
pub enum Availability {
    #[display("Нет в наличии")]
    NotAvailable = 0,
    #[display("В наличии")]
    Available = 1,
    #[display("Под заказ")]
    OnOrder = 2,
}

impl From<u8> for Availability {
    fn from(a: u8) -> Self {
        match a {
            0 => Self::NotAvailable,
            1 => Self::Available,
            _ => Self::OnOrder,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model(pub String);

pub fn parse_duration(duration: &str) -> Result<std::time::Duration, anyhow::Error> {
    let duration = duration
        .to_lowercase()
        .replace(".", "")
        .replace("сек", "s")
        .replace("с", "s")
        .replace("мин", "m")
        .replace("м", "m")
        .replace("час", "h")
        .replace("ч", "h");
    duration_str::parse(duration).map_err(|dur| anyhow::anyhow!("Unable to parse duration {dur}"))
}
