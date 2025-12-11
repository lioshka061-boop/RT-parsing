use crate::product::Product;
use crate::shop::ExportOptions;
use crate::shop::Shop;
use anyhow::anyhow;
use derive_more::Display;
use itertools::Itertools;
use photon_rs::transform::SamplingFilter;
use photon_rs::PhotonImage;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::hash::{Hash, Hasher};
use typesafe_repository::async_ops::{Add, Get, ListBy, Remove};
use typesafe_repository::{GetIdentity, Identity, IdentityBy, IdentityOf, Repository};
use xxhash_rust::xxh3::Xxh3;

pub mod service;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct WatermarkGroup {
    pub name: String,
    pub shop_id: IdentityOf<Shop>,
    pub elements: HashMap<String, WatermarkOptions>,
}

impl WatermarkGroup {
    pub fn contains_element<'a, T: Into<&'a str>>(&self, elem: T) -> bool {
        self.elements.contains_key(elem.into())
    }
}

impl Identity for WatermarkGroup {
    type Id = (u64, IdentityOf<Shop>);
}

impl GetIdentity for WatermarkGroup {
    fn id(&self) -> Self::Id {
        let mut hasher = Xxh3::with_seed(self.shop_id.as_u64_pair().0);
        self.hash(&mut hasher);
        (hasher.digest(), self.shop_id)
    }
}

impl IdentityBy<IdentityOf<Shop>> for WatermarkGroup {
    fn id_by(&self) -> IdentityOf<Shop> {
        self.shop_id
    }
}

impl Hash for WatermarkGroup {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        for (k, v) in self.elements.iter().sorted() {
            k.hash(state);
            v.hash(state);
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct WatermarkOptions {
    #[serde(default)]
    pub size: WatermarkSize,
    pub horizontal_position: WatermarkPosition,
    pub vertical_position: WatermarkPosition,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkSize {
    Width(Decimal),
    Height(Decimal),
    BoundingBox { width: Decimal, height: Decimal },
}

impl Default for WatermarkSize {
    fn default() -> Self {
        Self::BoundingBox {
            width: dec!(10),
            height: dec!(10),
        }
    }
}

impl Default for WatermarkOptions {
    fn default() -> Self {
        Self {
            size: WatermarkSize::default(),
            horizontal_position: WatermarkPosition::End,
            vertical_position: WatermarkPosition::End,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Display)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkPosition {
    #[display("start")]
    Start,
    #[display("center")]
    Center,
    #[display("end")]
    End,
}

pub fn apply(
    mut image: PhotonImage,
    watermark: &PhotonImage,
    opts: WatermarkOptions,
) -> Result<PhotonImage, anyhow::Error> {
    let resize = |watermark, width, height| {
        photon_rs::transform::resize(
            watermark,
            width as u32,
            height as u32,
            SamplingFilter::Nearest,
        )
    };
    let watermark = match opts.size {
        WatermarkSize::Width(w) => {
            let w = w
                .to_f32()
                .ok_or(anyhow!("Unable to convert decimal to f32"))?;
            let width = image.get_width() as f32 / 100. * w;
            let ratio = watermark.get_width() as f32 / width;
            let height = watermark.get_height() as f32 / ratio;
            resize(watermark, width, height)
        }
        WatermarkSize::Height(h) => {
            let h = h
                .to_f32()
                .ok_or(anyhow!("Unable to convert decimal to f32"))?;
            let height = image.get_height() as f32 / 100. * h;
            let ratio = watermark.get_height() as f32 / height;
            let width = watermark.get_width() as f32 / ratio;
            resize(&watermark, width, height)
        }
        WatermarkSize::BoundingBox { width, height } => {
            let h = height
                .to_f32()
                .ok_or(anyhow!("Unable to convert decimal to f32"))?;
            let w = width
                .to_f32()
                .ok_or(anyhow!("Unable to convert decimal to f32"))?;

            let width = image.get_width() as f32 / 100. * w;
            let w_ratio = watermark.get_width() as f32 / width;
            let height = image.get_height() as f32 / 100. * h;
            let h_ratio = watermark.get_height() as f32 / height;

            let ratio = w_ratio.min(h_ratio);

            let width = watermark.get_width() as f32 / ratio;
            let height = watermark.get_height() as f32 / ratio;
            resize(&watermark, width, height)
        }
    };
    let x = match opts.horizontal_position {
        WatermarkPosition::Start => 0,
        WatermarkPosition::Center => image.get_width() / 2 - watermark.get_width() / 2,
        WatermarkPosition::End => image.get_width() - watermark.get_width(),
    };
    let y = match opts.vertical_position {
        WatermarkPosition::Start => 0,
        WatermarkPosition::Center => image.get_height() / 2 - watermark.get_height() / 2,
        WatermarkPosition::End => image.get_height() - watermark.get_height(),
    };
    photon_rs::multiple::watermark(&mut image, &watermark, x as i64, y as i64);
    Ok(image)
}

pub fn apply_to_product_map<T: BuildHasher + Clone>(
    mut dto: HashMap<ExportOptions, Vec<Product>, T>,
    shop_id: &str,
    addr: &str,
) -> Result<HashMap<ExportOptions, Vec<Product>, T>, anyhow::Error> {
    for ((group_id, _), list) in dto
        .iter_mut()
        .filter_map(|(o, l)| Some((o.watermarks.as_ref()?, l)))
    {
        for dto in list.iter_mut() {
            dto.images.iter_mut().for_each(|i| {
                *i = format!("{addr}/shop/{shop_id}/watermark/{i}/{group_id}");
            });
        }
    }
    Ok(dto)
}

pub trait WatermarkGroupRepository:
    Repository<WatermarkGroup, Error = anyhow::Error>
    + Get<WatermarkGroup>
    + Add<WatermarkGroup>
    + Remove<WatermarkGroup>
    + ListBy<WatermarkGroup, IdentityOf<Shop>>
    + Send
    + Sync
{
}
