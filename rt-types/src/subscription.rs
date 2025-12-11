use crate::shop::ShopLimits;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use typesafe_repository::macros::Id;
use typesafe_repository::{GetIdentity, Identity, IdentityBy, IdentityOf, RefIdentity};
use uuid::Uuid;

pub mod repository;
pub mod service;

pub type SubscriptionVersion = u32;

#[derive(Serialize, Deserialize, Id, Debug, Clone, PartialEq, Eq)]
#[Id(get_id, ref_id)]
pub struct Subscription {
    #[id]
    pub id: Uuid,
    pub limits: ShopLimits,
    pub maximum_shops: NonZeroU32,
    pub price: Decimal,
    pub name: String,
    pub version: SubscriptionVersion,
    #[serde(default)]
    pub yanked: bool,
}

impl IdentityBy<(IdentityOf<Self>, SubscriptionVersion)> for Subscription {
    fn id_by(&self) -> (IdentityOf<Self>, SubscriptionVersion) {
        (self.id, self.version)
    }
}

impl IdentityBy<IdentityOf<Self>> for Subscription {
    fn id_by(&self) -> IdentityOf<Self> {
        self.id
    }
}
