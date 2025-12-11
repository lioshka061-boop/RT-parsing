use crate::shop::Shop;
use crate::subscription::{Subscription, SubscriptionVersion};
use argon2::{Variant, Version};
use derive_more::{Deref, Display};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use typesafe_repository::macros::Id;
use typesafe_repository::{GetIdentity, Identity, IdentityBy, IdentityOf, RefIdentity};

pub mod repository;
pub mod service;

pub const PASSWORD_LENGTH: u32 = 64;
pub const MIN_PASSWORD_LENGTH: u32 = 5;
pub const DEFAULT_ARGON_CONFIG: argon2::Config = argon2::Config {
    variant: Variant::Argon2i,
    version: Version::Version13,
    mem_cost: 65535,
    time_cost: 10,
    lanes: 4,
    secret: &[],
    ad: &[],
    hash_length: PASSWORD_LENGTH,
};

pub fn generate_salt() -> Salt {
    let mut salt = [0; 512];
    StdRng::from_entropy().fill_bytes(&mut salt);
    salt
}

pub type Salt = [u8; 512];

#[derive(
    Deref, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Display,
)]
pub struct Login(pub String);

#[derive(Id, Serialize, Deserialize, Debug, Clone)]
#[Id(ref_id, get_id)]
pub struct UserCredentials {
    #[id]
    pub login: Login,
    pub password: Password,
    pub access: BTreeSet<Access>,
    pub subscription: Option<(IdentityOf<Subscription>, SubscriptionVersion)>,
    #[id_by]
    pub registration_token: Option<RegistrationToken>,
}

impl UserCredentials {
    pub fn available_shops(&self) -> Vec<IdentityOf<Shop>> {
        self.access
            .iter()
            .filter_map(|a| match a {
                Access::Shop(id) => Some(id),
                _ => None,
            })
            .cloned()
            .collect()
    }
    pub fn has_access_to(&self, shop: &IdentityOf<Shop>) -> bool {
        self.access.iter().any(|a| match a {
            Access::Shop(id) if id == shop => true,
            Access::Moderation | Access::ControlPanel => true,
            _ => false,
        })
    }
    pub fn has_access_to_control_panel(&self) -> bool {
        self.access
            .iter()
            .any(|a| matches!(a, Access::ControlPanel))
    }
    pub fn is_admin(&self) -> bool {
        self.has_access_to_control_panel()
    }
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize, Display)]
pub enum Access {
    Shop(IdentityOf<Shop>),
    Moderation,
    ControlPanel,
}

impl TryFrom<String> for Access {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s {
            x if x == Self::Moderation.to_string() => Ok(Self::Moderation),
            x if x == Self::ControlPanel.to_string() => Ok(Self::ControlPanel),
            ref x => Ok(Self::Shop(uuid::Uuid::parse_str(&x).map_err(|_| s)?)),
        }
    }
}

#[derive(Clone, Debug, Deref, Serialize, Deserialize, PartialEq)]
pub struct Password {
    #[deref]
    password: String,
    #[serde(with = "serde_arrays")]
    salt: Salt,
}

impl Password {
    pub fn new(password: String, salt: Salt) -> Result<Password, anyhow::Error> {
        if password.len() < MIN_PASSWORD_LENGTH as usize {
            return Err(anyhow::anyhow!(
                "Password cannot be shorter than {MIN_PASSWORD_LENGTH}"
            ));
        }
        Ok(Self { password, salt })
    }
    pub fn check(&self, input: &String) -> Result<bool, anyhow::Error> {
        Ok(argon2::verify_encoded(&self.password, input.as_bytes())?)
    }
    pub fn generate(input: String, salt: Salt) -> Result<Password, anyhow::Error> {
        let password: String =
            argon2::hash_encoded(input.as_bytes(), &salt, &DEFAULT_ARGON_CONFIG)?
                .try_into()
                .unwrap();
        Ok(Self { password, salt })
    }
    pub fn salt(&self) -> &Salt {
        &self.salt
    }
    pub fn password(&self) -> &String {
        &self.password
    }
}

#[derive(Debug, Display, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RegistrationToken(pub String);
