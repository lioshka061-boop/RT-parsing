use anyhow::Context as AnyhowContext;
use async_trait::async_trait;
use rt_types::shop::{Shop, ShopRepository};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use typesafe_repository::{
    async_ops::{Get, List, Remove, Save},
    IdentityOf, Repository,
};
use uuid::Uuid;

pub mod controllers;

pub struct FileSystemShopRepository {}

impl FileSystemShopRepository {
    pub fn new() -> Self {
        Self {}
    }
}

impl Repository<Shop> for FileSystemShopRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Get<Shop> for FileSystemShopRepository {
    async fn get_one(&self, id: &IdentityOf<Shop>) -> Result<Option<Shop>, anyhow::Error> {
        match read_shop(&id) {
            Ok(shop) => Ok(Some(shop)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[async_trait]
impl Save<Shop> for FileSystemShopRepository {
    async fn save(&self, shop: Shop) -> Result<(), anyhow::Error> {
        let path = format!("{CONFIG_DIR}/{}.yml", shop.id);
        tokio::fs::create_dir_all(CONFIG_DIR).await?;
        tokio::fs::write(&path, serde_json::to_string_pretty(&shop)?).await?;
        let meta = tokio::fs::metadata(&path).await?;
        let mut perm = meta.permissions();
        perm.set_mode(0o777);
        tokio::fs::set_permissions(&path, perm).await?;
        Ok(())
    }
}

#[async_trait]
impl List<Shop> for FileSystemShopRepository {
    async fn list(&self) -> Result<Vec<Shop>, anyhow::Error> {
        let mut shops = vec![];
        let res = match std::fs::read_dir(CONFIG_DIR) {
            Ok(r) => r,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(err) => return Err(err.into()),
        };
        for f in res {
            let file_name = &f?
                .file_name()
                .to_str()
                .ok_or(anyhow::anyhow!("Unable to convert file name to string"))?
                .replace(".yml", "");
            let id = match Uuid::parse_str(file_name) {
                Ok(id) => id,
                Err(err) => {
                    log::error!("Unable to parse id from file {file_name}: {err}");
                    continue;
                }
            };
            shops.push(read_shop(&id)?);
        }
        Ok(shops)
    }
}

#[async_trait]
impl Remove<Shop> for FileSystemShopRepository {
    async fn remove(&self, id: &IdentityOf<Shop>) -> Result<(), anyhow::Error> {
        std::fs::remove_file(format!("{CONFIG_DIR}/{id}.yml"))
            .context("Unable to remove configuration file")?;
        Ok(())
    }
}

impl ShopRepository for FileSystemShopRepository {}

const CONFIG_DIR: &str = "cfg.d";

pub fn read_shop(id: &IdentityOf<Shop>) -> Result<Shop, anyhow::Error> {
    let config = std::fs::read_to_string(format!("{CONFIG_DIR}/{id}.yml"))?;
    let value = serde_json::from_str(&config)?;
    let value = match value {
        Value::Object(mut map) => {
            map.insert("id".to_string(), Value::String(id.to_string()));
            Value::Object(map)
        }
        val => val,
    };
    let config = serde_json::from_value(value)?;
    Ok(config)
}
