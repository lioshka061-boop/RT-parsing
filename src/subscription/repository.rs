use crate::SqlWrapper;
use async_trait::async_trait;
use futures::stream::{StreamExt, TryStreamExt};
use rt_types::shop::ShopLimits;
use rt_types::subscription::{
    repository::SubscriptionRepository, Subscription, SubscriptionVersion,
};
use std::num::NonZero;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{Client, Row};
use typesafe_repository::async_ops::{Add, GetBy, List, ListBy, RemoveBy, Save};
use typesafe_repository::{IdentityOf, Repository};
use uuid::Uuid;

impl TryFrom<Row> for SqlWrapper<ShopLimits> {
    type Error = anyhow::Error;

    fn try_from(r: Row) -> Result<Self, Self::Error> {
        Ok(SqlWrapper(ShopLimits {
            maximum_exports: r.try_get::<_, i64>("maximum_exportS")? as u32,
            links_per_export: r.try_get::<_, i64>("links_per_export")? as u32,
            unique_links: r.try_get::<_, i64>("unique_links")? as u32,
            descriptions: NonZero::new(r.try_get::<_, i64>("descriptions")? as u32),
            maximum_description_size: r.try_get::<_, i64>("maximum_description_size")? as u32,
            categories: NonZero::new(r.try_get::<_, i64>("categories")? as u32),
            minimum_update_rate: Duration::from_millis(
                r.try_get::<_, i64>("minimum_update_rate")? as u64
            ),
        }))
    }
}

impl TryFrom<Row> for SqlWrapper<Subscription> {
    type Error = anyhow::Error;

    fn try_from(r: Row) -> Result<Self, Self::Error> {
        Ok(SqlWrapper(Subscription {
            id: r.try_get("id")?,
            maximum_shops: NonZero::new(r.try_get::<_, i64>("maximum_shops")? as u32)
                .ok_or(anyhow::anyhow!("Maximum shops must be >0"))?,
            price: r.try_get("price")?,
            name: r.try_get("name")?,
            version: r.try_get::<_, i64>("version")? as u32,
            yanked: r.try_get("yanked")?,
            limits: SqlWrapper::try_from(r)?.0,
        }))
    }
}

pub struct PostgresSubscriptionRepository {
    client: Arc<Client>,
}

impl PostgresSubscriptionRepository {
    pub async fn new(client: Arc<Client>) -> Result<Self, anyhow::Error> {
        Ok(Self { client })
    }
}

impl Repository<Subscription> for PostgresSubscriptionRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl GetBy<Subscription, (IdentityOf<Subscription>, SubscriptionVersion)>
    for PostgresSubscriptionRepository
{
    async fn get_by(
        &self,
        (id, ver): &(IdentityOf<Subscription>, SubscriptionVersion),
    ) -> Result<Option<Subscription>, Self::Error> {
        let ver = *ver as i64;
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![id, &ver];
        let mut res = pin!(
            self.client
                .query_raw(
                    "SELECT * FROM subscription WHERE id = $1 AND version = $2",
                    params,
                )
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(SqlWrapper::<Subscription>::try_from)
            .map(|r| r.map(SqlWrapper::into_inner))
            .transpose()?)
    }
}

#[async_trait]
impl GetBy<Subscription, Uuid> for PostgresSubscriptionRepository {
    async fn get_by(
        &self,
        id: &IdentityOf<Subscription>,
    ) -> Result<Option<Subscription>, Self::Error> {
        let mut res = pin!(
            self.client
                .query_raw(
                    "SELECT * FROM subscription WHERE id = $1 ORDER BY version DESC",
                    &[id]
                )
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(SqlWrapper::<Subscription>::try_from)
            .map(|r| r.map(SqlWrapper::into_inner))
            .transpose()?)
    }
}

#[async_trait]
impl ListBy<Subscription, IdentityOf<Subscription>> for PostgresSubscriptionRepository {
    async fn list_by(
        &self,
        id: &IdentityOf<Subscription>,
    ) -> Result<Vec<Subscription>, Self::Error> {
        let res = self
            .client
            .query_raw("SELECT * FROM subscription WHERE id = $1", &[id])
            .await?;
        res.try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(SqlWrapper::<Subscription>::try_from)
            .map(|r| r.map(SqlWrapper::into_inner))
            .collect()
    }
}

#[async_trait]
impl Add<Subscription> for PostgresSubscriptionRepository {
    async fn add(&self, sub: Subscription) -> Result<(), Self::Error> {
        self.client
            .execute(
                "INSERT INTO subscription (id, maximum_shops, price, name, version, yanked, maximum_exports, links_per_export, unique_links, descriptions, maximum_description_size, categories, minimum_update_rate) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                &[
                    &sub.id,
                    &(sub.maximum_shops.get() as i64),
                    &sub.price,
                    &sub.name,
                    &(sub.version as i64),
                    &sub.yanked,
                    &(sub.limits.maximum_exports as i64),
                    &(sub.limits.links_per_export as i64),
                    &(sub.limits.unique_links as i64),
                    &(sub.limits.descriptions.map(NonZero::get).unwrap_or_default() as i64),
                    &(sub.limits.maximum_description_size as i64),
                    &(sub.limits.categories.map(NonZero::get).unwrap_or_default() as i64),
                    &(sub.limits.minimum_update_rate.as_millis() as i64),
                ],
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Save<Subscription> for PostgresSubscriptionRepository {
    async fn save(&self, sub: Subscription) -> Result<(), Self::Error> {
        self.client
            .execute(
                "INSERT INTO subscription (id, maximum_shops, price, name, version, yanked, maximum_exports, links_per_export, unique_links, descriptions, maximum_description_size, categories, minimum_update_rate) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (id, version) DO UPDATE 
                SET id = $1, maximum_shops = $2, price = $3, name = $4, version = $5, yanked = $6, maximum_exports = $7, links_per_export = $8, unique_links = $9, descriptions = $10, maximum_description_size = $11, categories = $12, minimum_update_rate = $13", 
                &[
                    &sub.id,
                    &(sub.maximum_shops.get() as i64),
                    &sub.price,
                    &sub.name,
                    &(sub.version as i64),
                    &sub.yanked,
                    &(sub.limits.maximum_exports as i64),
                    &(sub.limits.links_per_export as i64),
                    &(sub.limits.unique_links as i64),
                    &(sub.limits.descriptions.map(NonZero::get).unwrap_or_default() as i64),
                    &(sub.limits.maximum_description_size as i64),
                    &(sub.limits.categories.map(NonZero::get).unwrap_or_default() as i64),
                    &(sub.limits.minimum_update_rate.as_millis() as i64),
                ],
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl List<Subscription> for PostgresSubscriptionRepository {
    async fn list(&self) -> Result<Vec<Subscription>, Self::Error> {
        self.client
            .query("SELECT * FROM subscription", &[])
            .await?
            .into_iter()
            .map(SqlWrapper::<Subscription>::try_from)
            .map(|r| r.map(SqlWrapper::into_inner))
            .collect()
    }
}

#[async_trait]
impl RemoveBy<Subscription, Uuid> for PostgresSubscriptionRepository {
    async fn remove_by(&self, id: &IdentityOf<Subscription>) -> Result<(), Self::Error> {
        self.client
            .execute("DELETE FROM subscription WHERE id = $1", &[id])
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RemoveBy<Subscription, (IdentityOf<Subscription>, SubscriptionVersion)>
    for PostgresSubscriptionRepository
{
    async fn remove_by(
        &self,
        (id, version): &(IdentityOf<Subscription>, SubscriptionVersion),
    ) -> Result<(), Self::Error> {
        self.client
            .execute(
                "DELETE FROM subscription WHERE id = $1 AND version = $2",
                &[id, &(*version as i64)],
            )
            .await?;
        Ok(())
    }
}

impl SubscriptionRepository for PostgresSubscriptionRepository {}
