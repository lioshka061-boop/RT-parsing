use crate::row_stream_to_vec;
use async_trait::async_trait;
use futures::StreamExt;
use rt_types::access::Login;
use rt_types::access::UserCredentials;
use rt_types::subscription::{Subscription, SubscriptionVersion};
use rust_decimal::Decimal;
use std::pin::pin;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio_postgres::{Client, Row};
use typesafe_repository::async_ops::{Get, List, ListBy, Save};
use typesafe_repository::macros::Id;
use typesafe_repository::{GetIdentity, Identity, IdentityBy, IdentityOf, RefIdentity, Repository};
use uuid::Uuid;

pub mod service;

#[derive(Id, Debug, Clone)]
#[Id(get_id, ref_id)]
pub struct Payment {
    #[id]
    pub id: Uuid,
    #[id_by]
    pub user: IdentityOf<UserCredentials>,
    pub subscription: (IdentityOf<Subscription>, SubscriptionVersion),
    pub paid_days: u16,
    pub amount: Decimal,
    pub currency: String,
    pub status: PaymentStatus,
}

impl Payment {
    pub fn paid_due(&self) -> Option<OffsetDateTime> {
        if let PaymentStatus::Completed { date } = self.status {
            Some(date + Duration::seconds(self.paid_days as i64 * 24 * 60 * 60))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum PaymentStatus {
    Pending {
        due: OffsetDateTime,
    },
    Completed {
        date: OffsetDateTime,
    },
    Failed {
        date: OffsetDateTime,
        reason: String,
    },
}

pub trait PaymentRepository:
    Repository<Payment, Error = anyhow::Error>
    + Save<Payment>
    + Get<Payment>
    + ListBy<Payment, IdentityOf<UserCredentials>>
    + List<Payment>
    + Send
    + Sync
{
}

impl PaymentRepository
    for typesafe_repository::inmemory::InMemoryRepository<Payment, anyhow::Error>
{
}

impl TryFrom<Row> for Payment {
    type Error = anyhow::Error;

    fn try_from(r: Row) -> Result<Self, Self::Error> {
        let status: String = r.try_get("status")?;
        let date: OffsetDateTime = r.try_get("date")?;
        let status = match status.as_str() {
            "pending" => PaymentStatus::Pending { due: date },
            "completed" => PaymentStatus::Completed { date },
            "failed" => PaymentStatus::Failed {
                date,
                reason: r.try_get("reason")?,
            },
            x => return Err(anyhow::anyhow!("Unknown payment status: {x}")),
        };
        Ok(Payment {
            id: r.try_get("id")?,
            user: Login(r.try_get("user_id")?),
            subscription: (
                r.try_get("subscription_id")?,
                r.try_get::<_, i64>("subscription_version")? as u32,
            ),
            paid_days: r.try_get::<_, i32>("paid_days")? as u16,
            amount: r.try_get("amount")?,
            currency: r.try_get("currency")?,
            status,
        })
    }
}

pub struct PostgresPaymentRepository {
    client: Arc<Client>,
}

impl PostgresPaymentRepository {
    pub async fn new(client: Arc<Client>) -> Result<Self, anyhow::Error> {
        Ok(Self { client })
    }
}

impl Repository<Payment> for PostgresPaymentRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Save<Payment> for PostgresPaymentRepository {
    async fn save(&self, payment: Payment) -> Result<(), Self::Error> {
        let (status, date, reason) = match payment.status {
            PaymentStatus::Pending { due } => ("pending", due, None),
            PaymentStatus::Completed { date } => ("completed", date, None),
            PaymentStatus::Failed { date, reason } => ("failed", date, Some(reason)),
        };
        self.client
            .execute(
                "INSERT INTO payment \
                (id, user_id, subscription_id, subscription_version, paid_days, \
                 amount, currency, status, date, reason) \
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                ON CONFLICT (id) DO UPDATE \
                SET id = $1, user_id = $2, subscription_id = $3, subscription_version = $4, \
                paid_days = $5, amount = $6, currency = $7, status = $8, date = $9, reason = $10",
                &[
                    &payment.id,
                    &payment.user.0,
                    &payment.subscription.0,
                    &(payment.subscription.1 as i64),
                    &(payment.paid_days as i32),
                    &payment.amount,
                    &payment.currency,
                    &status,
                    &date,
                    &reason,
                ],
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Get<Payment> for PostgresPaymentRepository {
    async fn get_one(&self, id: &IdentityOf<Payment>) -> Result<Option<Payment>, Self::Error> {
        let mut res = pin!(
            self.client
                .query_raw("SELECT * FROM payment WHERE id = $1", &[id],)
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(Payment::try_from)
            .transpose()?)
    }
}

#[async_trait]
impl ListBy<Payment, IdentityOf<UserCredentials>> for PostgresPaymentRepository {
    async fn list_by(
        &self,
        user: &IdentityOf<UserCredentials>,
    ) -> Result<Vec<Payment>, Self::Error> {
        let res = self
            .client
            .query_raw("SELECT * FROM payment WHERE user = $1", &[&user.0])
            .await?;
        row_stream_to_vec(res).await
    }
}

#[async_trait]
impl List<Payment> for PostgresPaymentRepository {
    async fn list(&self) -> Result<Vec<Payment>, Self::Error> {
        let res = self.client.query("SELECT * FROM payment", &[]).await?;
        res.into_iter().map(Payment::try_from).collect()
    }
}

impl PaymentRepository for PostgresPaymentRepository {}
