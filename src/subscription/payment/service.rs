use crate::subscription::payment::{Payment, PaymentRepository, PaymentStatus};
use actix::prelude::*;
use actix_broker::BrokerIssue;
use rt_types::access::UserCredentials;
use std::sync::Arc;
use time::OffsetDateTime;
use typesafe_repository::IdentityOf;

pub struct PaymentService {
    repo: Arc<dyn PaymentRepository>,
}

impl PaymentService {
    pub fn new(repo: Arc<dyn PaymentRepository>) -> Self {
        Self { repo }
    }
}

impl Actor for PaymentService {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "Result<Option<Payment>, anyhow::Error>")]
pub struct HasValidPayment(pub IdentityOf<UserCredentials>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Add(pub Payment);

#[derive(Message)]
#[rtype(result = "Result<Vec<Payment>, anyhow::Error>")]
pub struct List;

#[derive(Message)]
#[rtype(result = "Result<Vec<Payment>, anyhow::Error>")]
pub struct ListByUser(pub IdentityOf<UserCredentials>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Confirm(pub IdentityOf<Payment>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct SetFailed(pub IdentityOf<Payment>, pub String);

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct PaymentConfirmed(pub IdentityOf<Payment>);

impl Handler<Add> for PaymentService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Add(payment): Add, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                repo.save(payment).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<List> for PaymentService {
    type Result = ResponseActFuture<Self, Result<Vec<Payment>, anyhow::Error>>;

    fn handle(&mut self, _: List, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(async move { Ok(repo.list().await?) }.into_actor(self))
    }
}

impl Handler<ListByUser> for PaymentService {
    type Result = ResponseActFuture<Self, Result<Vec<Payment>, anyhow::Error>>;

    fn handle(&mut self, ListByUser(user): ListByUser, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let res = repo.list().await?;
                Ok(res.into_iter().filter(|p| p.user == user).collect())
            }
            .into_actor(self),
        )
    }
}

impl Handler<Confirm> for PaymentService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Confirm(payment): Confirm, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let mut payment = repo
                    .get_one(&payment)
                    .await?
                    .ok_or(anyhow::anyhow!("Payment not found"))?;
                let last_completed_payment = repo
                    .list_by(&payment.user)
                    .await?
                    .into_iter()
                    .filter_map(|p| p.paid_due())
                    .max();
                let date = match last_completed_payment {
                    Some(date) => date.max(OffsetDateTime::now_utc()),
                    None => OffsetDateTime::now_utc(),
                };
                payment.status = PaymentStatus::Completed { date };
                repo.save(payment).await?;
                Ok(())
            }
            .into_actor(self)
            .map(move |res, act, _| {
                if res.is_ok() {
                    act.issue_system_async(PaymentConfirmed(payment));
                }
                res
            }),
        )
    }
}

impl Handler<SetFailed> for PaymentService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(
        &mut self,
        SetFailed(payment, reason): SetFailed,
        _: &mut Self::Context,
    ) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let mut payment = repo
                    .get_one(&payment)
                    .await?
                    .ok_or(anyhow::anyhow!("Payment not found"))?;
                payment.status = PaymentStatus::Failed {
                    date: OffsetDateTime::now_utc(),
                    reason,
                };
                repo.save(payment).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<HasValidPayment> for PaymentService {
    type Result = ResponseActFuture<Self, Result<Option<Payment>, anyhow::Error>>;

    fn handle(
        &mut self,
        HasValidPayment(user): HasValidPayment,
        _: &mut Self::Context,
    ) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let res = repo.list().await?;
                Ok(res
                    .into_iter()
                    .filter(|p| p.user == user)
                    .find(|p| match p.paid_due() {
                        Some(due) if due >= OffsetDateTime::now_utc() => true,
                        _ => false,
                    }))
            }
            .into_actor(self),
        )
    }
}
