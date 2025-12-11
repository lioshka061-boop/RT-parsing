use crate::access::service::SubscriptionGuard;
use crate::access::UserCredentials;
use crate::subscription::repository::SubscriptionRepository;
use crate::subscription::{Subscription, SubscriptionVersion};
use actix::prelude::*;
use itertools::Itertools;
use std::sync::Arc;
use typesafe_repository::IdentityOf;

pub struct SubscriptionService {
    repository: Arc<dyn SubscriptionRepository>,
}

impl Actor for SubscriptionService {
    type Context = Context<Self>;
}

impl SubscriptionService {
    pub fn new(repository: Arc<dyn SubscriptionRepository>) -> Self {
        Self { repository }
    }
}

pub struct UserSubscription(Subscription, IdentityOf<UserCredentials>);

impl UserSubscription {
    pub fn user_id(&self) -> &IdentityOf<UserCredentials> {
        &self.1
    }
    pub fn inner(&self) -> &Subscription {
        &self.0
    }
    pub fn into_inner(self) -> Subscription {
        self.0
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<Subscription>, anyhow::Error>")]
pub struct Get(pub IdentityOf<Subscription>);

#[derive(Message)]
#[rtype(result = "Result<Option<UserSubscription>, anyhow::Error>")]
pub struct GetBy(pub UserCredentials);

#[derive(Message)]
#[rtype(result = "Result<Option<Subscription>, anyhow::Error>")]
pub struct GetVersion(pub IdentityOf<Subscription>, pub SubscriptionVersion);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Add(pub Subscription);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Remove(
    pub IdentityOf<Subscription>,
    pub SubscriptionVersion,
    pub SubscriptionGuard,
);

#[derive(Message)]
#[rtype(result = "Result<SubscriptionVersion, anyhow::Error>")]
pub struct Update(pub Subscription);

#[derive(Message)]
#[rtype(result = "Result<Vec<Subscription>, anyhow::Error>")]
pub struct List;

#[derive(Message)]
#[rtype(result = "Result<Vec<Subscription>, anyhow::Error>")]
pub struct ListLatest;

#[derive(Message)]
#[rtype(result = "Result<Vec<Subscription>, anyhow::Error>")]
pub struct ListVersions(pub IdentityOf<Subscription>);

impl Handler<Get> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Option<Subscription>, anyhow::Error>>;

    fn handle(&mut self, Get(id): Get, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let res = repo.get_by(&id).await?;
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetVersion> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Option<Subscription>, anyhow::Error>>;

    fn handle(
        &mut self,
        GetVersion(id, version): GetVersion,
        _: &mut Context<Self>,
    ) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let res = repo.get_by(&(id, version)).await?;
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetBy> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Option<UserSubscription>, anyhow::Error>>;

    fn handle(&mut self, GetBy(user): GetBy, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                if let Some(sub) = &user.subscription {
                    let res = repo
                        .get_by(sub)
                        .await?
                        .ok_or(anyhow::anyhow!("Subscription not found"))?;
                    Ok(Some(UserSubscription(res, user.login)))
                } else {
                    Ok(None)
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<Add> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Add(sub): Add, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                repo.add(sub).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<List> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Vec<Subscription>, anyhow::Error>>;

    fn handle(&mut self, _: List, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let res = repo.list().await?;
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ListLatest> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Vec<Subscription>, anyhow::Error>>;

    fn handle(&mut self, _: ListLatest, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let res = repo.list().await?;
                let res = res.into_iter().into_group_map_by(|s| s.id);
                let res = res
                    .into_values()
                    .filter_map(|mut v| {
                        v.sort_by_key(|v| v.version);
                        v.pop()
                    })
                    .collect();
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ListVersions> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<Vec<Subscription>, anyhow::Error>>;

    fn handle(&mut self, ListVersions(id): ListVersions, _: &mut Self::Context) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let res = repo.list_by(&id).await?;
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<Update> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<SubscriptionVersion, anyhow::Error>>;

    fn handle(&mut self, Update(mut sub): Update, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                let mut latest_sub = repo
                    .get_by(&sub.id)
                    .await?
                    .ok_or(anyhow::anyhow!("Subscription does not exist"))?;
                let version = latest_sub.version + 1;
                sub.version = version;
                latest_sub.yanked = true;
                let res = tokio::join!(repo.save(sub), repo.save(latest_sub));
                res.0?;
                res.1?;
                Ok(version)
            }
            .into_actor(self),
        )
    }
}

impl Handler<Remove> for SubscriptionService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Remove(id, version, _): Remove, _: &mut Context<Self>) -> Self::Result {
        let repo = self.repository.clone();
        Box::pin(
            async move {
                repo.remove_by(&(id, version)).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}
