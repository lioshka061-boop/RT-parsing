use crate::access::UserCredentials;
use crate::shop::{Shop, ShopRepository};
use crate::subscription::service::UserSubscription;
use actix::prelude::*;
use actix_broker::BrokerIssue;
use anyhow::Context as AnyhowContext;
use std::sync::Arc;
use typesafe_repository::IdentityOf;

pub struct ShopService {
    repo: Arc<dyn ShopRepository>,
}

impl ShopService {
    pub fn new(repo: Arc<dyn ShopRepository>) -> Self {
        Self { repo }
    }
}

impl Actor for ShopService {
    type Context = Context<Self>;
}

pub struct UserShops(Vec<Shop>, IdentityOf<UserCredentials>);

impl UserShops {
    pub fn user_id(&self) -> &IdentityOf<UserCredentials> {
        &self.1
    }
    pub fn into_inner(self) -> Vec<Shop> {
        self.0
    }
    pub fn inner(&self) -> &Vec<Shop> {
        &self.0
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<Shop>, anyhow::Error>")]
pub struct Get(pub IdentityOf<Shop>);

#[derive(Message)]
#[rtype(result = "Result<Vec<Shop>, anyhow::Error>")]
pub struct List;

#[derive(Message)]
#[rtype(result = "Result<UserShops, anyhow::Error>")]
pub struct ListBy(pub IdentityOf<UserCredentials>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Add(pub Shop, pub CreateShopPermission);

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ShopCreated(pub IdentityOf<Shop>, pub IdentityOf<UserCredentials>);

/// Instance of this struct indicates, that user can create shops
pub struct CreateShopPermission(IdentityOf<UserCredentials>);

impl CreateShopPermission {
    pub fn acquire(
        user: &UserCredentials,
        shops: &UserShops,
        subscription: &Option<UserSubscription>,
    ) -> Option<Self> {
        let max = match &subscription {
            Some(sub) => sub.inner().maximum_shops,
            None if user.has_access_to_control_panel() => {
                return Some(CreateShopPermission(user.login.clone()))
            }
            None => return None,
        };
        if shops.inner().len() < max.get() as usize {
            Some(Self(user.login.clone()))
        } else {
            None
        }
    }
    pub fn user_id(&self) -> &IdentityOf<UserCredentials> {
        &self.0
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Update(pub Shop);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Remove(pub IdentityOf<Shop>);

impl Handler<Get> for ShopService {
    type Result = ResponseActFuture<Self, Result<Option<Shop>, anyhow::Error>>;

    fn handle(&mut self, Get(id): Get, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move { repo.get_one(&id).await.context("Unable to get shop info") }
                .into_actor(self),
        )
    }
}

impl Handler<List> for ShopService {
    type Result = ResponseActFuture<Self, Result<Vec<Shop>, anyhow::Error>>;

    fn handle(&mut self, _: List, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(async move { repo.list().await.context("Unable to list shops") }.into_actor(self))
    }
}

impl Handler<ListBy> for ShopService {
    type Result = ResponseActFuture<Self, Result<UserShops, anyhow::Error>>;

    fn handle(&mut self, ListBy(user): ListBy, ctx: &mut Self::Context) -> Self::Result {
        let addr = ctx.address();
        Box::pin(
            async move {
                let list = addr.send(List).await??;
                Ok(UserShops(
                    list.into_iter().filter(|s| s.owner == user).collect(),
                    user,
                ))
            }
            .into_actor(self),
        )
    }
}

impl Handler<Add> for ShopService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Add(shop, perm): Add, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        let id = shop.id;
        Box::pin(
            async move {
                repo.save(shop).await?;
                Ok(())
            }
            .into_actor(self)
            .map(move |res, act, _| {
                if res.is_ok() {
                    act.issue_system_async(ShopCreated(id, perm.user_id().clone()));
                }
                res
            }),
        )
    }
}

impl Handler<Update> for ShopService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Update(shop): Update, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                repo.save(shop).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<Remove> for ShopService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Remove(id): Remove, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move { repo.remove(&id).await.context("Unable to remove shop") }.into_actor(self),
        )
    }
}
