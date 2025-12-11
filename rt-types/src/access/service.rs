use crate::access::{
    generate_salt, repository::UserCredentialsRepository, Access, Login, Password,
    RegistrationToken, UserCredentials,
};
use crate::shop::service::ShopCreated;
use crate::subscription::{Subscription, SubscriptionVersion};
use actix::prelude::*;
use actix::ResponseActFuture;
use actix_broker::BrokerSubscribe;
use log_error::*;
use rand::{distributions, Rng, SeedableRng};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use typesafe_repository::IdentityOf;

pub struct UserCredentialsService {
    repo: Arc<dyn UserCredentialsRepository>,
    tokens: BTreeSet<RegistrationToken>,
}

impl UserCredentialsService {
    pub fn new(repo: Arc<dyn UserCredentialsRepository>) -> Self {
        Self {
            repo,
            tokens: BTreeSet::new(),
        }
    }
}

impl Actor for UserCredentialsService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.subscribe_system_async::<ShopCreated>(ctx);
        let admin_pwd = std::env::var("ADMIN_PASSWORD").unwrap_or_else(|err| {
            log::error!("Unable to get admin password from env: {err}");
            let pwd = rand::thread_rng()
                .sample_iter(distributions::Alphanumeric)
                .take(20)
                .map(char::from)
                .collect::<String>();
            log::info!("Admin password is {pwd}");
            pwd
        });
        log::info!("Admin password is {admin_pwd}");
        #[allow(clippy::unwrap_used)]
        let admin_pwd = Password::generate(admin_pwd, generate_salt()).unwrap();
        let admin = UserCredentials {
            login: Login("admin".to_string()),
            password: admin_pwd,
            access: [Access::ControlPanel].into_iter().collect(),
            registration_token: None,
            subscription: None,
        };
        let addr = ctx.address();
        ctx.spawn(
            async move {
                addr.send(Update(admin))
                    .await
                    .log_error("Unable to add admin")
                    .log_error("Unable to add admin");
                log::info!("Admin updated successfully");
                for _ in 0..10 {
                    addr.send(GenerateToken)
                        .await
                        .log_error("Unable to generate token");
                }
            }
            .into_actor(self),
        );
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<UserCredentials>, anyhow::Error>")]
pub struct Get(pub IdentityOf<UserCredentials>);

#[derive(Deserialize)]
pub struct UserCredentialsDto {
    login: Login,
    password: String,
}

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Register(pub UserCredentialsDto, pub RegistrationToken);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Update(pub UserCredentials);

#[derive(Message)]
#[rtype(result = "Result<Vec<UserCredentials>, anyhow::Error>")]
pub struct List;

#[derive(Message)]
#[rtype(result = "RegistrationToken")]
pub struct GenerateToken;

#[derive(Message)]
#[rtype(result = "BTreeSet<RegistrationToken>")]
pub struct ListTokens;

/// Instance of this struct indicates, that subscription is not used and can be deleted
pub struct SubscriptionGuard(IdentityOf<Subscription>, SubscriptionVersion);

impl SubscriptionGuard {
    pub fn id(&self) -> &IdentityOf<Subscription> {
        &self.0
    }
    pub fn version(&self) -> &SubscriptionVersion {
        &self.1
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<SubscriptionGuard>, anyhow::Error>")]
pub struct SubscriptionUsed(pub IdentityOf<Subscription>, pub SubscriptionVersion);

impl Handler<Get> for UserCredentialsService {
    type Result = ResponseActFuture<Self, Result<Option<UserCredentials>, anyhow::Error>>;

    fn handle(&mut self, Get(id): Get, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(async move { repo.get_one(&id).await }.into_actor(self))
    }
}

impl Handler<Register> for UserCredentialsService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(
        &mut self,
        Register(UserCredentialsDto { login, password }, token): Register,
        _: &mut Self::Context,
    ) -> Self::Result {
        let repo = self.repo.clone();
        let token_valid = self.tokens.remove(&token);
        Box::pin(
            async move {
                if !token_valid {
                    return Err(anyhow::anyhow!("Token not found"));
                }
                let creds = UserCredentials {
                    login,
                    password: Password::generate(password, generate_salt())?,
                    access: [].into_iter().collect(),
                    registration_token: Some(token),
                    subscription: None,
                };
                repo.save(creds).await
            }
            .into_actor(self),
        )
    }
}

impl Handler<Update> for UserCredentialsService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Update(creds): Update, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(async move { repo.save(creds).await }.into_actor(self))
    }
}

impl Handler<List> for UserCredentialsService {
    type Result = ResponseActFuture<Self, Result<Vec<UserCredentials>, anyhow::Error>>;

    fn handle(&mut self, _: List, _: &mut Self::Context) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(async move { repo.list().await }.into_actor(self))
    }
}

impl Handler<GenerateToken> for UserCredentialsService {
    type Result = MessageResult<GenerateToken>;

    fn handle(&mut self, _: GenerateToken, _: &mut Self::Context) -> Self::Result {
        let token = rand::rngs::StdRng::from_entropy()
            .sample_iter(distributions::Alphanumeric)
            .take(20)
            .map(char::from)
            .collect::<String>();
        let token = RegistrationToken(token.to_uppercase());
        self.tokens.insert(token.clone());
        MessageResult(token)
    }
}

impl Handler<ListTokens> for UserCredentialsService {
    type Result = MessageResult<ListTokens>;

    fn handle(&mut self, _: ListTokens, _: &mut Self::Context) -> Self::Result {
        MessageResult(self.tokens.clone())
    }
}

impl Handler<SubscriptionUsed> for UserCredentialsService {
    type Result = ResponseActFuture<Self, Result<Option<SubscriptionGuard>, anyhow::Error>>;

    fn handle(
        &mut self,
        SubscriptionUsed(id, version): SubscriptionUsed,
        _: &mut Self::Context,
    ) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let list = repo.list().await?;
                if list.iter().any(|u| u.subscription == Some((id, version))) {
                    Ok(None)
                } else {
                    Ok(Some(SubscriptionGuard(id, version)))
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<ShopCreated> for UserCredentialsService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(
        &mut self,
        ShopCreated(shop, user): ShopCreated,
        _: &mut Self::Context,
    ) -> Self::Result {
        let repo = self.repo.clone();
        Box::pin(
            async move {
                let user = repo
                    .get_one(&user)
                    .await
                    .log_error("Unable to get user")
                    .ok_or(anyhow::anyhow!("User not found"))
                    .log_error("Unable to update user permissions")
                    .flatten();
                if let Some(mut user) = user {
                    user.access.insert(Access::Shop(shop));
                    repo.save(user).await.log_error("Unable to save user");
                }
            }
            .into_actor(self),
        )
    }
}
