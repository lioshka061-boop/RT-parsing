use crate::SqlWrapper;
use async_trait::async_trait;
use bytes::BytesMut;
use futures::stream::StreamExt;
use rt_types::access::repository::UserCredentialsRepository;
use rt_types::access::{Access, Login, Password, RegistrationToken, UserCredentials};
use std::error::Error;
use std::pin::pin;
use std::sync::Arc;
use tokio_postgres::row::Row;
use tokio_postgres::types::{IsNull, ToSql, Type};
use tokio_postgres::Client;
use typesafe_repository::async_ops::{Find, Get, List, Remove, Save};
use typesafe_repository::{IdentityOf, Repository};

pub struct PostgresUserCredentialsRepository {
    client: Arc<Client>,
}

impl PostgresUserCredentialsRepository {
    pub async fn new(client: Arc<Client>) -> Result<Self, tokio_postgres::error::Error> {
        Ok(Self { client })
    }
}

impl Repository<UserCredentials> for PostgresUserCredentialsRepository {
    type Error = anyhow::Error;
}

impl TryFrom<Row> for SqlWrapper<UserCredentials> {
    type Error = anyhow::Error;

    fn try_from(r: Row) -> Result<Self, Self::Error> {
        Ok(SqlWrapper(UserCredentials {
            login: Login(r.try_get("login")?),
            password: Password::new(
                r.try_get("password")?,
                r.try_get::<_, &[u8]>("salt")?.try_into()?,
            )?,
            access: r
                .try_get::<_, Vec<String>>("access")?
                .into_iter()
                .map(Access::try_from)
                .collect::<Result<_, <Access as TryFrom<String>>::Error>>()
                .map_err(|s| anyhow::anyhow!("Invalid access entry: {s}"))?,
            subscription: r.try_get::<_, Option<_>>("subscription_id")?.zip(
                r.try_get::<_, Option<i64>>("subscription_version")?
                    .map(|v| v as u32),
            ),
            registration_token: r
                .try_get::<_, Option<String>>("registration_token")?
                .map(RegistrationToken),
        }))
    }
}

impl ToSql for SqlWrapper<Access> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_string().to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        String::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_string().to_sql_checked(ty, out)
    }
}

#[async_trait]
impl Get<UserCredentials> for PostgresUserCredentialsRepository {
    async fn get_one(
        &self,
        id: &IdentityOf<UserCredentials>,
    ) -> Result<Option<UserCredentials>, Self::Error> {
        let mut res = pin!(
            self.client
                .query_raw("SELECT * FROM user_credentials WHERE login = $1", &[&id.0])
                .await?
        );
        Ok(res
            .next()
            .await
            .transpose()?
            .map(SqlWrapper::<UserCredentials>::from_sql)
            .transpose()?)
    }
}

#[async_trait]
impl Find<UserCredentials, RegistrationToken> for PostgresUserCredentialsRepository {
    async fn find(
        &self,
        token: &RegistrationToken,
    ) -> Result<Option<UserCredentials>, Self::Error> {
        let res = self
            .client
            .query_one(
                "SELECT * FROM user_credentials WHERE registration_token = $1",
                &[&token.0],
            )
            .await?;
        Ok(Some(SqlWrapper::from_sql(res)?))
    }
}

#[async_trait]
impl List<UserCredentials> for PostgresUserCredentialsRepository {
    async fn list(&self) -> Result<Vec<UserCredentials>, Self::Error> {
        let res = self
            .client
            .query("SELECT * FROM user_credentials", &[])
            .await?;
        res.into_iter().map(SqlWrapper::from_sql).collect()
    }
}

#[async_trait]
impl Save<UserCredentials> for PostgresUserCredentialsRepository {
    async fn save(&self, user: UserCredentials) -> Result<(), Self::Error> {
        let access = &user.access.into_iter().map(SqlWrapper).collect::<Vec<_>>();
        let registration_token = user.registration_token.map(|t| t.0);
        self.client
            .execute(
                "INSERT INTO user_credentials (login, password, salt, access, registration_token, subscription_id, subscription_version) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (login) DO UPDATE 
                SET login = $1, password = $2, salt = $3, access = $4, registration_token = $5, subscription_id = $6, subscription_version = $7",
                &[
                    &user.login.0,
                    user.password.password(),
                    user.password.salt(),
                    &access,
                    &registration_token,
                    &user.subscription.map(|(id, _)| id),
                    &user.subscription.map(|(_, ver)| ver as i64),
                ]).await?;
        Ok(())
    }
}

#[async_trait]
impl Remove<UserCredentials> for PostgresUserCredentialsRepository {
    async fn remove(&self, id: &IdentityOf<UserCredentials>) -> Result<(), anyhow::Error> {
        self.client
            .execute("DELETE FROM user_credentials WHERE id = $1", &[&id.0])
            .await?;
        Ok(())
    }
}

impl UserCredentialsRepository for PostgresUserCredentialsRepository {}
