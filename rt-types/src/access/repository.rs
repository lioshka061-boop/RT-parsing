use crate::access::{RegistrationToken, UserCredentials};
use typesafe_repository::async_ops::{Find, Get, List, Remove, Save};
use typesafe_repository::Repository;
use typesafe_repository::{SelectBy, Selector};

pub trait UserCredentialsRepository:
    Repository<UserCredentials, Error = anyhow::Error>
    + Get<UserCredentials>
    + Find<UserCredentials, RegistrationToken>
    + List<UserCredentials>
    + Save<UserCredentials>
    + Remove<UserCredentials>
    + Send
    + Sync
{
}

impl Selector for RegistrationToken {}
impl SelectBy<RegistrationToken> for UserCredentials {}
