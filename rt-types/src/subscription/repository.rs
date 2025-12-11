use crate::subscription::{Subscription, SubscriptionVersion};
use typesafe_repository::async_ops::{Add, GetBy, List, ListBy, RemoveBy, Save};
use typesafe_repository::{IdentityOf, Repository};

pub trait SubscriptionRepository:
    Repository<Subscription, Error = anyhow::Error>
    + GetBy<Subscription, (IdentityOf<Subscription>, SubscriptionVersion)>
    + GetBy<Subscription, IdentityOf<Subscription>>
    + ListBy<Subscription, IdentityOf<Subscription>>
    + Add<Subscription>
    + Save<Subscription>
    + List<Subscription>
    + RemoveBy<Subscription, IdentityOf<Subscription>>
    + RemoveBy<Subscription, (IdentityOf<Subscription>, SubscriptionVersion)>
{
}
