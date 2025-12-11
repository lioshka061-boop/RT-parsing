use crate::product::Product;
use async_trait::async_trait;
use crate::shop::Shop;
use lazy_regex::regex;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::BuildHasher;
use typesafe_repository::async_ops::{Get, Remove, Save, Select};
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{SelectBy, Selector};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct CategoriesShop {
    catalog: Categories,
}

#[derive(Serialize, Deserialize)]
pub struct Categories {
    #[serde(rename = "$value", default)]
    pub categories: Vec<CategoryDto>,
}

#[derive(Serialize, Deserialize)]
pub struct CategoryDto {
    #[serde(rename = "$value")]
    pub name: String,
    #[serde(rename = "@id", default = "random_id")]
    pub id: IdentityOf<Category>,
    #[serde(rename = "@parentID")]
    pub parent_id: Option<IdentityOf<Category>>,
    pub regex: Option<String>,
}

fn random_id() -> Uuid {
    Uuid::new_v4()
}

#[derive(Id, Clone, Debug)]
pub struct Category {
    pub name: String,
    #[id]
    pub id: Uuid,
    pub parent_id: Option<IdentityOf<Category>>,
    pub regex: Option<Regex>,
    pub shop_id: IdentityOf<Shop>,
}

pub struct By<T>(pub T);
pub struct ByParentId(pub IdentityOf<Category>);
pub struct TopLevel<T>(pub T);

impl<T> Selector for By<T> {}
impl<T> SelectBy<By<T>> for Category {}

impl Selector for ByParentId {}
impl SelectBy<ByParentId> for Category {}

impl<T> Selector for TopLevel<By<T>> {}
impl<T> SelectBy<TopLevel<By<T>>> for Category {}

impl PartialEq for Category {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Category {}

impl std::hash::Hash for Category {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        self.id.hash(state)
    }
}

impl TryFrom<(IdentityOf<Shop>, CategoryDto)> for Category {
    type Error = regex::Error;

    fn try_from(
        (
            shop_id,
            CategoryDto {
                name,
                id,
                parent_id,
                regex,
            },
        ): (IdentityOf<Shop>, CategoryDto),
    ) -> Result<Category, Self::Error> {
        Ok(Category {
            name,
            id,
            parent_id,
            regex: regex.map(TryInto::try_into).transpose()?,
            shop_id,
        })
    }
}

pub fn parse_categories<R: std::io::BufRead>(
    r: R,
    shop_id: IdentityOf<Shop>,
) -> Result<Vec<Category>, anyhow::Error> {
    let shop: CategoriesShop = quick_xml::de::from_reader(r)?;
    Ok(shop
        .catalog
        .categories
        .into_iter()
        .map(|c| (shop_id, c))
        .map(TryInto::<Category>::try_into)
        .collect::<Result<_, regex::Error>>()?)
}

pub fn assign_categories<'a, P: IntoIterator<Item = Product>>(
    dto: P,
    categories: &'a HashSet<Category, impl BuildHasher>,
) -> impl Iterator<Item = Product> + 'a
where
    <P as IntoIterator>::IntoIter: 'a,
{
    let mut categories: Vec<_> = categories
        .into_iter()
        .map(|c| (count_parents(categories, &c), c))
        .collect();
    categories.sort_by(|(a, _), (b, _)| a.cmp(b));
    categories.reverse();
    let categories = categories.into_iter().map(|(_, c)| c).collect::<Vec<_>>();
    dto.into_iter().map(move |mut p| {
        let category = categories.iter().find(|c| {
            p.category.as_ref().is_some_and(|ca| ca == &c.id)
                || c.regex.as_ref().is_some_and(|r| r.is_match(&p.title))
        });
        p.category = category.map(|c| c.id.clone());
        p
    })
}

pub fn count_parents<'a, T>(categories: &'a T, category: &Category) -> usize
where
    &'a T: IntoIterator<Item = &'a Category> + 'a,
{
    count_parents_internal(categories, category, vec![])
}

fn count_parents_internal<'a, T>(
    categories: &'a T,
    category: &Category,
    mut backtrace: Vec<&'a Category>,
) -> usize
where
    &'a T: IntoIterator<Item = &'a Category> + 'a,
{
    match &category.parent_id {
        Some(id) => {
            let parent = categories.into_iter().find(|c| &c.id == id);
            if let Some(parent) = parent {
                if backtrace.iter().any(|c| &c.id == id) {
                    return 0;
                }
                backtrace.push(parent);
                1 + count_parents_internal(categories, parent, backtrace)
            } else {
                0
            }
        }
        None => 0,
    }
}

#[async_trait]
pub trait CategoryRepository:
    Repository<Category, Error = anyhow::Error>
    + Save<Category>
    + Get<Category>
    + Select<Category, ByParentId>
    + Select<Category, By<IdentityOf<Shop>>>
    + Select<Category, TopLevel<By<IdentityOf<Shop>>>>
    + Remove<Category>
    + Send
    + Sync
{
    async fn clear(&self) -> Result<(), Self::Error>;
    async fn count_by(&self, by: &By<IdentityOf<Shop>>) -> Result<usize, Self::Error>;
}
