use crate::control::{
    render_template, see_other, ControlPanelAccess, Record, Response, ShopAccess,
};
use crate::export::{self, ExportService};
use actix::prelude::*;
use actix_web::web::{Data, Form, Path};
use actix_web::{get, post};
use anyhow::Context as AnyhowContext;
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::shop::{
    service::{CreateShopPermission, ShopService},
    Shop,
};
use rt_types::subscription::service::SubscriptionService;
use rt_types::{shop, subscription};
use serde::Deserialize;
use typesafe_repository::IdentityOf;
use uuid::Uuid;

#[derive(Template)]
#[template(path = "shop/add.html")]
pub struct AddShopPage {
    user: UserCredentials,
}

#[get("/shop/add")]
async fn add_shop_page(user: Record<UserCredentials>) -> Response {
    render_template(AddShopPage { user: user.t })
}

#[derive(Deserialize)]
pub struct ShopDto {
    name: String,
}

#[post("/shop/add")]
async fn add_shop(
    f: Form<ShopDto>,
    shop_service: Data<Addr<ShopService>>,
    user: Record<UserCredentials>,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let user = user.t;
    let ShopDto { name } = f.into_inner();
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let id = Uuid::new_v4();
    let limits = subscription.as_ref().map(|s| s.inner().limits.clone());
    let shop = Shop {
        id,
        is_suspended: false,
        name,
        owner: user.login.clone(),
        export_entries: vec![],
        limits,
        default_custom_options: None,
        image_proxy: false,
    };
    let shops = shop_service
        .send(shop::service::ListBy(user.login.clone()))
        .await??;
    let perm = CreateShopPermission::acquire(&user, &shops, &subscription)
        .ok_or(anyhow::anyhow!("User cannot create shops"))?;
    shop_service.send(shop::service::Add(shop, perm)).await??;
    Ok(see_other(&format!("/shop/{id}")))
}

#[derive(Template)]
#[template(path = "shop/remove.html")]
pub struct RemoveShopPage {
    user: UserCredentials,
    shop: Shop,
}

#[get("/shop/{shop_id}/remove")]
async fn remove_shop_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(RemoveShopPage { user, shop })
}

#[post("/shop/{shop_id}/remove")]
async fn remove_shop(
    shop_service: Data<Addr<ShopService>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    shop_service.send(shop::service::Remove(shop.id)).await??;
    Ok(see_other("/shops"))
}

#[derive(Template)]
#[template(path = "shop/settings.html")]
pub struct SettingsPage {
    shop: Shop,
    user: UserCredentials,
}

#[get("/shop/{shop_id}/settings")]
async fn settings_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(SettingsPage { shop, user })
}

#[derive(Deserialize, Debug)]
pub struct ShopConfigurationDto {
    pub name: Option<String>,
}

impl ShopConfigurationDto {
    pub fn apply(self, mut shop: Shop) -> Shop {
        if let Some(name) = self.name {
            shop.name = name;
        }
        shop
    }
}

#[post("/shop/{shop_id}/settings")]
async fn update_settings(
    shop_id: Path<IdentityOf<Shop>>,
    dto: Form<ShopConfigurationDto>,
    shop_service: Data<Addr<ShopService>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop_id.into_inner();
    let dto = dto.into_inner();
    let shop = dto.apply(shop);
    shop_service
        .send(shop::service::Update(shop))
        .await?
        .context("Unable to update shop")?;
    Ok(see_other(&format!("/shop/{shop_id}/settings")))
}

#[post("/control_panel/shops/{shop_id}/suspend_toggle")]
async fn shop_suspend_toggle(
    ControlPanelAccess { .. }: ControlPanelAccess,
    ShopAccess { mut shop, .. }: ShopAccess,
    shop_service: Data<Addr<ShopService>>,
    export_service: Data<Addr<ExportService>>,
) -> Response {
    shop.is_suspended = !shop.is_suspended;
    export_service
        .send(export::SuspendByShop(shop.id, shop.is_suspended))
        .await?
        .context("Unable to suspend exports")?;
    shop_service
        .send(shop::service::Update(shop))
        .await?
        .context("Unable to update shop")?;
    Ok(see_other(&format!("/control_panel/shops/")))
}
