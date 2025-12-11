use crate::control::{
    deserialize_decimal_form, render_template, see_other, ControllerError, FileInfo, Record,
    RecordGuard, RecordResponse, Response, ShopAccess, ShopControllerError,
};
use crate::export;
use crate::export::ExportService;
use actix::Addr;
use actix_files::NamedFile;
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::dev::Payload;
use actix_web::web::Data;
use actix_web::{
    get,
    http::header::ContentType,
    post,
    web::{Form, Path, Query},
    FromRequest, HttpRequest, HttpResponse,
};
use anyhow::{anyhow, Context};
use askama::Template;
use async_trait::async_trait;
use cached::proc_macro::cached;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use lazy_regex::lazy_regex;
use once_cell::sync::Lazy;
use photon_rs::PhotonImage;
use reqwest::StatusCode;
use rt_types::access::UserCredentials;
use rt_types::shop::Shop;
use rt_types::watermark::{
    apply, WatermarkGroup, WatermarkGroupRepository, WatermarkOptions, WatermarkPosition,
    WatermarkSize,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_stream::wrappers::ReadDirStream;
use typesafe_repository::{
    async_ops::{Add, Get, ListBy, Remove},
    GetIdentity, IdentityOf, Repository,
};
use uuid::Uuid;

use rt_types::watermark::service::WatermarkService;

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Default)]
pub struct WatermarkOptionsDto {
    #[serde(deserialize_with = "deserialize_decimal_form")]
    #[serde(default)]
    pub width_percent: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_decimal_form")]
    #[serde(default)]
    pub height_percent: Option<Decimal>,
    #[serde(default)]
    pub horizontal_position: Option<WatermarkPosition>,
    #[serde(default)]
    pub vertical_position: Option<WatermarkPosition>,
}

impl Into<WatermarkOptions> for WatermarkOptionsDto {
    fn into(self) -> WatermarkOptions {
        let default = WatermarkOptions::default();
        WatermarkOptions {
            size: match (self.width_percent, self.height_percent) {
                (Some(w), None) => WatermarkSize::Width(w),
                (None, Some(h)) => WatermarkSize::Height(h),
                (Some(width), Some(height)) => WatermarkSize::BoundingBox { width, height },
                (None, None) => default.size,
            },
            horizontal_position: self
                .horizontal_position
                .unwrap_or(default.horizontal_position),
            vertical_position: self.vertical_position.unwrap_or(default.vertical_position),
        }
    }
}

#[get("/shop/{shop_id}/watermark/{link:.+}/{watermark}")]
pub async fn apply_watermark(
    q: Query<WatermarkOptionsDto>,
    path: Path<(IdentityOf<Shop>, String, String)>,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    user_credentials: Option<Record<UserCredentials>>,
    req: HttpRequest,
) -> Response {
    if user_credentials.is_none() {
        log::info!(
            "Watermark apply request from {:?} at {}",
            req.peer_addr(),
            req.uri()
        );
    }
    let q = q.into_inner();
    let (shop_id, link, watermark) = path.into_inner();
    let image = match download_image(link).await {
        Ok(image) => image,
        Err(err) => {
            return Ok(HttpResponse::BadRequest()
                .content_type(ContentType::html())
                .body(err.to_string()))
        }
    };
    let ins = std::time::Instant::now();
    let res = apply_watermark_or_group(
        &watermark,
        image.deref(),
        q.into(),
        shop_id,
        watermark_group_repository.get_ref().clone(),
    )
    .await;
    log::info!("{}ms", ins.elapsed().as_millis());
    match res {
        Ok(image) => Ok(HttpResponse::Ok().body(image.get_bytes())),
        Err(err) => Ok(HttpResponse::BadRequest()
            .content_type(ContentType::html())
            .body(err.to_string())),
    }
}

#[cached(result, size = 128)]
async fn download_image(url: String) -> Result<bytes::Bytes, anyhow::Error> {
    let res = reqwest::get(url).await.context("Unable to get image")?;
    if res.status() != StatusCode::OK {
        return Err(anyhow!("{}", res.status()).into());
    }
    let ty = res.headers().get(reqwest::header::CONTENT_TYPE);
    match ty.and_then(|ty| {
        Some(mime::Mime::from_str(ty.to_str().ok()?).context("Unable to parse mime type"))
    }) {
        None => (),
        Some(Ok(m)) if m == mime::IMAGE_JPEG || m == mime::IMAGE_PNG => (),
        Some(Ok(_)) => return Err(anyhow::anyhow!("Mime type not supported")),
        Some(Err(err)) => return Err(err.into()),
    }
    Ok(res.bytes().await.context("Unable to get response bytes")?)
}

#[cached(result, size = 32)]
async fn open_image(s: String) -> Result<Vec<u8>, std::io::Error> {
    tokio::fs::read(s).await
}

#[cached(
    result,
    key = "(String, Vec<u8>, WatermarkOptions, IdentityOf<Shop>)",
    convert = r#"{ (watermark.to_string(), image.into(), opts.clone(), shop_id) }"#,
    size = 128
)]
async fn apply_watermark_or_group(
    watermark: &str,
    image: &[u8],
    opts: WatermarkOptions,
    shop_id: IdentityOf<Shop>,
    watermark_group_repository: Arc<dyn WatermarkGroupRepository>,
) -> Result<PhotonImage, anyhow::Error> {
    let image = photon_rs::native::open_image_from_bytes(image).context("Unable to open image")?;
    let res = match watermark.parse() {
        Ok(id) => {
            let watermarks = watermark_group_repository
                .get_one(&(id, shop_id))
                .await?
                .ok_or(anyhow::anyhow!("Watermark group not found"))?;
            let mut image = image;
            for (name, opts) in watermarks.elements {
                let watermark = open_image(format!("./watermark/{shop_id}/{name}")).await?;
                let watermark = photon_rs::native::open_image_from_bytes(&watermark)
                    .context("Unable to open watermark image")?;
                image =
                    tokio::task::spawn_blocking(move || apply(image, &watermark, opts)).await??;
            }
            Ok(image)
        }
        Err(_) => {
            let watermark = open_image(format!("./watermark/{shop_id}/{watermark}")).await?;
            let watermark = photon_rs::native::open_image_from_bytes(&watermark)
                .context("Unable to open watermark image")?;
            let image =
                tokio::task::spawn_blocking(move || apply(image, &watermark, opts)).await??;
            Ok(image)
        }
    };
    res
}

#[derive(Template)]
#[template(path = "shop/watermark.html")]
pub struct WatermarkSettings {
    user: UserCredentials,
    shop: Shop,
    images: Vec<FileInfo>,
    groups: Vec<WatermarkGroup>,
}

#[get("/shop/{shop_id}/watermark")]
pub async fn watermark_settings(
    ShopAccess { user, shop }: ShopAccess,
    watermark_group_repo: Data<Arc<dyn WatermarkGroupRepository>>,
) -> Response {
    let shop_id = shop.id;
    let path = format!("./watermark/{shop_id}");
    let res = match tokio::fs::read_dir(&path).await {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(&path)
                .await
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            tokio::fs::set_permissions(&path, perm)
                .await
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            tokio::fs::read_dir(&path)
                .await
                .context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let images: Vec<_> = ReadDirStream::new(res.map_err(ShopControllerError::with(&user, &shop))?)
        .map(|e| async {
            let e = e?;
            let meta = e.metadata().await?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok::<_, anyhow::Error>(FileInfo {
                name: file_name,
                last_modified: OffsetDateTime::from(meta.modified()?),
                size: meta.len(),
            })
        })
        .buffered(10)
        .try_collect()
        .await
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;
    let groups = watermark_group_repo.list_by(&shop.id).await?;
    render_template(WatermarkSettings {
        user,
        shop,
        images,
        groups,
    })
}

#[derive(MultipartForm, Debug)]
pub struct WatermarkQuery {
    file: TempFile,
}

#[post("/shop/{shop_id}/watermark")]
pub async fn upload_watermark(
    ShopAccess { .. }: ShopAccess,
    q: MultipartForm<WatermarkQuery>,
    path: Path<IdentityOf<Shop>>,
) -> Response {
    let shop_id = path.into_inner();
    let name = match q.file.file_name.clone() {
        Some(name) => name,
        None => Uuid::new_v4().to_string(),
    };
    tokio::fs::copy(q.file.file.path(), format!("./watermark/{shop_id}/{name}"))
        .await
        .context("Unable to save watermark image")?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[get("/shop/{shop_id}/watermark/{name}")]
pub async fn show_watermark(
    ShopAccess { .. }: ShopAccess,
    path: Path<(IdentityOf<Shop>, String)>,
    req: HttpRequest,
) -> Response {
    let (shop_id, name) = path.into_inner();
    let path = format!("./watermark/{shop_id}/{name}");
    return Ok(NamedFile::open(path)
        .context("Unable to open file")
        .map_err(ControllerError::InternalServerError)?
        .into_response(&req));
}

#[post("/shop/{shop_id}/watermark/{name}/delete")]
pub async fn delete_watermark(
    ShopAccess { .. }: ShopAccess,
    path: Path<(IdentityOf<Shop>, String)>,
) -> Response {
    let (shop_id, name) = path.into_inner();
    tokio::fs::remove_file(format!("./watermark/{shop_id}/{name}"))
        .await
        .context("Unable to delete watermark")?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[derive(Deserialize)]
pub struct UpdateWatermarkQuery {
    pub name: String,
}

#[post("/shop/{shop_id}/watermark/{name}")]
pub async fn update_watermark(
    q: Form<UpdateWatermarkQuery>,
    path: Path<(IdentityOf<Shop>, String)>,
    watermark_service: Data<Addr<WatermarkService>>,
) -> Response {
    let q = q.into_inner();
    let (shop_id, name) = path.into_inner();
    watermark_service
        .send(rt_types::watermark::service::RenameWatermark {
            shop_id,
            from: name,
            to: q.name,
        })
        .await??;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[derive(Template)]
#[template(path = "shop/watermark_generate.html")]
pub struct GenerateWatermarkLinkPage {
    user: UserCredentials,
    shop: Shop,
    name: String,
}

#[derive(Template)]
#[template(path = "shop/watermark_group_generate.html")]
pub struct GenerateWatermarkGroupLinkPage {
    user: UserCredentials,
    shop: Shop,
    group: WatermarkGroup,
}

#[get("/shop/{shop_id}/watermark/{name}/generate")]
pub async fn generate_watermark_link_page(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { user, shop }: ShopAccess,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
) -> Response {
    let (shop_id, name) = path.into_inner();
    match name.parse() {
        Ok(id) => {
            let group = watermark_group_repository
                .get_one(&(id, shop_id))
                .await?
                .ok_or(anyhow::anyhow!("Watermark group not found"))?;
            render_template(GenerateWatermarkGroupLinkPage { user, shop, group })
        }
        Err(_) => render_template(GenerateWatermarkLinkPage { user, shop, name }),
    }
}

#[derive(Deserialize)]
pub struct GenerateWatermarkQuery {
    link: String,
}

#[post("/shop/{shop_id}/watermark/{name}/generate")]
pub async fn generate_watermark_link(
    path: Path<(IdentityOf<Shop>, String)>,
    q: Form<GenerateWatermarkQuery>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let link = q.link;
    let (shop_id, name) = path.into_inner();
    Ok(see_other(&format!(
        "/shop/{shop_id}/watermark/{link}/{name}"
    )))
}

pub static PREVIEW_IMAGE: Lazy<PhotonImage> =
    Lazy::new(|| photon_rs::PhotonImage::new([255; 1280 * 720 * 4].into(), 1280, 720));

#[get("/shop/{shop_id}/watermark/{name}/preview")]
pub async fn preview(
    q: Query<WatermarkOptionsDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
) -> Response {
    let q = q.into_inner();
    let (shop_id, watermark) = path.into_inner();
    let image = Lazy::force(&PREVIEW_IMAGE).clone();
    let image = match watermark.parse() {
        Ok(id) => {
            let watermarks = watermark_group_repository
                .get_one(&(id, shop_id))
                .await?
                .ok_or(anyhow::anyhow!("Watermark group not found"))?;
            let mut image = image;
            for (name, opts) in watermarks.elements {
                let watermark =
                    photon_rs::native::open_image(&format!("./watermark/{shop_id}/{name}"))
                        .context("Unable to open watermark image")?;
                image = apply(image, &watermark, opts)?;
            }
            image
        }
        Err(_) => {
            let watermark =
                photon_rs::native::open_image(&format!("./watermark/{shop_id}/{watermark}"))
                    .context("Unable to open watermark image")?;
            let image = apply(image, &watermark, q.into())?;
            image
        }
    };
    Ok(HttpResponse::Ok().body(image.get_bytes()))
}

pub struct FilesystemWatermarkGroupRepository {}

impl FilesystemWatermarkGroupRepository {
    pub fn new() -> Self {
        Self {}
    }
}

impl Repository<WatermarkGroup> for FilesystemWatermarkGroupRepository {
    type Error = anyhow::Error;
}

#[cached(result, size = 32)]
fn read_file(id: IdentityOf<WatermarkGroup>) -> Result<Option<WatermarkGroup>, anyhow::Error> {
    let (id, shop_id) = id;
    let file = match std::fs::File::open(format!("./watermark.grp.d/{shop_id}/{id}")) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(Some(serde_json::from_reader(file)?))
}

#[async_trait]
impl Get<WatermarkGroup> for FilesystemWatermarkGroupRepository {
    async fn get_one(
        &self,
        id: &IdentityOf<WatermarkGroup>,
    ) -> Result<Option<WatermarkGroup>, anyhow::Error> {
        let id = *id;
        tokio::task::spawn_blocking(move || read_file(id)).await?
    }
}

#[async_trait]
impl Add<WatermarkGroup> for FilesystemWatermarkGroupRepository {
    async fn add(&self, group: WatermarkGroup) -> Result<(), anyhow::Error> {
        let (id, shop_id) = group.id();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::create(format!("./watermark.grp.d/{shop_id}/{id}"))?;
            serde_json::to_writer(file, &group)?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}

#[async_trait]
impl Remove<WatermarkGroup> for FilesystemWatermarkGroupRepository {
    async fn remove(
        &self,
        (id, shop_id): &IdentityOf<WatermarkGroup>,
    ) -> Result<(), anyhow::Error> {
        tokio::fs::remove_file(format!("./watermark.grp.d/{shop_id}/{id}")).await?;
        Ok(())
    }
}

#[async_trait]
impl ListBy<WatermarkGroup, IdentityOf<Shop>> for FilesystemWatermarkGroupRepository {
    async fn list_by(
        &self,
        shop_id: &IdentityOf<Shop>,
    ) -> Result<Vec<WatermarkGroup>, anyhow::Error> {
        let mut res = vec![];
        let path = format!("./watermark.grp.d/{shop_id}");
        let dir = match std::fs::read_dir(&path) {
            Ok(r) => Ok(r),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(&path).context("Unable to create dir")?;
                let meta = std::fs::metadata(&path)
                    .context("Unable to read file metadata")
                    .map_err(ControllerError::InternalServerError)?;
                let mut perm = meta.permissions();
                perm.set_mode(0o777);
                std::fs::set_permissions(&path, perm).context("Unable to set dir permissions")?;
                std::fs::read_dir(&path).context("Unable to read dir")
            }
            Err(err) => Err(err).context("Unable to read dir"),
        }?;
        for e in dir {
            let e = e?;
            let f = std::fs::File::open(format!(
                "./watermark.grp.d/{shop_id}/{}",
                e.file_name()
                    .to_str()
                    .ok_or(anyhow::anyhow!("Unable to convert OsString to str"))?
            ))?;
            res.push(serde_json::from_reader(f)?);
        }
        Ok(res)
    }
}

impl WatermarkGroupRepository for FilesystemWatermarkGroupRepository {}

#[derive(Deserialize)]
pub struct AddWatermarkGroupDto {
    pub name: String,
}

#[post("/shop/{shop_id}/watermark/group/add")]
pub async fn add_watermark_group(
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    q: Form<AddWatermarkGroupDto>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let name = q.into_inner().name;
    watermark_group_repository
        .add(WatermarkGroup {
            name,
            shop_id,
            elements: HashMap::new(),
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[derive(Deserialize)]
pub struct WatermarkGroupOperationDto {
    watermark: String,
    #[serde(flatten)]
    options: WatermarkOptionsDto,
    #[serde(default)]
    _preserve: bool,
}

#[derive(Template)]
#[template(path = "shop/watermark_group_push.html")]
pub struct PushWatermarkPage {
    user: UserCredentials,
    shop: Shop,
    group: WatermarkGroup,
    watermarks: Vec<String>,
}

#[get("/shop/{shop_id}/watermark/group/{group_id}/push")]
pub async fn push_watermark_to_group_page(
    ShopAccess { shop, user }: ShopAccess,
    path: Path<(IdentityOf<Shop>, u64)>,
    group: Record<WatermarkGroup>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let path = format!("./watermark/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path)
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm)
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let watermarks: Vec<_> = res
        .map_err(ShopControllerError::with(&user, &shop))?
        .map(|e| {
            let e = e?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(file_name)
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;
    render_template(PushWatermarkPage {
        watermarks,
        shop,
        group: group.t,
        user,
    })
}

#[post("/shop/{shop_id}/watermark/group/{group_id}/push")]
pub async fn push_watermark_to_group(
    dto: Form<WatermarkGroupOperationDto>,
    path: Path<(IdentityOf<Shop>, u64)>,
    ShopAccess { .. }: ShopAccess,
    group: Record<WatermarkGroup>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let dto = dto.into_inner();
    group
        .map(|g| {
            g.elements.insert(dto.watermark, dto.options.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[post("/shop/{shop_id}/watermark/group/{group_id}/edit/{name}")]
pub async fn edit_watermark_group_entry(
    dto: Form<WatermarkOptionsDto>,
    path: Path<(IdentityOf<Shop>, u64, String)>,
    ShopAccess { .. }: ShopAccess,
    group: Record<WatermarkGroup>,
) -> Response {
    let (shop_id, _, name) = path.into_inner();
    let dto = dto.into_inner();
    group
        .try_map(|g| {
            let elem = g
                .elements
                .get_mut(&name)
                .ok_or(anyhow::anyhow!("Watermark not found in group"))?;
            *elem = dto.into();
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[post("/shop/{shop_id}/watermark/group/{group_id}/remove")]
pub async fn remove_watermark_group(
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    path: Path<(IdentityOf<Shop>, u64)>,
    export_service: Data<Addr<ExportService>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, group_id) = path.into_inner();
    let exports = export_service.send(export::GetAllStatus(shop_id)).await?;
    if let Some((_, export)) = exports
        .iter()
        .find(|(_, e)| e.entry.uses_watermark(&group_id.to_string()))
    {
        return Err(anyhow::anyhow!(
            "Unable to remove watermark group that is being used\nSee export {} ({})",
            export.entry.file_name(None),
            export.entry.generate_hash()
        )
        .into());
    }
    watermark_group_repository
        .remove(&(group_id, shop_id))
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

#[post("/shop/{shop_id}/watermark/group/{group_id}/remove/{name}")]
pub async fn remove_watermark_group_entry(
    path: Path<(IdentityOf<Shop>, u64, String)>,
    ShopAccess { .. }: ShopAccess,
    group: Record<WatermarkGroup>,
) -> Response {
    let (shop_id, _, name) = path.into_inner();
    group
        .map(|g| {
            g.elements.remove(&name);
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/watermark")))
}

impl RecordResponse for RecordGuard<WatermarkGroup> {
    type Response = Result<(), anyhow::Error>;
}

impl FromRequest for Record<WatermarkGroup> {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let repo = Data::<Arc<dyn WatermarkGroupRepository>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract WatermarkGroupRepository from request")
                })?;
            let id = req
                .match_info()
                .get("group_id")
                .ok_or(anyhow::anyhow!("Unable to extract group id from request"))?
                .parse()
                .context("Unable to parse group id")?;
            let shop_id = req
                .match_info()
                .get("shop_id")
                .ok_or(anyhow::anyhow!("Unable to extract shop id from request"))?
                .parse()
                .context("Unable to parse shop id")?;
            let group = repo
                .get_one(&(id, shop_id))
                .await?
                .ok_or(anyhow::anyhow!("Group not found"))?;
            Ok(Self {
                t: group,
                g: RecordGuard {
                    f: Box::new(move |mut e| {
                        let repo = repo.clone();
                        Box::pin(async move {
                            if id != e.id().0 {
                                let regex = lazy_regex!(r"v(\d)+$");
                                if let Some(index) = regex.captures(&e.name).and_then(|c| c.get(1))
                                {
                                    let index: u64 = index.as_str().parse()?;
                                    e.name = regex
                                        .replace(&e.name, format!("v{}", index + 1))
                                        .to_string();
                                } else {
                                    e.name = format!("{} v1", e.name);
                                }
                                Ok(repo.add(e).await?)
                            } else {
                                Ok(())
                            }
                        })
                    }),
                },
            })
        })
    }
}
