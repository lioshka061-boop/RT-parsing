use crate::empty_string_as_none_parse;
use crate::export::{
    self, AddExportPermission, Export, ExportService, ExportStatus, UpdateExportEntryPermission,
};
use crate::watermark::WatermarkOptionsDto;
use crate::{dt, tt};
use actix::fut::{ready, Ready};
use actix::prelude::*;
use actix_files::NamedFile;
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_session::Session;
use actix_web::{
    dev::{forward_ready, Payload, Service, ServiceRequest, ServiceResponse, Transform},
    get,
    http::header::ContentType,
    post,
    web::{Data, Form, Json, Path, Query},
    Either, FromRequest, HttpMessage, HttpRequest, HttpResponse,
};
use anyhow::{anyhow, Context};
use askama::Template;
use derive_more::{Display, Error};
use futures::future::LocalBoxFuture;
use log_error::LogError;
use regex::Regex;
use reqwest::Method;
use rt_types::access::service::UserCredentialsService;
use rt_types::access::{self, Login, RegistrationToken, UserCredentials};
use rt_types::category::{
    parse_categories, By, ByParentId, Category, CategoryRepository, TopLevel,
};
use rt_types::shop::{self, service::ShopService};
use rt_types::shop::{
    Discount, DtParsingOptions, ExportEntry, ExportEntryLink, ExportOptions, FileFormat,
    ParsingCategoriesAction, Shop, TtParsingOptions,
};
use rt_types::subscription::{self, service::SubscriptionService, Subscription};
use rt_types::watermark::{WatermarkGroup, WatermarkGroupRepository};
use rt_types::Availability;
use rt_types::{DescriptionOptions, Pause, Resume};
use rust_decimal::Decimal;
use serde::{
    de::{self, Error as DeError},
    Deserialize, Serialize,
};
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::future::Future;
use std::io::BufReader;
use std::ops::Deref;
use std::os::unix::fs::PermissionsExt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use time::format_description::well_known::iso8601;
use time::OffsetDateTime;
use typesafe_repository::GetIdentity;
use typesafe_repository::IdentityOf;
use uuid::Uuid;

pub mod landing;

pub type Response = Result<HttpResponse, ControllerError>;
pub type ShopResponse = Result<HttpResponse, ShopControllerError>;
pub type InputData<T> = Either<Form<T>, Json<T>>;

pub const MAX_DESCRIPTION_SIZE: usize = 512 * 1024;

#[derive(Debug, Display, Error)]
pub enum ControllerError {
    NotFound,
    Unauthorized,
    Forbidden,
    #[error(ignore)]
    InternalServerError(anyhow::Error),
    #[error(ignore)]
    CorruptedData(String),
    #[error(ignore)]
    #[display("Invalid field {field}")]
    InvalidInput {
        field: String,
        msg: String,
    },
}

#[derive(Debug, Display, Error)]
#[display("{error}")]
pub struct ShopControllerError {
    #[error(ignore)]
    shop: Option<Shop>,
    #[error(ignore)]
    user: Option<UserCredentials>,
    error: ControllerError,
}

impl ShopControllerError {
    pub fn with_user<'a, E: Into<ControllerError>, U: Into<Option<&'a UserCredentials>>>(
        user: U,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: None,
            user: user.into().cloned(),
            error: error.into(),
        }
    }
    pub fn with_shop<'a, E: Into<ControllerError>, S: Into<Option<&'a Shop>>>(
        shop: S,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: shop.into().cloned(),
            user: None,
            error: error.into(),
        }
    }
    pub fn with<
        'a,
        E: Into<ControllerError>,
        U: Into<Option<&'a UserCredentials>>,
        S: Into<Option<&'a Shop>>,
    >(
        user: U,
        shop: S,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: shop.into().cloned(),
            user: user.into().cloned(),
            error: error.into(),
        }
    }
    pub fn or<'a, U: Into<Option<&'a UserCredentials>>, S: Into<Option<&'a Shop>>>(
        u: U,
        s: S,
    ) -> impl FnOnce(Self) -> Self {
        move |ShopControllerError { shop, user, error }| ShopControllerError {
            shop: shop.or_else(|| s.into().cloned()),
            user: user.or_else(|| u.into().cloned()),
            error,
        }
    }
}

impl From<ControllerError> for ShopControllerError {
    fn from(error: ControllerError) -> ShopControllerError {
        Self {
            error,
            user: None,
            shop: None,
        }
    }
}

impl From<ShopControllerError> for ControllerError {
    fn from(error: ShopControllerError) -> ControllerError {
        error.error
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ControllerError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        err.downcast::<ControllerError>()
            .map(|e| *e)
            .unwrap_or_else(|e| ControllerError::InternalServerError(anyhow!(e)))
    }
}

impl From<anyhow::Error> for ControllerError {
    fn from(err: anyhow::Error) -> Self {
        Self::InternalServerError(err)
    }
}

impl From<actix::MailboxError> for ControllerError {
    fn from(err: actix::MailboxError) -> Self {
        Self::InternalServerError(err.into())
    }
}

impl actix_web::error::ResponseError for ControllerError {
    fn error_response(&self) -> HttpResponse {
        log::warn!("{self:?}\n");
        use ControllerError::*;
        match self {
            NotFound => NotFoundPage { user: None }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::NotFound()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::NotFound().body(())),
            Unauthorized => HttpResponse::SeeOther()
                .insert_header(("Location", "/login"))
                .body(()),
            Forbidden => ForbiddenPage { user: None }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::Forbidden()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::Forbidden().body(())),
            InternalServerError(err) => InternalServerErrorPage {
                error: format!("{err:?}"),
                user: None,
            }
            .render()
            .log_error("Unable to render error template")
            .map(|res| {
                HttpResponse::InternalServerError()
                    .content_type(ContentType::html())
                    .body(res)
            })
            .unwrap_or_else(|| HttpResponse::InternalServerError().body(err.to_string())),
            CorruptedData(err) => HttpResponse::InternalServerError().body(err.to_string()),
            InvalidInput { field, msg } => {
                HttpResponse::BadRequest().body(format!("{field}\n{msg}"))
            }
        }
    }
}

impl actix_web::error::ResponseError for ShopControllerError {
    fn error_response(&self) -> HttpResponse {
        log::warn!("{:?}", self.error);
        match (&self.error, self.shop.clone(), self.user.clone()) {
            (ControllerError::NotFound, shop, user) => {
                let template = match shop {
                    Some(shop) => ShopNotFoundPage { user, shop }.render(),
                    None => NotFoundPage { user }.render(),
                };
                template
                    .log_error("Unable to render error template")
                    .map(|res| {
                        HttpResponse::NotFound()
                            .content_type(ContentType::html())
                            .body(res)
                    })
                    .unwrap_or_else(|| HttpResponse::NotFound().body(()))
            }
            (ControllerError::Unauthorized, ..) => HttpResponse::SeeOther()
                .insert_header(("Location", "/login"))
                .body(()),
            (ControllerError::Forbidden, _, user) => ForbiddenPage { user }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::Forbidden()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::Forbidden().body(())),
            (ControllerError::InternalServerError(err), shop, user) => {
                log::warn!("{}", err.backtrace());
                let res = match shop {
                    Some(shop) => ShopInternalServerErrorPage {
                        error: err.to_string(),
                        user,
                        shop,
                    }
                    .render(),
                    None => InternalServerErrorPage {
                        error: err.to_string(),
                        user,
                    }
                    .render(),
                };
                res.log_error("Unable to render error template")
                    .map(|res| {
                        HttpResponse::InternalServerError()
                            .content_type(ContentType::html())
                            .body(res)
                    })
                    .unwrap_or_else(|| HttpResponse::NotFound().body(()))
            }
            (ControllerError::CorruptedData(err), ..) => {
                HttpResponse::InternalServerError().body(err.to_string())
            }
            (ControllerError::InvalidInput { field, msg }, ..) => {
                HttpResponse::BadRequest().body(format!("{field}\n{msg}"))
            }
        }
    }
}

#[derive(Template)]
#[template(path = "500.html")]
pub struct InternalServerErrorPage {
    error: String,
    user: Option<UserCredentials>,
}

#[derive(Template)]
#[template(path = "shop/500.html")]
pub struct ShopInternalServerErrorPage {
    error: String,
    user: Option<UserCredentials>,
    shop: Shop,
}

#[derive(Template)]
#[template(path = "404.html")]
pub struct NotFoundPage {
    user: Option<UserCredentials>,
}

pub async fn not_found(user: Option<Record<UserCredentials>>) -> Response {
    render_template(NotFoundPage {
        user: user.map(|u| u.t),
    })
}

#[derive(Template)]
#[template(path = "403.html")]
pub struct ForbiddenPage {
    user: Option<UserCredentials>,
}

#[derive(Template)]
#[template(path = "shop/404.html")]
pub struct ShopNotFoundPage {
    user: Option<UserCredentials>,
    shop: Shop,
}

#[derive(Clone)]
pub struct Identity {
    pub login: String,
}

impl FromRequest for Identity {
    type Error = ControllerError;
    type Future = Ready<Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(
            req.extensions()
                .get::<Identity>()
                .cloned()
                .ok_or_else(|| ControllerError::Unauthorized),
        )
    }
}

impl FromRequest for Record<UserCredentials> {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let login = Login(
                Identity::extract(&req)
                    .await
                    .map_err(|_err| ControllerError::Unauthorized)?
                    .login,
            );
            let user_credentials_service = Data::<Addr<UserCredentialsService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract UserCredentialsService from request")
                })
                .map_err(ControllerError::from)?;
            let user = user_credentials_service
                .send(access::service::Get(login.clone()))
                .await
                .map_err(ControllerError::from)?
                .map_err(ControllerError::from)?
                .ok_or(ControllerError::Unauthorized)
                .map_err(ControllerError::from)?;
            let s = user_credentials_service.clone();
            Ok(Self {
                t: user,
                g: RecordGuard {
                    f: Box::new(move |u| {
                        let s = s.clone();
                        Box::pin(async move { Ok(s.send(access::service::Update(u)).await??) })
                    }),
                },
            })
        })
    }
}

impl RecordResponse for RecordGuard<UserCredentials> {
    type Response = Result<(), anyhow::Error>;
}

pub struct ShopAccess {
    pub shop: Shop,
    pub user: UserCredentials,
}

impl FromRequest for ShopAccess {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let user = Record::<UserCredentials>::extract(&req)
                .await
                .map_err(ControllerError::from)?
                .t;
            let path = req.match_info().get("shop_id");
            let path = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!("Unable to extract shop id from request"))
                        .map_err(ShopControllerError::with(&user, None))?
                        .1
                }
            };
            let shop_id: IdentityOf<Shop> = Uuid::parse_str(path)
                .context("Unable to extract shop id from request")
                .map_err(ShopControllerError::with(&user, None))?;
            let shop_service = Data::<Addr<ShopService>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ShopService from request"))
                .map_err(ShopControllerError::with(&user, None))?;
            let shop = shop_service
                .send(shop::service::Get(shop_id))
                .await
                .map_err(ShopControllerError::with(&user, None))?
                .map_err(ShopControllerError::with(&user, None))?
                .ok_or(ControllerError::NotFound)
                .map_err(ShopControllerError::with(&user, None))?;
            let owned_shops = shop_service
                .send(shop::service::ListBy(user.id()))
                .await
                .map_err(ShopControllerError::with(&user, None))?
                .map_err(ShopControllerError::with(&user, None))?
                .into_inner();
            if (user.has_access_to(&shop.id) || owned_shops.contains(&shop))
                && !(req.method() == Method::POST && shop.is_suspended)
            {
                Ok(ShopAccess { shop, user })
            } else {
                Err(ControllerError::Forbidden).map_err(ShopControllerError::with(&user, &shop))
            }
        })
    }
}

pub struct ShopActionAccess {
    pub shop: Shop,
    pub user: UserCredentials,
}

impl FromRequest for ShopActionAccess {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let ShopAccess { shop, user } = ShopAccess::extract(&req).await?;
            if shop.is_suspended {
                Err(ShopControllerError::with(&user, &shop)(
                    ControllerError::Forbidden,
                ))
            } else {
                Ok(Self { shop, user })
            }
        })
    }
}

pub struct ControlPanelAccess {
    pub user: UserCredentials,
}

impl FromRequest for ControlPanelAccess {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let user = Record::<UserCredentials>::extract(&req).await?.t;
            if user.has_access_to_control_panel() {
                Ok(Self { user })
            } else {
                Err(ControllerError::Forbidden)
            }
        })
    }
}

pub struct SessionMiddlewareFactory {}

impl<S, B: 'static> Transform<S, ServiceRequest> for SessionMiddlewareFactory
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Transform = SessionMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(SessionMiddleware {
            service: Arc::new(service),
        }))
    }
}

pub struct SessionMiddleware<S> {
    service: Arc<S>,
}

impl<S, B> Service<ServiceRequest> for SessionMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        Box::pin(async move {
            let session = req.extract::<Session>().await?;
            match session.get::<String>("login") {
                Ok(Some(l)) => {
                    let identity = Identity { login: l };
                    req.extensions_mut().insert(identity);
                }
                Err(err) => {
                    log::error!("Unable to extract login from session:\n{err:?}");
                    req.extensions_mut().insert(None::<Identity>);
                }
                _ => (),
            }
            let res = service.call(req).await?;
            Ok(res)
        })
    }
}

#[derive(Deserialize)]
pub struct LoginDto {
    pub login: String,
    pub password: String,
}

pub fn see_other(location: &str) -> HttpResponse {
    HttpResponse::SeeOther()
        .insert_header(("Location", location))
        .json(())
}

pub fn render_template(t: impl Template) -> Result<HttpResponse, ControllerError> {
    let result = t
        .render()
        .map_err(|x| ControllerError::InternalServerError(anyhow!(x)))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(result))
}

fn parse_vendor<T: AsRef<str> + ToString>(s: T) -> String {
    crate::parse_vendor_from_link(s.as_ref()).unwrap_or(s.to_string())
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexPage {
    pub export_status: Vec<(String, Export, Vec<(FileFormat, FileInfo)>)>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[derive(Serialize)]
struct ExportStatusJson {
    hash: String,
    status: export::ExportStatus,
    ready: bool,
    file_name: String,
}

#[get("/shop/{shop_id}/status")]
async fn export_status_json(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let export_status = export_service
        .send(export::GetAllStatus(shop_id))
        .await
        .context("Unable to send message to ExportService")?;
    let mut res = Vec::with_capacity(export_status.len());
    for (hash, export) in export_status {
        let file_name = export.entry.file_name(None);
        let ready = file_info(format!("export/{shop_id}/{file_name}"))
            .log_error("Unable to get file info")
            .flatten()
            .map(|info| info.last_modified >= export.entry.edited_time)
            .unwrap_or(false);
        res.push(ExportStatusJson {
            hash,
            status: export.status().clone(),
            ready,
            file_name,
        });
    }
    Ok(HttpResponse::Ok().json(res))
}

#[get("/shop/{shop_id}")]
async fn index(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let export_status = export_service
        .send(export::GetAllStatus(shop.id))
        .await
        .context("Unable to send message to ExportService")?;
    let mut export_status = export_status.into_iter().collect::<Vec<_>>();
    export_status.sort_by_key(|(_, e)| e.entry.file_name(None));
    let shop_id = shop.id;
    let export_status = export_status
        .into_iter()
        .map(|(h, e)| {
            let mut f = vec![];
            let formats = [
                FileFormat::Csv,
                FileFormat::Xml,
                FileFormat::Xlsx,
                FileFormat::HoroshopCsv,
                FileFormat::HoroshopCategories,
            ];
            for format in formats {
                let info = file_info(format!(
                    "export/{shop_id}/{}",
                    e.entry.file_name(format.clone())
                ))
                .log_error("Unable to get file info")
                .flatten();
                if let Some(info) = info {
                    f.push((format, info));
                }
            }
            (h, e, f)
        })
        .collect();
    render_template(IndexPage {
        shop,
        export_status,
        user,
    })
}

#[derive(Template)]
#[template(path = "parsing.html")]
pub struct ParsingPage {
    dt_progress: Result<dt::parser::ParsingProgress, anyhow::Error>,
    tt_progress: Result<tt::parser::ParsingProgress, anyhow::Error>,
    user: UserCredentials,
}

#[get("/control_panel/parsing")]
async fn parsing(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    tt_parser: Data<Arc<Addr<tt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
) -> Response {
    let (dt_progress, tt_progress) = tokio::join!(
        dt_parser.send(dt::parser::GetProgress),
        tt_parser.send(tt::parser::GetProgress),
    );
    let (dt_progress, tt_progress) = (
        dt_progress.context("Unable to send message to dt::ParserService")?,
        tt_progress.context("Unable to send message to tt::ParserService")?,
    );
    render_template(ParsingPage {
        dt_progress,
        tt_progress,
        user,
    })
}

#[derive(Template)]
#[template(path = "parsing_product_info.html")]
pub struct ProductInfoPage {
    product: dt::product::Product,
    user: UserCredentials,
}

#[derive(Deserialize)]
pub struct DtParseQuery {
    pub link: String,
}

#[post("/control_panel/dt/parse")]
async fn dt_parse(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtParseQuery>,
) -> Response {
    let form = form.into_inner();
    let product = dt_parser
        .send(dt::parser::Parse(form.link))
        .await?
        .map_err(anyhow::Error::from)?;
    render_template(ProductInfoPage { product, user })
}

#[derive(Template)]
#[template(path = "dt_parsing_page_info.html")]
pub struct DtPageInfoPage {
    links: Vec<String>,
    url: String,
    user: UserCredentials,
}

#[post("/control_panel/dt/parse_page")]
async fn dt_parse_page(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtParseQuery>,
) -> Response {
    let form = form.into_inner();
    let url = "design-tuning.com".to_string();
    let links = dt_parser
        .send(dt::parser::ParsePage(form.link))
        .await?
        .context("Unable to parse page")?;
    render_template(DtPageInfoPage { links, url, user })
}

#[derive(Deserialize)]
pub struct DtProductInfoQuery {
    pub article: String,
}

#[post("/control_panel/dt/product_info")]
async fn dt_product_info(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtProductInfoQuery>,
) -> Response {
    let form = form.into_inner();
    let product = dt_parser
        .send(dt::parser::ProductInfo(form.article))
        .await?
        .map_err(anyhow::Error::from)?;
    if let Some(product) = product {
        render_template(ProductInfoPage { product, user })
    } else {
        Err(anyhow::anyhow!("Товар не найден").into())
    }
}

#[derive(Deserialize)]
struct ExportInfoQuery {
    with_new_link: Option<String>,
}

#[derive(Template)]
#[template(path = "export_info.html")]
struct ExportInfoPage {
    export: ExportViewDto,
    hash: String,
    with_new_link: bool,
    descriptions: Vec<String>,
    shop: ShopDto,
    user: UserCredentials,
    watermarks: Vec<String>,
    groups: Vec<WatermarkGroupDto>,
}

#[derive(Serialize, Clone)]
struct ExportViewDto {
    status: String,
    entry: ExportEntry,
}

impl From<export::Export> for ExportViewDto {
    fn from(e: export::Export) -> Self {
        Self {
            status: e.status().to_string(),
            entry: e.entry().clone(),
        }
    }
}

#[derive(Serialize, Clone)]
struct ShopDto {
    id: String,
    name: String,
    is_suspended: bool,
}

impl From<Shop> for ShopDto {
    fn from(s: Shop) -> Self {
        Self {
            id: s.id.to_string(),
            name: s.name,
            is_suspended: s.is_suspended,
        }
    }
}

#[derive(Serialize, Clone)]
struct WatermarkGroupDto {
    id: String,
    name: String,
}

impl From<WatermarkGroup> for WatermarkGroupDto {
    fn from(w: WatermarkGroup) -> Self {
        Self {
            id: w.id().1.to_string(),
            name: w.name,
        }
    }
}

impl WatermarkGroupDto {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[get("/shop/{shop_id}/export_info/{hash}")]
async fn export_info(
    path: Path<(IdentityOf<Shop>, String)>,
    q: Query<ExportInfoQuery>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let export = export_service
        .send(export::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to ExportService")?;
    let export = match export {
        Some(export) => Some(ExportViewDto::from(export)),
        None => None,
    };
    let groups = watermark_group_repository.list_by(&shop_id).await?;
    let path = format!("./description/{shop_id}");
    let res = match std::fs::read_dir(&path) {
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
    };
    let descriptions = res
        .context("Unable to read descriptions")?
        .map(|d| {
            d?.file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)?;
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
    match export {
        Some(export) => render_template(ExportInfoPage {
            export: export.into(),
            hash,
            with_new_link: q.with_new_link.is_some(),
            descriptions,
            shop: shop.into(),
            user: user.into(),
            watermarks,
            groups: groups.into_iter().map(Into::into).collect(),
        }),
        None => Ok(see_other(&format!("/shop/{shop_id}"))),
    }
}

#[post("/shop/{shop_id}/export_info/{hash}/remove")]
async fn remove_export(
    hash: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = hash.into_inner();
    export_service
        .send(export::Remove(hash.clone()))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/export_info")]
async fn add_export(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let shop_id = shop.id;
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let permission = AddExportPermission::acquire(&user, &shop, &subscription)
        .ok_or(anyhow::anyhow!("This shop cannot contain any more exports"))?;
    export_service
        .send(export::Add(permission, ExportEntry::default()))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryDto {
    pub file_name: Option<String>,
    #[serde(deserialize_with = "deserialize_parse_form::<_, u64>")]
    pub update_rate: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryLinkDto {
    pub vendor_name: Option<String>,
    pub link: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_prefix: bool,
    pub title_prefix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_suffix: bool,
    pub title_suffix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_prefix_ua: bool,
    pub title_prefix_ua: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_suffix_ua: bool,
    pub title_suffix_ua: Option<String>,
    pub title_replacements: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub format_years: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub only_available: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub discount: bool,
    #[serde(deserialize_with = "deserialize_parse_form::<_, u64>")]
    pub discount_duration: Option<u64>,
    #[serde(deserialize_with = "deserialize_parse_form::<_, usize>")]
    pub discount_percent: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_vendor: bool,
    pub description_action: Option<String>,
    pub description_path: Option<String>,
    pub description_action_ua: Option<String>,
    pub description_path_ua: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub delivery_time: bool,
    #[serde(deserialize_with = "deserialize_parse_form::<_, usize>")]
    pub delivery_time_duration: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub adjust_price: bool,
    #[serde(deserialize_with = "deserialize_decimal_form")]
    pub adjust_price_by: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub categories: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub convert_to_uah: bool,
    pub watermark: Option<WatermarkType>,
    pub watermark_name: Option<String>,
    #[serde(flatten)]
    pub watermark_options: Option<WatermarkOptionsDto>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub set_availability_enabled: bool,
    pub set_availability: Option<Availability>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkType {
    Watermark,
    Group,
}

impl TryInto<ExportEntryLink> for ExportEntryLinkDto {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ExportEntryLink, Self::Error> {
        Ok(ExportEntryLink {
            vendor_name: self.vendor_name.as_ref().filter(|v| !v.is_empty()).cloned(),
            link: self
                .link
                .clone()
                .ok_or_else(|| anyhow!("Export entry must have a link"))?,
            options: Some(self.into()),
        })
    }
}

impl Into<ExportOptions> for ExportEntryLinkDto {
    fn into(self) -> ExportOptions {
        let parse_replacements = |raw: Option<String>| -> Option<Vec<(String, String)>> {
            let raw = raw?;
            let pairs: Vec<_> = raw
                .lines()
                .filter_map(|line| {
                    let mut parts = line.splitn(2, '=');
                    let from = parts.next().map(str::trim).unwrap_or_default();
                    let to = parts.next().map(str::trim).unwrap_or_default();
                    if !from.is_empty() && !to.is_empty() {
                        Some((from.to_string(), to.to_string()))
                    } else {
                        None
                    }
                })
                .collect();
            if pairs.is_empty() {
                None
            } else {
                Some(pairs)
            }
        };
        ExportOptions {
            title_prefix: self
                .title_prefix
                .filter(|_| self.add_title_prefix)
                .filter(|s| !s.trim().is_empty()),
            title_prefix_ua: self
                .title_prefix_ua
                .filter(|_| self.add_title_prefix_ua)
                .filter(|s| !s.trim().is_empty()),
            title_suffix: self
                .title_suffix
                .filter(|_| self.add_title_suffix)
                .filter(|s| !s.trim().is_empty()),
            title_suffix_ua: self
                .title_suffix_ua
                .filter(|_| self.add_title_suffix_ua)
                .filter(|s| !s.trim().is_empty()),
            title_replacements: parse_replacements(self.title_replacements),
            only_available: self.only_available,
            discount: self
                .discount_duration
                .zip(self.discount_percent)
                .filter(|_| self.discount)
                .map(|(duration, percent)| Discount {
                    duration: Duration::from_secs(duration * 60 * 60),
                    percent,
                }),
            format_years: self.format_years,
            add_vendor: self.add_vendor,
            description: self
                .description_path
                .zip(self.description_action)
                .and_then(|(path, action)| DescriptionOptions::try_from(action, path)),
            description_ua: self
                .description_path_ua
                .zip(self.description_action_ua)
                .and_then(|(path, action)| DescriptionOptions::try_from(action, path)),
            adjust_price: self.adjust_price_by.filter(|_| self.adjust_price),
            delivery_time: self.delivery_time_duration.filter(|_| self.delivery_time),
            categories: self.categories,
            convert_to_uah: self.convert_to_uah,
            custom_options: None,
            watermarks: self
                .watermark_name
                .filter(|_| self.watermark.is_some())
                .zip(Some(
                    self.watermark_options
                        .filter(|_| matches!(self.watermark, Some(WatermarkType::Watermark)))
                        .map(Into::into),
                )),
            set_availability: self
                .set_availability
                .filter(|_| self.set_availability_enabled),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryDtDto {
    #[serde(flatten)]
    options: ExportEntryLinkDto,
}

impl Into<DtParsingOptions> for ExportEntryDtDto {
    fn into(self) -> DtParsingOptions {
        DtParsingOptions {
            options: self.options.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParsingCategoriesActionDto {
    BeforeTitle,
    AfterTitle,
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryTtDto {
    #[serde(flatten)]
    options: ExportEntryLinkDto,
    append_categories: Option<ParsingCategoriesActionDto>,
    categories_separator: Option<String>,
}

impl Into<TtParsingOptions> for ExportEntryTtDto {
    fn into(self) -> TtParsingOptions {
        let separator = self
            .categories_separator
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .unwrap_or_else(|| "+".to_string());
        let append_categories = match self.append_categories {
            Some(ParsingCategoriesActionDto::BeforeTitle) => {
                Some(ParsingCategoriesAction::BeforeTitle { separator })
            }
            Some(ParsingCategoriesActionDto::AfterTitle) => {
                Some(ParsingCategoriesAction::AfterTitle { separator })
            }
            None => None,
        };
        TtParsingOptions {
            options: self.options.into(),
            append_categories,
        }
    }
}

pub fn deserialize_bool_form<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;

    match s {
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        _ => Err(de::Error::unknown_variant(
            s,
            &["on", "off", "true", "false"],
        )),
    }
}

pub fn deserialize_option_bool_form<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;

    match s {
        Some("on" | "true") => Ok(Some(true)),
        Some("off" | "false") => Ok(Some(false)),
        Some(s) => Err(de::Error::unknown_variant(
            s,
            &["on", "off", "true", "false"],
        )),
        None => Ok(None),
    }
}

pub fn deserialize_decimal_form<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;
    s.filter(|s| !s.trim().is_empty())
        .map(Decimal::from_str_exact)
        .transpose()
        .map_err(D::Error::custom)
}

fn deserialize_parse_form<'de, D, RT>(deserializer: D) -> Result<Option<RT>, D::Error>
where
    D: de::Deserializer<'de>,
    RT: FromStr,
    <RT as FromStr>::Err: std::fmt::Display,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;
    s.filter(|s| !s.trim().is_empty())
        .map(FromStr::from_str)
        .transpose()
        .map_err(D::Error::custom)
}

#[post("/shop/{shop_id}/export_info/{export_hash}")]
async fn update_export(
    form: Form<ExportEntryDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let dto = form.into_inner();

    let hash = export_entry
        .map(|export_entry| {
            export_entry.file_name = dto.file_name.clone();
            if let Some(rate) = dto.update_rate {
                export_entry.update_rate = Duration::from_secs(rate * 60 * 60);
            }
        })
        .await?;

    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/copy_export/{export_hash}")]
async fn copy_export(
    path: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let mut export_entry = export_service
        .send(export::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to ExportService")?
        .ok_or(anyhow::anyhow!("Export entry does not exist"))?
        .entry;
    export_entry.created_time = OffsetDateTime::now_utc();
    export_entry.edited_time = OffsetDateTime::now_utc();
    export_entry.file_name = Some(format!("{} (Копия)", export_entry.file_name(None)));
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let permission = AddExportPermission::acquire(&user, &shop, &subscription)
        .ok_or(anyhow::anyhow!("This shop cannot contain any more exports"))?;
    export_service
        .send(export::Add(permission, export_entry))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/{link_hash}")]
async fn update_export_link(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _, link_hash) = path.into_inner();
    let new_link = form.into_inner();
    let (mut export_entry, g) = export_entry.into_inner();
    let link = export_entry
        .entry
        .get_link_by_hash_mut(link_hash)
        .ok_or(anyhow::anyhow!("Export entry link does not exist"))?;
    let description = new_link
        .description_path
        .clone()
        .zip(new_link.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = new_link
        .description_path_ua
        .clone()
        .zip(new_link.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    *link = new_link.try_into()?;
    let hash = g.save(export_entry).await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/new")]
async fn add_export_link(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    export_entry: Record<Export>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let new_link = form.into_inner().try_into()?;

    let hash = export_entry
        .map(|export_entry| {
            if let Some(links) = &mut export_entry.entry.links {
                links.push(new_link);
            } else {
                export_entry.entry.links = Some(vec![new_link]);
            }
        })
        .await?;

    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_dt")]
async fn add_export_dt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.dt_parsing.is_none() {
                export_entry.entry.dt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_maxton")]
async fn add_export_maxton(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.maxton_parsing.is_none() {
                export_entry.entry.maxton_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_pl")]
async fn add_export_pl(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.pl_parsing.is_none() {
                export_entry.entry.pl_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_skm")]
async fn add_export_skm(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.skm_parsing.is_none() {
                export_entry.entry.skm_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_dt_tt")]
async fn add_export_dt_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.dt_tt_parsing.is_none() {
                export_entry.entry.dt_tt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_jgd")]
async fn add_export_jgd(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.jgd_parsing.is_none() {
                export_entry.entry.jgd_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_tt")]
async fn add_export_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.tt_parsing.is_none() {
                export_entry.entry.tt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_davi")]
async fn add_export_davi(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.davi_parsing.is_none() {
                export_entry.entry.davi_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_dt")]
async fn remove_export_dt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.dt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_maxton")]
async fn remove_export_maxton(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.maxton_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_pl")]
async fn remove_export_pl(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.pl_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_skm")]
async fn remove_export_skm(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.skm_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_dt_tt")]
async fn remove_export_dt_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.dt_tt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_jgd")]
async fn remove_export_jgd(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.jgd_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_tt")]
async fn remove_export_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.tt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_davi")]
async fn remove_export_davi(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.davi_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/{link_hash}/remove")]
async fn remove_export_link(
    path: Path<(IdentityOf<Shop>, String, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let (shop_id, hash, link_hash) = path.into_inner();
    let hash = export_entry
        .filter_map(move |entry| {
            let link = entry.remove_link_by_hash(link_hash);
            link.is_some()
        })
        .await
        .transpose()?
        .unwrap_or(hash);
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/dt")]
async fn update_export_dt(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.dt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/maxton")]
async fn update_export_maxton(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.maxton_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/pl")]
async fn update_export_pl(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.pl_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/skm")]
async fn update_export_skm(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.skm_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/dt_tt")]
async fn update_export_dt_tt(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.dt_tt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/jgd")]
async fn update_export_jgd(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.jgd_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/tt")]
async fn update_export_tt(
    form: Form<ExportEntryTtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.tt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/davi")]
async fn update_export_davi(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .description_path
        .clone()
        .zip(opts.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .description_path_ua
        .clone()
        .zip(opts.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.davi_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/stop_dt")]
async fn stop_dt(
    _identity: Identity,
    addr: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    log::info!("Pause");
    addr.send(Pause)
        .await
        .context("Unable to send message to ParserService")?;
    Ok(see_other("/shops"))
}

#[post("/resume_dt")]
async fn resume_dt(
    _identity: Identity,
    addr: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    log::info!("Resume");
    addr.send(Resume)
        .await
        .context("Unable to send message to ParserService")?;
    Ok(see_other("/shops"))
}

#[post("/shop/{shop_id}/start_export/{hash}")]
async fn start_export(
    path: Path<(IdentityOf<Shop>, String)>,
    addr: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    addr.send(export::Start(hash))
        .await
        .context("Unable to send message to ExportService")?;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/start_export_all")]
async fn start_export_all(
    path: Path<IdentityOf<Shop>>,
    addr: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    addr.send(export::StartAll)
        .await
        .context("Unable to send message to ExportService")?;
    let shop_id = path.into_inner();
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[derive(MultipartForm, Debug)]
pub struct DescriptionQuery {
    file: TempFile,
}

#[post("/shop/{shop_id}/upload_description")]
async fn upload_description_file(
    q: MultipartForm<DescriptionQuery>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let name = match q.file.file_name {
        Some(name) => name,
        None => return Ok(see_other("/description?err=empty_filename")),
    };
    let description_count = std::fs::read_dir(format!("./description/{}", shop.id))
        .context("Unable to read description dir")?
        .count();
    if shop
        .limits
        .as_ref()
        .and_then(|l| l.descriptions)
        .is_some_and(|d| description_count >= d.get() as usize)
    {
        return Err(anyhow::anyhow!("Description limit exceeded").into());
    }
    let shop_id = shop.id;
    std::fs::copy(
        q.file.file.path(),
        format!("./description/{shop_id}/{name}"),
    )
    .map_err(anyhow::Error::new)?;
    Ok(see_other(&format!("/shop/{shop_id}/description")))
}

#[post("/shop/{shop_id}/remove_description{path:/.+}")]
async fn remove_description_file(
    path: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, path) = path.into_inner();
    let status = export_service
        .send(export::GetAllStatus(shop_id))
        .await
        .context("Unable to send message to ExportService")?;
    let file_used = status.values().any(|e| {
        e.entry
            .tt_parsing
            .as_ref()
            .and_then(|p| p.options.description.as_ref())
            .is_some_and(|d| d.value() == &path)
            || e.entry
                .dt_parsing
                .as_ref()
                .and_then(|p| p.options.description.as_ref())
                .is_some_and(|d| d.value() == &path)
            || e.entry.links.as_ref().is_some_and(|l| {
                l.iter().any(|l| {
                    l.options
                        .as_ref()
                        .and_then(|o| o.description.as_ref())
                        .is_some_and(|d| d.value() == &path)
                })
            })
    });
    if file_used {
        return Ok(see_other("/shop/{shop_id}/description?err=file_used"));
    }
    let path = match path.ends_with("/shops") {
        true => &path[..path.len() - 1],
        false => &path,
    };
    let path = format!("./description/{shop_id}{}", path);
    std::fs::remove_file(path).context("Unable to remove file")?;
    Ok(see_other(&format!("/shop/{shop_id}/description")))
}

#[derive(Template)]
#[template(path = "categories.html")]
pub struct CategoriesPage {
    pub categories: Vec<Category>,
    pub total_categories: usize,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/categories")]
async fn categories_page(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let categories = category_repo.select(&TopLevel(By(shop.id))).await?;
    let total_categories = category_repo.count_by(&By(shop.id)).await?;
    let categories = categories
        .into_iter()
        .filter(|c| c.parent_id.is_none())
        .collect();
    render_template(CategoriesPage {
        categories,
        total_categories,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "category.html")]
pub struct CategoryPage {
    pub category: Category,
    pub subcategories: Vec<Category>,
    pub categories: Vec<Category>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/categories/{id}")]
async fn category_page(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (_, category_id) = path.into_inner();
    let category = category_repo
        .get_one(&category_id)
        .await?
        .ok_or(ControllerError::NotFound)?;
    let subcategories = category_repo.select(&ByParentId(category_id)).await?;
    let categories = category_repo.select(&By(shop.id)).await?;
    render_template(CategoryPage {
        category,
        subcategories,
        categories,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "import_categories.html")]
pub struct ImportCategoriesPage {
    shop: Shop,
    user: UserCredentials,
}

#[get("/shop/{shop_id}/categories/import")]
async fn import_categories_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(ImportCategoriesPage { shop, user })
}

#[derive(MultipartForm, Debug)]
pub struct ImportCategoriesQuery {
    file: TempFile,
}

#[post("/shop/{shop_id}/categories/import")]
async fn import_categories(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: MultipartForm<ImportCategoriesQuery>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let file = q.file.file.as_file();
    let categories = parse_categories(BufReader::new(file), shop.id)?;
    for c in categories {
        category_repo.save(c).await?;
    }
    Ok(see_other(&format!("/shop/{}/categories", shop.id)))
}

#[post("/shop/{shop_id}/categories/clear")]
async fn clear_categories(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let categories = category_repo.select(&By(shop.id)).await?;
    for c in categories {
        category_repo.remove(&c.id).await?;
    }
    Ok(see_other(&format!("/shop/{}/categories", shop.id)))
}

#[derive(Deserialize, Debug)]
pub struct CategoryDto {
    pub name: String,
    pub regex: String,
    #[serde(default)]
    pub parent_id: Option<String>,
}

#[post("/shop/{shop_id}/categories/update/{id}")]
async fn update_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: Form<CategoryDto>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let (shop_id, id) = path.into_inner();
    let regex = Regex::new(&q.regex).context("Unable to compile regex")?;
    let mut category = category_repo.get_one(&id).await?;
    if let Some(category) = &mut category {
        category.regex = Some(regex);
        category.name = q.name;
        category.parent_id = q
            .parent_id
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?;
        category_repo.save(category.clone()).await?;
    }
    if let Some(id) = category.and_then(|c| c.parent_id) {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    }
}

#[post("/shop/{shop_id}/categories/add")]
async fn add_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: Form<CategoryDto>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let CategoryDto {
        name,
        regex,
        parent_id,
    } = q.into_inner();
    let id = Uuid::new_v4();
    let regex = Regex::new(&regex).context("Unable to compile regex")?;
    let category = Category {
        id: id,
        name,
        parent_id: parent_id
            .clone()
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?,
        regex: Some(regex),
        shop_id,
    };
    category_repo.save(category).await?;
    if let Some(id) = parent_id {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories")))
    }
}

#[post("/shop/{shop_id}/category/delete/{id}")]
async fn delete_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    req: HttpRequest,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, id) = path.into_inner();
    category_repo.remove(&id).await?;
    if let Some(re) = req.headers().get(actix_web::http::header::REFERER) {
        Ok(see_other(
            re.to_str().context("Unable to parse referer header")?,
        ))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories")))
    }
}

pub struct FileInfo {
    pub name: String,
    pub last_modified: OffsetDateTime,
    pub size: u64,
}

impl FileInfo {
    pub fn format_size(&self) -> String {
        match self.size {
            x if x >= (1024 * 1024 * 1024) => {
                format!("{:.2} GB", (x as f32 / 1024. / 1024. / 1024.))
            }
            x if x >= (1024 * 1024) => format!("{:.2} MB", (x as f32 / 1024. / 1024.)),
            x if x >= 1024 => format!("{:.2} KB", (x as f32 / 1024.)),
            x => format!("{} Bytes", x),
        }
    }
    pub fn format_modified_ago(&self) -> String {
        let now = OffsetDateTime::now_utc();
        let duration = now - self.last_modified;
        crate::format_duration_short(&duration.unsigned_abs())
    }
    pub fn format_modified_at(&self) -> Option<String> {
        let format_description = iso8601::Iso8601::<
            {
                iso8601::Config::DEFAULT
                    .set_time_precision(iso8601::TimePrecision::Minute {
                        decimal_digits: None,
                    })
                    .set_formatted_components(iso8601::FormattedComponents::DateTime)
                    .encode()
            },
        >;
        self.last_modified
            .format(&format_description)
            .log_error("Unable to format time")
    }
}

#[derive(Deserialize)]
pub struct DescriptionsPageQuery {
    err: Option<DescriptionQueryError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptionQueryError {
    EmptyFilename,
    FileUsed,
    #[serde(untagged)]
    Other(String),
}

#[derive(Template)]
#[template(path = "descriptions.html")]
struct DescriptionsTemplate<'a> {
    pub directory: &'a str,
    pub files: Vec<FileInfo>,
    pub err: Option<String>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/description{path:(/.*)?}")]
pub async fn descriptions_page(
    path: Path<(IdentityOf<Shop>, Option<String>)>,
    req: HttpRequest,
) -> ShopResponse {
    let (shop_id, path) = path.into_inner();
    let q: Option<Query<DescriptionsPageQuery>> = match Query::from_query(req.query_string()) {
        Ok(q) => Some(q),
        Err(err) => {
            log::error!("Unable to parse query: {err}");
            None
        }
    };
    let path = format!(
        "./description/{shop_id}/{}",
        path.map(|p| p.replace(&format!("/{shop_id}/description/"), ""))
            .unwrap_or_default(),
    );
    let meta = std::fs::metadata(&path)
        .context("Unable to read file metadata")
        .map_err(ControllerError::InternalServerError)?;
    if meta.is_file() {
        return Ok(NamedFile::open(path)
            .context("Unable to open file")
            .map_err(ControllerError::InternalServerError)?
            .into_response(&req));
    }
    let ShopAccess { shop, user } = ShopAccess::extract(&req).await?;
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path)
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm)
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let mut files: Vec<_> = res
        .map_err(ShopControllerError::with(&user, &shop))?
        .map(|e| {
            let e = e?;
            let meta = e.metadata()?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(FileInfo {
                name: file_name,
                last_modified: OffsetDateTime::from(meta.modified()?),
                size: meta.len(),
            })
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;
    let err = match q.and_then(|q| q.into_inner().err) {
        Some(DescriptionQueryError::EmptyFilename) => Some("Имя файла не указано".to_string()),
        Some(DescriptionQueryError::FileUsed) => {
            Some("Этот файл используется, и не может быть удален".to_string())
        }
        Some(DescriptionQueryError::Other(s)) => Some(s),
        None => None,
    };
    files.sort_by(|a, b| a.name.cmp(&b.name));
    render_template(DescriptionsTemplate {
        directory: &path,
        files,
        err,
        shop: shop.clone(),
        user: user.clone(),
    })
    .map_err(ShopControllerError::with(&user, &shop))
}

#[derive(Template)]
#[template(path = "export.html")]
struct ExportTemplate<'a> {
    pub directory: &'a str,
    pub files: Vec<FileInfo>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/export")]
async fn export_page(
    path: Path<IdentityOf<Shop>>,
    req: HttpRequest,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let shop_id = path.into_inner();
    let path = req
        .path()
        .replace(&format!("/shop/{shop_id}"), "")
        .replacen("/shops", "", 1);
    let path = format!("{path}/{shop_id}");
    let res = match std::fs::read_dir(&path) {
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
    };
    let mut files: Vec<_> = res
        .context("Unable to read directory")?
        .map(|e| {
            let e = e?;
            let meta = e.metadata()?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(FileInfo {
                name: file_name,
                last_modified: OffsetDateTime::from(meta.modified()?),
                size: meta.len(),
            })
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")?;
    files.sort_by(|a, b| a.name.cmp(&b.name));
    render_template(ExportTemplate {
        directory: &path,
        files,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "control_panel/index.html")]
pub struct ControlPanelPage {
    user: UserCredentials,
    status: Vec<(String, ServiceStatus)>,
}

#[derive(Display)]
pub enum ServiceStatus {
    #[display("Работает")]
    Up,
    #[display("Не работает")]
    Down,
}

impl From<bool> for ServiceStatus {
    fn from(v: bool) -> Self {
        match v {
            true => Self::Up,
            false => Self::Down,
        }
    }
}

#[get("/control_panel")]
async fn control_panel(
    ControlPanelAccess { user }: ControlPanelAccess,
    shop_service: Data<Addr<ShopService>>,
    export_service: Data<Arc<Addr<ExportService>>>,
    tt_service: Data<Arc<Addr<tt::parser::ParserService>>>,
    dt_service: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    render_template(ControlPanelPage {
        status: vec![
            ("shop_service".to_string(), shop_service.connected().into()),
            (
                "export_service".to_string(),
                export_service.connected().into(),
            ),
            ("dt_service".to_string(), dt_service.connected().into()),
            ("tt_service".to_string(), tt_service.connected().into()),
        ],
        user,
    })
}

#[derive(Template)]
#[template(path = "control_panel/users.html")]
pub struct ControlPanelUsersPage {
    user: UserCredentials,
    users: Vec<UserCredentials>,
    tokens: BTreeSet<RegistrationToken>,
}

#[get("/control_panel/users")]
async fn control_panel_users(
    ControlPanelAccess { user }: ControlPanelAccess,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let users = user_credentials_service
        .send(access::service::List)
        .await??;
    let tokens = user_credentials_service
        .send(access::service::ListTokens)
        .await?;
    render_template(ControlPanelUsersPage {
        user,
        users,
        tokens,
    })
}

#[derive(Template)]
#[template(path = "control_panel/edit_user.html")]
pub struct ControlPanelEditUserPage {
    user: UserCredentials,
    edited_user: UserCredentials,
    subscription: Option<Subscription>,
    subscriptions: Vec<Subscription>,
}

#[get("/control_panel/users/{user_id}/edit")]
async fn control_panel_edit_user_page(
    ControlPanelAccess { user }: ControlPanelAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
    user_id: Path<IdentityOf<UserCredentials>>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let edited_user = user_credentials_service
        .send(access::service::Get(user_id.into_inner()))
        .await??
        .ok_or(ControllerError::NotFound)?;
    let subscriptions = subscription_service
        .send(subscription::service::List)
        .await??;
    let subscription = match edited_user.subscription {
        Some((id, ver)) => Some(
            subscriptions
                .iter()
                .find(|s| id == s.id && ver == s.version)
                .cloned()
                .ok_or(ControllerError::NotFound)?,
        ),
        None => None,
    };
    render_template(ControlPanelEditUserPage {
        user,
        edited_user,
        subscription,
        subscriptions,
    })
}

#[derive(Deserialize)]
pub struct EditUserCredentialsDto {
    #[serde(deserialize_with = "empty_string_as_none_parse")]
    subscription: Option<IdentityOf<Subscription>>,
}

#[post("/control_panel/users/{user_id}/edit")]
async fn control_panel_edit_user(
    ControlPanelAccess { .. }: ControlPanelAccess,
    dto: Form<EditUserCredentialsDto>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
    subscription_service: Data<Addr<SubscriptionService>>,
    user_id: Path<IdentityOf<UserCredentials>>,
) -> Response {
    let user_id = user_id.into_inner();
    let mut user = user_credentials_service
        .send(access::service::Get(user_id.clone()))
        .await??
        .ok_or(ControllerError::NotFound)?;
    let dto = dto.into_inner();
    let subscription = match dto.subscription {
        Some(id) => Some(
            subscription_service
                .send(subscription::service::Get(id))
                .await??
                .ok_or(ControllerError::NotFound)?,
        ),
        None => None,
    };
    user.subscription = subscription.map(|s| (s.id, s.version));
    user_credentials_service
        .send(access::service::Update(user))
        .await??;
    Ok(see_other(&format!("/control_panel/users/{user_id}/edit")))
}

#[derive(Template)]
#[template(path = "control_panel/shops.html")]
pub struct ControlPanelShopsPage {
    user: UserCredentials,
    shops: Vec<Shop>,
}

#[get("/control_panel/shops")]
async fn control_panel_shops(
    ControlPanelAccess { user }: ControlPanelAccess,
    shop_service: Data<Addr<ShopService>>,
) -> Response {
    let shops = shop_service.send(shop::service::List).await??;
    render_template(ControlPanelShopsPage { user, shops })
}

#[derive(Template)]
#[template(path = "control_panel/settings.html")]
pub struct ControlPanelSettingsPage {
    user: UserCredentials,
}

#[get("/control_panel/settings")]
async fn control_panel_settings(ControlPanelAccess { user }: ControlPanelAccess) -> Response {
    render_template(ControlPanelSettingsPage { user })
}

pub fn into_user_option<'a, T>(t: T) -> Option<&'a UserCredentials>
where
    T: Into<Option<&'a UserCredentials>>,
{
    t.into()
}

pub struct Record<T>
where
    T: Clone + 'static,
    RecordGuard<T>: RecordResponse,
{
    pub g: RecordGuard<T>,
    pub t: T,
}

impl<T: Clone + 'static> Record<T>
where
    RecordGuard<T>: RecordResponse,
{
    #[must_use]
    pub fn into_inner(self) -> (T, RecordGuard<T>) {
        (self.t, self.g)
    }

    pub async fn map(
        mut self,
        f: impl FnOnce(&mut T),
    ) -> <RecordGuard<T> as RecordResponse>::Response {
        f(&mut self.t);
        self.g.save(self.t).await
    }

    pub async fn try_map<E>(
        mut self,
        f: impl FnOnce(&mut T) -> Result<(), E>,
    ) -> Result<<RecordGuard<T> as RecordResponse>::Response, E> {
        f(&mut self.t)?;
        Ok(self.g.save(self.t).await)
    }

    pub async fn filter_map(
        mut self,
        f: impl FnOnce(&mut T) -> bool,
    ) -> Option<<RecordGuard<T> as RecordResponse>::Response> {
        let res = f(&mut self.t);
        if res {
            Some(self.g.save(self.t).await)
        } else {
            None
        }
    }
}

#[must_use]
pub struct RecordGuard<T>
where
    Self: RecordResponse,
{
    pub f: Box<dyn Fn(T) -> Pin<Box<dyn Future<Output = <Self as RecordResponse>::Response>>>>,
}

pub trait RecordResponse {
    type Response;
}

impl RecordResponse for RecordGuard<ExportEntry> {
    type Response = Result<String, anyhow::Error>;
}

impl RecordResponse for RecordGuard<Export> {
    type Response = Result<String, anyhow::Error>;
}

impl<T: Clone + 'static> RecordGuard<T>
where
    Self: RecordResponse,
{
    #[must_use]
    pub async fn save(self, t: T) -> <Self as RecordResponse>::Response {
        let f = self.f;
        f(t).await
    }
}

impl FromRequest for Record<Export> {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let service = Data::<Arc<Addr<ExportService>>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ExportService from request"))?;
            let subscription_service = Data::<Addr<SubscriptionService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract SubscriptionService from request")
                })?;
            let path = req.match_info().get("export_hash");
            let hash = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!(
                            "Unable to extract export entry hash from request"
                        ))?
                        .1
                }
            };
            let hash = hash.to_string();
            let export = service
                .send(export::GetStatus(hash.clone()))
                .await?
                .ok_or(ControllerError::NotFound)?;
            let shop_id = export.shop;
            let s = service.clone();
            let ShopAccess { user, .. } = ShopAccess::extract(&req).await?;
            Ok(Self {
                t: export,
                g: RecordGuard {
                    f: Box::new(move |mut e| {
                        let s = s.clone();
                        let hash = hash.clone();
                        let user = user.clone();
                        let subscription_service = subscription_service.clone();
                        Box::pin(async move {
                            let subscription = subscription_service
                                .send(subscription::service::GetBy(user))
                                .await??;
                            e.entry.edited_time = OffsetDateTime::now_utc();
                            let permission =
                                UpdateExportEntryPermission::acquire(e.entry, hash, &subscription)
                                    .ok_or(anyhow::anyhow!("Permission denied"))?;
                            Ok(s.send(export::Update(shop_id, permission)).await??)
                        })
                    }),
                },
            })
        })
    }
}

impl FromRequest for Record<ExportEntry> {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let service = Data::<Arc<Addr<ExportService>>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ExportService from request"))?;
            let subscription_service = Data::<Addr<SubscriptionService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract SubscriptionService from request")
                })?;
            let path = req.match_info().get("export_hash");
            let hash = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!(
                            "Unable to extract export entry hash from request"
                        ))?
                        .1
                }
            };
            let hash = hash.to_string();
            let export = service
                .send(export::GetStatus(hash.clone()))
                .await?
                .ok_or(anyhow::anyhow!("Unable to find export entry by hash"))?;
            let shop_id = export.shop;
            let s = service.clone();
            let e = export.entry;
            let ShopAccess { user, .. } = ShopAccess::extract(&req).await?;
            Ok(Self {
                t: e,
                g: RecordGuard {
                    f: Box::new(move |mut e| {
                        let s = s.clone();
                        let hash = hash.clone();
                        let user = user.clone();
                        let subscription_service = subscription_service.clone();
                        Box::pin(async move {
                            let subscription = subscription_service
                                .send(subscription::service::GetBy(user))
                                .await??;
                            e.edited_time = OffsetDateTime::now_utc();
                            let permission =
                                UpdateExportEntryPermission::acquire(e, hash, &subscription)
                                    .ok_or(anyhow::anyhow!("Permission denied"))?;
                            Ok(s.send(export::Update(shop_id, permission)).await??)
                        })
                    }),
                },
            })
        })
    }
}

pub fn file_info<P: AsRef<std::path::Path>>(p: P) -> Result<Option<FileInfo>, anyhow::Error> {
    let name = p
        .as_ref()
        .file_name()
        .ok_or_else(|| anyhow!("File without filename"))?
        .to_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("Unable to convert file name"))
        .map_err(std::io::Error::other)?;
    let file = match std::fs::File::open(p) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let meta = file.metadata()?;
    Ok(Some(FileInfo {
        name,
        last_modified: OffsetDateTime::from(meta.modified()?),
        size: meta.len(),
    }))
}

pub async fn check_description(shop_id: IdentityOf<Shop>, name: &str) -> Result<(), anyhow::Error> {
    match tokio::fs::read_to_string(format!("./description/{shop_id}/{name}",)).await {
        Ok(d) if d.len() > MAX_DESCRIPTION_SIZE => Err(anyhow::anyhow!(
            "Description size must be < {}kB",
            MAX_DESCRIPTION_SIZE / 1024
        )
        .into()),
        Ok(_) => Ok(()),
        Err(err) => Err(anyhow::anyhow!("Unable to open description file: {err}").into()),
    }
}
