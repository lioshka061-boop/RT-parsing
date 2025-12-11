use crate::external_import::{Item, Offer, Vendored};
use crate::SELF_ADDR;
use crate::{dt, tt};
use crate::{parse_vendor_from_link, uploader};
use actix::prelude::*;
use actix_broker::BrokerSubscribe;
use anyhow::anyhow;
use anyhow::Context as AnyhowContext;
use currency_service::{CurrencyService, ListRates};
use derive_more::Display;
use serde::Serialize;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use log_error::LogError;
use once_cell::sync::Lazy;
use reqwest::Client;
use rt_types::access::UserCredentials;
use rt_types::category::{self, By};
use rt_types::product::{Product, UaTranslation};
use rt_types::shop::service::ShopService;
use rt_types::shop::ConfigurationChanged;
use rt_types::shop::{
    self, ExportEntry, ExportEntryLink, ExportOptions, FileFormat, ParsingCategoriesAction, Shop,
};
use rt_types::subscription::service::UserSubscription;
use rt_types::watermark::service::WatermarkUpdated;
use rt_types::Availability;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, Notify, RwLock};
use typesafe_repository::IdentityOf;
use xxhash_rust::xxh64::xxh64;

pub const MAX_RETRY_COUNT: usize = 30;

pub struct ExportService {
    client: Client,
    entries: Vec<(IdentityOf<Shop>, ExportEntry)>,
    tt_repo: Arc<dyn tt::product::ProductRepository>,
    trans_repo: Arc<dyn tt::product::TranslationRepository>,
    dt_repo: Arc<dyn dt::product::ProductRepository>,
    davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
    category_repo: Arc<dyn category::CategoryRepository>,
    shop_service: Addr<ShopService>,
    currency_service: Addr<CurrencyService>,
    export: HashMap<String, Arc<RwLock<Export>>>,
}

impl ExportService {
    pub fn new(
        client: Client,
        entries: Vec<(IdentityOf<Shop>, ExportEntry)>,
        tt_repo: Arc<dyn tt::product::ProductRepository>,
        trans_repo: Arc<dyn tt::product::TranslationRepository>,
        dt_repo: Arc<dyn dt::product::ProductRepository>,
        davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
        category_repo: Arc<dyn category::CategoryRepository>,
        shop_service: Addr<ShopService>,
        currency_service: Addr<CurrencyService>,
    ) -> Self {
        Self {
            client,
            entries,
            tt_repo,
            trans_repo,
            dt_repo,
            davi_repo,
            category_repo,
            shop_service,
            currency_service,
            export: HashMap::new(),
        }
    }
    pub async fn start_export_cycle(
        client: Client,
        export: Arc<RwLock<Export>>,
        dt_repo: Arc<dyn dt::product::ProductRepository>,
        tt_repo: Arc<dyn tt::product::ProductRepository>,
        davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
        category_repo: Arc<dyn category::CategoryRepository>,
        trans_repo: Arc<dyn tt::product::TranslationRepository>,
        currency_service: Addr<CurrencyService>,
    ) {
        let (mut entry, start_notify, stop_notify, mut shop, mut rx) = {
            let e = export.read().await;
            (
                e.entry.clone(),
                e.start.clone(),
                e.stop.clone(),
                e.shop.clone(),
                e.suspend_tx.subscribe(),
            )
        };
        let mut file_name = entry.file_name(FileFormat::Csv);
        let mut retry_count = 0;
        match tokio::fs::metadata(format!("./export/{shop}/{file_name}"))
            .await
            .map(|m| m.modified())
        {
            Ok(Ok(m)) => match std::time::SystemTime::now().duration_since(m) {
                Ok(d) if d < entry.update_rate => {
                    {
                        let mut export = export.write().await;
                        export.status = ExportStatus::Success;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(entry.update_rate - d) => (),
                        _ = start_notify.notified() => (),
                        _ = stop_notify.notified() => return,
                    }
                }
                Ok(_) => (),
                Err(err) => {
                    log::error!("Unable to calculate duration since last modified: {err}");
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Ok(Err(err)) | Err(err) => {
                log::error!("Unable to read export file metadata: {err}");
            }
        }
        loop {
            {
                let mut export = export.write().await;
                export.status = ExportStatus::Enqueued;
                if entry != export.entry {
                    entry = export.entry.clone();
                    file_name = entry.file_name(None);
                }
                shop = export.shop;
                if let Some(true) = rx.try_recv().log_error("Unable to read suspend rx") {
                    export.status = ExportStatus::Suspended;
                    drop(export);
                    loop {
                        match rx.recv().await.log_error("Unable to read suspend rx") {
                            Some(false) => break,
                            _ => continue,
                        }
                    }
                    continue;
                }
            }
            let permit = match SEMAPHORE.acquire().await {
                Ok(p) => Some(p),
                Err(err) => {
                    log::warn!("Unable to acquire semaphore permit: {err}");
                    None
                }
            };
            log::info!("Generating {file_name}");
            let shop_id = shop.to_string();
            let (res, _) = tokio::join!(
                do_export(
                    &entry,
                    shop,
                    client.clone(),
                    &shop_id,
                    dt_repo.clone(),
                    tt_repo.clone(),
                    davi_repo.clone(),
                    category_repo.clone(),
                    trans_repo.clone(),
                    currency_service.clone(),
                ),
                async {
                    let mut export = export.write().await;
                    export.status = ExportStatus::InProgress;
                }
            );
            drop(permit);
            let status = match res {
                Ok(_) => {
                    log::info!("{file_name} has been generated");
                    retry_count = 0;
                    ExportStatus::Success
                }
                Err(ExportError::Download(_, uploader::DownloadFromLinkError::Other(err))) => {
                    log::error!("Unable to generate {file_name}: {err}");
                    if retry_count < MAX_RETRY_COUNT {
                        retry_count += 1;
                        continue;
                    } else {
                        retry_count = 0;
                        ExportStatus::Failure(err.to_string())
                    }
                }
                Err(ExportError::Download(
                    l,
                    uploader::DownloadFromLinkError::UnableToParse { err, content },
                )) => {
                    let hash = &xxh64(l.as_bytes(), l.len() as u64);
                    if let Err(err) = std::fs::write(format!("{hash:x}.xml.tmp"), content) {
                        log::error!("Unable to write unparsed content of link {}: {err}", l);
                    } else {
                        log::error!("Unable to parse link: {err}\nUnparsed content of link {} written as {hash:x}", l);
                    }
                    ExportStatus::Failure(err.to_string())
                }
                Err(err) => {
                    log::error!("Unable to generate {file_name}: {err}");
                    ExportStatus::Failure(err.to_string())
                }
            };
            {
                let mut export = export.write().await;
                export.status = status;
            }
            tokio::select! {
                _ = tokio::time::sleep(entry.update_rate) => (),
                _ = start_notify.notified() => (),
                _ = stop_notify.notified() => return,
            }
        }
    }
}

pub struct AddExportPermission(IdentityOf<Shop>);

impl AddExportPermission {
    pub fn acquire(
        user: &UserCredentials,
        shop: &Shop,
        subscription: &Option<UserSubscription>,
    ) -> Option<Self> {
        if shop.owner != user.login {
            return None;
        }
        if subscription.as_ref().is_some_and(|sub| {
            shop.export_entries.len() > sub.inner().limits.maximum_exports as usize
        }) {
            return None;
        }
        Some(Self(shop.id))
    }
    pub fn shop_id(&self) -> &IdentityOf<Shop> {
        &self.0
    }
}

pub struct UpdateExportEntryPermission(ExportEntry, String);

impl UpdateExportEntryPermission {
    pub fn acquire(
        export: ExportEntry,
        hash: String,
        subscription: &Option<UserSubscription>,
    ) -> Option<Self> {
        if export
            .links
            .as_ref()
            .zip(subscription.as_ref())
            .is_some_and(|(l, s)| l.len() >= s.inner().limits.links_per_export as usize)
        {
            return None;
        }
        Some(Self(export, hash))
    }
    pub fn into_inner(self) -> (ExportEntry, String) {
        (self.0, self.1)
    }
}

#[derive(Message)]
#[rtype(result = "Option<Export>")]
pub struct GetStatus(pub String);

#[derive(Message)]
#[rtype(result = "HashMap<String, Export>")]
pub struct GetAllStatus(pub IdentityOf<Shop>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Add(pub AddExportPermission, pub ExportEntry);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Remove(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Start(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct StartAll;

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct Update(pub IdentityOf<Shop>, pub UpdateExportEntryPermission);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Cleanup;

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct SuspendByShop(pub IdentityOf<Shop>, pub bool);

#[derive(Debug, Clone)]
pub struct Export {
    pub shop: IdentityOf<Shop>,
    pub status: ExportStatus,
    pub entry: ExportEntry,
    start: Arc<Notify>,
    suspend_tx: broadcast::Sender<bool>,
    stop: Arc<Notify>,
}

impl Export {
    pub fn status(&self) -> &ExportStatus {
        &self.status
    }
    pub fn entry(&self) -> &ExportEntry {
        &self.entry
    }
}

#[derive(Clone, Debug, Display, Serialize)]
pub enum ExportStatus {
    #[display("В очереди")]
    Enqueued,
    #[display("В процессе")]
    InProgress,
    #[display("Экспорт успешно завершен")]
    Success,
    #[display("Экспорт приостановлен")]
    Suspended,
    #[display("Экспорт завершен с ошибкой: {:?}", _0)]
    Failure(String),
}

impl Actor for ExportService {
    type Context = Context<Self>;

    fn start(mut self) -> Addr<Self>
    where
        Self: Actor<Context = Context<Self>>,
    {
        for (shop, entry) in &self.entries {
            let (suspend_tx, _) = broadcast::channel(20);
            self.export.insert(
                entry.generate_hash().to_string(),
                Arc::new(RwLock::new(Export {
                    shop: *shop,
                    entry: entry.clone(),
                    start: Arc::new(Notify::new()),
                    stop: Arc::new(Notify::new()),
                    suspend_tx,
                    status: ExportStatus::InProgress,
                })),
            );
        }
        for e in self.export.values() {
            tokio::task::spawn_local(Self::start_export_cycle(
                self.client.clone(),
                e.clone(),
                self.dt_repo.clone(),
                self.tt_repo.clone(),
                self.davi_repo.clone(),
                self.category_repo.clone(),
                self.trans_repo.clone(),
                self.currency_service.clone(),
            ));
        }
        Context::new().run(self)
    }

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.subscribe_system_async::<ConfigurationChanged>(ctx);
        self.subscribe_system_async::<WatermarkUpdated>(ctx);
        ctx.address().do_send(Cleanup);
    }
}

impl Handler<ConfigurationChanged> for ExportService {
    type Result = ();

    fn handle(&mut self, _msg: ConfigurationChanged, _ctx: &mut Self::Context) -> Self::Result {
        // self.entries = msg.0.export_entries;
    }
}

impl Handler<WatermarkUpdated> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(
        &mut self,
        WatermarkUpdated { shop_id, from, to }: WatermarkUpdated,
        _: &mut Self::Context,
    ) -> Self::Result {
        let entries = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in entries {
                let mut e = e.write().await;
                if e.shop != shop_id {
                    continue;
                }
                let entry = &mut e.entry;
                entry
                    .links
                    .iter_mut()
                    .flatten()
                    .filter_map(|l| l.options.as_mut())
                    .chain(entry.tt_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.dt_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.maxton_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.davi_parsing.iter_mut())
                    .filter_map(|o| o.watermarks.as_mut())
                    .filter(|(n, _)| *n == from)
                    .for_each(|(n, _)| *n = to.clone());
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetStatus> for ExportService {
    type Result = ResponseActFuture<Self, Option<Export>>;

    fn handle(&mut self, GetStatus(hash): GetStatus, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.get(&hash).cloned();
        let fut = async move {
            if let Some(export) = export {
                Some(export.read().await.clone())
            } else {
                None
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetAllStatus> for ExportService {
    type Result = ResponseActFuture<Self, HashMap<String, Export>>;

    fn handle(
        &mut self,
        GetAllStatus(shop): GetAllStatus,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let export = self.export.clone();
        let fut = async move {
            let mut res = HashMap::new();
            for (h, e) in export.iter() {
                let e = e.read().await;
                if e.shop == shop {
                    res.insert(h.clone(), e.clone());
                }
            }
            res
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Start> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, Start(hash): Start, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.get(&hash).cloned();
        let fut = async move {
            if let Some(n) = export {
                n.read().await.start.notify_waiters()
            } else {
                log::warn!("Export entry {hash} not found");
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<StartAll> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: StartAll, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in export {
                e.read().await.start.notify_waiters();
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Update> for ExportService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(
        &mut self,
        Update(shop, permission): Update,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (entry, hash) = permission.into_inner();
        let self_addr = ctx.address().clone();
        let h = hash.clone();
        let addr = self.shop_service.clone();
        let ent = entry.clone();
        let new_hash = ent.generate_hash();
        let nh = new_hash.clone();

        let ex = self.export.get(&hash).cloned();
        let fut = async move {
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await?
                .context("Unable to read shop")?
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            let e = shop
                .export_entries
                .iter_mut()
                .find(|e| e.generate_hash().to_string() == h);
            if let Some(e) = e {
                *e = ent.clone();
            } else {
                log::warn!("Entry not found in config");
                shop.export_entries.push(ent.clone())
            }
            addr.send(shop::service::Update(shop))
                .await?
                .context("Unable to update shop")?;
            if let Some(export) = ex {
                {
                    let mut ex = export.write().await;
                    ex.entry = ent.clone();
                }
            } else {
                log::warn!("Entry not found in export");
            }
            self_addr.do_send(Cleanup);
            Ok(nh.to_string())
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _ctx| {
            if res.is_err() {
                return res;
            }
            let export = act.export.remove(&hash);
            if let Some(export) = export {
                act.export.insert(new_hash.to_string(), export);
            }
            res
        }))
    }
}

impl Handler<Add> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Add(shop, entry): Add, _: &mut Context<Self>) -> Self::Result {
        let addr = self.shop_service.clone();
        let shop = *shop.shop_id();
        let (suspend_tx, _) = broadcast::channel(20);
        let e = Arc::new(RwLock::new(Export {
            shop,
            entry: entry.clone(),
            start: Arc::new(Notify::new()),
            stop: Arc::new(Notify::new()),
            suspend_tx,
            status: ExportStatus::InProgress,
        }));
        let client = self.client.clone();
        let dt_repo = self.dt_repo.clone();
        let tt_repo = self.tt_repo.clone();
        let davi_repo = self.davi_repo.clone();
        let trans_repo = self.trans_repo.clone();
        let category_repo = self.category_repo.clone();
        let currency_service = self.currency_service.clone();
        let new_entry = entry.clone();
        let fut = async move {
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await?
                .context("Unable to read shop")?
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            shop.export_entries.push(new_entry);
            addr.send(shop::service::Update(shop)).await??;
            Ok(())
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _| {
            act.export
                .insert(entry.generate_hash().to_string(), e.clone());
            tokio::task::spawn_local(Self::start_export_cycle(
                client,
                e,
                dt_repo,
                tt_repo,
                davi_repo,
                category_repo,
                trans_repo,
                currency_service,
            ));
            res
        }))
    }
}

impl Handler<Remove> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Remove(hash): Remove, ctx: &mut Context<Self>) -> Self::Result {
        let addr = self.shop_service.clone();
        let self_addr = ctx.address().clone();
        let e = self.export.remove(&hash);
        let fut = async move {
            let shop;
            if let Some(e) = e {
                let e = e.read().await;
                e.stop.notify_waiters();
                shop = e.shop;
            } else {
                return Err(anyhow::anyhow!("Export entry worker not found"));
            }
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await??
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            let e = shop
                .export_entries
                .iter_mut()
                .enumerate()
                .find(|(_, e)| e.generate_hash().to_string() == hash);
            if let Some((i, _)) = e {
                shop.export_entries.remove(i);
            }
            addr.send(shop::service::Update(shop))
                .await?
                .context("Unable to update shop")?;
            self_addr.do_send(Cleanup);
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Cleanup> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: Cleanup, _: &mut Context<Self>) -> Self::Result {
        let entries = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            let entries = stream::iter(entries)
                .map(|e| async move {
                    let e = e.read().await;
                    (e.shop, e.entry.clone())
                })
                .buffered(10)
                .collect::<Vec<_>>()
                .await;
            if let Err(err) = cleanup(entries) {
                log::error!("Unable to perform cleanup: {err:?}");
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<SuspendByShop> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(
        &mut self,
        SuspendByShop(shop, val): SuspendByShop,
        _: &mut Self::Context,
    ) -> Self::Result {
        let export = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in export {
                let e = e.read().await;
                if e.shop == shop {
                    println!("suspend: {val}");
                    e.suspend_tx.send(val)?;
                }
            }
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

pub fn cleanup<E: IntoIterator<Item = (IdentityOf<Shop>, ExportEntry)>>(
    entries: E,
) -> Result<(), anyhow::Error> {
    let map = entries
        .into_iter()
        .map(|(s, e)| (s, e.file_name(None)))
        .into_group_map();
    for (shop_id, file_names) in map {
        for d in std::fs::read_dir(format!("export/{shop_id}"))? {
            let d = d?;
            let file_name = d
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))?;
            if !d.metadata()?.is_dir()
                && !file_names
                    .iter()
                    .any(|f| f.contains(&file_name) || file_name.contains(f))
            {
                std::fs::remove_file(d.path())?;
            }
        }
    }
    Ok(())
}

pub async fn parse_from_links(
    l: &[ExportEntryLink],
    client: reqwest::Client,
) -> Result<
    (
        HashMap<ExportEntryLink, Vec<Offer>>,
        HashMap<ExportEntryLink, Vec<Item>>,
    ),
    ExportError,
> {
    let ext = futures::stream::iter(l.iter())
        .map(|l| {
            let client = client.clone();
            async move {
                match uploader::download_from_link(&l.link, client).await {
                    Ok(items) => Ok((l, items)),
                    Err(err) => Err(ExportError::Download(l.link.clone(), err)),
                }
            }
        })
        .buffer_unordered(1024)
        .try_collect::<Vec<_>>()
        .await?;
    let (off, ext): (Vec<_>, Vec<_>) = ext.into_iter().partition_map(|(v, p)| match p {
        uploader::DownloadResult::Offers(o) => itertools::Either::Left((v, o)),
        uploader::DownloadResult::Items(i) => itertools::Either::Right((v, i)),
    });
    let off = off.into_iter().into_group_map_by(|(v, _)| *v);
    let ext = ext.into_iter().into_group_map_by(|(v, _)| *v);
    let offers = off
        .into_iter()
        .map(|(k, v)| (k.clone(), v.into_iter().flat_map(|(_, v)| v).collect()))
        .collect();
    let external_items = ext
        .into_iter()
        .map(|(k, v)| (k.clone(), v.into_iter().flat_map(|(_, v)| v).collect()))
        .collect();
    Ok((offers, external_items))
}

#[derive(Debug, Display)]
pub enum ExportError {
    #[display("Unable to download items from link: {:?}", _0)]
    Download(String, uploader::DownloadFromLinkError),
    #[display("{}", _0)]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for ExportError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<std::io::Error> for ExportError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

static SEMAPHORE: Semaphore = Semaphore::const_new(3);

fn ensure_bilingual(p: &mut Product) {
    let ua_title = p
        .ua_translation
        .as_ref()
        .map(|t| t.title.clone())
        .filter(|t| !t.is_empty());
    if p.title.is_empty() {
        if let Some(title) = ua_title.clone() {
            p.title = title;
        }
    }
    if ua_title.is_none() {
        let title = if p.title.is_empty() {
            String::new()
        } else {
            p.title.clone()
        };
        p.ua_translation.get_or_insert(UaTranslation {
            title,
            description: None,
        });
    }

    let desc_ru = p
        .description
        .clone()
        .or_else(|| p.ua_translation.as_ref().and_then(|t| t.description.clone()));
    let desc_ua = p
        .ua_translation
        .as_ref()
        .and_then(|t| t.description.clone())
        .or_else(|| desc_ru.clone());

    p.description = desc_ru;
    if let Some(ua) = p.ua_translation.as_mut() {
        ua.description = desc_ua;
    } else {
        p.ua_translation = Some(UaTranslation {
            title: p.title.clone(),
            description: desc_ua,
        });
    }
}

pub async fn do_export(
    entry: &ExportEntry,
    shop: IdentityOf<Shop>,
    client: Client,
    shop_id: &str,
    dt_repo: Arc<dyn dt::product::ProductRepository>,
    tt_repo: Arc<dyn tt::product::ProductRepository>,
    davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
    category_repo: Arc<dyn category::CategoryRepository>,
    trans_repo: Arc<dyn tt::product::TranslationRepository>,
    currency_service: Addr<CurrencyService>,
) -> Result<(), ExportError> {
    match tokio::fs::create_dir_all(format!("/tmp/export/{shop}")).await {
        Ok(_) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(err) => {
            log::error!("Unable to create temporal directory for shop {shop}: {err}");
        }
    }
    match tokio::fs::create_dir_all(format!("./export/{shop}")).await {
        Ok(_) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(err) => {
            log::error!("Unable to create export directory for shop {shop}: {err}");
        }
    }
    let (dt, tt, davi, res) = tokio::join!(
        async {
            match (
                entry.dt_parsing.as_ref().map(|x| x.options.only_available),
                entry
                    .maxton_parsing
                    .as_ref()
                    .map(|x| x.options.only_available),
            ) {
                (Some(true), Some(true)) | (Some(true), None) | (None, Some(true)) => {
                    Ok::<_, anyhow::Error>(Some(
                        dt_repo.select(&dt::product::AvailableSelector).await?,
                    ))
                }
                (Some(false), _) | (_, Some(false)) => Ok(Some(dt_repo.list().await?)),
                (None, None) => Ok(None),
            }
        },
        async {
            match &entry.tt_parsing.as_ref().map(|x| x.options.only_available) {
                Some(true) => Ok::<_, anyhow::Error>(Some(
                    tt_repo.select(&tt::product::AvailableSelector).await?,
                )),
                Some(false) => Ok(Some(tt_repo.list().await?)),
                None => Ok(None),
            }
        },
        async {
            match &entry.davi_parsing.as_ref().map(|x| x.only_available) {
                Some(true) => Ok::<_, anyhow::Error>(Some(
                    davi_repo
                        .select(&rt_types::product::AvailableSelector)
                        .await?,
                )),
                Some(false) => Ok(Some(davi_repo.list().await?)),
                None => Ok(None),
            }
        },
        async {
            match &entry.links {
                Some(l) => Some(parse_from_links(l, client.clone()).await),
                None => None,
            }
            .transpose()
            .map(Option::unzip)
        }
    );
    let (dt, tt, davi) = (dt?, tt?, davi?);
    let (offers, items) = res?;

    let mut res: HashMap<ExportOptions, Vec<Product>, _> =
        HashMap::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new());
    let categories = category_repo.select(&By(shop)).await?.into_iter().fold(
        HashSet::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new()),
        |mut r, e| {
            r.insert(e);
            r
        },
    );
    let categories_used = entry
        .dt_parsing
        .as_ref()
        .is_some_and(|opts| opts.options.categories)
        || entry
            .tt_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .maxton_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .davi_parsing
            .as_ref()
            .is_some_and(|opts| opts.categories)
        || items.as_ref().is_some_and(|i| {
            i.iter()
                .any(|(e, _)| e.options.as_ref().is_some_and(|opts| opts.categories))
        })
        || offers.as_ref().is_some_and(|i| {
            i.iter()
                .any(|(e, _)| e.options.as_ref().is_some_and(|opts| opts.categories))
        });
    let mut categories = if categories_used {
        categories
    } else {
        HashSet::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new())
    };
    let rates = match currency_service.send(ListRates).await {
        Ok(rates) => rates
            .into_iter()
            .map(|(k, v)| (k, v * dec!(1.07)))
            .collect(),
        Err(err) => {
            log::error!("Unable to list rates: {err}");
            HashMap::new()
        }
    };
    if let Some(offers) = offers {
        for (e, i) in offers {
            let vendor = e
                .vendor_name
                .unwrap_or_else(|| match parse_vendor_from_link(&e.link) {
                    Some(v) => v,
                    None => {
                        log::warn!("Unable to parse vendor from link {}", &e.link);
                        String::new()
                    }
                });
            let opts = e.options.unwrap_or_default();
            if i.is_empty() {
                log::warn!("Empty items list for offers: {opts:#?}");
            }
            let items =
                rt_types::product::convert(i.into_iter().map(Vendored::with_vendor(vendor)));
            let items: Vec<_> = if opts.categories {
                rt_types::category::assign_categories(items, &categories).collect()
            } else {
                items.collect()
            };
            let items = match opts.convert_to_uah {
                true => items
                    .into_iter()
                    .map(|mut i| {
                        if let Some(rate) = rates.get(&i.currency) {
                            i.currency = "UAH".to_string();
                            i.price *= rate;
                        }
                        i
                    })
                    .collect(),
                false => items,
            };
            let mut items: Vec<_> = items.into_iter().collect();
            items.iter_mut().for_each(ensure_bilingual);
            res.insert(opts, items);
        }
    }
    if let Some(items) = items {
        for (e, i) in items {
            let vendor = e
                .vendor_name
                .unwrap_or_else(|| match parse_vendor_from_link(&e.link) {
                    Some(v) => v,
                    None => {
                        log::warn!("Unable to parse vendor from link {}", &e.link);
                        String::new()
                    }
                });
            let opts = e.options.unwrap_or_default();
            if i.is_empty() {
                log::warn!("Empty items list for items: {opts:#?}");
            }
            let offers =
                rt_types::product::convert(i.into_iter().map(Vendored::with_vendor(vendor)));
            let offers: Vec<_> = if opts.categories {
                rt_types::category::assign_categories(offers, &categories).collect()
            } else {
                offers.collect()
            };
            let mut offers = match opts.convert_to_uah {
                true => offers
                    .into_iter()
                    .map(|mut i| {
                        if let Some(rate) = rates.get(&i.currency) {
                            i.currency = "UAH".to_string();
                            i.price *= rate;
                        }
                        i
                    })
                    .collect(),
                false => offers,
            };
            offers.iter_mut().for_each(ensure_bilingual);
            if let Some(entry) = res.get_mut(&opts) {
                entry.append(&mut offers);
            } else {
                res.insert(opts, offers);
            }
        }
    }
    if let Some((options, products)) = entry.tt_parsing.as_ref().zip(tt) {
        let products = match &options.append_categories {
            Some(ParsingCategoriesAction::BeforeTitle { separator }) => products
                .into_iter()
                .map(|mut p| {
                    if let Some(category) = &p.category {
                        p.title = format!("{} {} {}", category.trim(), separator.trim(), p.title);
                    }
                    p
                })
                .collect(),
            Some(ParsingCategoriesAction::AfterTitle { separator }) => products
                .into_iter()
                .map(|mut p| {
                    if let Some(category) = &p.category {
                        p.title = format!("{} {} {}", p.title, separator.trim(), category.trim());
                    }
                    p
                })
                .collect(),
            None => products,
        };
        let products: Vec<_> = stream::iter(products)
            .map(|mut p| async {
                let trans = trans_repo.get_one(&p.id).await?;
                if let Some(trans) = trans {
                    p.title = trans.title;
                    p.description = trans.description;
                    Ok::<_, anyhow::Error>(Some(p))
                } else {
                    Ok(None)
                }
            })
            .buffered(10)
            .filter_map(|p| async { p.transpose() })
            .try_collect()
            .await?;
        let dto = rt_types::product::convert(products.into_iter());
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        dto.iter_mut().for_each(ensure_bilingual);
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    let (maxton, dt): (Option<Vec<_>>, Option<Vec<_>>) = dt
        .map(|dt| {
            dt.into_iter().partition(|p| {
                p.article.ends_with("-M")
                    || p.title.to_lowercase().contains("maxton")
                    || p.description
                        .as_ref()
                        .is_some_and(|d| d.to_lowercase().contains("maxton"))
            })
        })
        .unzip();
    if let Some((options, products)) = entry.dt_parsing.as_ref().zip(dt) {
        let products = products.into_iter().map(|mut p| {
            if p.article.contains("JGD") {
                p.available = Availability::OnOrder;
            }
            p
        });
        let dto = rt_types::product::convert(products);
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, products)) = entry.maxton_parsing.as_ref().zip(maxton) {
        let dto = rt_types::product::convert(products.into_iter().map(dt::product::MaxtonProduct))
            .map(|mut p| {
                p.available = Availability::OnOrder;
                p
            });
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, dto)) = entry.davi_parsing.as_ref().zip(davi) {
        let new_categories = rt_parsing_davi::get_categories(&dto, categories.clone(), shop);
        for c in new_categories.iter() {
            category_repo
                .save(c.clone())
                .await
                .log_error("Unable to save new category for davi product");
        }
        categories = new_categories;
        let dto: Vec<_> = dto
            .into_iter()
            .map(|d: rt_parsing_davi::Product| d.enrich(categories.iter()))
            .collect();
        let mut dto = dto.into_iter().map(Into::into).collect();
        if let Some(entry) = res.get_mut(&options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.clone(), dto);
        }
    }
    let instant = std::time::Instant::now();

    let count: usize = res.values().map(|v| v.len()).sum();

    let file_path = |file_name| format!("/tmp/export/{shop}/{file_name}");
    let c = categories.clone();
    let xlsx_filename = file_path(entry.file_name(FileFormat::Xlsx));
    let f = xlsx_filename.clone();
    let i = std::time::Instant::now();
    let id = shop_id.to_string();

    let res = rt_types::watermark::apply_to_product_map(res, shop_id, &Lazy::force(&SELF_ADDR))?;
    let res: HashMap<_, _> = res
        .into_iter()
        .map(|(o, p)| {
            let only_available = o.only_available;
            let p = p.into_iter().filter(|p| {
                if only_available {
                    if let Availability::Available | Availability::OnOrder = p.available {
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            });
            if let Some(av) = &o.set_availability {
                let p = p
                    .map(|mut p| {
                        p.available = av.clone();
                        p
                    })
                    .collect();
                (o, p)
            } else {
                (o, p.collect())
            }
        })
        .collect();
    let r = res.clone();

    tokio::task::spawn_blocking(move || {
        crate::xlsx::write_xlsx_dto_map(&f, r, c, &id)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("Unable to join thread")??;

    let xlsx = i.elapsed().as_millis();

    let xml_filename = format!("{}", file_path(entry.file_name(FileFormat::Xml)));
    let i = std::time::Instant::now();
    crate::xml::write_dto_map(&xml_filename, &res, categories, shop_id).await?;
    let xml = i.elapsed().as_millis();

    let horoshop_filename = format!("{}", file_path(entry.file_name(FileFormat::HoroshopCsv)));
    let i = std::time::Instant::now();
    crate::horoshop::export_csv(horoshop_filename.clone(), &res, category_repo.clone()).await?;

    let horoshop_filename = format!(
        "{}",
        file_path(entry.file_name(FileFormat::HoroshopCategories))
    );
    crate::horoshop::export_csv_categories(horoshop_filename.clone(), &res, category_repo.clone())
        .await?;
    let horoshop = i.elapsed().as_millis();
    let i = std::time::Instant::now();
    let csv_filename = format!("{}", file_path(entry.file_name(FileFormat::Csv)));
    crate::csv::write_dto_map(&csv_filename, res, shop_id).await?;
    log::info!(
        "Time:\nxlsx: {xlsx}ms\nxml: {xml}ms\ncsv: {}ms\nhoroshop: {horoshop}ms",
        i.elapsed().as_millis()
    );
    log::info!(
        "Performance: {} items/min",
        (count as f32 / instant.elapsed().as_millis() as f32) * 1000. * 60.
    );

    let res = tokio::join!(
        tokio::fs::copy(&xlsx_filename, xlsx_filename.replace("/tmp", ".")),
        tokio::fs::copy(&xml_filename, xml_filename.replace("/tmp", ".")),
        tokio::fs::copy(&csv_filename, csv_filename.replace("/tmp", ".")),
        tokio::fs::copy(&horoshop_filename, horoshop_filename.replace("/tmp", ".")),
    );
    res.0?;
    res.1?;
    res.2?;

    let (a, b, c, d) = tokio::join!(
        tokio::fs::remove_file(&xlsx_filename),
        tokio::fs::remove_file(&xml_filename),
        tokio::fs::remove_file(&csv_filename),
        tokio::fs::remove_file(&horoshop_filename),
    );
    let res = a
        .context(xlsx_filename)
        .and(b.context(xml_filename))
        .and(c.context(csv_filename))
        .and(d.context(horoshop_filename));
    if let Err(err) = res {
        log::error!("Unable to remove tmp file: {err}");
    }
    Ok(())
}
