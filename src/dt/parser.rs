use crate::cache;
use crate::dt::{
    product::{Product, ProductRepository},
    selectors,
};
use crate::{format_raw_html, Model, Url};
use actix::prelude::*;
use actix_broker::BrokerSubscribe;
use anyhow::anyhow;
use derive_more::Display;
use derive_more::Error;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use lazy_regex::regex;
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use rt_types::shop::ConfigurationChanged;
use rt_types::{Availability, Pause, Resume};
use scraper::{node::Node, Html};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::signal;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio_util::sync::CancellationToken;
use typesafe_repository::IdentityOf;

#[derive(Message)]
#[rtype(result = "Result<ParsingProgress, anyhow::Error>")]
pub struct GetProgress;

#[derive(Message)]
#[rtype(result = "Result<Product, ProductParsingError>")]
pub struct Parse(pub String);

#[derive(Message)]
#[rtype(result = "Result<Vec<String>, ProductParsingError>")]
pub struct ParsePage(pub String);

#[derive(Message)]
#[rtype(result = "Result<Option<Product>, anyhow::Error>")]
pub struct ProductInfo(pub IdentityOf<Product>);

pub struct ParsingProgress {
    pub ready: u64,
    pub total: u64,
    pub stage: ParsingStage,
}

#[derive(Clone, Display)]
pub enum ParsingStage {
    #[display("Пауза")]
    Pause,
    #[display("Парсинг брендов")]
    Brands,
    #[display("Парсинг моделей")]
    Models,
    #[display("Парсинг списка товаров")]
    ProductList,
    #[display("Парсинг товаров")]
    Products,
}

#[derive(Debug, Display, Error)]
pub enum ProductParsingError {
    NoArticle,
    ParsingError(ParsingError),
}

impl From<anyhow::Error> for ProductParsingError {
    fn from(err: anyhow::Error) -> Self {
        ParsingError::from(err).into()
    }
}

impl From<reqwest::Error> for ProductParsingError {
    fn from(err: reqwest::Error) -> Self {
        ParsingError::from(err).into()
    }
}

impl From<ParsingError> for ProductParsingError {
    fn from(err: ParsingError) -> Self {
        ProductParsingError::ParsingError(err)
    }
}

#[derive(Debug, Display, Error)]
pub enum ParsingError {
    #[error(ignore)]
    BrowserCheck(String),
    #[error(ignore)]
    MissingHref(String),
    Network(reqwest::Error),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for ParsingError {
    fn from(err: anyhow::Error) -> Self {
        ParsingError::Other(err)
    }
}

impl From<reqwest::Error> for ParsingError {
    fn from(err: reqwest::Error) -> ParsingError {
        ParsingError::Network(err)
    }
}

impl From<reqwest_middleware::Error> for ParsingError {
    fn from(err: reqwest_middleware::Error) -> ParsingError {
        match err {
            reqwest_middleware::Error::Middleware(err) => ParsingError::Other(err),
            reqwest_middleware::Error::Reqwest(err) => ParsingError::Network(err),
        }
    }
}

#[derive(Clone)]
pub struct ParsingOptions {
    pub url: String,
    pub repo: Arc<dyn ProductRepository>,
    pub client: ClientWithMiddleware,
    pub progress_bar: Option<Arc<ProgressBar>>,
    pub parallel_downloads: usize,
    pub stage: ParsingStage,
}

impl ParsingOptions {
    pub fn new(
        url: String,
        repo: Arc<dyn ProductRepository>,
        client: ClientWithMiddleware,
        progress_bar: Option<Arc<ProgressBar>>,
        parallel_downloads: usize,
    ) -> Self {
        Self {
            url,
            repo,
            client,
            progress_bar,
            parallel_downloads,
            stage: ParsingStage::Pause,
        }
    }
}

pub async fn parse_brands(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(&url).send().await?.text().await?;
    if is_browser_check(&body) {
        panic!("Cannot proceed");
    }
    let document = Html::parse_document(&body);
    let brands: Vec<_> = document
        .select(&selectors::BRANDS)
        .filter_map(
            |e| match (e.attr("href"), e.last_child().map(|c| c.value())) {
                (Some(url), Some(Node::Text(t))) => {
                    Some((Url(url.to_string()), format_raw_html(t.to_string())))
                }
                (None, _) => {
                    log::error!("Brand without link:\n{e:?}");
                    None
                }
                (_, _) => {
                    log::error!("Unable to parse brand name:\n{e:?}");
                    None
                }
            },
        )
        .collect();
    Ok(brands)
}

pub async fn parse_categories(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(&url).send().await?.text().await?;
    if is_browser_check(&body) {
        panic!("Cannot proceed");
    }
    let document = Html::parse_document(&body);
    let categories: Vec<_> = document
        .select(&selectors::CATEGORIES)
        .filter_map(
            |e| match (e.attr("href"), e.last_child().map(|c| c.value())) {
                (Some(url), Some(Node::Text(t))) => {
                    Some((Url(url.to_string()), format_raw_html(t.to_string())))
                }
                (None, _) => {
                    log::error!("Category without link:\n{e:?}");
                    None
                }
                (_, _) => {
                    log::error!("Unable to parse category name:\n{e:?}");
                    None
                }
            },
        )
        .collect();
    Ok(categories)
}

pub async fn parse_subcategories(
    categories: &[(Url, String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String, &String)>, ParsingError> {
    let (client, url, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    stream::iter(categories)
        .map(|(Url(link), category)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{link}").replace("///", "/"))
                    .send()
                    .await
                    .map(|body| (link, body, category))
            }
        })
        .buffer_unordered(parallel_downloads)
        .map(|res| {
            let pb = pb.clone();
            async move {
                let (link, body, category) = res?;
                let body = body.text().await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let document = Html::parse_document(&body);
                let subcategories: Vec<_> = document
                    .select(&selectors::SUBCATEGORY)
                    .map(|e| (e.attr("href"), e.inner_html()))
                    .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                    .collect();
                if subcategories.is_empty() {
                    return Ok(vec![(Url(link.clone()), category.clone(), category)]);
                }
                subcategories
                    .into_iter()
                    .map(|(url, v)| {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        Ok((Url(url), v, category))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn parse_models(
    brands: &[(Url, String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String, &String)>, ParsingError> {
    let (client, url, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    stream::iter(brands)
        .map(|(Url(link), brand)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{link}").replace("///", "/"))
                    .send()
                    .await
                    .map(|body| (link, body, brand))
            }
        })
        .buffer_unordered(parallel_downloads)
        .map(|res| {
            let pb = pb.clone();
            async move {
                let (link, body, brand) = res?;
                let body = body.text().await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let document = Html::parse_document(&body);
                let models: Vec<_> = document
                    .select(&selectors::MODEL)
                    .map(|e| (e.attr("href"), e.inner_html()))
                    .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                    .collect();
                models
                    .into_iter()
                    .map(|(url, v)| {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        Ok((Url(url), v, brand))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn parse_product_lists<'a>(
    models: &'a [(Url, String, &String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, &'a String, &'a String)>, ParsingError> {
    let (client, url, repo, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.repo.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    let url = if url.ends_with('/') {
        url[..url.len() - 1].to_string()
    } else {
        url
    };
    stream::iter(models)
        .map(|(Url(link), model, brand)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{}", format_link(link)))
                    .send()
                    .await
                    .map(|body| (link, body, model, brand))
            }
        })
        .buffered(parallel_downloads)
        .map(|res| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let (link, body, model, &brand) = res?;
                let mut body = body.text().await?;
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let mut res = vec![(link.clone(), body.clone(), model, brand)];
                let regex =
                    regex!(r"a href=.([a-z|0-9|\-\/_]*).( target=._self.)? aria-label=.Next");
                loop {
                    let next = regex
                        .captures(&body)
                        .map(|c| c.get(1).map(|m| m.as_str().to_string()));
                    let n = match next {
                        Some(Some(n)) => n,
                        Some(None) => {
                            log::warn!("Unable to parse link to next page");
                            break;
                        }
                        None => break,
                    };
                    let url = format!("{url}/{}", format_link(&n));
                    let response = match client.get(url.clone()).send().await {
                        Ok(response) => response,
                        Err(err) => {
                            log::error!("Unable to parse products list at {url}: {err}");
                            continue;
                        }
                    };
                    body = match response.text().await {
                        Ok(text) => text,
                        Err(err) => {
                            log::error!("Unable to parse products list at {url}: {err}");
                            continue;
                        }
                    };
                    res.push((link.clone(), body.clone(), model, brand));
                }
                Ok(res)
            }
        })
        .buffered(parallel_downloads * 2)
        .flat_map(|links| match links {
            Ok(links) => stream::iter(links.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .map(|res| {
            let repo = repo.clone();
            let pb = pb.clone();
            async move {
                let (link, body, model, brand) = res?;
                let document = Html::parse_document(&body);
                let items: Vec<_> = document
                    .select(&selectors::PRODUCT_ITEM)
                    .map(|e| (e.attr("href").map(str::to_string), e.inner_html()))
                    .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                    .collect();
                let urls: Vec<(Url, &String, &String)> = items
                    .into_iter()
                    .map(|(url, _)| {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        Ok((Url(url), model, brand))
                    })
                    .collect::<Result<Vec<_>, ParsingError>>()?;
                let products: Vec<_> = repo.list_by(&Model(model.to_string())).await?;
                let mut urls: Vec<_> = urls
                    .into_iter()
                    .map(|(url, model, brand)| {
                        (
                            url.clone(),
                            model,
                            brand,
                            products.iter().find(|p| p.url.0 == url.0),
                        )
                    })
                    .collect();
                urls.sort_by(|a, b| {
                    a.3.map(|x| x.last_visited)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                        .partial_cmp(
                            &b.3.map(|x| x.last_visited)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        )
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                let urls: Vec<_> = urls
                    .into_iter()
                    .filter_map(|(url, model, brand, product)| match product {
                        Some(product) if product.is_outdated() => Some((url, model, brand)),
                        None => Some((url, model, brand)),
                        Some(product) => {
                            log::info!("Skipping up to date product parsing: {}", product.article);
                            None
                        }
                    })
                    .collect();
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                Ok(urls)
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect()
        .await
}

pub async fn parse_product_list_page(
    link: &String,
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<Url>, ParsingError> {
    let client = {
        let opts = options.read().await;
        opts.client.clone()
    };
    let client = client.clone();
    let res = client.get(link).send().await?;
    let body: String = res.text().await?;
    if is_browser_check(&body) {
        return Err(ParsingError::BrowserCheck(link.clone()));
    }
    let document = Html::parse_document(&body);
    let items: Vec<_> = document
        .select(&selectors::PRODUCT_ITEM)
        .map(|e| (e.attr("href").map(str::to_string), e.inner_html()))
        .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
        .collect();
    let urls: Vec<Url> = items
        .into_iter()
        .map(|(url, _)| {
            let url = url
                .ok_or(ParsingError::MissingHref(link.clone()))?
                .to_string();
            Ok(Url(url))
        })
        .collect::<Result<Vec<_>, ParsingError>>()?;
    Ok(urls)
}

pub async fn parse_products<M, B>(
    links: &[(Url, M, B)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<usize, anyhow::Error>
where
    M: AsRef<str> + std::fmt::Display,
    B: AsRef<str> + std::fmt::Display,
{
    let (client, url, repo, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.repo.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    let len = stream::iter(links)
        .map(|(Url(link), model, brand)| {
            client
                .get(format!("{url}/{link}").replace("///", "/"))
                .send()
                .map(move |res| res.map(|body| (link, body, model, brand)))
        })
        .buffer_unordered(parallel_downloads)
        .filter_map(|res| async {
            match res {
                Ok(res) => Some(res),
                Err(err) => {
                    log::error!("Unable to parse product: {err}");
                    None
                }
            }
        })
        .map(|(link, body, model, brand)| {
            let repo = repo.clone();
            let pb = pb.clone();
            async move {
                let body = body
                    .text()
                    .await
                    .map_err(|err| ParsingError::Other(err.into()))?;
                if is_browser_check(&body) {
                    return Err::<_, ProductParsingError>(
                        ParsingError::BrowserCheck(link.clone()).into(),
                    );
                }
                let product = {
                    let document = Html::parse_document(&body);

                    parse_product(brand, model, link, document)?
                };
                log::info!("Saved {}", product.article);
                repo.save(product).await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                Ok(())
            }
        })
        .buffered(2048)
        .collect::<Vec<_>>()
        .await
        .len();
    Ok(len)
}

pub fn parse_product<M, B>(
    brand: B,
    model: M,
    link: &str,
    document: Html,
) -> Result<Product, ProductParsingError>
where
    M: AsRef<str> + std::fmt::Display,
    B: AsRef<str> + std::fmt::Display,
{
    let select = |selector| {
        document
            .select(selector)
            .map(|v| format_raw_html(v.inner_html()).to_string())
            .next()
    };

    let article_regex = regex!(r"(?i)(арт:?)? *(<.*>)?(.*) *$");
    let article = select(&selectors::ARTICLE)
        .map(|a| {
            article_regex
                .captures(&a)
                .and_then(|c| c.get(3))
                .map(|c| c.as_str().to_string())
                .ok_or(a)
        })
        .ok_or_else(|| ProductParsingError::NoArticle)?
        .map_err(|raw| anyhow!("Unable to parse article {raw} for item at {link}"))?;
    let title = select(&selectors::TITLE)
        .ok_or_else(|| anyhow!("Missing title for item {article} at {link}"))?;
    let description = select(&selectors::DESCRIPTION);
    if description.is_none() {
        log::warn!("Missing description for item {article} at {link}");
    }
    let category = select(&selectors::CATEGORY);
    if category.is_none() {
        log::warn!("Missing category for item {article} at {link}");
    }
    let price = select(&selectors::PRICE).map(|s| s.parse()).transpose();
    if let Err(err) = &price {
        log::warn!("Unable to parse price: {err}");
    }
    let price = price.ok().flatten();
    let mut available = match document.select(&selectors::AVAILABLE).count() > 0 {
        true => Availability::Available,
        false => Availability::NotAvailable,
    };
    if let Availability::NotAvailable = available {
        available = match document.select(&selectors::AVAILABLE_ON_ORDER).next() {
            Some(x) if x.inner_html().to_lowercase().contains("доступно под заказ") => {
                Availability::OnOrder
            }
            _ => Availability::NotAvailable,
        };
    }
    let mut images = document
        .select(&selectors::GALLERY_IMAGES)
        .filter_map(|v| {
            let href = v.attr("src");
            if href.is_none() {
                log::warn!("Image without link for item {link}");
            }
            href
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let logo = document
        .select(&selectors::LOGO)
        .map(|v| v.attr("src"))
        .next()
        .flatten()
        .map(ToString::to_string);
    if let Some(logo) = logo {
        images.insert(0, logo);
    }
    Ok(Product {
        title,
        description,
        article,
        category,
        price,
        available,
        brand: brand.to_string(),
        model: Model(model.to_string()),
        url: Url(link.to_string()),
        last_visited: OffsetDateTime::now_utc(),
        images,
    })
}

pub fn is_browser_check(s: &str) -> bool {
    s.contains("<title>Browser check, please wait ...</title>")
}

pub struct ParserService {
    opts: Arc<RwLock<ParsingOptions>>,
    pb_style: Option<ProgressStyle>,
    token: CancellationToken,
    stop_notify: Arc<Notify>,
    start_notify: Arc<Notify>,
}

impl ParserService {
    pub fn new(
        opts: ParsingOptions,
        pb_style: Option<ProgressStyle>,
        token: CancellationToken,
    ) -> Self {
        Self {
            opts: Arc::new(RwLock::new(opts)),
            pb_style,
            token,
            stop_notify: Arc::new(Notify::new()),
            start_notify: Arc::new(Notify::new()),
        }
    }
}

impl Actor for ParserService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.subscribe_system_async::<ConfigurationChanged>(ctx);
        let opts = self.opts.clone();
        let pb_style = self.pb_style.clone();
        let token = self.token.clone();
        let stop_notify = self.stop_notify.clone();
        let start_notify = self.start_notify.clone();
        tokio::task::spawn_local(async move {
            let stop = Arc::new(AtomicBool::new(false));
            tokio::task::spawn({
                let stop = stop.clone();
                let stop_notify = stop_notify.clone();
                async move {
                    loop {
                        stop_notify.notified().await;
                        stop.store(true, Ordering::SeqCst);
                    }
                }
            });
            match crate::cache::read_links(LINKS_PATH) {
                Ok(res) if !res.is_empty() => {
                    log::info!("Found {} cached links", res.len());
                    loop {
                        if stop.load(Ordering::SeqCst) {
                            start_notify.notified().await;
                            stop.store(false, Ordering::SeqCst);
                        }
                        let res = tokio::select! {
                            res = products_parsing(res.clone(), pb_style.clone(), opts.clone()) => res,
                            _ = stop_notify.notified() => continue,
                        };
                        if let Err(err) = res {
                            log::error!("Unable to parse dt products: {err}");
                        }
                        break;
                    }
                }
                Ok(_) => (),
                Err(err) => {
                    log::error!("Unable to read links from file: {err}");
                }
            };
            loop {
                if stop.load(Ordering::SeqCst) {
                    start_notify.notified().await;
                    stop.store(false, Ordering::SeqCst);
                }
                let res = tokio::select! {
                    res = work_cycle(opts.clone(), pb_style.clone(), token.clone()) => res,
                    _ = stop_notify.notified() => continue,
                };
                match res {
                    Ok(ControlFlow::Continue(())) => continue,
                    Ok(ControlFlow::Break(())) => break,
                    Err(err) => {
                        log::error!("Unable to parse dt products: {err}");
                        continue;
                    }
                }
            }
        });
        log::info!("DT parser started");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        log::info!("DT parser stopped");
    }
}

impl Handler<ConfigurationChanged> for ParserService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _msg: ConfigurationChanged, _ctx: &mut Self::Context) -> Self::Result {
        let fut = async move {};
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Pause> for ParserService {
    type Result = ();

    fn handle(&mut self, _: Pause, _ctx: &mut Self::Context) {
        self.stop_notify.notify_waiters();
    }
}

impl Handler<Resume> for ParserService {
    type Result = ();

    fn handle(&mut self, _: Resume, _ctx: &mut Self::Context) {
        self.start_notify.notify_waiters();
    }
}

impl Handler<GetProgress> for ParserService {
    type Result = ResponseActFuture<Self, Result<ParsingProgress, anyhow::Error>>;

    fn handle(&mut self, _: GetProgress, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            Ok(ParsingProgress {
                ready: opts
                    .progress_bar
                    .as_ref()
                    .map(|p| p.position())
                    .unwrap_or(0),
                total: opts
                    .progress_bar
                    .as_ref()
                    .and_then(|p| p.length())
                    .unwrap_or(0),
                stage: opts.stage.clone(),
            })
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Parse> for ParserService {
    type Result = ResponseActFuture<Self, Result<Product, ProductParsingError>>;

    fn handle(&mut self, Parse(link): Parse, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                let body = opts
                    .client
                    .get(format!("{link}"))
                    .send()
                    .await
                    .map_err(|err| ParsingError::Other(err.into()))?;
                let body: String = body.text().await?;
                let document = Html::parse_document(&body);
                let product = parse_product("", "", &link, document)?;
                opts.repo.save(product.clone()).await?;
                Ok(product)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ParsePage> for ParserService {
    type Result = ResponseActFuture<Self, Result<Vec<String>, ProductParsingError>>;

    fn handle(&mut self, ParsePage(link): ParsePage, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                let client = reqwest::Client::new();
                let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
                let url = &opts.url;
                let opts = Arc::new(RwLock::new(ParsingOptions {
                    client: reqwest_middleware::ClientBuilder::new(client)
                        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                        .build(),
                    ..opts.clone()
                }));
                let res = parse_product_list_page(&link, opts)
                    .await?
                    .into_iter()
                    .map(|a| a.0)
                    .collect();
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ProductInfo> for ParserService {
    type Result = ResponseActFuture<Self, Result<Option<Product>, anyhow::Error>>;

    fn handle(&mut self, ProductInfo(id): ProductInfo, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                opts.repo.get_one(&id).await
            }
            .into_actor(self),
        )
    }
}

pub async fn work_cycle(
    options: Arc<RwLock<ParsingOptions>>,
    pb_style: Option<ProgressStyle>,
    token: CancellationToken,
) -> Result<ControlFlow<(), ()>, anyhow::Error> {
    {
        let mut options = options.write().await;
        options.stage = ParsingStage::Brands;
    }
    let brands = tokio::select! {
        brands = parse_brands(options.clone()) => brands?,
        _ = token.cancelled() => return Ok(ControlFlow::Break(())),
    };

    log::info!("{} brands", brands.len());
    log::info!("{:?}", brands.iter().map(|(_, v)| v).collect::<Vec<_>>());
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(brands.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);

    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::Models;
    }

    let models = match cache::read_models(MODELS_PATH) {
        Ok(m) if m.is_empty() => {
            let models;
            loop {
                let res = tokio::select! {
                    res = parse_models(&brands, options.clone()) => res,
                    _ = token.cancelled() => return Ok(ControlFlow::Break(())),
                };
                match res {
                    Ok(m) => {
                        models = m;
                        break;
                    }
                    Err(err) => {
                        log::error!("Unable to parse models: {err:?}");
                        continue;
                    }
                }
            }
            if let Err(err) = cache::write_models(
                MODELS_PATH,
                models
                    .clone()
                    .into_iter()
                    .map(|(Url(url), model, brand)| cache::Model {
                        url,
                        model,
                        brand: brand.clone(),
                    })
                    .collect(),
            ) {
                log::warn!("Unable to write models: {err}");
            }
            models
        }
        Ok(models) => models
            .into_iter()
            .filter_map(|m| {
                let brand = brands.iter().map(|(_, b)| b).find(|&b| *b == m.brand);
                match brand {
                    Some(brand) => Some((Url(m.url), m.model, brand)),
                    None => {
                        log::warn!("Unable to find brand for model {}", m.model);
                        None
                    }
                }
            })
            .collect(),
        Err(err) => {
            log::error!("Unable to read models: {err}");
            return Ok(ControlFlow::Continue(()));
        }
    };
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    log::info!("{} total models", models.len());
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(models.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);

    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::ProductList;
    }
    let res;
    loop {
        let r = tokio::select! {
            r = parse_product_lists(&models, options.clone()) => r,
            _ = token.cancelled() => return Ok(ControlFlow::Break(())),
        };
        match r {
            Ok(r) => {
                res = r;
                break;
            }
            Err(err) => {
                log::error!("Unable to parse product lists: {err:?}");
                if let Some(pb) = &pb {
                    pb.reset();
                }
                continue;
            }
        }
    }
    if let Some(pb) = pb.clone() {
        pb.finish_and_clear();
    }
    let fut = async {
        let categories = parse_categories(options.clone()).await?;
        let subcategories = parse_subcategories(&categories, options.clone()).await;
        match subcategories {
            Ok(r) => match parse_product_lists(&r, options.clone()).await {
                Ok(res) => {
                    products_parsing(res, pb_style.clone(), options.clone()).await?;
                }
                Err(err) => {
                    log::error!("Unable to parse product lists in subcategory: {err:?}");
                }
            },
            Err(err) => {
                log::error!("Unable to parse subcategories: {err:?}");
            }
        };
        log::info!("{} total links", res.len());
        if products_parsing(res, pb_style.clone(), options).await? {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    };
    tokio::select! {
        r = fut => r,
        _ = token.cancelled() => Ok(ControlFlow::Break(())),
    }
}

pub async fn products_parsing<M, B>(
    res: Vec<(Url, M, B)>,
    pb_style: Option<ProgressStyle>,
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<bool, anyhow::Error>
where
    M: AsRef<str> + std::fmt::Display + Into<String> + Clone,
    B: AsRef<str> + std::fmt::Display + Into<String> + Clone,
{
    let token = CancellationToken::new();
    let (tx, mut rx) = mpsc::channel(100);
    let r = res
        .iter()
        .cloned()
        .map(|(u, m, b)| (u.clone(), m.into(), b.into()))
        .collect::<Vec<_>>();
    let t = token.clone();
    tokio::spawn(async move {
        let mut res = r;
        let token = t;
        tokio::select! {
            sig = signal::ctrl_c() => match sig {
                Ok(()) => {
                    token.cancel();
                    let count = rx.recv().await;
                    if let Some(count) = count {
                        if res.len() > count {
                            res.drain(0..count);
                        } else {
                            res.drain(0..);
                        }
                    }
                    #[allow(clippy::unwrap_used)]
                    cache::write_links(LINKS_PATH, &res).unwrap();
                },
                Err(err) => {
                    log::error!("Unable to listen for shutdown: {err}");
                }
            },
            count = (rx.recv()) => {
                if let Some(count) = count {
                    if res.len() > count {
                        res.drain(0..count);
                    } else {
                        res.drain(0..);
                    }
                }
                #[allow(clippy::unwrap_used)]
                cache::write_links(LINKS_PATH, &res).unwrap();
            }
        }
    });
    let total_chunks = res.len() / CHUNK_SIZE;
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(res.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);
    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::Products;
    }
    for (i, links) in res.chunks(CHUNK_SIZE).enumerate() {
        let r = tokio::select! {
            r = parse_products(links, options.clone()) => r,
            _ = token.cancelled() => {
                if let Err(err) = tx.send(CHUNK_SIZE).await {
                    log::error!("Unable to send chunk via tx: {err}");
                }
                return Ok(true)
            },
        };
        if let Err(err) = r {
            log::error!("Unable to parse products chunk {i}: {err}");
        } else if pb.as_ref().map(|pb| pb.is_hidden()).unwrap_or(true) {
            log::info!("Chunk {i} of {total_chunks} done");
        }
    }
    if let Err(err) = cache::clean_links(LINKS_PATH) {
        log::error!("Unable to clean links: {err}");
    }
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }
    if let Err(err) = tx.send(res.len()).await {
        log::error!("Unable to send chunk via tx: {err}");
    }
    Ok(false)
}

static MODELS_PATH: &str = "models.yml";
static LINKS_PATH: &str = "links.yml";
static CHUNK_SIZE: usize = 50;

pub fn format_link(s: &str) -> &str {
    if let Some(s) = s.strip_prefix('/') {
        s
    } else {
        s
    }
}
