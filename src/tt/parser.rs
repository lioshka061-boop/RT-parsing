use crate::tt::{
    product::{Product, ProductRepository, Translation, TranslationRepository},
    selectors,
};
use actix::prelude::*;
use anyhow::Context as AnyhowContext;
use derive_more::Display;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use lazy_regex::regex;
use rand::prelude::*;
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use rt_types::shop::ConfigurationChanged;
use rt_types::{Availability, Model, Url};
use rust_decimal::Decimal;
use scraper::Html;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{Notify, RwLock};
use typesafe_repository::{GetIdentity, IdentityOf};

#[derive(Message)]
#[rtype(result = "Result<ParsingProgress, anyhow::Error>")]
pub struct GetProgress;

#[derive(Message)]
#[rtype(result = "Result<usize, anyhow::Error>")]
pub struct GetCount;

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct GenerateCsv;

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct GenerateCsvAll;

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct ImportTranslations(pub Vec<Translation>);

#[derive(Message)]
#[rtype(result = "Result<usize, anyhow::Error>")]
pub struct GetTranslatedCount;

pub struct ParserService {
    opts: Arc<RwLock<ParsingOptions>>,
    completed: Arc<Notify>,
}

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
    #[display("Парсинг категорий")]
    Categories,
    #[display("Парсинг списка товаров")]
    ProductList,
    #[display("Парсинг товаров")]
    Products,
}

impl ParserService {
    pub fn new(opts: ParsingOptions) -> Self {
        Self {
            opts: Arc::new(RwLock::new(opts)),
            completed: Arc::new(Notify::new()),
        }
    }
}

impl Actor for ParserService {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        log::info!("TT parser started");
        let opts = self.opts.clone();
        let completed = self.completed.clone();
        tokio::task::spawn_local(async move {
            loop {
                let res = work_cycle(opts.clone()).await;
                if let Err(err) = res {
                    log::error!("Unable to parse tt products: {err}");
                }
                completed.notify_waiters();
            }
        });
    }
}

#[derive(Message)]
#[rtype(result = "Arc<Notify>")]
pub struct GetCompletedNotify;

impl Handler<GetCompletedNotify> for ParserService {
    type Result = Arc<Notify>;

    fn handle(&mut self, _: GetCompletedNotify, _ctx: &mut Context<Self>) -> Self::Result {
        self.completed.clone()
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

impl Handler<GetCount> for ParserService {
    type Result = ResponseActFuture<Self, Result<usize, anyhow::Error>>;

    fn handle(&mut self, _: GetCount, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            opts.repo.count().await
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GenerateCsv> for ParserService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(&mut self, _: GenerateCsv, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            let translation_repo = opts.translation_repo.clone();
            let translation = stream::iter(opts.repo.list().await?.into_iter())
                .map(|p| {
                    let translation_repo = translation_repo.clone();
                    let id = p.id();
                    async move { Ok::<_, anyhow::Error>((p, translation_repo.exists(&id).await?)) }
                })
                .buffered(10)
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .filter(|(_, exists)| !*exists)
                .map(|(p, _)| p)
                .map(Into::<Translation>::into)
                .collect::<Vec<_>>();
            let file = std::fs::File::create("static/tt_translation.csv")?;
            let mut perm = file.metadata()?.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions("static/tt_translation.csv", perm)?;
            let mut wtr = csv::Writer::from_writer(file);
            for t in translation {
                wtr.serialize(t)?;
            }
            Ok("static/tt_translation.csv".to_string())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GenerateCsvAll> for ParserService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(&mut self, _: GenerateCsvAll, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            let translation = opts
                .repo
                .list()
                .await?
                .into_iter()
                .map(Into::<Translation>::into)
                .collect::<Vec<_>>();
            let file = std::fs::File::create("static/tt_translation_all.csv")?;
            let mut perm = file.metadata()?.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions("static/tt_translation_all.csv", perm)?;
            let mut wtr = csv::Writer::from_writer(file);
            for t in translation {
                wtr.serialize(t)?;
            }
            Ok("static/tt_translation_all.csv".to_string())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<ImportTranslations> for ParserService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, msg: ImportTranslations, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            let trans_repo = opts.translation_repo.clone();
            let products = stream::iter(opts.repo.list().await?.into_iter())
                .map(|p| {
                    let translation_repo = trans_repo.clone();
                    let id = p.id();
                    async move { Ok::<_, anyhow::Error>((p, translation_repo.exists(&id).await?)) }
                })
                .buffered(10)
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .filter(|(_, exists)| *exists)
                .map(|(p, _)| p)
                .collect::<Vec<_>>();
            for t in msg.0 {
                let product = products.iter().find(|p| p.id == t.id);
                if product
                    .cloned()
                    .is_some_and(|p| Into::<Translation>::into(p) == t)
                {
                    continue;
                }
                trans_repo.save(t).await?;
            }
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetTranslatedCount> for ParserService {
    type Result = ResponseActFuture<Self, Result<usize, anyhow::Error>>;

    fn handle(&mut self, _: GetTranslatedCount, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            let translation_repo = opts.translation_repo.clone();
            let count = stream::iter(opts.repo.list().await?.into_iter())
                .map(move |p| {
                    let translation_repo = translation_repo.clone();
                    let id = p.id();
                    async move { translation_repo.exists(&id).await }
                })
                .buffered(10)
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .filter(|exists| *exists)
                .count();
            Ok(count)
        };
        Box::pin(fut.into_actor(self))
    }
}

pub async fn work_cycle(opts: Arc<RwLock<ParsingOptions>>) -> Result<(), anyhow::Error> {
    {
        let mut options = opts.write().await;
        options.stage = ParsingStage::Brands;
    }
    let (mut brands, mut categories) = parse_brands_and_categories(opts.clone()).await?;
    let mut rng = rand::thread_rng();
    brands.shuffle(&mut rng);
    categories.shuffle(&mut rng);
    for (url, brand) in brands.into_iter() {
        log::info!("{brand} parsing");
        {
            let mut options = opts.write().await;
            options.stage = ParsingStage::Models;
        }
        let models = parse_brand_models(opts.clone(), url.clone()).await?;
        log::info!("{} models", models.len());
        {
            let mut options = opts.write().await;
            options.stage = ParsingStage::Products;
        }
        let r = parse_brand_pages(opts.clone(), url, brand).await?;
        for (url, model) in models {
            parse_model_pages(opts.clone(), url, model).await?;
        }
        log::info!("{} products", r);
    }
    for (url, category) in categories.into_iter() {
        {
            let mut options = opts.write().await;
            options.stage = ParsingStage::Products;
        }
        log::info!("{category} parsing");
        let r = parse_category_pages(opts.clone(), url, category).await?;
        log::info!("{} products updated", r);
    }
    Ok(())
}

impl Handler<ConfigurationChanged> for ParserService {
    type Result = ();

    fn handle(&mut self, _: ConfigurationChanged, _ctx: &mut Self::Context) {}
}

#[derive(Clone)]
pub struct ParsingOptions {
    pub url: String,
    pub client: ClientWithMiddleware,
    pub repo: Arc<dyn ProductRepository>,
    pub translation_repo: Arc<dyn TranslationRepository>,
    pub progress_bar: Option<Arc<ProgressBar>>,
    pub stage: ParsingStage,
}

impl ParsingOptions {
    pub fn new(
        url: String,
        client: ClientWithMiddleware,
        repo: Arc<dyn ProductRepository>,
        translation_repo: Arc<dyn TranslationRepository>,
        progress_bar: Option<Arc<ProgressBar>>,
    ) -> Self {
        Self {
            url,
            repo,
            translation_repo,
            client,
            progress_bar,
            stage: ParsingStage::Pause,
        }
    }
}

pub async fn parse_brand_models(
    options: Arc<RwLock<ParsingOptions>>,
    url: Url,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let client = {
        let opts = options.read().await;
        opts.client.clone()
    };
    let body = client.get(&url.0).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let models = document
        .select(&selectors::AVAILABLE_MODELS)
        .filter(|e| !e.inner_html().trim().starts_with("- "))
        .filter_map(|e| match e.attr("value") {
            Some(value) => {
                let model = e.inner_html();
                let url = replace_page_index(&url.0, 0);
                Some((
                    Url(url.replace("0,0.html", &format!("{value},0.html"))),
                    model,
                ))
            }
            None => {
                log::error!("Model without value:\n{e:?}");
                None
            }
        })
        .collect();
    Ok(models)
}

pub async fn parse_brands_and_categories(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<(Vec<(Url, String)>, Vec<(Url, String)>), anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(url.clone()).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let brands = document
        .select(&selectors::BRANDS)
        .filter_map(|e| match e.attr("value") {
            Some(value) => {
                let brand = e.attr("title")?;
                let b = brand.replace(' ', "_");
                Some((
                    Url(format!("{url}/{b}-{value},0,0,0.html")),
                    brand.to_string(),
                ))
            }
            None => {
                log::error!("Brand without value:\n{e:?}");
                None
            }
        })
        .collect();
    let categories = document
        .select(&selectors::CATEGORIES)
        .filter_map(|e| match e.attr("value") {
            Some(value) => {
                let cat = e.attr("title")?;
                let c = cat.replace(' ', "_");
                Some((Url(format!("{url}/{value},{c}")), cat.to_string()))
            }
            None => {
                log::error!("Category without value:\n{e:?}");
                None
            }
        })
        .collect();
    Ok((brands, categories))
}

pub async fn parse_brands(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(url.clone()).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let brands = document
        .select(&selectors::BRANDS)
        .filter_map(|e| match e.attr("value") {
            Some(value) => {
                let brand = e.attr("title")?;
                let b = brand.replace(' ', "_");
                Some((
                    Url(format!("{url}/{b}-{value},0,0,0.html")),
                    brand.to_string(),
                ))
            }
            None => {
                log::error!("Brand without value:\n{e:?}");
                None
            }
        })
        .collect();
    Ok(brands)
}

pub async fn parse_brand_pages(
    options: Arc<RwLock<ParsingOptions>>,
    Url(url): Url,
    brand: String,
) -> Result<usize, anyhow::Error> {
    let mut index = 0;
    let mut res = 0;
    loop {
        let url = replace_page_index(&url, index);
        log::info!("parsing {url}");
        let r = parse_page(options.clone(), Url(url), &brand).await?;
        if r == 0 {
            break;
        }
        res += r;
        index += 1;
    }
    Ok(res)
}

#[derive(Debug)]
pub struct ProductList {
    pub page_count: usize,
    pub products: Vec<Url>,
}

const MAX_IMAGES: usize = 15;

pub async fn parse_page(
    options: Arc<RwLock<ParsingOptions>>,
    url: Url,
    brand: &str,
) -> Result<usize, anyhow::Error> {
    let (client, opts_url, repo) = {
        let opts = options.read().await;
        (opts.client.clone(), opts.url.clone(), opts.repo.clone())
    };
    let body = client.get(&url.0).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let products = futures::stream::iter(document.select(&selectors::PRODUCT))
        .map(|e| {
            let code = e
                .select(&selectors::PRODUCT_CODE)
                .next()
                .map(|e| e.inner_html().replace("Symbol: ", ""))
                .ok_or_else(|| anyhow::anyhow!("Product without code"))?;
            let price = e
                .select(&selectors::PRODUCT_PRICE)
                .next()
                .map(|e| e.inner_html().replace("PLN", ""))
                .ok_or_else(|| anyhow::anyhow!("Product without price: {code}"))?;
            let price = Decimal::from_str_exact(price.trim()).context("Unable to parse price")?;
            let image = e
                .select(&selectors::PRODUCT_IMAGE)
                .next()
                .and_then(|e| e.attr("src").map(|x| x.to_string()));
            let link = e
                .select(&selectors::PRODUCT_LINK)
                .next()
                .and_then(|e| e.attr("href").map(|x| x.to_string()))
                .ok_or_else(|| anyhow::anyhow!("Product without url: {code}"))?;
            let title = e
                .select(&selectors::PRODUCT_TITLE)
                .next()
                .map(|e| e.inner_html().to_string())
                .ok_or_else(|| anyhow::anyhow!("Product without title"))?;
            let availability = e
                .select(&selectors::PRODUCT_AVAILABILITY_INDICATOR)
                .last()
                .map(|e| e.attr("src").is_some_and(|s| s.contains("avl1")));
            let availability = match availability {
                Some(true) => Availability::OnOrder,
                Some(false) => Availability::NotAvailable,
                None => {
                    log::warn!("Unable to parse availability for product {code}");
                    Availability::NotAvailable
                }
            };

            Ok::<_, anyhow::Error>((code, title, price, image, link, availability))
        })
        .map(|res| {
            let options = options.clone();
            async move {
                let (code, title, price, img, url, availability) = res?;
                let id = parse_id_from_url(&url)
                    .ok_or_else(|| anyhow::anyhow!("Cannot parse id from url: {url:?}"))?
                    .to_string();
                let desc = match get_product_description(options, &id).await {
                    Ok(desc) => Some(desc),
                    Err(err) => {
                        log::warn!("Unable to get product {code} description: {err}");
                        None
                    }
                };
                Ok::<_, anyhow::Error>((code, id, title, price, img, url, desc, availability))
            }
        })
        .buffer_unordered(1024)
        .map(|res| {
            let url = &opts_url;
            let client = client.clone();
            async move {
                let (code, id, title, price, img, p_url, desc, availability) = res?;
                let img = img.ok_or_else(|| anyhow::anyhow!("Product with no image: {id}"))?;
                let (ext, img) = match parse_and_trim_img(&img) {
                    (Some(ext), img) => (ext, img),
                    (None, _) => return Err(anyhow::anyhow!("Unable to parse image {img}")),
                };
                let img = img.replace("_view3", "").replace("/main", "");
                let mut index = 0;
                let mut images = vec![];
                while let Ok(true) = client
                    .head(format!("{url}/{img}{index}.{ext}"))
                    .send()
                    .await
                    .map(|r| {
                        r.status() == StatusCode::OK
                            && r.headers().get("Content-Type").is_some_and(|t| {
                                !t.to_str()
                                    .ok()
                                    .is_some_and(|t| t.to_lowercase().contains("html"))
                            })
                    })
                {
                    if index > MAX_IMAGES {
                        log::warn!("Product {code} has more than {MAX_IMAGES} images");
                        break;
                    }
                    images.push(format!("{url}/{img}{index}.{ext}"));
                    index += 1;
                }
                Ok((code, id, title, price, images, desc, p_url, availability))
            }
        })
        .map(|res| async {
            let (article, id, title, price, images, description, url, available) = res.await?;
            let model = parse_model(&title, brand).unwrap_or_default();
            let model = Model(model);
            let p = Product {
                id: id.to_string(),
                title,
                description,
                price,
                article,
                brand: brand.to_string(),
                model,
                category: None,
                available,
                url: Url(url.to_string()),
                last_visited: OffsetDateTime::now_utc(),
                images,
            };
            repo.save(p).await?;
            Ok::<_, anyhow::Error>(())
        })
        .buffer_unordered(1024)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(products.len())
}

pub async fn parse_category_pages(
    options: Arc<RwLock<ParsingOptions>>,
    Url(url): Url,
    category: String,
) -> Result<usize, anyhow::Error> {
    let mut index = 2;
    let mut res = parse_category_page(options.clone(), Url(url.clone()), &category).await?;
    let mut url = url;
    loop {
        url = replace_category_page_index(&url, index);
        log::info!("parsing {url}");
        let r = parse_category_page(options.clone(), Url(url.clone()), &category).await?;
        if r == 0 {
            break;
        }
        res += r;
        index += 1;
    }
    Ok(res)
}

pub async fn parse_model_pages(
    options: Arc<RwLock<ParsingOptions>>,
    Url(url): Url,
    model: String,
) -> Result<usize, anyhow::Error> {
    let mut index = 0;
    let mut res = 0;
    let mut url = url;
    loop {
        url = replace_page_index(&url, index);
        let r = parse_model_page(options.clone(), Url(url.clone()), &model).await?;
        if r == 0 {
            break;
        }
        res += r;
        index += 1;
    }
    Ok(res)
}

pub async fn parse_category_page(
    options: Arc<RwLock<ParsingOptions>>,
    url: Url,
    category: &str,
) -> Result<usize, anyhow::Error> {
    let (client, repo) = {
        let opts = options.read().await;
        (opts.client.clone(), opts.repo.clone())
    };
    let body = client.get(&url.0).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let products = document
        .select(&selectors::PRODUCT)
        .filter_map(|e| {
            let code = e
                .select(&selectors::PRODUCT_CODE)
                .next()
                .map(|e| e.inner_html().replace("Symbol: ", ""));
            match code {
                Some(r) => Some(r),
                None => {
                    log::error!("Product without article in category {category}");
                    None
                }
            }
        })
        .collect::<Vec<IdentityOf<Product>>>();
    let count = products.len();
    if count > 0 {
        repo.update_category_where(products, category.to_string())
            .await?;
    }
    Ok(count)
}

pub async fn parse_model_page(
    options: Arc<RwLock<ParsingOptions>>,
    url: Url,
    model: &str,
) -> Result<usize, anyhow::Error> {
    let (client, repo) = {
        let opts = options.read().await;
        (opts.client.clone(), opts.repo.clone())
    };
    let body = client.get(&url.0).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let products = document
        .select(&selectors::PRODUCT)
        .filter_map(|e| {
            let code = e
                .select(&selectors::PRODUCT_CODE)
                .next()
                .map(|e| e.inner_html().replace("Symbol: ", ""));
            match code {
                Some(r) => Some(r),
                None => {
                    log::error!("Product without article in model {model}");
                    None
                }
            }
        })
        .collect::<Vec<IdentityOf<Product>>>();
    let count = products.len();
    if count > 0 {
        repo.update_model_where(products, model.to_string()).await?;
    }
    Ok(count)
}

pub async fn get_product_description(
    options: Arc<RwLock<ParsingOptions>>,
    id: &str,
) -> Result<String, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let url = format!("{url}/_template/_show_normal/_show_charlong.php?itemId={id}");
    let text = client.get(&url).send().await?.text().await?;
    Ok(text)
}

pub fn parse_and_trim_img(s: &str) -> (Option<&str>, String) {
    let regex = regex!(r"(\d*)\.([^.]*)$");
    (
        regex
            .captures(s)
            .filter(|c| c.get(1).is_some_and(|c| !c.as_str().is_empty()))
            .and_then(|c| c.get(2))
            .map(|c| c.as_str()),
        regex.replace(s, "").into(),
    )
}

pub fn parse_id_from_url(s: &str) -> Option<&str> {
    let regex = regex!(r".*-(\d.*)i");
    regex.captures(s).and_then(|c| c.get(1)).map(|c| c.as_str())
}

pub fn replace_page_index(s: &str, index: u64) -> String {
    let regex = regex!(r",\d+.html");
    regex.replace(s, format!(r",{index}.html")).into()
}

pub fn replace_category_page_index(s: &str, index: u64) -> String {
    let regex = regex!(r"(\d+),[a-zA-Z_]+$");
    if regex.is_match(s) {
        regex
            .replace(s, "category.php?catx=$1&lic=0&cv=1")
            .to_string()
    } else {
        let regex = regex!(r"cv=\d+");
        let s = regex.replace(s, format!("cv={index}"));
        let regex = regex!(r"lic=\d+");
        regex.replace(&s, format!("lic={}", index * 12)).to_string()
    }
}

pub fn parse_model(title: &str, brand: &str) -> Option<String> {
    let regex = regex!(r"(?i)fits (.*)");
    if regex.is_match(title) {
        regex
            .captures(title)
            .and_then(|c| c.get(1))
            .filter(|c| c.len() > brand.len())
            .map(|c| &c.as_str()[(brand.len() + 1)..])
            .map(ToString::to_string)
    } else {
        match regex::Regex::new(&format!(
            r"(?i){brand} ?(.* \d\d)(\.\d\d)?(-\d\d)?(.\d\d)?.*$"
        )) {
            Ok(regex) if regex.is_match(title) => {
                Some(regex.replace(title, "$1$2$3$4").to_string())
            }
            Ok(_) => {
                match regex::Regex::new(&format!(
                    r"(?i){brand} ?(([^\/]*)(\/[^\/ ]* ?[^\/ ]*)*)(^| .*)"
                )) {
                    Ok(regex) => Some(regex.replace(title, "$1").to_string()),
                    Err(err) => {
                        log::warn!(
                            "Unable to construct model parsing regex\
                            for title {title} and brand {brand}: {err}"
                        );
                        None
                    }
                }
            }
            Err(err) => {
                log::warn!(
                    "Unable to construct model parsing regex\
                    for title {title} and brand {brand}: {err}"
                );
                None
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn parses_model() {
        assert_eq!(
            Some("1 E87/E81/82/88 04-11".to_string()),
            parse_model(
                "HEADLIGHTS ANGEL EYES BLACK fits BMW 1 E87/E81/82/88 04-11",
                "BMW"
            )
        );
        assert_eq!(
            Some("f20 / 21 11-12.14".to_string()),
            parse_model(
                "heAdlights TRUE DRL blAck fits BMW f20 / 21 11-12.14",
                "BMW",
            )
        );
        assert_eq!(
            Some("forester / impreza / legacy / outback".to_string()),
            parse_model(
                "side direction side direction in the mirror smoke LED FITS SUBARU forester / impreza / legacy / outback",
                "SUBARU"
            )
        );
        assert_eq!(
            Some("swift 05.05-10".to_string()),
            parse_model("SUZUKI swift 05.05-10 chrome LED", "SUZUKI")
        );
    }

    #[test]
    fn replaces_page_index() {
        assert_eq!(
            "https://tuning-tec.com/AUDI-3685,0,0,10.html",
            replace_page_index("https://tuning-tec.com/AUDI-3685,0,0,0.html", 10)
        );
    }

    #[test]
    fn extracts_image_info() {
        assert_eq!(
            (Some("jpg"), "some_img".to_string()),
            parse_and_trim_img("some_img0.jpg")
        );
        assert_eq!(
            (Some("png"), "some_img.jpg".to_string()),
            parse_and_trim_img("some_img.jpg0.png")
        );
        assert_eq!(
            (Some("12345"), "some_img.awd".to_string()),
            parse_and_trim_img("some_img.awd0.12345")
        );
        assert_eq!(
            (None, "some_img".to_string()),
            parse_and_trim_img("some_img.jpg")
        );
        assert_eq!((None, "adwlkdj".to_string()), parse_and_trim_img("adwlkdj"));
    }

    #[test]
    fn parses_id_from_url() {
        assert_eq!(
            Some("878"),
            parse_id_from_url("headlights_angel_eyes_black_fits_audi_a4_11.9412.98_lpau11-878i")
        );
    }
}
