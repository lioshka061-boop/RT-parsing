use actix::prelude::*;
use anyhow::Context as AnyhowContext;
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::sync::Arc;
use time::macros::{offset, time};
use time::{OffsetDateTime, Time};
use tokio::sync::RwLock;

static RATES_FILE: &str = "currency_rates.csv";
static RATES_URL: &str = "https://index.minfin.com.ua/exchange/nbu/curr/";

#[allow(clippy::unwrap_used)]
static TABLE_SELECTOR: Lazy<Selector> = Lazy::new(|| Selector::parse("table").unwrap());
#[allow(clippy::unwrap_used)]
static CURRENCY_SELECTOR: Lazy<Selector> =
    Lazy::new(|| Selector::parse("tr td:nth-child(2)").unwrap());
#[allow(clippy::unwrap_used)]
static MULTIPLIER_SELECTOR: Lazy<Selector> =
    Lazy::new(|| Selector::parse("tr td:nth-child(3)").unwrap());
#[allow(clippy::unwrap_used)]
static RATE_SELECTOR: Lazy<Selector> = Lazy::new(|| Selector::parse("tr td:nth-child(5)").unwrap());

static UPDATE_RATE: std::time::Duration = std::time::Duration::from_secs(60 * 60 * 4);

pub struct CurrencyService {
    rates: Arc<RwLock<HashMap<String, Decimal>>>,
}

impl CurrencyService {
    pub fn new() -> Self {
        let rates = match read_rates(RATES_FILE) {
            Ok(rates) => rates,
            Err(err) => {
                log::warn!("Unable to read currency rates cache: {err}");
                None
            }
        }
        .map(RwLock::new)
        .map(Arc::new)
        .unwrap_or_default();
        Self { rates }
    }
}

fn read_rates(file: &str) -> Result<Option<HashMap<String, Decimal>>, anyhow::Error> {
    match std::fs::read_to_string(file) {
        Ok(file) => file
            .lines()
            .filter_map(|l| {
                let mut split = l.split(',');
                Some((split.next()?, split.next()?))
            })
            .map(|(c, r)| {
                let r = Decimal::from_str_exact(r)
                    .context(format!("Unable to parse rate {r} for currency {c}"))?;
                Ok((c.to_string(), r))
            })
            .collect::<Result<_, _>>()
            .map(Some),

        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            Err(err).context("Unable to open currency rates file")
        }
        Err(_) => Ok(None),
    }
}

async fn write_rates(file: &str, rates: &HashMap<String, Decimal>) -> Result<(), anyhow::Error> {
    let res = rates
        .iter()
        .map(|(k, v)| format!("{k},{v}\n"))
        .collect::<String>();
    tokio::fs::write(file, res).await?;
    Ok(())
}

async fn download_rates(url: &str) -> Result<HashMap<String, Decimal>, anyhow::Error> {
    let resp = reqwest::get(url)
        .await
        .context("Unable to download currency rates")?
        .text()
        .await?;
    let document = Html::parse_document(&resp);
    let entries = document
        .select(&TABLE_SELECTOR)
        .filter(|e| {
            e.inner_html()
                .contains("<caption>Официальный валютный курс НБУ")
        })
        .map(|e| {
            e.select(&CURRENCY_SELECTOR)
                .zip(e.select(&MULTIPLIER_SELECTOR))
                .zip(e.select(&RATE_SELECTOR))
                .map(|((c, m), r)| {
                    let m: Decimal = m
                        .inner_html()
                        .replace(",", ".")
                        .parse()
                        .context(format!("Unable to parse multiplier {}", m.inner_html()))?;
                    let r: Decimal = r
                        .inner_html()
                        .replace(",", ".")
                        .parse()
                        .context(format!("Unable to parse rate {}", r.inner_html()))?;
                    Ok((c.inner_html().to_uppercase().to_string(), r / m))
                })
                .collect()
        })
        .next()
        .ok_or(anyhow::anyhow!("No tables found on page"))?;
    entries
}

impl Actor for CurrencyService {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        let rates = self.rates.clone();
        tokio::spawn(async move {
            loop {
                let time_now = OffsetDateTime::now_utc().to_offset(offset!(+3)).time();
                let delta = (0..=(time::Duration::DAY / UPDATE_RATE) as u32)
                    .map(|i| Time::MIDNIGHT + (UPDATE_RATE * i))
                    .filter(|t| t > &time_now)
                    .map(|t| t - time_now)
                    .min()
                    .unwrap_or_else(|| {
                        time!(23:59) - (time_now - time::Duration::seconds(60 * 10))
                    });
                log::info!(
                    "Next rate update in {}h{}m",
                    delta.whole_hours(),
                    delta.whole_minutes() % 60
                );
                let sleep = tokio::time::sleep(
                    delta
                        .try_into()
                        .expect("Unable to convert time::Duration to std::time::Duration"),
                );
                sleep.await;
                let mut rates = rates.write().await;
                *rates = match download_rates(RATES_URL).await {
                    Ok(r) => r,
                    Err(err) => {
                        log::error!("Unable to download rates: {err}");
                        continue;
                    }
                };
                if let Err(err) = write_rates(RATES_FILE, &rates).await {
                    log::error!("Unable to write rates: {err}");
                }
            }
        });
    }
}

#[derive(Message)]
#[rtype(result = "Option<Decimal>")]
pub struct GetRate(pub String);

#[derive(Message)]
#[rtype(result = "HashMap<String, Decimal>")]
pub struct ListRates;

impl Handler<GetRate> for CurrencyService {
    type Result = ResponseActFuture<Self, Option<Decimal>>;

    fn handle(&mut self, GetRate(currency): GetRate, _: &mut Self::Context) -> Self::Result {
        let rates = self.rates.clone();
        Box::pin(async move { rates.read().await.get(&currency).cloned() }.into_actor(self))
    }
}

impl Handler<ListRates> for CurrencyService {
    type Result = ResponseActFuture<Self, HashMap<String, Decimal>>;

    fn handle(&mut self, _: ListRates, _: &mut Self::Context) -> Self::Result {
        let rates = self.rates.clone();
        Box::pin(async move { rates.read().await.clone() }.into_actor(self))
    }
}
