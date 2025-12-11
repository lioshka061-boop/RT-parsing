#![deny(clippy::unwrap_used)]
#![allow(clippy::from_over_into)]
#![allow(clippy::mutable_key_type)]

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, DeflateOption, ZipEntryBuilder};
use futures::stream::{StreamExt, TryStreamExt};
use futures_lite::AsyncWriteExt;
use log_error::LogError;
use once_cell::sync::Lazy;
use refinery::embed_migrations;
use serde::de::IntoDeserializer;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

pub mod access;
pub mod cache;
pub mod category;
pub mod control;
pub mod csv;
pub mod dt;
pub mod export;
pub mod external_import;
pub mod facebook;
pub mod horoshop;
pub mod invoice;
pub mod shop;
pub mod subscription;
pub mod tt;
pub mod uploader;
pub mod watermark;
pub mod xlsx;
pub mod xml;

embed_migrations!("./migrations");

pub static SELF_ADDR: Lazy<String> = Lazy::new(|| {
    envmnt::get_parse("SELF_ADDR")
        .context("SELF_ADDR not set")
        .log_error("Unable to get SELF_ADDR")
        .unwrap_or("0.0.0.0".to_string())
});

#[derive(Debug)]
pub struct SqlWrapper<T>(pub T);

impl<T> SqlWrapper<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> SqlWrapper<T> {
    pub fn from_sql<R>(r: R) -> Result<T, <Self as TryFrom<R>>::Error>
    where
        Self: TryFrom<R>,
    {
        r.try_into().map(|w: Self| w.0)
    }
}

pub async fn compress_file<S: AsRef<std::path::Path>>(path: S) -> Result<String, anyhow::Error> {
    let path = path.as_ref();
    let res_path = format!("{}.zip", path.display());
    let mut file = tokio::fs::File::open(path).await?;
    let mut res_file = tokio::fs::File::create(&res_path).await?;

    let builder = ZipEntryBuilder::new(
        path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| anyhow!("No filename for path {path:?}"))?
            .into(),
        Compression::Deflate,
    )
    .deflate_option(DeflateOption::Fast)
    .unix_permissions(0o777);
    let mut w = ZipFileWriter::with_tokio(&mut res_file);
    let mut writer = w.write_entry_stream(builder).await?;
    let mut buf = [0; 1024];
    loop {
        let c = file.read(&mut buf).await?;
        if c == 0 {
            break;
        }
        writer.write(&buf[..c]).await?;
        buf = [0; 1024];
    }
    writer.close().await?;
    w.close().await?;
    tokio::fs::remove_file(path).await?;
    Ok(res_path)
}

pub struct RateLimiter(Arc<Notify>);

impl RateLimiter {
    pub fn new(rpm: u64) -> Self {
        let notify = Arc::new(Notify::new());
        let n = notify.clone();
        let duration = Duration::from_millis(60_000 / rpm);
        tokio::spawn(async move {
            let notify = n;
            loop {
                sleep(duration).await;
                notify.notify_one();
            }
        });
        Self(notify)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        let notify = Arc::new(Notify::new());
        let n = notify.clone();
        tokio::spawn(async move {
            let notify = n;
            loop {
                sleep(DURATION).await;
                notify.notify_one();
            }
        });
        Self(notify)
    }
}

const RPM: u64 = 20;
const DURATION: Duration = Duration::from_millis(60_000 / RPM);

#[async_trait]
impl reqwest_ratelimit::RateLimiter for RateLimiter {
    async fn acquire_permit(&self) {
        self.0.notified().await;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model(pub String);

pub fn format_raw_html<S: ToString>(s: S) -> String {
    s.to_string().replace('\n', "").trim().to_string()
}

use lazy_regex::regex;

pub fn parse_vendor_from_link<S: AsRef<str>>(l: S) -> Option<String> {
    let regex = regex!(r"(?U)https?:\/\/(([^\.]+)\.)*(com|net|ua|org|one)(\.[^\.]+)*");
    regex
        .captures(l.as_ref())
        .and_then(|c| c.get(2))
        .map(|m| m.as_str().to_string())
}

pub fn duration_until_midnight() -> Duration {
    let now = time::OffsetDateTime::now_utc();
    let mut next_midnight = time::OffsetDateTime::now_utc()
        .replace_time(time::Time::MIDNIGHT + time::Duration::hours(1));
    if now > next_midnight {
        next_midnight += time::Duration::DAY;
    }
    Duration::from_millis((next_midnight - now).whole_milliseconds() as u64)
}

pub fn last_midnight() -> time::OffsetDateTime {
    let now = time::OffsetDateTime::now_utc();
    let mut midnight = time::OffsetDateTime::now_utc()
        .replace_time(time::Time::MIDNIGHT + time::Duration::hours(1));
    if now < midnight {
        midnight -= time::Duration::DAY;
    }
    midnight
}

pub fn last_week() -> time::OffsetDateTime {
    last_midnight() - (time::Duration::DAY * 7)
}

#[derive(Deserialize)]
pub struct CategoryEntry {
    name: String,
    id: u128,
    parent_id: Option<u128>,
}

pub async fn import_categories(
    path: &std::path::Path,
    repo: Arc<category::SqliteCategoryRepository>,
) -> Result<(), anyhow::Error> {
    let data = tokio::fs::read_to_string(path).await?;
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .delimiter(b',')
        .create_deserializer(data.as_bytes());
    let mut records = rdr.deserialize::<CategoryEntry>();
    while let Some(entry) = records.next().await.transpose()? {
        repo.update_by_name(
            entry.name,
            Uuid::from_u128(entry.id),
            entry.parent_id.map(Uuid::from_u128),
        )
        .await?
    }
    Ok(())
}

pub fn format_duration_short(duration: &std::time::Duration) -> String {
    let days = duration.as_millis() / 1000 / 60 / 60 / 24;
    let hours = duration.as_millis() / 1000 / 60 / 60 % 24;
    let minutes = duration.as_millis() / 1000 / 60 % 60;
    let seconds = duration.as_millis() / 1000 % 60;
    let whole_millis = duration.as_millis();
    if days > 0 {
        return format!("{days}д.");
    }
    if hours > 0 {
        return format!("{hours}ч.");
    }
    if minutes > 0 {
        return format!("{minutes}мин.");
    }
    if seconds > 0 {
        return format!("{seconds}с.");
    }
    if whole_millis < 1000 {
        return format!("{whole_millis}мс.");
    }
    return "Только что".to_string();
}

pub fn format_duration(duration: &std::time::Duration) -> String {
    let mut res = vec![];
    let days = duration.as_millis() / 1000 / 60 / 60 / 24;
    let hours = duration.as_millis() / 1000 / 60 / 60 % 24;
    let minutes = duration.as_millis() / 1000 / 60 % 60;
    let seconds = duration.as_millis() / 1000 % 60;
    let whole_millis = duration.as_millis();
    if days > 0 {
        res.push(format!("{days}д."));
    }
    if hours > 0 {
        res.push(format!("{hours}ч."));
    }
    if minutes > 0 {
        res.push(format!("{minutes}мин."));
    }
    if seconds > 0 {
        res.push(format!("{seconds}с."));
    }
    if whole_millis < 1000 {
        res.push(format!("{whole_millis}мс."));
    }
    itertools::intersperse(res, " ".to_string()).collect()
}

pub fn parse_duration(duration: &str) -> Result<Duration, anyhow::Error> {
    let duration = duration
        .to_lowercase()
        .replace(".", "")
        .replace("сек", "s")
        .replace("с", "s")
        .replace("мин", "m")
        .replace("м", "m")
        .replace("час", "h")
        .replace("ч", "h");
    duration_str::parse(duration).map_err(|dur| anyhow::anyhow!("Unable to parse duration {dur}"))
}

pub fn empty_string_as_none<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    let opt = Option::<String>::deserialize(de)?;
    let opt = opt.as_deref();
    match opt {
        None | Some("") | Some("all") | Some("any") => Ok(None),
        Some(s) => T::deserialize(s.into_deserializer()).map(Some),
    }
}

fn empty_string_as_none_parse<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let opt = Option::<String>::deserialize(de)?;
    let opt = opt.as_deref();
    match opt {
        None | Some("") | Some("all") | Some("any") => Ok(None),
        Some(s) => s
            .parse()
            .map_err(|err| serde::de::Error::custom(format!("{err:?}")))
            .map(Some),
    }
}

pub async fn row_stream_to_vec<T: TryFrom<tokio_postgres::Row, Error = anyhow::Error>>(
    r: tokio_postgres::RowStream,
) -> Result<Vec<T>, anyhow::Error> {
    r.try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(T::try_from)
        .collect()
}

#[cfg(test)]
pub mod test {

    use super::*;

    #[test]
    fn parses_vendor_from_link() {
        assert_eq!(
            Some("ddaudio".to_string()),
            parse_vendor_from_link("https://prom.ddaudio.com.ua/uploads")
        );
        assert_eq!(
            Some("restalauto".to_string()),
            parse_vendor_from_link("https://restalauto.com.ua/products_feed.xml")
        );
    }
}
