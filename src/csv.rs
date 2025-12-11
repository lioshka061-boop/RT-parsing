use crate::xlsx::*;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, DeflateOption, ZipEntryBuilder};
use itertools::Itertools;
use rt_types::product::Product;
use rt_types::shop::{Discount, ExportOptions};
use rt_types::{Availability, DescriptionOptions};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::iter;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use uuid::Uuid;

static EMPTY_STRING: String = String::new();

pub async fn write_dto_map(
    path: &str,
    items: HashMap<ExportOptions, Vec<Product>, impl BuildHasher>,
    shop_id: &str,
) -> Result<(), anyhow::Error> {
    let map = HashMap::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new());
    let items: HashMap<_, _, _> = items.into_iter().fold(map, |mut r, (k, v)| {
        r.insert(k, v);
        r
    });

    let titles: Vec<String> = items
        .iter()
        .flat_map(|(opts, products)| {
            products.iter().map(|p| {
                let base = if p.title.is_empty() {
                    p.ua_translation
                        .as_ref()
                        .map(|t| t.title.clone())
                        .unwrap_or_default()
                } else {
                    p.title.clone()
                };
                crate::xlsx::build_title(opts, &base, false)
            })
        })
        .collect();
    let titles = titles.iter();
    let titles_ua: Vec<String> = items
        .iter()
        .flat_map(|(opts, products)| {
            products.iter().map(|p| {
                let title = p
                    .ua_translation
                    .as_ref()
                    .map(|t| t.title.clone())
                    .filter(|t| !t.is_empty())
                    .unwrap_or_else(|| p.title.clone());
                crate::xlsx::build_title(opts, &title, true)
            })
        })
        .collect();
    let titles_ua = titles_ua.iter();

    let product_ids = items.values().flatten().map(|Product { id, .. }| id);
    let codes: Vec<_> = items
        .values()
        .flatten()
        .map(|Product { article, .. }| article.clone())
        .map(|d| {
            (
                d.char_indices().nth(25).map(|(x, _)| x).unwrap_or(d.len()),
                d,
            )
        })
        .map(|(x, d)| d[..x].to_string())
        .collect();
    let descriptions: Vec<String> = items
        .iter()
        .flat_map(|(o, i)| {
            let description_options = o.description.as_ref().and_then(|d| match d {
                DescriptionOptions::Replace(d) => {
                    match std::fs::read_to_string(format!("./description/{shop_id}/{d}")) {
                        Ok(d) => Some(DescriptionOptions::Replace(d)),
                        Err(err) => {
                            log::error!("Unable to read description {d}: {err}");
                            None
                        }
                    }
                }
                DescriptionOptions::Append(d) => {
                    match std::fs::read_to_string(format!("./description/{shop_id}/{d}")) {
                        Ok(d) => Some(DescriptionOptions::Append(d)),
                        Err(err) => {
                            log::error!("Unable to read description {d}: {err}");
                            None
                        }
                    }
                }
            });
            let i: Vec<String> = i
                .iter()
                .map(|i| {
                    i.description.clone().or_else(|| {
                        i.ua_translation
                            .as_ref()
                            .and_then(|t| t.description.clone())
                    })
                })
                .map(|d| match &description_options {
                    Some(DescriptionOptions::Replace(d)) => Some(d.clone()),
                    Some(DescriptionOptions::Append(a)) => d.map(|mut d| {
                        d.push_str(a);
                        d
                    }),
                    None => d,
                })
                .map(|d| {
                    d.map(|d: String| format_replica(&d))
                        .map(|d: String| {
                            if o.description.is_none() {
                                trim_images(&d)
                            } else {
                                d
                            }
                        })
                        .unwrap_or_default()
                })
                .collect();
            i
        })
        .map(|d| {
            (
                d.char_indices()
                    .nth(32_000)
                    .map(|(x, _)| x)
                    .unwrap_or(d.len()),
                d,
            )
        })
        .map(|(x, d)| d[..x].to_string())
        .collect();
    let descriptions_ua: Vec<String> = items
        .iter()
        .flat_map(|(o, i)| {
            let description_options = o.description_ua.as_ref().and_then(|d| match d {
                DescriptionOptions::Replace(d) => {
                    match std::fs::read_to_string(format!("./description/{shop_id}/{d}")) {
                        Ok(d) => Some(DescriptionOptions::Replace(d)),
                        Err(err) => {
                            log::error!("Unable to read description {d}: {err}");
                            None
                        }
                    }
                }
                DescriptionOptions::Append(d) => {
                    match std::fs::read_to_string(format!("./description/{shop_id}/{d}")) {
                        Ok(d) => Some(DescriptionOptions::Append(d)),
                        Err(err) => {
                            log::error!("Unable to read description {d}: {err}");
                            None
                        }
                    }
                }
            });
            let i: Vec<String> = i
                .iter()
                .map(|i| {
                    i.ua_translation
                        .clone()
                        .and_then(|t| t.description)
                        .or(i.description.clone())
                })
                .map(|d| match &description_options {
                    Some(DescriptionOptions::Replace(d)) => Some(d.clone()),
                    Some(DescriptionOptions::Append(a)) => d.map(|mut d| {
                        d.push_str(a);
                        d
                    }),
                    None => d,
                })
                .map(|d| {
                    d.map(|d| format_replica(&d))
                        .map(|d| {
                            if o.description.is_none() {
                                trim_images(&d)
                            } else {
                                d
                            }
                        })
                        .unwrap_or_default()
                })
                .collect();
            i
        })
        .map(|d| {
            (
                d.char_indices()
                    .nth(32_000)
                    .map(|(x, _)| x)
                    .unwrap_or(d.len()),
                d,
            )
        })
        .map(|(x, d)| d[..x].to_string())
        .map(|d| d.replace("\n", ""))
        .collect();
    let (images, keywords, currency): (Vec<_>, Vec<_>, Vec<_>) = items
        .values()
        .flatten()
        .map(|p| {
            (
                itertools::intersperse(p.images.iter().take(10).cloned(), ",".to_string())
                    .collect(),
                p.keywords.clone().unwrap_or_default(),
                p.currency.clone(),
            )
        })
        .multiunzip();
    let available: Vec<_> = items
        .iter()
        .flat_map(|(o, i)| {
            i.iter().map(|Product { available, .. }| match available {
                Availability::Available => "!".to_string(),
                Availability::NotAvailable => "-".to_string(),
                Availability::OnOrder => o
                    .delivery_time
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "0".to_string()),
            })
        })
        .collect();
    let prices: Vec<_> = items
        .iter()
        .flat_map(|(o, i)| {
            let adjust = o.adjust_price.unwrap_or(Decimal::ONE);
            i.iter().map(|p| p.price * adjust).collect::<Vec<Decimal>>()
        })
        .map(|price| format!("{price}"))
        .collect();
    let items_categories: Vec<_> = items
        .iter()
        .flat_map(|(_, i)| {
            i.iter()
                .map(|p| {
                    p.category
                        .as_ref()
                        .map(Uuid::as_u64_pair)
                        .map(|(a, _)| a)
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let in_stock: Vec<_> = items
        .values()
        .flatten()
        .map(|p| match p.in_stock {
            Some(q) => format!("{q}"),
            None => String::new(),
        })
        .collect();
    let empty = String::new();
    let default_unit = String::from("шт.");
    let unit = vec![&default_unit; prices.len()];
    let (sale, sale_until, sale_from): (Vec<_>, Vec<_>, Vec<_>) = items
        .iter()
        .flat_map(|(o, i)| match o.discount {
            Some(Discount { percent, duration }) => i
                .iter()
                .map(|p| match p.available {
                    Availability::Available | Availability::OnOrder => {
                        Some((format!("{percent}%"), duration))
                    }
                    _ => None,
                })
                .collect::<Vec<Option<(String, std::time::Duration)>>>(),
            None => i.iter().map(|_| None).collect(),
        })
        .map(|s| {
            if let Some((sale, duration)) = s {
                let time: time::OffsetDateTime = time::OffsetDateTime::now_utc() + duration;
                let from_time = time::OffsetDateTime::now_utc();
                let (time, from_time) =
                    match time::format_description::parse("[day].[month].[year]") {
                        Ok(time_format) => {
                            match (time.format(&time_format), from_time.format(&time_format)) {
                                (Ok(time), Ok(from_time)) => (time, from_time),
                                (Err(err1), Err(err2)) => {
                                    log::error!("Unable to format time: \n{err1:?}\n{err2:?}");
                                    ("".to_string(), "".to_string())
                                }
                                _ => {
                                    log::error!("Unable to format time");
                                    ("".to_string(), "".to_string())
                                }
                            }
                        }
                        Err(err) => {
                            log::error!("Unable to parse time format: {err:?}");
                            ("".to_string(), "".to_string())
                        }
                    };
                let sale_until = time;
                let sale_from = from_time;
                (sale, sale_until, sale_from)
            } else {
                (String::new(), String::new(), String::new())
            }
        })
        .multiunzip();
    let columns: Vec<(_, Box<dyn Iterator<Item = &String>>)> = vec![
        ("Код_товара", Box::new(codes.iter())),
        ("Название_позиции", Box::new(titles)),
        ("Название_позиции_укр", Box::new(titles_ua)),
        ("Идентификатор_товара", Box::new(product_ids)),
        ("Цена", Box::new(prices.iter())),
        ("Валюта", Box::new(currency.iter())),
        ("Ссылка_изображения", Box::new(images.iter())),
        ("Поисковые_запросы", Box::new(keywords.iter())),
        ("Единица_измерения", Box::new(unit.into_iter())),
        ("Наличие", Box::new(available.iter())),
        ("Количество", Box::new(in_stock.iter())),
        ("Описание_укр", Box::new(descriptions_ua.iter())),
        ("Описание", Box::new(descriptions.iter())),
        ("Знижка", Box::new(sale.iter())),
        ("Номер_групи", Box::new(items_categories.iter())),
        ("Термін_дії_знижки_до", Box::new(sale_until.iter())),
        ("Термін_дії_знижки_від", Box::new(sale_from.iter())),
    ];
    let len = codes.len();
    let mut columns: Box<dyn Iterator<Item = (&str, Box<dyn Iterator<Item = &String>>)>> =
        Box::new(columns.into_iter());
    let notes: Option<Box<dyn Iterator<Item = &String>>> = Some(Box::new(items.iter().flat_map(
        |(o, i)| match o.add_vendor {
            true => i.iter().map(|p| &p.vendor).collect::<Vec<&String>>(),
            false => i.iter().map(|_| &empty).collect::<Vec<&String>>(),
        },
    )));
    if let Some(n) = notes {
        columns = Box::new(columns.chain(iter::once(("Личные_Заметки", n))));
    }
    let params_names = items
        .iter()
        .flat_map(|(_, products)| products.iter().map(|p| &p.params))
        .max_by_key(|n| n.len());
    let params = params_names.into_iter().flat_map(|_name| {
        let values = items
            .iter()
            .flat_map(|(_, p)| p)
            .flat_map(move |i| i.params.values());
        let names = items
            .iter()
            .flat_map(|(_, p)| p)
            .flat_map(move |i| i.params.keys());
        let values: Box<dyn Iterator<Item = &String>> = Box::new(values);
        let names: Box<dyn Iterator<Item = &String>> = Box::new(names);
        let measure: Box<dyn Iterator<Item = &String>> =
            Box::new(vec![&EMPTY_STRING; len].into_iter());
        [
            ("Название_Характеристики", names),
            ("Измерение_Характеристики", measure),
            ("Значение_Характеристики", values),
        ]
    });

    columns = Box::new(columns.chain(params));

    let mut res_file = tokio::fs::File::create(&path).await?;
    let mut w = ZipFileWriter::with_tokio(&mut res_file);
    let builder = ZipEntryBuilder::new(
        std::path::Path::new(path)
            .file_name()
            .and_then(|f| f.to_str())
            .map(|f| f.replace(".zip", ""))
            .ok_or_else(|| anyhow::anyhow!("No filename for path {path:?}"))?
            .into(),
        Compression::Deflate,
    )
    .deflate_option(DeflateOption::Fast)
    .unix_permissions(0o777);
    let mut zip_writer = w.write_entry_stream(builder).await?.compat_write();
    let mut writer = csv_async::AsyncWriterBuilder::new()
        .quote_style(csv_async::QuoteStyle::NonNumeric)
        .create_writer(&mut zip_writer);

    let (names, mut values): (Vec<_>, Vec<_>) = columns.unzip();
    let record = csv_async::StringRecord::from(names);
    writer.write_record(record.iter()).await?;
    for _ in 0..len {
        let mut buf = Vec::with_capacity(2048);
        for v in values.iter_mut() {
            let bytes = v.next().map(|s| s.replace('\n', "")).unwrap_or_default();
            buf.push(bytes);
        }
        writer.write_record(buf).await?;
    }
    writer.flush().await?;
    drop(writer);
    zip_writer.into_inner().close().await?;
    w.close().await?;
    Ok(())
}
