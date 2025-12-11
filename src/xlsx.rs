use itertools::Itertools;
use lazy_regex::regex;
use rt_types::category::Category;
use rt_types::product::Product;
use rt_types::shop::{Discount, ExportOptions};
use rt_types::{Availability, DescriptionOptions};
use rust_decimal::Decimal;
use rust_xlsxwriter::Workbook;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::iter;
use uuid::Uuid;

static EMPTY_STRING: String = String::new();

pub fn write_xlsx_dto_map(
    path: &str,
    items: HashMap<ExportOptions, Vec<Product>, impl BuildHasher>,
    categories: HashSet<Category, impl BuildHasher>,
    shop_id: &str,
) -> Result<(), anyhow::Error> {
    let items: HashMap<_, _> = items
        .into_iter()
        .map(|(o, i)| {
            (
                o,
                i.into_iter()
                    .filter(|i| matches!(i.available, Availability::OnOrder))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();

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
                build_title(opts, &base, false)
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
                build_title(opts, &title, true)
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
                        d.push_str(&a);
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
                        d.push_str(&a);
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
        ("Описание", Box::new(descriptions.iter())),
        ("Описание_укр", Box::new(descriptions_ua.iter())),
        ("Знижка", Box::new(sale.iter())),
        ("Идентификатор_группы", Box::new(items_categories.iter())),
        ("Термін_дії_знижки_до", Box::new(sale_until.iter())),
        ("Термін_дії_знижки_від", Box::new(sale_from.iter())),
    ];
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
        .flat_map(|(_, products)| products.iter().flat_map(|p| p.params.keys().dedup()))
        .collect::<HashSet<_>>();
    let len = codes.len();
    let params = params_names.into_iter().flat_map(|name| {
        let values = items.iter().flat_map(|(_, p)| p).map(move |i| {
            i.params
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v)
                .unwrap_or(&EMPTY_STRING)
        });
        let values: Box<dyn Iterator<Item = &String>> = Box::new(values);
        let names: Box<dyn Iterator<Item = &String>> = Box::new(vec![name; len].into_iter());
        let measure: Box<dyn Iterator<Item = &String>> =
            Box::new(vec![&EMPTY_STRING; len].into_iter());
        [
            ("Название_Характеристики", names),
            ("Измерение_Характеристики", measure),
            ("Значение_Характеристики", values),
        ]
    });

    columns = Box::new(columns.chain(params));

    for (i, (name, values)) in columns.enumerate() {
        let mut max_width = name.len();
        sheet.set_column_width(i as u16, max_width as f64)?;
        sheet.write_string(0, i as u16, name)?;
        for (e, value) in values.enumerate() {
            if value.char_indices().count() > 32_000 {
                return Err(anyhow::anyhow!(
                    "Column {e} {name} has exceeded excel character limit"
                ));
            }
            if value.len() > max_width {
                max_width = value.len();
                sheet.set_column_width(i as u16, max_width as f64)?;
            }
            sheet.write_string(e as u32 + 1, i as u16, value)?;
        }
    }

    if !categories.is_empty() {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Export Groups Sheet")?;
        sheet.write_string(0, 0, "Назва_групи")?;

        sheet.write_string(0, 1, "Ідентифікатор_групи")?;
        sheet.write_string(0, 2, "Номер_батьківської_групи")?;
        for (i, c) in categories.iter().enumerate() {
            sheet.write_string(i as u32 + 1, 0, &c.name)?;
            sheet.write_string(i as u32 + 1, 1, c.id.as_u64_pair().0.to_string())?;
            sheet.write_string(
                i as u32 + 1,
                2,
                c.parent_id
                    .as_ref()
                    .map(Uuid::as_u64_pair)
                    .map(|(a, _)| a)
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            )?;
        }
    }
    workbook.save(path)?;
    Ok(())
}

pub fn read_unique_ids(path: &str) -> Result<HashMap<String, String>, anyhow::Error> {
    let mut reader = csv::Reader::from_path(path)?;
    let r = reader.deserialize();
    Ok(r.collect::<Result<_, _>>()?)
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdRecord {
    pub article: String,
    pub id: String,
}

pub fn read_ids(path: &str) -> Result<Vec<IdRecord>, anyhow::Error> {
    let mut reader = csv::Reader::from_path(path)?;
    let r = reader.deserialize();
    Ok(r.collect::<Result<_, _>>()?)
}

pub fn capitalize(input: &str) -> String {
    let mut c = input.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().chain(c).collect(),
    }
}

pub fn format_replica(input: &str) -> String {
    let regex = regex!(r"(?i)(реплик.!?|репліка!?|копи.!?|не оригинал)");
    if !regex.is_match(input) {
        return input.to_string();
    }
    let regex = regex!(r"(?i)(реплика|копия!?) оригина[A-я]*");
    let input = regex.replace_all(input, r"$1");
    let regex = regex!(r"(?i)([\w|А-я]*ая)? ?(реплика|копия!?)( )([\w|А-я]*)(ов)([ |<])?");
    let input = regex.replace_all(&input, r"$3$4и$6");
    let regex = regex!(r"(?i)([\w|А-я]*ая)? ?(реплика|копия!?)( )([\w|А-я]*)(ок)([ |<])?");
    let input = regex.replace_all(&input, r"$3$4ки$6");
    let regex = regex!(r"(?i)([\w|А-я]*[ая])? ?(реплика|копия!?)( )([\w|А-я]*)(а)([ |<])");
    let input = regex.replace_all(&input, r"$3$4$6");
    let regex = regex!(r"(?i)([\w|А-я]*[ая])? ?(реплика|копия!?)( )([\w|А-я]*)(и)([ |<])");
    let input = regex.replace_all(&input, r"$3$4а$6");
    let regex = regex!(r"(?i)([\w|А-я]*ая)? ?(реплика|копия!?)( )([\w|А-я]*)([ |<]?)");
    let input = regex.replace_all(&input, r"$3$4$5");
    let regex = regex!(r"(?i)не оригинал");
    let input = regex.replace_all(&input, r"");
    let regex = regex!(r"(?i)(реплик.|реплік.|копи.)");
    let input = regex.replace_all(&input, r"");
    input.to_string().replace("()", "").replace("  ", " ")
}

pub fn build_title(opts: &ExportOptions, base: &str, is_ua: bool) -> String {
    let mut title = if opts.format_years {
        format_years(base)
    } else {
        base.to_string()
    };
    title = format_replica(&title);
    if let Some(repls) = &opts.title_replacements {
        for (from, to) in repls {
            if !from.is_empty() {
                title = title.replace(from, to);
            }
        }
    }
    let prefix = if is_ua {
        opts.title_prefix_ua.as_deref()
    } else {
        opts.title_prefix.as_deref()
    }
    .map(str::trim)
    .filter(|s| !s.is_empty());
    let suffix = if is_ua {
        opts.title_suffix_ua.as_deref()
    } else {
        opts.title_suffix.as_deref()
    }
    .map(str::trim)
    .filter(|s| !s.is_empty());
    let mut parts = Vec::new();
    if let Some(p) = prefix {
        parts.push(p);
    }
    parts.push(title.trim());
    if let Some(s) = suffix {
        parts.push(s);
    }
    parts.join(" ")
}

pub fn trim_images(input: &str) -> String {
    let regex = regex!(r"(?i)<img[^<>]*>");
    regex.replace_all(input, "").to_string()
}

pub fn format_model(model: &str, brand: &str) -> String {
    let regex = regex!(r"\(?\d+ ?[\.-]+ ?[\d\.]*\)?");
    let model = regex.replace(model, "");
    model.replace(brand, "").trim().to_string()
}

pub fn format_years(input: &str) -> String {
    let regex = regex!(r"(?i)([^ ])?гг(\.)?");
    let no_space = regex
        .captures(input.as_ref())
        .and_then(|c| c.get(1))
        .is_some();
    if no_space {
        regex.replace_all(input, r"$1 годов").to_string()
    } else {
        regex.replace_all(input, r"годов").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_years() {
        assert_eq!(("2005-2010 годов"), format_years("2005-2010гг."));
        assert_eq!(("2005-2010 годов"), format_years("2005-2010гг"));
        assert_eq!(("2005-2010 годов"), format_years("2005-2010 гг"));
        assert_eq!(("2005-2010 годов"), format_years("2005-2010 гг."));
    }

    #[test]
    fn formats_replica() {
        let format_replica = |input| format_replica(input).trim().to_string();

        let input = "реплика воздухозаборников модели 540i";
        assert_eq!("воздухозаборники модели 540i", format_replica(input));

        let input = "качественная реплика Alpina";
        assert_eq!("Alpina", format_replica(input));

        let input = "Качественная реплика спойлера BMW E65 Schnitzer";
        assert_eq!("спойлер BMW E65 Schnitzer", format_replica(input));

        let input = "качественная реплика спойлера S-line";
        assert_eq!("спойлер S-line", format_replica(input));

        let input = "Точная копия решетки GTI";
        assert_eq!("решетка GTI", format_replica(input));

        let input = "Точная копия оригинальных накладок";
        assert_eq!("накладки", format_replica(input));

        let input = "Качественная реплика спойлера BMW E65 Schnitzer <abc>";
        assert_eq!("спойлер BMW E65 Schnitzer <abc>", format_replica(input));

        let input = "<cde> Качественная реплика спойлера BMW E65 Schnitzer <abc>";
        assert_eq!(
            "<cde> спойлер BMW E65 Schnitzer <abc>",
            format_replica(input)
        );
        let input = "<cde> Качественная копия спойлера BMW E65 Schnitzer <abc>";
        assert_eq!(
            "<cde> спойлер BMW E65 Schnitzer <abc>",
            format_replica(input)
        );
        let input = "<cde> Качественная Копия! спойлера BMW E65 Schnitzer <abc>";
        assert_eq!(
            "<cde> спойлер BMW E65 Schnitzer <abc>",
            format_replica(input)
        );

        let input = "<span>Двойные насадки на выхлопные трубы в стиле W222 S63 AMG</span><br><span>материал: нержавеющая сталь<br></span>не оригинал<br>цена за комплект (2 шт)<br><br>";
        assert_eq!(
            "<span>Двойные насадки на выхлопные трубы в стиле W222 S63 AMG</span><br><span>материал: нержавеющая сталь<br></span><br>цена за комплект (2 шт)<br><br>",
            format_replica(input)
        );

        let input = "подходит для реплики бампера";
        assert_eq!("подходит для бампера", format_replica(input),);
    }

    #[test]
    fn trims_images() {
        let input = "<p>Hello world</p>";
        assert_eq!(input, trim_images(input));

        let input = "<p>Hello <img src=\"test\" /></p>";
        assert_eq!("<p>Hello </p>", trim_images(input));

        let input = "<p>Hello world</p><img src=\"test\">";
        assert_eq!("<p>Hello world</p>", trim_images(input));

        let input = "<img src=\"test\"><p>Hello world</p>";
        assert_eq!("<p>Hello world</p>", trim_images(input));
    }

    #[test]
    fn formats_model() {
        let model = "BMW 318i (2005-2009)";
        let brand = "BMW";
        assert_eq!("318i", format_model(model, brand));

        let model = "318i (2005-2009)";
        let brand = "BMW";
        assert_eq!("318i", format_model(model, brand));

        let model = "318i (2005-)";
        let brand = "BMW";
        assert_eq!("318i", format_model(model, brand));

        let model = "318i 2005-...";
        let brand = "BMW";
        assert_eq!("318i", format_model(model, brand));
    }
}
