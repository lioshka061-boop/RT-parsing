use crate::xlsx::{format_replica, trim_images};
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, DeflateOption, ZipEntryBuilder};
use quick_xml::escape::escape;
use quick_xml::events::{
    attributes::Attribute, BytesCData, BytesEnd, BytesStart, BytesText, Event,
};
use quick_xml::name::QName;
use quick_xml::writer::Writer;
use rt_types::category::Category;
use rt_types::product::Product;
use rt_types::shop::ExportOptions;
use rt_types::{Availability, DescriptionOptions};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::BuildHasher;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use uuid::Uuid;

pub async fn write_dto_map(
    path: &str,
    items: &HashMap<ExportOptions, Vec<Product>, impl BuildHasher>,
    categories: HashSet<Category, impl BuildHasher>,
    shop_id: &str,
) -> Result<(), anyhow::Error> {
    let map = HashMap::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new());
    let items: HashMap<_, _, _> = items
        .into_iter()
        .map(|(o, i)| {
            (
                o,
                i.into_iter()
                    .filter(|i| !matches!(i.available, Availability::OnOrder))
                    .collect::<Vec<_>>(),
            )
        })
        .fold(map, |mut r, (k, v)| {
            r.insert(k, v);
            r
        });
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
    let mut writer = Writer::new(&mut zip_writer);
    writer
        .write_event_async(Event::Start(BytesStart::new("shop")))
        .await?;
    writer
        .write_event_async(Event::Start(BytesStart::new("categories")))
        .await?;
    for c in categories {
        let id = c.id.as_u64_pair().0.to_string();
        let mut attrs = vec![Attribute {
            key: QName("id".as_bytes()),
            value: id.as_bytes().into(),
        }];
        if let Some((id, _)) = &c.parent_id.as_ref().map(Uuid::as_u64_pair) {
            let parent_id = id.to_string().as_bytes().to_vec();
            attrs.push(Attribute {
                key: QName("parentId".as_bytes()),
                value: parent_id.into(),
            });
        }
        writer
            .create_element("category")
            .with_attributes(attrs)
            .write_text_content_async(BytesText::new(&c.name))
            .await?;
    }
    writer
        .write_event_async(Event::End(BytesEnd::new("categories")))
        .await?;
    writer
        .write_event_async(Event::Start(BytesStart::new("offers")))
        .await?;

    for (o, i) in items {
        let proc_description = |d: &DescriptionOptions| match d {
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
        };
        let description = o.description.as_ref().and_then(proc_description);
        let description_ua = o.description.as_ref().and_then(proc_description);
        for i in i {
            let available = match i.available {
                Availability::Available => "true",
                _ => "false",
            };
            let id = escape(&i.id);
            let in_stock = i
                .in_stock
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            let in_stock = in_stock.as_bytes().into();
            let attrs = vec![
                Attribute {
                    key: QName(b"available"),
                    value: available.as_bytes().into(),
                },
                Attribute {
                    key: QName(b"id"),
                    value: id.as_bytes().into(),
                },
                Attribute {
                    key: QName(b"selling_type"),
                    value: b"r".into(),
                },
                Attribute {
                    key: QName(b"in_stock"),
                    value: in_stock,
                },
            ];

            let title_fn = |s: &str, opts: &ExportOptions, is_ua: bool| -> String {
                escape(&crate::xlsx::build_title(opts, s, is_ua)).to_string()
            };

            let description_fn = format_replica;
            let description_fn: Box<dyn Fn(&str) -> String> = match o.description {
                Some(_) => Box::new(move |s| escape(&description_fn(&trim_images(s))).to_string()),
                None => Box::new(|s| escape(&description_fn(s)).to_string()),
            };

            let price_fn = |p| p * o.adjust_price.unwrap_or(Decimal::ONE);

            writer
                .create_element("offer")
                .with_attributes(attrs)
                .write_inner_content_async::<_, _, quick_xml::Error>(|writer| async {
                    let base_title_ru = if i.title.is_empty() {
                        i.ua_translation
                            .as_ref()
                            .map(|t| t.title.as_str())
                            .unwrap_or("")
                    } else {
                        &i.title
                    };
                    let base_title_ua = i
                        .ua_translation
                        .as_ref()
                        .map(|t| t.title.as_str())
                        .unwrap_or(base_title_ru);
                    writer
                        .create_element("name")
                        .write_text_content_async(BytesText::new(&title_fn(
                            base_title_ru,
                            o,
                            false,
                        )))
                        .await?;
                    writer
                        .create_element("name_ua")
                        .write_text_content_async(BytesText::new(&title_fn(base_title_ua, o, true)))
                        .await?;
                    writer
                        .create_element("barcode")
                        .write_text_content_async(BytesText::new(&escape(&i.article)))
                        .await?;
                    writer
                        .create_element("price")
                        .write_text_content_async(BytesText::new(&price_fn(i.price).to_string()))
                        .await?;
                    writer
                        .create_element("currencyId")
                        .write_text_content_async(BytesText::new(&escape(&i.currency)))
                        .await?;
                    if let Some(c) = i.category {
                        writer
                            .create_element("categoryId")
                            .write_text_content_async(BytesText::new(&escape(
                                &c.as_u64_pair().0.to_string(),
                            )))
                            .await?;
                    }
                    match &description {
                        Some(DescriptionOptions::Replace(d)) => {
                            writer
                                .create_element("description")
                                .write_cdata_content_async(BytesCData::new(description_fn(&d)))
                                .await?;
                        }
                        Some(DescriptionOptions::Append(a)) => {
                            if let Some(d) = &i.description {
                                writer
                                    .create_element("description")
                                    .write_cdata_content_async(BytesCData::new(description_fn(
                                        &format!("{d} {a}"),
                                    )))
                                    .await?;
                            }
                        }
                        None => {
                            if let Some(d) = &i.description {
                                writer
                                    .create_element("description")
                                    .write_cdata_content_async(BytesCData::new(description_fn(&d)))
                                    .await?;
                            }
                        }
                    }
                    match &description_ua {
                        Some(DescriptionOptions::Replace(d)) => {
                            writer
                                .create_element("description_ua")
                                .write_cdata_content_async(BytesCData::new(description_fn(&d)))
                                .await?;
                        }
                        Some(DescriptionOptions::Append(a)) => {
                            if let Some(d) = i
                                .ua_translation
                                .as_ref()
                                .and_then(|t| t.description.as_ref())
                            {
                                writer
                                    .create_element("description_ua")
                                    .write_cdata_content_async(BytesCData::new(description_fn(
                                        &format!("{d} {a}"),
                                    )))
                                    .await?;
                            }
                        }
                        None => {
                            if let Some(d) = i
                                .ua_translation
                                .as_ref()
                                .and_then(|t| t.description.as_ref())
                            {
                                writer
                                    .create_element("description_ua")
                                    .write_cdata_content_async(BytesCData::new(description_fn(&d)))
                                    .await?;
                            }
                        }
                    }
                    if let Some(discount) = &o.discount {
                        writer
                            .create_element("discount")
                            .write_text_content_async(BytesText::new(&format!(
                                "{}%",
                                discount.percent
                            )))
                            .await?;
                    }
                    if let Some(keywords) = &i.keywords {
                        writer
                            .create_element("keywords")
                            .write_text_content_async(BytesText::new(&escape(keywords)))
                            .await?;
                    }
                    for param in &i.params {
                        writer
                            .create_element("param")
                            .with_attribute(Attribute {
                                key: QName(b"name"),
                                value: escape(&param.0).as_bytes().into(),
                            })
                            .write_text_content_async(BytesText::new(&escape(&param.1)))
                            .await?;
                    }
                    for image in i.images.iter().take(10) {
                        writer
                            .create_element("picture")
                            .write_text_content_async(BytesText::new(&escape(image)))
                            .await?;
                    }
                    Ok(writer)
                })
                .await?;
        }
    }
    writer
        .write_event_async(Event::End(BytesEnd::new("offers")))
        .await?;
    writer
        .write_event_async(Event::End(BytesEnd::new("shop")))
        .await?;
    zip_writer.into_inner().close().await?;
    w.close().await?;
    Ok(())
}
