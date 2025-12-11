use crate::control::{render_template, see_other, ControlPanelAccess, Response};
use crate::tt::parser::{self, ParserService};
use crate::tt::product::Translation;
use actix::Addr;
use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{get, post, web::Data};
use anyhow::Context;
use askama::Template;
use rt_types::access::UserCredentials;
use std::sync::Arc;

#[derive(Template)]
#[template(path = "tt/overview.html")]
struct OverviewPage {
    count: usize,
    translated_count: usize,
    user: UserCredentials,
}

#[get("/tt/overview")]
async fn overview(
    service: Data<Arc<Addr<ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
) -> Response {
    let count = service
        .send(parser::GetCount)
        .await
        .context("Unable to send message to ParserService")??;
    let translated_count = service
        .send(parser::GetTranslatedCount)
        .await
        .context("Unable to send message to ParserService")??;
    render_template(OverviewPage {
        count,
        translated_count,
        user,
    })
}

#[get("/tt/translations")]
async fn translation_file(
    service: Data<Arc<Addr<ParserService>>>,
    ControlPanelAccess { .. }: ControlPanelAccess,
) -> Response {
    service
        .send(parser::GenerateCsv)
        .await
        .context("Unable to send message to ParserService")??;
    Ok(see_other("/static/tt_translation.csv"))
}

#[get("/tt/all_translations")]
async fn all_translation_file(
    service: Data<Arc<Addr<ParserService>>>,
    ControlPanelAccess { .. }: ControlPanelAccess,
) -> Response {
    service
        .send(parser::GenerateCsvAll)
        .await
        .context("Unable to send message to ParserService")??;
    Ok(see_other("/static/tt_translation_all.csv"))
}

#[derive(MultipartForm, Debug)]
pub struct ImportTranslationQuery {
    file: TempFile,
}

#[post("/tt/translations")]
async fn import_translation_file(
    service: Data<Arc<Addr<ParserService>>>,
    q: MultipartForm<ImportTranslationQuery>,
    ControlPanelAccess { .. }: ControlPanelAccess,
) -> Response {
    let q = q.into_inner();
    let file = q.file.file.as_file();
    let mut reader = csv::Reader::from_reader(file);
    let translations = reader
        .deserialize()
        .collect::<Result<Vec<Translation>, _>>()
        .context("Unable to deserialize translations")?;
    service
        .send(parser::ImportTranslations(translations))
        .await
        .context("Unable to send message to ParserService")??;
    Ok(see_other("/tt/overview"))
}
