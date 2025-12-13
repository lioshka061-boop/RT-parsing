use actix::prelude::*;
use actix_session::storage::CookieSessionStore;
use actix_session::SessionMiddleware;
use actix_web::cookie::Key;
use actix_web::middleware::TrailingSlash;
use actix_web::{guard, web::Data, App, HttpServer};
use anyhow::Context as AnyhowContext;
use indicatif::ProgressStyle;
use rand::{distributions, Rng};
use reqwest::cookie::Jar;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use rt_parsing::{
    access,
    category::SqliteCategoryRepository,
    control,
    dt::{self, parser::ParsingOptions},
    export,
    export::ExportService,
    invoice, shop, subscription, tt, watermark,
    watermark::FilesystemWatermarkGroupRepository,
    RateLimiter,
};
use rt_types::category::CategoryRepository;
use rt_types::watermark::WatermarkGroupRepository;
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio_rusqlite::Connection;
use tokio_util::sync::CancellationToken;

static DEFAULT_ACCEPT_ENCODING: &str = "br;q=1.0, gzip;q=0.6, deflate;q=0.4, *;q=0.2";

#[actix_web::main]
async fn main() -> Result<(), anyhow::Error> {
    if let Err(env::VarError::NotPresent) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "INFO,html5ever=error");
    }
    pretty_env_logger::formatted_timed_builder()
        .parse_default_env()
        .init();

    match std::fs::File::open(".env") {
        Ok(_) => envmnt::load_file(".env")?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::File::create(".env")?;
            envmnt::load_file(".env")?;
        }
        Err(err) => {
            panic!("Unable to open .env file: {err}");
        }
    }

    let conn = Connection::open("storage/storage_dt.db").await?;
    let dt_repo: Arc<dyn dt::product::ProductRepository + Send> =
        Arc::new(dt::product::SqliteProductRepository::init(conn).await?);

    let conn = Connection::open("storage/storage_tt.db").await?;
    let tt_repo: Arc<dyn tt::product::ProductRepository + Send> =
        Arc::new(tt::product::SqliteProductRepository::init(conn).await?);
    let tt_trans_repo: Arc<dyn tt::product::TranslationRepository> = Arc::new(
        tt::product::FileSystemTranslationRepository::new("tt_trans.d".to_string()),
    );

    let token = CancellationToken::new();
    let pb_style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40} {pos:>7}/{len:7} {msg}");
    let pb_style = match pb_style {
        Ok(p) => Some(p.progress_chars("=-")),
        Err(err) => {
            log::warn!("Unable to initialize progress bar: {err}");
            None
        }
    };

    let mut map = HeaderMap::new();

    map.append(
        reqwest::header::ACCEPT_ENCODING,
        HeaderValue::from_str(DEFAULT_ACCEPT_ENCODING)?,
    );

    let cookies = Arc::new(Jar::default());
    let url = "https://tuning-tec.com".parse::<reqwest::Url>()?;
    cookies.add_cookie_str("lang=eng", &url);

    let conn = Connection::open("storage/categories.db").await?;
    let category_repository: Arc<dyn CategoryRepository> =
        Arc::new(SqliteCategoryRepository::init(conn.clone()).await?);

    let shop_repository = Arc::new(shop::FileSystemShopRepository::new());
    let shop_service = rt_types::shop::service::ShopService::new(shop_repository).start();

    let mut entries = vec![];
    let mut suspended_shops = vec![];
    for shop in shop_service.send(rt_types::shop::service::List).await?? {
        entries.append(
            &mut shop::read_shop(&shop.id)?
                .export_entries
                .into_iter()
                .map(|e| (shop.id, e))
                .collect(),
        );
        if shop.is_suspended {
            suspended_shops.push(shop.id);
        }
    }
    let currency_service = currency_service::CurrencyService::new().start();

    let postgres_password: String =
        envmnt::get_parse("POSTGRES_PASSWORD").context("POSTGRES_PASSWORD not set")?;
    let postgres_username: String =
        envmnt::get_parse("POSTGRES_USER").context("POSTGRES_USER not set")?;
    let postgres_host: String =
        envmnt::get_parse("POSTGRES_HOST").context("POSTGRES_USER not set")?;
    let (mut client, connection) = tokio_postgres::connect(
        &format!("host={postgres_host} user={postgres_username} dbname={postgres_username} password={postgres_password}"),
        tokio_postgres::NoTls,
    )
    .await.context("Unable to connect to postgres db")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("connection error: {}", e);
        }
    });

    rt_parsing::migrations::runner()
        .run_async(&mut client)
        .await?;

    let client = Arc::new(client);
    let user_credentials_repository =
        Arc::new(access::repository::PostgresUserCredentialsRepository::new(client.clone()).await?);
    let user_credentials_service =
        rt_types::access::service::UserCredentialsService::new(user_credentials_repository).start();

    let subscription_repository = Arc::new(
        subscription::repository::PostgresSubscriptionRepository::new(client.clone()).await?,
    );
    let subscription_service =
        rt_types::subscription::service::SubscriptionService::new(subscription_repository).start();

    let watermark_group_repository: Arc<dyn WatermarkGroupRepository> =
        Arc::new(FilesystemWatermarkGroupRepository::new());
    let watermark_service =
        rt_types::watermark::service::WatermarkService::new(watermark_group_repository.clone())
            .start();

    let payment_repository: Arc<dyn subscription::payment::PaymentRepository> =
        Arc::new(subscription::payment::PostgresPaymentRepository::new(client.clone()).await?);
    let payment_service =
        subscription::payment::service::PaymentService::new(payment_repository.clone()).start();

    let davi_repo: Arc<dyn rt_parsing_davi::ProductRepository> = Arc::new(
        rt_parsing_davi::PostgresProductRepository::new(client.clone()),
    );

    let client = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .http2_adaptive_window(true)
        .use_rustls_tls()
        .default_headers(map)
        .build()?;

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let davi_client = ClientBuilder::new(client.clone())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .with(reqwest_ratelimit::all(RateLimiter::new(240)))
        .build();

    let davi_service = rt_parsing_davi::ParserService::new(rt_parsing_davi::ParsingOptions {
        client: davi_client,
        repo: davi_repo.clone(),
    })
    .start();

    let wayforpay_secret_key: String =
        envmnt::get_parse("WAYFORPAY_SECRET_KEY").context("WAYFORPAY_SECRET_KEY not set")?;
    let wayforpay_merchant_account: String = envmnt::get_parse("WAYFORPAY_MERCHANT_ACCOUNT")
        .context("WAYFORPAY_MERCHANT_ACCOUNT not set")?;
    let invoice_service = invoice::service::InvoiceService::new(
        wayforpay_secret_key,
        wayforpay_merchant_account,
        client.clone(),
    )
    .start();

    let export_service = ExportService::new(
        client.clone(),
        entries,
        tt_repo.clone(),
        tt_trans_repo.clone(),
        dt_repo.clone(),
        davi_repo.clone(),
        category_repository.clone(),
        shop_service.clone(),
        currency_service,
    )
    .start();

    for shop in suspended_shops {
        export_service
            .send(export::SuspendByShop(shop, true))
            .await??;
    }

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .with(reqwest_ratelimit::all(RateLimiter::new(30)))
        .build();

    let options = ParsingOptions::new(
        "http://design-tuning.com".to_string(),
        dt_repo.clone(),
        client.clone(),
        None,
        50,
    );

    let t = token.clone();
    tokio::spawn(async {
        let token = t;
        match signal::ctrl_c().await {
            Ok(_) => token.cancel(),
            Err(err) => log::error!("Unable to listen to shutdown: {err}"),
        }
    });
    let secret_key = match envmnt::get_parse("SESSION_KEY") {
        Ok(v) => v,
        Err(envmnt::errors::EnvmntError::Missing(_)) => {
            let key = rand::thread_rng()
                .sample_iter(distributions::Alphanumeric)
                .take(64)
                .map(char::from)
                .collect::<String>();
            let mut f = std::fs::File::options().append(true).open(".env")?;
            f.write_all(format!("SESSION_KEY={key}").as_bytes())?;
            key
        }
        Err(err) => {
            panic!("Unable to read secret key: {err}")
        }
    };
    log::info!("{secret_key}");
    let opts = tt::parser::ParsingOptions::new(
        "https://tuning-tec.com".to_string(),
        client,
        tt_repo.clone(),
        tt_trans_repo.clone(),
        None,
    );
    let tt_service = tt::parser::ParserService::new(opts).start();
    let dt_service =
        dt::parser::ParserService::new(options.clone(), pb_style.clone(), token.clone()).start();
    let secret_key = Key::from(secret_key.as_bytes());
    HttpServer::new(move || {
        App::new()
            .wrap(actix_web::middleware::Compress::default())
            .wrap(control::SessionMiddlewareFactory {})
            .wrap(
                SessionMiddleware::builder(CookieSessionStore::default(), secret_key.clone())
                    .cookie_http_only(false)
                    .cookie_secure(false)
                    .build(),
            )
            .wrap(actix_web::middleware::NormalizePath::new(
                TrailingSlash::Trim,
            ))
            .app_data(Data::new(category_repository.clone()))
            .app_data(Data::new(dt_repo.clone()))
            .app_data(Data::new(Arc::new(dt_service.clone())))
            .app_data(Data::new(Arc::new(tt_service.clone())))
            .app_data(Data::new(Arc::new(export_service.clone())))
            .app_data(Data::new(export_service.clone()))
            .app_data(Data::new(davi_service.clone()))
            .app_data(Data::new(shop_service.clone()))
            .app_data(Data::new(user_credentials_service.clone()))
            .app_data(Data::new(subscription_service.clone()))
            .app_data(Data::new(watermark_group_repository.clone()))
            .app_data(Data::new(watermark_service.clone()))
            .app_data(Data::new(payment_service.clone()))
            .app_data(Data::new(invoice_service.clone()))
            .service(actix_files::Files::new("/static", "static"))
            .service(actix_files::Files::new("/export", "export"))
            .service(access::controllers::shops)
            .service(access::controllers::register)
            .service(access::controllers::register_page)
            .service(access::controllers::log_in)
            .service(access::controllers::log_out)
            .service(access::controllers::login_page)
            .service(access::controllers::me_subscription_page)
            .service(access::controllers::me_subscriptions_page)
            .service(access::controllers::apply_subscription)
            .service(access::controllers::me_page)
            .service(subscription::controllers::subscriptions_page)
            .service(subscription::controllers::subscription_versions_page)
            .service(subscription::controllers::add_subscription_page)
            .service(subscription::controllers::add_subscription)
            .service(subscription::controllers::edit_subscription_page)
            .service(subscription::controllers::edit_subscription)
            .service(subscription::controllers::remove_subscription)
            .service(subscription::controllers::copy_subscription)
            .service(subscription::controllers::pay)
            .service(subscription::controllers::confirm_invoice)
            .service(control::control_panel)
            .service(control::control_panel_shops)
            .service(control::control_panel_users)
            .service(control::control_panel_edit_user_page)
            .service(control::control_panel_edit_user)
            .service(control::control_panel_settings)
            .service(shop::controllers::remove_shop_page)
            .service(shop::controllers::remove_shop)
            .service(shop::controllers::add_shop_page)
            .service(shop::controllers::add_shop)
            .service(shop::controllers::shop_suspend_toggle)
            .service(control::parsing)
            .service(control::dt_parse)
            .service(control::dt_parse_page)
            .service(control::dt_product_info)
            .service(control::stop_dt)
            .service(control::resume_dt)
            .service(control::start_export)
            .service(control::start_export_all)
            .service(control::add_export)
            .service(control::remove_export)
            .service(control::export_info)
            .service(control::update_export)
            .service(control::update_export_dt)
            .service(control::update_export_maxton)
            .service(control::update_export_pl)
            .service(control::update_export_dt_tt)
            .service(control::update_export_skm)
            .service(control::update_export_jgd)
            .service(control::update_export_tt)
            .service(control::update_export_davi)
            .service(control::add_export_link)
            .service(control::add_export_dt)
            .service(control::add_export_maxton)
            .service(control::add_export_pl)
            .service(control::add_export_dt_tt)
            .service(control::add_export_skm)
            .service(control::add_export_jgd)
            .service(control::add_export_tt)
            .service(control::add_export_davi)
            .service(control::remove_export_link)
            .service(control::remove_export_dt)
            .service(control::remove_export_maxton)
            .service(control::remove_export_pl)
            .service(control::remove_export_dt_tt)
            .service(control::remove_export_skm)
            .service(control::remove_export_jgd)
            .service(control::remove_export_tt)
            .service(control::remove_export_davi)
            .service(control::update_export_link)
            .service(control::upload_description_file)
            .service(control::remove_description_file)
            .service(control::copy_export)
            .service(control::categories_page)
            .service(control::update_category)
            .service(control::delete_category)
            .service(control::add_category)
            .service(control::import_categories)
            .service(control::clear_categories)
            .service(control::import_categories_page)
            .service(control::category_page)
            .service(control::descriptions_page)
            .service(control::export_page)
            .service(watermark::preview)
            .service(watermark::remove_watermark_group)
            .service(watermark::remove_watermark_group_entry)
            .service(watermark::watermark_settings)
            .service(watermark::upload_watermark)
            .service(watermark::delete_watermark)
            .service(watermark::update_watermark)
            .service(watermark::generate_watermark_link_page)
            .service(watermark::generate_watermark_link)
            .service(watermark::add_watermark_group)
            .service(watermark::push_watermark_to_group_page)
            .service(watermark::push_watermark_to_group)
            .service(watermark::edit_watermark_group_entry)
            .service(watermark::show_watermark)
            .service(watermark::apply_watermark)
            .service(invoice::controllers::successful_payment)
            .service(shop::controllers::settings_page)
            .service(shop::controllers::update_settings)
            .service(tt::controllers::overview)
            .service(tt::controllers::translation_file)
            .service(tt::controllers::all_translation_file)
            .service(tt::controllers::import_translation_file)
            .service(control::shop_products)
            .service(control::shop_product_update)
            .service(control::export_status_json)
            .service(control::index)
            .service(control::landing::index)
            .service(control::site_api::list_products)
            .service(control::site_api::get_product)
            .service(control::catalog::search)
            .service(control::product::view)
            .default_service(
                actix_web::web::route()
                    .guard(guard::Not(guard::Get()))
                    .to(control::not_found),
            )
    })
    .bind(("0.0.0.0", 8080))
    .unwrap()
    .run()
    .await?;
    Ok(())
}
