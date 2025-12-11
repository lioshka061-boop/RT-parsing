pub mod controllers;
pub mod parser;
pub mod product;

pub use product::Product;

pub mod selectors {
    #![allow(clippy::unwrap_used)]

    use once_cell::sync::Lazy;
    use scraper::Selector;

    pub static BRANDS: Lazy<Selector> = Lazy::new(|| {
        Selector::parse(".car_search_y > div:nth-child(3) > select > option").unwrap()
    });
    pub static CATEGORIES: Lazy<Selector> =
        Lazy::new(|| Selector::parse("#sel_kategoria > option").unwrap());
    pub static AVAILABLE_MODELS: Lazy<Selector> = Lazy::new(|| {
        Selector::parse(".car_search_y > div:nth-child(4) > select > option").unwrap()
    });
    pub static PRODUCT: Lazy<Selector> = Lazy::new(|| Selector::parse(".icon_main_block").unwrap());
    pub static PRODUCT_LINK: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".product_list_pic > a.link").unwrap());
    pub static PRODUCT_CODE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".product_list_code").unwrap());
    pub static PRODUCT_PRICE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".icon_main_block_price_c").unwrap());
    pub static PRODUCT_IMAGE: Lazy<Selector> = Lazy::new(|| {
        Selector::parse(".icon_main_block > .product_list_pic > a.link > img").unwrap()
    });
    pub static PRODUCT_TITLE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".product_list_title_nohover > h2").unwrap());
    pub static PRODUCT_AVAILABILITY_INDICATOR: Lazy<Selector> =
        Lazy::new(|| Selector::parse("div img").unwrap());

    pub static DESCRIPTION_EN: Lazy<Selector> =
        Lazy::new(|| Selector::parse("table tr > td:nth-child(2)").unwrap());
    pub static DESCRIPTION_PL: Lazy<Selector> =
        Lazy::new(|| Selector::parse("table tr > td:nth-child(1)").unwrap());
}
