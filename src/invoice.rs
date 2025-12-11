use derive_builder::Builder;
use derive_more::Display;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub mod controllers;
pub mod service;

#[derive(Serialize, Builder, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AcceptPayment {
    pub merchant_account: String,
    pub merchant_domain_name: String,
    pub merchant_signature: String,
    pub service_url: Option<String>,
    pub return_url: Option<String>,
    pub order_reference: String,
    pub order_date: i64,
    pub amount: Decimal,
    #[builder(default = "TransactionType::Auto")]
    pub merchant_transaction_type: TransactionType,
    #[builder(default = "1")]
    pub api_version: u8,
    pub currency: Currency,
    pub product_name: Vec<String>,
    pub product_count: Vec<u16>,
    pub product_price: Vec<Decimal>,
    #[builder(default)]
    pub client_account_id: Option<String>,
    pub regular_mode: RegularMode,
    #[builder(default = "\"preset\".to_string()")]
    pub regular_behavior: String,
    #[builder(default = "0")]
    pub regular_on: u8,
}

impl AcceptPayment {
    pub fn products(&self) -> impl Iterator<Item = (&String, &u16, &Decimal)> {
        self.product_name
            .iter()
            .zip(self.product_count.iter())
            .zip(self.product_price.iter())
            .map(|((a, b), c)| (a, b, c))
    }
}

#[derive(Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct CheckPaymentStatus {
    #[builder(default = "\"CHECK_STATUS\".to_string()", setter(skip))]
    pub transaction_type: String,
    pub merchant_account: String,
    pub order_reference: String,
    pub merchant_signature: String,
    #[builder(default = "2", setter(skip))]
    pub api_version: u8,
}

#[derive(Serialize, Deserialize, Display, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum TransactionStatus {
    #[display("approved")]
    Approved,
    #[display("declined")]
    Declined,
}

#[derive(Deserialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct CheckPaymentStatusResponse {
    pub merchant_account: String,
    pub order_reference: String,
    pub merchant_signature: String,
    pub amount: Decimal,
    pub currency: String,
    pub auth_code: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    #[serde(with = "time::serde::timestamp")]
    pub created_date: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub processing_date: OffsetDateTime,
    pub transaction_status: TransactionStatus,
    pub reason: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AcceptPaymentResponse {
    pub merchant_account: String,
    pub order_reference: String,
    pub merchant_signature: String,
    pub amount: Decimal,
    pub currency: String,
    pub auth_code: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    #[serde(with = "time::serde::timestamp")]
    pub created_date: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub processing_date: OffsetDateTime,
    pub transaction_status: String,
    pub reason: String,
    pub fee: Option<Decimal>,
}

#[derive(Serialize, Display, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RegularMode {
    #[display("client")]
    Cient,
    #[display("none")]
    None,
    #[display("once")]
    Once,
    #[display("daily")]
    Daily,
    #[display("weekly")]
    Weekly,
    #[display("quarterly")]
    Quarterly,
    #[display("monthly")]
    Monthly,
    #[display("halfyearly")]
    Halfyearly,
    #[display("yearly")]
    Yearly,
}

#[derive(Serialize, Builder, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateInvoice {
    #[builder(default = "\"CREATE_INVOICE\".to_string()")]
    pub transaction_type: String,
    pub merchant_account: String,
    #[builder(default)]
    pub merchant_transaction_type: Option<TransactionType>,
    pub merchant_domain_name: String,
    pub merchant_signature: String,
    #[builder(default = "1")]
    pub api_version: usize,
    #[builder(default)]
    pub language: Option<Language>,
    #[builder(default)]
    pub notify_method: Option<NotifyMethod>,
    #[builder(default)]
    pub service_url: Option<String>,
    pub order_reference: String,
    pub order_date: i64,
    pub amount: Decimal,
    pub currency: Currency,
    #[builder(default)]
    pub order_timeout: Option<usize>,
    #[builder(default)]
    pub hold_timeout: Option<usize>,
    pub product_name: Vec<String>,
    pub product_price: Vec<Decimal>,
    pub product_count: Vec<usize>,
    #[builder(default)]
    pub payment_systems: Option<Vec<PaymentSystem>>,
    #[builder(default)]
    pub client_first_name: Option<String>,
    #[builder(default)]
    pub client_last_name: Option<String>,
    #[builder(default)]
    pub client_email: Option<String>,
    #[builder(default)]
    pub client_phone: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceResult {
    pub reason_code: String,
    pub invoice_url: Option<String>,
}

#[derive(Serialize, Display, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransactionType {
    #[display("AUTO")]
    Auto,
    #[display("AUTH")]
    Auth,
    #[display("SALE")]
    Sale,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum Language {
    Ru,
    Ua,
    En,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum NotifyMethod {
    Sms,
    Email,
    Bot,
    All,
}

#[derive(Serialize, Debug, Display, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    #[display("UAH")]
    Uah,
    #[display("USD")]
    Usd,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum PaymentSystem {
    Card,
    GooglePay,
    ApplePay,
    Privat24,
    IpTerminal,
    Delay,
    BankCash,
    QrCode,
    MasterPass,
    VisaCheckout,
    Bot,
    PayParts,
    PayPartsMono,
    PayPartsPrivat,
    PayPartsAbank,
    InstantAbank,
    GlobusPlus,
    PayPartsOschad,
    OnusInstallment,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceConfirmation {
    pub merchant_account: String,
    pub order_reference: String,
    pub merchant_signature: String,
    pub amount: Decimal,
    pub currency: String,
    pub auth_code: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub card_pan: Option<String>,
    pub card_type: Option<String>,
    pub issuer_bank_country: Option<String>,
    pub issuer_bank_name: Option<String>,
    pub rec_token: Option<String>,
    pub transaction_status: String,
    pub reason: Option<String>,
    pub reason_code: Option<String>,
    pub fee: Option<String>,
    pub payment_system: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceConfirmationResponse {
    pub order_reference: String,
    pub status: String,
    #[serde(with = "time::serde::timestamp")]
    pub time: OffsetDateTime,
    pub signature: String,
}
