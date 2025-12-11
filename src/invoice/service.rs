use crate::invoice::{
    CheckPaymentStatusBuilder, CheckPaymentStatusResponse, InvoiceConfirmation,
    InvoiceConfirmationResponse, InvoiceResult, TransactionStatus,
};
use crate::subscription::payment::Payment;
use actix::prelude::*;
use anyhow::Context as AnyhowContext;
use hmac::Mac;
use md5::Md5;
use serde::Deserialize;
use std::collections::HashMap;
use time::OffsetDateTime;
use typesafe_repository::IdentityOf;

#[derive(Deserialize)]
pub struct UrlResponse {
    pub url: String,
}

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct CreateInvoice(pub crate::invoice::CreateInvoice);

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct AcceptPayment(pub crate::invoice::AcceptPayment);

#[derive(Message)]
#[rtype(result = "Result<Option<(TransactionStatus, Option<String>)>, anyhow::Error>")]
pub struct CheckPaymentStatus(pub IdentityOf<Payment>);

#[derive(Message)]
#[rtype(result = "Result<InvoiceConfirmationResponse, anyhow::Error>")]
pub struct ConfirmInvoice(pub InvoiceConfirmation);

pub struct InvoiceService {
    invoices: HashMap<String, crate::invoice::CreateInvoice>,
    secret_key: String,
    merchant_account: String,
    client: reqwest::Client,
}

impl InvoiceService {
    pub fn new(secret_key: String, merchant_account: String, client: reqwest::Client) -> Self {
        Self {
            invoices: HashMap::new(),
            secret_key,
            merchant_account,
            client,
        }
    }
}

impl Actor for InvoiceService {
    type Context = Context<Self>;
}

impl Handler<ConfirmInvoice> for InvoiceService {
    type Result = ResponseActFuture<Self, Result<InvoiceConfirmationResponse, anyhow::Error>>;

    fn handle(
        &mut self,
        ConfirmInvoice(confirmation): ConfirmInvoice,
        _: &mut Self::Context,
    ) -> Self::Result {
        let invoice = self.invoices.remove(&confirmation.order_reference);
        let secret_key = self.secret_key.clone();
        Box::pin(
            async move {
                if let Some(_invoice) = invoice {
                    let mut hasher = hmac::Hmac::<Md5>::new_from_slice(secret_key.as_bytes())
                        .context("Unable to init hasher")?;
                    let order_reference = confirmation.order_reference;
                    let status = "accept".to_string();
                    let time = OffsetDateTime::now_utc();
                    let hash_input =
                        format!("{};{};{}", order_reference, status, time.unix_timestamp());
                    hasher.update(&hash_input.as_bytes());
                    Ok(InvoiceConfirmationResponse {
                        order_reference,
                        status,
                        time,
                        signature: format!("{:x}", hasher.finalize().into_bytes()),
                    })
                } else {
                    Err(anyhow::anyhow!("Invoice not found"))
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<CreateInvoice> for InvoiceService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(
        &mut self,
        CreateInvoice(invoice): CreateInvoice,
        _: &mut Self::Context,
    ) -> Self::Result {
        let client = self.client.clone();
        let inv = invoice.clone();
        Box::pin(
            async move {
                let res = client
                    .post("https://api.wayforpay.com/api")
                    .json(&inv)
                    .send()
                    .await
                    .context("Unable to send invoice request")?;
                let res = res.text().await.context("")?;
                println!("{res}");
                let res: InvoiceResult =
                    serde_json::from_str(&res).context("Unable to deserialize invoice result")?;
                if let Some(url) = res.invoice_url {
                    Ok(url)
                } else {
                    Err(anyhow::anyhow!("Invoice creation failed: {res:?}"))
                }
            }
            .into_actor(self)
            .map(|res, act, _| {
                let res = res?;
                // act.invoices.insert(invoice.order_reference, invoice);
                Ok(res)
            }),
        )
    }
}

impl Handler<AcceptPayment> for InvoiceService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(
        &mut self,
        AcceptPayment(payment): AcceptPayment,
        _: &mut Self::Context,
    ) -> Self::Result {
        let client = self.client.clone();
        Box::pin(
            async move {
                let payment = &payment;
                let res = client
                    .post("https://secure.wayforpay.com/pay?behavior=offline")
                    .json(&payment)
                    .send()
                    .await
                    .context("Unable to send accept payment request")?;
                println!("{res:?}");
                let res = res.text().await.context("")?;
                println!("{res}");
                let res: UrlResponse = serde_json::from_str(&res)
                    .context("Unable to deserialize accept payment result")?;
                Ok(res.url)
            }
            .into_actor(self),
        )
    }
}

impl Handler<CheckPaymentStatus> for InvoiceService {
    type Result =
        ResponseActFuture<Self, Result<Option<(TransactionStatus, Option<String>)>, anyhow::Error>>;

    fn handle(
        &mut self,
        CheckPaymentStatus(reference): CheckPaymentStatus,
        _: &mut Self::Context,
    ) -> Self::Result {
        let secret_key = self.secret_key.clone();
        let client = self.client.clone();
        let merchant_account = self.merchant_account.clone();
        Box::pin(
            async move {
                let mut hasher = hmac::Hmac::<Md5>::new_from_slice(secret_key.as_bytes())
                    .context("Unable to init hasher")?;
                let hash_input = format!("{};{}", merchant_account, reference);
                hasher.update(&hash_input.as_bytes());
                let signature = format!("{:x}", hasher.finalize().into_bytes());
                let check_status = CheckPaymentStatusBuilder::default()
                    .merchant_account(merchant_account.clone())
                    .order_reference(reference.to_string())
                    .merchant_signature(signature)
                    .build()?;
                let res = client
                    .post("https://api.wayforpay.com/api")
                    .json(&check_status)
                    .send()
                    .await
                    .context("Unable to send accept payment request")?;
                let text = res.text().await?;
                println!("{text}");
                let res: CheckPaymentStatusResponse = serde_json::from_str(&text)?;
                Ok(Some((res.transaction_status, res.reason)))
            }
            .into_actor(self),
        )
    }
}
