use crate::control::{render_template, ControllerError, Record, Response};
use crate::invoice::{self, service::InvoiceService, TransactionStatus};
use crate::subscription::payment::{self, service::PaymentService, Payment};
use actix::Addr;
use actix_web::{
    post,
    web::{Data, Query},
};
use askama::Template;
use rt_types::access::UserCredentials;
use serde::Deserialize;
use typesafe_repository::IdentityOf;

#[derive(Template)]
#[template(path = "successful_payment.html")]
pub struct SuccessfulPaymentPage {
    user: Option<UserCredentials>,
    status: TransactionStatus,
    reason: Option<String>,
}

#[derive(Deserialize)]
pub struct SuccessfulPaymentQuery {
    payment: IdentityOf<Payment>,
}

#[post("/invoice/completed")]
pub async fn successful_payment(
    user: Option<Record<UserCredentials>>,
    q: Query<SuccessfulPaymentQuery>,
    invoice_service: Data<Addr<InvoiceService>>,
    payment_service: Data<Addr<PaymentService>>,
) -> Response {
    let q = q.into_inner();
    let user = user.map(|u| u.into_inner().0);
    let res = invoice_service
        .send(invoice::service::CheckPaymentStatus(q.payment))
        .await??;
    if let Some((status, reason)) = res {
        // if let TransactionStatus::Approved = status {
        payment_service
            .send(payment::service::Confirm(q.payment))
            .await??;
        // }
        render_template(SuccessfulPaymentPage {
            user,
            status,
            reason,
        })
    } else {
        Err(ControllerError::NotFound)
    }
}
