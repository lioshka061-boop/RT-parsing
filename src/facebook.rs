use rt_types::product::Product;
use rt_types::Availability;
use rust_decimal::Decimal;
use serde::{Serialize, Serializer};

#[derive(Serialize)]
pub struct Entry {
    pub id: String,
    pub title: String,
    pub description: String,
    pub availability: String,
    pub condition: Condition,
    pub price: Price,
    pub link: String,
    pub image_link: String,
    pub brand: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    New,
    Refurbished,
    Used,
}

pub struct Price {
    pub amount: Decimal,
    pub currency: String,
}

impl Serialize for Price {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{} {}", self.amount, self.currency))
    }
}

impl From<Product> for Entry {
    fn from(p: Product) -> Entry {
        let availability = match p.available {
            Availability::Available => "in stock",
            Availability::OnOrder | Availability::NotAvailable => "out of stock",
        }
        .to_string();
        Entry {
            id: p.id,
            title: p.title,
            description: p.description.unwrap_or_default(),
            availability,
            condition: Condition::New,
            price: Price {
                amount: p.price,
                currency: p.currency,
            },
            link: "".to_string(),
            image_link: p.images.first().cloned().unwrap_or_default(),
            brand: "rt-auto".to_string(),
        }
    }
}
