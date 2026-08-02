use anyhow::{Context, Result};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::{AMQPValue, FieldTable, ShortString},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use std::collections::BTreeMap;

/// RabbitMQ publisher handle shared by the sink seams.
///
/// Hides connection/channel setup and the publish envelope (persistent, JSON
/// content type, fresh message id) behind one `publish` method. Exchange
/// declaration is a separate method so each seam decides whether to do it.
pub struct Rabbitmq {
    channel: Channel,
}

impl Rabbitmq {
    pub async fn connect(amqp_url: &str) -> Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default())
            .await
            .context("Failed to connect to RabbitMQ")?;
        let channel = conn
            .create_channel()
            .await
            .context("Failed to open AMQP channel")?;

        Ok(Self { channel })
    }

    /// Declare a durable topic exchange (idempotent).
    pub async fn declare_exchange(&self, exchange: &str) -> Result<()> {
        self.channel
            .exchange_declare(
                exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("Failed to declare exchange")?;
        Ok(())
    }

    /// Publish `body` with the given string headers as AMQP table values.
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<()> {
        let mut fields: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
        for (k, v) in headers {
            fields.insert(
                ShortString::from(k.clone()),
                AMQPValue::LongString(v.clone().into()),
            );
        }

        self.channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                body,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2)
                    .with_message_id(uuid::Uuid::new_v4().to_string().into())
                    .with_headers(FieldTable::from(fields)),
            )
            .await
            .context("Failed to publish to RabbitMQ")?
            .await
            .context("Publish confirm failed")?;

        Ok(())
    }
}
