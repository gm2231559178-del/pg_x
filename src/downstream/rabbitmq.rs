#[cfg(feature = "rabbitmq")]
#[allow(clippy::module_inception)]
pub mod rabbitmq {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use lapin::{
        options::{BasicPublishOptions, ExchangeDeclareOptions},
        types::{AMQPValue, FieldTable, ShortString},
        BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    };
    use std::collections::BTreeMap;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        sink::Downstream,
    };

    // ─────────────────────────────────────────────────────────────────────────
    // Simple mode
    // ─────────────────────────────────────────────────────────────────────────

    /// Publishes every NOTIFY payload verbatim as the AMQP body.
    /// Exchange and routing key are fixed at construction time.
    pub struct SimpleRabbitMqDownstream {
        channel: Channel,
        exchange: String,
        routing_key: String,
    }

    impl SimpleRabbitMqDownstream {
        pub async fn connect(
            amqp_url: &str,
            exchange: impl Into<String>,
            routing_key: impl Into<String>,
        ) -> Result<Self> {
            let exchange = exchange.into();
            let routing_key = routing_key.into();

            let conn = Connection::connect(amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;

            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;

            // Declare the exchange as durable topic (idempotent).
            channel
                .exchange_declare(
                    &exchange,
                    ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .context("Failed to declare exchange")?;

            Ok(Self {
                channel,
                exchange,
                routing_key,
            })
        }
    }

    #[async_trait]
    impl Downstream for SimpleRabbitMqDownstream {
        fn name(&self) -> &str {
            "rabbitmq-simple"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let msg = SimpleMessage::from(event);
            let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;

            self.channel
                .basic_publish(
                    &self.exchange,
                    &self.routing_key,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default()
                        .with_content_type("application/json".into())
                        .with_delivery_mode(2), // persistent
                )
                .await
                .context("Failed to publish to RabbitMQ")?
                .await
                .context("Publish confirm failed")?;

            Ok(())
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Contract mode
    // ─────────────────────────────────────────────────────────────────────────

    /// Parses the NOTIFY payload as a [`ContractMessage`] and uses the
    /// embedded `routing` hints for exchange, routing key, and AMQP headers.
    /// Falls back to the configured defaults when hints are absent.
    pub struct ContractRabbitMqDownstream {
        channel: Channel,
        default_exchange: String,
        default_routing_key: String,
    }

    impl ContractRabbitMqDownstream {
        pub async fn connect(
            amqp_url: &str,
            default_exchange: impl Into<String>,
            default_routing_key: impl Into<String>,
        ) -> Result<Self> {
            let default_exchange = default_exchange.into();
            let default_routing_key = default_routing_key.into();

            let conn = Connection::connect(amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;

            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;

            Ok(Self {
                channel,
                default_exchange,
                default_routing_key,
            })
        }
    }

    #[async_trait]
    impl Downstream for ContractRabbitMqDownstream {
        fn name(&self) -> &str {
            "rabbitmq-contract"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            // Try to parse a ContractMessage; fall back to raw payload.
            let (exchange, routing_key, amqp_headers, body) = if let Some(contract) =
                ContractMessage::try_parse(&event.payload)
            {
                let r = &contract.meta.routing;

                let exchange = r
                    .rabbitmq_exchange
                    .clone()
                    .unwrap_or_else(|| self.default_exchange.clone());

                let routing_key = r
                    .rabbitmq_routing_key
                    .clone()
                    .unwrap_or_else(|| self.default_routing_key.clone());

                // Build AMQP FieldTable from the contract headers.

                let mut fields: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();

                for (k, v) in &r.rabbitmq_headers {
                    fields.insert(
                        ShortString::from(k.clone()),
                        AMQPValue::LongString(v.clone().into()),
                    );
                }

                // Inject envelope metadata
                if let Some(et) = &contract.meta.event_type {
                    fields.insert(
                        ShortString::from("x-event-type"),
                        AMQPValue::LongString(et.clone().into()),
                    );
                }

                fields.insert(
                    ShortString::from("x-pg-channel"),
                    AMQPValue::LongString(event.channel.clone().into()),
                );

                fields.insert(
                    ShortString::from("x-schema-version"),
                    AMQPValue::LongString(contract.meta.schema_version.clone().into()),
                );

                let body = serde_json::to_vec(&contract.data).context("Serialise contract data")?;

                (exchange, routing_key, FieldTable::from(fields), body)
            } else {
                // Plain payload — envelope it so consumers get consistent shape.
                let msg = SimpleMessage::from(event);
                let body = serde_json::to_vec(&msg).context("Serialise SimpleMessage")?;
                (
                    self.default_exchange.clone(),
                    self.default_routing_key.clone(),
                    FieldTable::default(),
                    body,
                )
            };

            self.channel
                .basic_publish(
                    &exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default()
                        .with_content_type("application/json".into())
                        .with_delivery_mode(2)
                        .with_headers(amqp_headers),
                )
                .await
                .context("Failed to publish to RabbitMQ")?
                .await
                .context("Publish confirm failed")?;

            Ok(())
        }
    }
}
