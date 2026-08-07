#[cfg(feature = "rabbitmq")]
#[allow(clippy::module_inception)]
pub mod rabbitmq {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use futures::StreamExt;
    use lapin::{
        options::{
            BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
            QueueBindOptions, QueueDeclareOptions,
        },
        types::FieldTable,
        Channel, Connection, ConnectionProperties, Consumer as LapinConsumer, Queue,
    };
    use std::collections::HashMap;
    use tracing::warn;

    use super::super::message_id::{derive_message_id, NativeId};
    use super::super::r#trait::{BrokerMessage, Consumer, DeliveryTag, RecvOutcome};

    pub struct RabbitMqConsumer {
        #[allow(dead_code)]
        connection: Connection,
        channel: Channel,
        consumer: tokio::sync::Mutex<LapinConsumer>,
        #[allow(dead_code)]
        amqp_url: String,
        queue: String,
        #[allow(dead_code)]
        exchange: Option<String>,
        #[allow(dead_code)]
        routing_key: Option<String>,
    }

    impl RabbitMqConsumer {
        pub async fn connect(
            amqp_url: &str,
            queue: &str,
            exchange: Option<&str>,
            routing_key: Option<&str>,
            prefetch_count: u16,
        ) -> Result<Self> {
            let conn = Connection::connect(amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;

            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;

            Self::declare_and_consume(&channel, queue, exchange, routing_key).await?;

            // Limit unacknowledged messages per consumer so a slow/long-running
            // consumer cannot be flooded with an unbounded backlog (which
            // exhausts broker memory and eventually trips the ack timeout).
            // 0 means no limit.
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await
                .context("Failed to set consumer prefetch count")?;

            let lapin_consumer = channel
                .basic_consume(
                    queue,
                    "pgx-consume",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .context("Failed to start consumer")?;

            Ok(Self {
                connection: conn,
                channel,
                consumer: tokio::sync::Mutex::new(lapin_consumer),
                amqp_url: amqp_url.to_string(),
                queue: queue.to_string(),
                exchange: exchange.map(|s| s.to_string()),
                routing_key: routing_key.map(|s| s.to_string()),
            })
        }

        async fn declare_and_consume(
            channel: &Channel,
            queue: &str,
            exchange: Option<&str>,
            routing_key: Option<&str>,
        ) -> Result<()> {
            let queue_opts = QueueDeclareOptions {
                durable: true,
                ..Default::default()
            };
            let declared: Queue = channel
                .queue_declare(queue, queue_opts, FieldTable::default())
                .await
                .context("Failed to declare queue")?;

            if let Some(ex) = exchange {
                let rk = routing_key.unwrap_or("");
                channel
                    .exchange_declare(
                        ex,
                        lapin::ExchangeKind::Topic,
                        lapin::options::ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .context("Failed to declare exchange")?;
                channel
                    .queue_bind(
                        declared.name().as_str(),
                        ex,
                        rk,
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .context("Failed to bind queue to exchange")?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Consumer for RabbitMqConsumer {
        fn name(&self) -> &str {
            "rabbitmq"
        }

        async fn recv(&self) -> Result<RecvOutcome> {
            let mut guard = self.consumer.lock().await;
            match guard.next().await {
                Some(Ok(delivery)) => {
                    let payload = String::from_utf8_lossy(&delivery.data).to_string();
                    let mut headers = HashMap::new();
                    headers.insert("x-exchange".to_string(), delivery.exchange.to_string());
                    headers.insert(
                        "x-routing-key".to_string(),
                        delivery.routing_key.to_string(),
                    );
                    let message_id = rabbit_message_id(
                        delivery
                            .properties
                            .message_id()
                            .as_ref()
                            .map(|s| s.as_str()),
                    );

                    Ok(RecvOutcome::Message(BrokerMessage {
                        topic: self.queue.clone(),
                        payload,
                        headers,
                        message_id,
                        delivery_tag: DeliveryTag::from_u64(delivery.delivery_tag),
                    }))
                }
                Some(Err(e)) => Err(e).context("RabbitMQ consumer stream error"),
                None => {
                    // Stream ended — channel/connection closed by broker
                    // (e.g. PRECONDITION_FAILED ack timeout) or network failure.
                    warn!("RabbitMQ consumer stream ended (channel may be closed by broker)");
                    Ok(RecvOutcome::Closed)
                }
            }
        }

        async fn ack(&self, tag: DeliveryTag) -> Result<()> {
            self.channel
                .basic_ack(tag.as_u64(), BasicAckOptions::default())
                .await
                .context("Failed to ack message — channel may be closed by broker (e.g. ack timeout exceeded)")
        }

        async fn nack(&self, tag: DeliveryTag, requeue: bool) -> Result<()> {
            self.channel
                .basic_nack(
                    tag.as_u64(),
                    BasicNackOptions {
                        requeue,
                        multiple: false,
                    },
                )
                .await
                .context("Failed to nack message — channel may be closed by broker")
        }
    }

    /// Derive the stable identity for dedupe from the AMQP `message_id`
    /// property. `None` when the property is missing or empty — there is
    /// deliberately no payload-hash fallback, since two distinct messages with
    /// identical bodies would otherwise falsely dedupe.
    fn rabbit_message_id(property_id: Option<&str>) -> Option<String> {
        let source = match property_id {
            Some(id) if !id.is_empty() => NativeId::Provided(id.to_string()),
            _ => NativeId::None,
        };
        derive_message_id(source)
    }

    #[cfg(test)]
    mod tests {
        use super::rabbit_message_id;
        use crate::consumer::r#trait::DeliveryTag;

        #[test]
        fn message_id_prefers_amqp_property() {
            assert_eq!(rabbit_message_id(Some("msg-1")), Some("msg-1".to_string()));
        }

        #[test]
        fn message_id_is_none_when_property_missing() {
            // No payload hash: identical bodies must not share an identity.
            assert_eq!(rabbit_message_id(None), None);
        }

        #[test]
        fn message_id_ignores_empty_property() {
            assert_eq!(rabbit_message_id(Some("")), None);
        }

        #[test]
        fn delivery_tag_round_trips_u64() {
            for tag in [0, 1, 42, u32::MAX as u64, u64::MAX] {
                let t = DeliveryTag::from_u64(tag);
                assert_eq!(t.as_u64(), tag);
            }
        }
    }
}
