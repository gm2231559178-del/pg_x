#[cfg(feature = "rabbitmq")]
#[allow(clippy::module_inception)]
pub mod rabbitmq {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use futures::StreamExt;
    use lapin::{
        options::{
            BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueBindOptions,
            QueueDeclareOptions,
        },
        types::FieldTable,
        Channel, Connection, ConnectionProperties, Consumer as LapinConsumer, Queue,
    };
    use std::collections::HashMap;
    use tracing::warn;

    use super::super::r#trait::{BrokerMessage, Consumer};

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
        ) -> Result<Self> {
            let conn = Connection::connect(amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;

            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;

            Self::declare_and_consume(&channel, queue, exchange, routing_key).await?;

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

        fn is_connected(&self) -> bool {
            self.channel.status().connected() && self.connection.status().connected()
        }

        async fn recv(&self) -> Option<BrokerMessage> {
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

                    Some(BrokerMessage {
                        topic: self.queue.clone(),
                        payload,
                        headers,
                        delivery_tag: delivery.delivery_tag,
                    })
                }
                Some(Err(e)) => {
                    warn!(error = %e, "RabbitMQ consumer stream error");
                    None
                }
                None => {
                    // Stream ended — channel/connection closed by broker
                    // (e.g. PRECONDITION_FAILED ack timeout) or network failure.
                    warn!("RabbitMQ consumer stream ended (channel may be closed by broker)");
                    None
                }
            }
        }

        async fn ack(&self, tag: u64) -> Result<()> {
            self.channel
                .basic_ack(tag, BasicAckOptions::default())
                .await
                .context("Failed to ack message — channel may be closed by broker (e.g. ack timeout exceeded)")
        }

        async fn nack(&self, tag: u64, requeue: bool) -> Result<()> {
            self.channel
                .basic_nack(
                    tag,
                    BasicNackOptions {
                        requeue,
                        multiple: false,
                    },
                )
                .await
                .context("Failed to nack message — channel may be closed by broker")
        }
    }
}
