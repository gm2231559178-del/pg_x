#[cfg(feature = "kafka")]
#[allow(clippy::module_inception)]
pub mod kafka {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use rdkafka::{
        config::ClientConfig,
        producer::{FutureProducer, FutureRecord},
        util::Timeout,
    };
    use std::time::Duration;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        sink::Downstream,
    };

    /// Publishes every NOTIFY payload verbatim to a fixed Kafka topic.
    pub struct SimpleKafkaDownstream {
        producer: FutureProducer,
        topic: String,
    }

    impl SimpleKafkaDownstream {
        pub fn connect(brokers: &str, topic: impl Into<String>) -> Result<Self> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .context("Failed to create Kafka producer")?;

            Ok(Self {
                producer,
                topic: topic.into(),
            })
        }
    }

    #[async_trait]
    impl Downstream for SimpleKafkaDownstream {
        fn name(&self) -> &str {
            "kafka-simple"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            let msg = SimpleMessage::from(event);
            let body = serde_json::to_string(&msg).context("Serialise SimpleMessage")?;

            self.producer
                .send(
                    FutureRecord::<String, String>::to(&self.topic).payload(&body),
                    Timeout::After(Duration::from_secs(5)),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("Kafka delivery error: {}", e))?;

            Ok(())
        }
    }

    /// Parses the NOTIFY payload as a [`ContractMessage`] and uses the
    /// embedded routing hints for topic, key, and record headers.
    pub struct ContractKafkaDownstream {
        producer: FutureProducer,
        default_topic: String,
    }

    impl ContractKafkaDownstream {
        pub fn connect(brokers: &str, default_topic: impl Into<String>) -> Result<Self> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .context("Failed to create Kafka producer")?;

            Ok(Self {
                producer,
                default_topic: default_topic.into(),
            })
        }
    }

    #[async_trait]
    impl Downstream for ContractKafkaDownstream {
        fn name(&self) -> &str {
            "kafka-contract"
        }

        async fn send(&self, event: &NotifyEvent) -> Result<()> {
            if let Some(contract) = ContractMessage::try_parse(&event.payload) {
                let r = &contract.meta.routing;
                let topic = r
                    .kafka_topic
                    .as_deref()
                    .unwrap_or(&self.default_topic)
                    .to_string();

                let body =
                    serde_json::to_string(&contract.data).context("Serialise contract data")?;

                let mut record: FutureRecord<String, String> =
                    FutureRecord::to(&topic).payload(&body);

                if let Some(key) = &r.kafka_key {
                    record = record.key(key);
                }

                // Attach Kafka headers from routing spec.
                let mut headers = rdkafka::message::OwnedHeaders::new();
                for (k, v) in &r.kafka_headers {
                    headers = headers.insert(rdkafka::message::Header {
                        key: k,
                        value: Some(v.as_bytes()),
                    });
                }
                if let Some(et) = &contract.meta.event_type {
                    headers = headers.insert(rdkafka::message::Header {
                        key: "x-event-type",
                        value: Some(et.as_bytes()),
                    });
                }
                let record = record.headers(headers);

                self.producer
                    .send(record, Timeout::After(Duration::from_secs(5)))
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("Kafka delivery error: {}", e))?;
            } else {
                // Plain payload fallback
                let msg = SimpleMessage::from(event);
                let body = serde_json::to_string(&msg).context("Serialise SimpleMessage")?;

                self.producer
                    .send(
                        FutureRecord::<String, String>::to(&self.default_topic).payload(&body),
                        Timeout::After(Duration::from_secs(5)),
                    )
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("Kafka delivery error: {}", e))?;
            }

            Ok(())
        }
    }
}
