#[cfg(feature = "kafka")]
#[allow(clippy::module_inception)]
pub mod kafka {
    use anyhow::{Context, Result};
    use async_trait::async_trait;

    use crate::downstream::{
        contract::{ContractMessage, NotifyEvent, SimpleMessage},
        delivery::kafka::Kafka,
        sink::Downstream,
    };

    /// Publishes every NOTIFY payload verbatim to a fixed Kafka topic.
    pub struct SimpleKafkaDownstream {
        kafka: Kafka,
        topic: String,
    }

    impl SimpleKafkaDownstream {
        pub fn connect(brokers: &str, topic: impl Into<String>) -> Result<Self> {
            Ok(Self {
                kafka: Kafka::connect(brokers)?,
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
            self.kafka.send(&self.topic, None, &[], &body).await
        }
    }

    /// Parses the NOTIFY payload as a [`ContractMessage`] and uses the
    /// embedded routing hints for topic, key, and record headers.
    pub struct ContractKafkaDownstream {
        kafka: Kafka,
        default_topic: String,
    }

    impl ContractKafkaDownstream {
        pub fn connect(brokers: &str, default_topic: impl Into<String>) -> Result<Self> {
            Ok(Self {
                kafka: Kafka::connect(brokers)?,
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

                let mut headers: Vec<(String, String)> = Vec::new();
                for (k, v) in &r.kafka_headers {
                    headers.push((k.clone(), v.clone()));
                }
                if let Some(et) = &contract.meta.event_type {
                    headers.push(("x-event-type".to_string(), et.clone()));
                }

                self.kafka
                    .send(&topic, r.kafka_key.as_deref(), &headers, &body)
                    .await
            } else {
                // Plain payload fallback
                let msg = SimpleMessage::from(event);
                let body = serde_json::to_string(&msg).context("Serialise SimpleMessage")?;
                self.kafka.send(&self.default_topic, None, &[], &body).await
            }
        }
    }
}
