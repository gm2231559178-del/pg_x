use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;

/// Kafka producer handle shared by the sink seams.
///
/// Hides producer construction and the per-record send (topic, key, headers,
/// body) behind one `send` method.
pub struct Kafka {
    producer: FutureProducer,
}

impl Kafka {
    pub fn connect(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    /// Publish `body` to `topic`, optionally keyed, with the given headers.
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&str>,
        headers: &[(String, String)],
        body: &str,
    ) -> Result<()> {
        let topic = topic.to_string();
        let body = body.to_string();
        let key = key.map(|s| s.to_string());

        let mut record: FutureRecord<String, String> = FutureRecord::to(&topic).payload(&body);
        if let Some(key) = &key {
            record = record.key(key);
        }

        let mut owned = OwnedHeaders::new();
        for (k, v) in headers {
            owned = owned.insert(rdkafka::message::Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }
        let record = record.headers(owned);

        self.producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka delivery error: {e}"))?;

        Ok(())
    }
}
