#[cfg(feature = "kafka")]
#[allow(clippy::module_inception)]
pub mod kafka {
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use rdkafka::{
        consumer::{CommitMode, Consumer as RdKafkaConsumer, StreamConsumer},
        message::{BorrowedMessage, Headers, Message},
        ClientConfig, TopicPartitionList,
    };
    use std::collections::HashMap;

    use super::super::r#trait::{BrokerMessage, Consumer};

    pub struct KafkaConsumer {
        consumer: StreamConsumer,
        topic: String,
        last_offsets: tokio::sync::Mutex<HashMap<i32, i64>>,
    }

    impl KafkaConsumer {
        pub async fn connect(brokers: &str, topic: &str, group_id: &str) -> Result<Self> {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("group.id", group_id)
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "latest")
                .set("max.poll.interval.ms", "300000")
                .create()
                .context("Failed to create Kafka consumer")?;

            consumer
                .subscribe(&[topic])
                .context("Failed to subscribe to Kafka topic")?;

            Ok(Self {
                consumer,
                topic: topic.to_string(),
                last_offsets: tokio::sync::Mutex::new(HashMap::new()),
            })
        }

        fn msg_to_broker(&self, msg: &BorrowedMessage) -> Option<BrokerMessage> {
            let payload = msg
                .payload()
                .map(|d| String::from_utf8_lossy(d).to_string())
                .unwrap_or_default();

            let mut headers = HashMap::new();
            headers.insert("x-partition".to_string(), msg.partition().to_string());
            headers.insert("x-offset".to_string(), msg.offset().to_string());

            if let Some(key) = msg.key() {
                headers.insert(
                    "x-key".to_string(),
                    String::from_utf8_lossy(key).to_string(),
                );
            }

            if let Some(hdrs) = msg.headers() {
                for i in 0..hdrs.count() {
                    let hdr = hdrs.get(i);
                    let val_str = String::from_utf8_lossy(hdr.value.unwrap_or(b"")).to_string();
                    headers.insert(hdr.key.to_string(), val_str);
                }
            }

            Some(BrokerMessage {
                topic: self.topic.clone(),
                payload,
                headers,
                delivery_tag: Self::encode_tag(msg.partition(), msg.offset()),
            })
        }

        fn encode_tag(partition: i32, offset: i64) -> u64 {
            ((partition as u64) << 32) | (offset as u64)
        }

        fn decode_tag(tag: u64) -> (i32, i64) {
            ((tag >> 32) as i32, (tag & 0xFFFF_FFFF) as i64)
        }
    }

    #[async_trait]
    impl Consumer for KafkaConsumer {
        fn name(&self) -> &str {
            "kafka"
        }

        async fn recv(&self) -> Option<BrokerMessage> {
            match self.consumer.recv().await {
                Ok(msg) => self.msg_to_broker(&msg),
                Err(e) => {
                    tracing::error!(error = %e, "Kafka recv error");
                    None
                }
            }
        }

        async fn ack(&self, tag: u64) -> Result<()> {
            let (partition, offset) = Self::decode_tag(tag);

            let mut offsets = self.last_offsets.lock().await;
            let last = offsets.entry(partition).or_insert(0i64);
            if offset > *last {
                *last = offset;
            }

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(*last + 1))
                .context("Failed to set offset for commit")?;
            self.consumer
                .commit(&tpl, CommitMode::Async)
                .context("Failed to commit Kafka offset")
        }

        async fn nack(&self, tag: u64, requeue: bool) -> Result<()> {
            if !requeue {
                self.ack(tag).await?;
            }
            Ok(())
        }
    }
}
