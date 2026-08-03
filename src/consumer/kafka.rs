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

    use super::super::message_id::{derive_message_id, NativeId};
    use super::super::r#trait::{BrokerMessage, Consumer, DeliveryTag, RecvOutcome};

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

        fn msg_to_broker(&self, msg: &BorrowedMessage) -> BrokerMessage {
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

            BrokerMessage {
                topic: self.topic.clone(),
                payload,
                headers,
                message_id: Some(Self::kafka_message_id(
                    msg.key()
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .as_deref(),
                    msg.partition(),
                    msg.offset(),
                )),
                delivery_tag: DeliveryTag::kafka(msg.partition(), msg.offset()),
            }
        }

        fn kafka_message_id(key: Option<&str>, partition: i32, offset: i64) -> String {
            match key {
                Some(k) if !k.is_empty() => {
                    derive_message_id(NativeId::Provided(k.to_string())).unwrap()
                }
                _ => derive_message_id(NativeId::KafkaPosition(partition, offset)).unwrap(),
            }
        }
    }

    #[async_trait]
    impl Consumer for KafkaConsumer {
        fn name(&self) -> &str {
            "kafka"
        }

        async fn recv(&self) -> Result<RecvOutcome> {
            match self.consumer.recv().await {
                Ok(msg) => Ok(RecvOutcome::Message(self.msg_to_broker(&msg))),
                Err(e) => Err(e).context("Kafka recv failed"),
            }
        }

        async fn ack(&self, tag: DeliveryTag) -> Result<()> {
            let (partition, offset) = tag.kafka_position();

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

        async fn nack(&self, tag: DeliveryTag, requeue: bool) -> Result<()> {
            if !requeue {
                self.ack(tag).await?;
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::KafkaConsumer;
        use crate::consumer::r#trait::DeliveryTag;

        #[test]
        fn message_id_prefers_record_key() {
            assert_eq!(
                KafkaConsumer::kafka_message_id(Some("order-42"), 0, 17),
                "order-42"
            );
        }

        #[test]
        fn message_id_falls_back_to_partition_offset() {
            assert_eq!(KafkaConsumer::kafka_message_id(None, 3, 42), "3:42");
        }

        #[test]
        fn message_id_ignores_empty_key() {
            assert_eq!(KafkaConsumer::kafka_message_id(Some(""), 3, 42), "3:42");
        }

        #[test]
        fn delivery_tag_round_trips_partition_offset() {
            // The packed convention splits the u64 as (partition << 32) | offset,
            // so offsets must fit in 32 bits — mirroring the original encoding.
            let cases = [(0, 0), (3, 42), (1024, u32::MAX as i64), (i32::MAX, 5)];
            for (partition, offset) in cases {
                let tag = DeliveryTag::kafka(partition, offset);
                assert_eq!(tag.kafka_position(), (partition, offset));
            }
        }
    }
}
