use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// A message received from a broker.
#[allow(dead_code)]
pub struct BrokerMessage {
    /// The queue/topic this message arrived on.
    pub topic: String,
    /// Raw payload body.
    pub payload: String,
    /// Message headers/metadata.
    pub headers: HashMap<String, String>,
    /// Stable identity for this message across redeliveries, when the broker
    /// provides one (Kafka record key or `<partition>:<offset>`; RabbitMQ AMQP
    /// `message_id` property). `None` means no native identity — the payload is
    /// deliberately not hashed, since two distinct identical-bodied messages
    /// would falsely dedupe.
    pub message_id: Option<String>,
    /// Opaque handle for ack/nack (delivery tag, offset, etc.).
    pub delivery_tag: DeliveryTag,
}

/// Opaque handle for ack/nack. Brokers encode their native tag into this, so
/// the packed `(partition, offset)` convention stays out of the interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryTag(u64);

impl DeliveryTag {
    /// Wrap a broker-native tag that fits in a `u64` (e.g. an AMQP delivery tag).
    #[cfg(any(feature = "kafka", feature = "rabbitmq", test))]
    pub fn from_u64(tag: u64) -> Self {
        Self(tag)
    }

    #[cfg(any(feature = "kafka", feature = "rabbitmq"))]
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Encode a Kafka `(partition, offset)` record position.
    #[cfg(feature = "kafka")]
    pub fn kafka(partition: i32, offset: i64) -> Self {
        Self(((partition as u64) << 32) | (offset as u64))
    }

    /// Decode a Kafka `(partition, offset)` record position.
    #[cfg(feature = "kafka")]
    pub fn kafka_position(&self) -> (i32, i64) {
        ((self.0 >> 32) as i32, (self.0 & 0xFFFF_FFFF) as i64)
    }
}

/// Outcome of [`Consumer::recv`]: a message, or a closed consumer.
#[allow(dead_code)]
pub enum RecvOutcome {
    /// A message arrived.
    Message(BrokerMessage),
    /// The consumer ended (connection/channel closed); no more messages.
    Closed,
}

/// Consumer pulls messages from a broker.
#[async_trait]
pub trait Consumer: Send + Sync {
    fn name(&self) -> &str;
    /// Receive the next message, blocking until one arrives. `Err` and
    /// [`RecvOutcome::Closed`] both mean no more messages will come and the
    /// session should escalate to reconnect; `Err` additionally carries the
    /// broker error.
    async fn recv(&self) -> Result<RecvOutcome>;
    /// Acknowledge successful processing.
    async fn ack(&self, tag: DeliveryTag) -> Result<()>;
    /// Negative acknowledgement (requeue = true to redeliver, false to discard/dead-letter).
    async fn nack(&self, tag: DeliveryTag, requeue: bool) -> Result<()>;
}

/// Sink receives a fully composed GraphQL document.
#[async_trait]
pub trait ConsumeSink: Send + Sync {
    fn name(&self) -> &str;
    /// Deliver `doc` to the sink. `msg_id` is `Some` only when idempotent mode
    /// is active, and lets the sink derive a stable key when it has no explicit
    /// document field to key on.
    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()>;
}
