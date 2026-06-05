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
    /// Opaque handle for ack/nack (delivery tag, offset, etc.).
    pub delivery_tag: u64,
}

/// Consumer pulls messages from a broker.
#[async_trait]
pub trait Consumer: Send + Sync {
    fn name(&self) -> &str;
    /// Receive the next message, blocking until one arrives.
    async fn recv(&self) -> Option<BrokerMessage>;
    /// Acknowledge successful processing.
    async fn ack(&self, tag: u64) -> Result<()>;
    /// Negative acknowledgement (requeue = true to redeliver, false to discard/dead-letter).
    async fn nack(&self, tag: u64, requeue: bool) -> Result<()>;
}

/// Sink receives a fully composed GraphQL document.
#[async_trait]
pub trait ConsumeSink: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, doc: &Value) -> Result<()>;
}
