use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A raw PostgreSQL NOTIFY payload received on a channel.
#[derive(Debug, Clone)]
pub struct NotifyEvent {
    /// The LISTEN channel name
    pub channel: String,
    /// The raw payload string from NOTIFY
    pub payload: String,
    /// The PID of the backend that sent the notification
    pub pid: i32,
}

// ─────────────────────────────────────────────────────────────────────────────
// Simple pass-through contract
// ─────────────────────────────────────────────────────────────────────────────

/// Used when the downstream just wants the raw payload forwarded verbatim.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleMessage {
    pub channel: String,
    pub payload: String,
    pub pid: i32,
}

impl From<&NotifyEvent> for SimpleMessage {
    fn from(e: &NotifyEvent) -> Self {
        Self {
            channel: e.channel.clone(),
            payload: e.payload.clone(),
            pid: e.pid,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Advanced data-contract message
// ─────────────────────────────────────────────────────────────────────────────

/// A structured upstream-authored message that carries routing metadata
/// *and* business data, allowing a single contract to drive any downstream
/// (RabbitMQ headers, Kafka headers, HTTP headers, env vars for shell, …).
///
/// Upstreams embed this JSON in the NOTIFY payload:
///
/// ```json
/// {
///   "meta": {
///     "routing": {
///       "rabbitmq_exchange": "orders",
///       "rabbitmq_routing_key": "order.created",
///       "rabbitmq_headers": {"x-priority": "1"},
///       "kafka_topic": "orders",
///       "kafka_key": "order-42",
///       "webhook_url": "https://example.com/hooks/orders",
///       "webhook_headers": {"Authorization": "Bearer token"},
///       "shell_env": {"ORDER_ID": "42"}
///     },
///     "schema_version": "1",
///     "event_type": "order.created"
///   },
///   "data": { ... arbitrary business object ... }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractMessage {
    pub meta: MessageMeta,
    /// Arbitrary business payload — whatever the upstream chose to emit.
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MessageMeta {
    pub routing: RoutingSpec,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    pub event_type: Option<String>,
}

fn default_schema_version() -> String {
    "1".to_string()
}

/// Per-downstream routing hints embedded by the upstream caller.
/// All fields are optional; each downstream uses only its own slice.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingSpec {
    // ── RabbitMQ ──────────────────────────────────────────────────────────────
    pub rabbitmq_exchange: Option<String>,
    pub rabbitmq_routing_key: Option<String>,
    /// Extra AMQP headers forwarded verbatim.
    #[serde(default)]
    pub rabbitmq_headers: HashMap<String, String>,

    // ── Kafka ─────────────────────────────────────────────────────────────────
    pub kafka_topic: Option<String>,
    pub kafka_key: Option<String>,
    /// Extra Kafka record headers forwarded verbatim.
    #[serde(default)]
    pub kafka_headers: HashMap<String, String>,

    // ── Webhook ───────────────────────────────────────────────────────────────
    /// Override the webhook URL for this message only.
    pub webhook_url: Option<String>,
    /// Extra HTTP headers forwarded verbatim.
    #[serde(default)]
    pub webhook_headers: HashMap<String, String>,

    // ── Shell ─────────────────────────────────────────────────────────────────
    /// Extra environment variables injected when the shell command runs.
    #[serde(default)]
    pub shell_env: HashMap<String, String>,
}

impl ContractMessage {
    /// Try to deserialize from a raw NOTIFY payload string.
    pub fn try_parse(payload: &str) -> Option<Self> {
        serde_json::from_str(payload).ok()
    }
}
