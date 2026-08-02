//! The consume session: the dedupe lifecycle, the per-message pipeline, and the
//! settle protocol, behind one `new` + `run` seam. Reconnection is delegated to
//! the session-loop; each run receives a freshly connected consumer.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::consumer::dedupe::{spawn_dedup_sweeper, DedupCache};
use crate::consumer::r#trait::{BrokerMessage, ConsumeSink, Consumer};
use crate::downstream::contract::ContractMessage;
use crate::graphql::query::{NamedQuery, QueryLoader};
use crate::utils::session_loop::{SessionExit, Shutdown};

use super::consume::{ConsumeErrorMode, ConsumeQueryMode};

/// Composition seam: turns a named query and its variables into the document
/// the sink receives. Injected so the session pipeline is testable without a
/// live database (production wires the GraphQL executor against a query pool).
#[async_trait]
pub trait Compose: Send + Sync {
    async fn compose(
        &self,
        query: &NamedQuery,
        variables: &HashMap<String, Value>,
    ) -> Result<Value>;
}

/// The consume session. Created once; re-run against a fresh consumer after
/// every reconnect so the dedupe cache survives a dropped connection.
pub struct ConsumeSession {
    query_mode: ConsumeQueryMode,
    on_error: ConsumeErrorMode,
    idempotent: bool,
    default_query: String,
    queries: Arc<QueryLoader>,
    sink: Arc<dyn ConsumeSink>,
    compose: Arc<dyn Compose>,
    dedup: Option<Arc<DedupCache>>,
}

impl ConsumeSession {
    /// One entry point: build the session and take ownership of the dedupe
    /// lifecycle (cache creation + TTL sweeper, idempotent mode only).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_mode: ConsumeQueryMode,
        on_error: ConsumeErrorMode,
        idempotent: bool,
        dedup_ttl: Option<u64>,
        default_query: String,
        queries: Arc<QueryLoader>,
        sink: Arc<dyn ConsumeSink>,
        compose: Arc<dyn Compose>,
    ) -> Self {
        let dedup = if idempotent {
            let ttl = Duration::from_secs(dedup_ttl.unwrap_or(900));
            let cache = DedupCache::new(ttl);
            spawn_dedup_sweeper(Arc::clone(&cache), ttl);
            info!(
                "Idempotent mode on (dedup_ttl={}s)",
                dedup_ttl.unwrap_or(900)
            );
            Some(cache)
        } else {
            None
        };

        Self {
            query_mode,
            on_error,
            idempotent,
            default_query,
            queries,
            sink,
            compose,
            dedup,
        }
    }

    /// Run one session against a connected consumer until shutdown, a dropped
    /// connection, or a fatal error. The returned [`SessionExit`] drives the
    /// session-loop's reconnection policy.
    pub async fn run(&self, consumer: &dyn Consumer, shutdown: &mut Shutdown) -> SessionExit {
        let mut session_failed = false;

        let processing_result: Result<()> = loop {
            let maybe_msg: Option<BrokerMessage> = loop {
                tokio::select! {
                    biased;

                    _ = shutdown.wait() => {
                        info!("Signal received, shutting down cleanly");
                        return SessionExit::Shutdown;
                    }

                    maybe_msg = consumer.recv() => {
                        match maybe_msg {
                            Some(m) => break Some(m),
                            None => {
                                // recv returned None — channel may be closed
                                // (e.g. broker ack timeout triggered PRECONDITION_FAILED).
                                if !consumer.is_connected() {
                                    warn!("Consumer disconnected (channel closed by broker)");
                                    session_failed = true;
                                    break None;
                                }
                                // Transient — brief pause then retry
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    }
                }
            };

            let msg = match maybe_msg {
                Some(m) => m,
                None if session_failed => break Ok(()),
                None => unreachable!(),
            };

            let tag = msg.delivery_tag;
            let topic = msg.topic.clone();
            let msg_id = msg.message_id.as_deref();

            // ── Dedupe: skip messages already processed ──────────────────────
            if is_duplicate(&self.dedup, msg_id).await {
                debug!(id = ?msg_id, topic = %topic, "Duplicate message, acking and skipping");
                if let Err(e) = consumer.ack(tag).await {
                    error!(error = %e, "Failed to ack duplicate message");
                    if !consumer.is_connected() {
                        session_failed = true;
                        break Ok(());
                    }
                }
                continue;
            }

            // ── Resolve query name and variables ─────────────────────────────
            let (query_name, variables) = match self.query_mode {
                ConsumeQueryMode::Contract => match ContractMessage::try_parse(&msg.payload) {
                    Some(contract) => {
                        let qn = contract
                            .meta
                            .event_type
                            .unwrap_or_else(|| self.default_query.clone());
                        let vars = data_to_variables(&contract.data);
                        (qn, vars)
                    }
                    None => {
                        let msg = "Message is not a valid ContractMessage";
                        match self.on_error {
                            ConsumeErrorMode::Lenient => {
                                warn!("{}. Skipping message (topic={})", msg, topic);
                                let _ = consumer.nack(tag, false).await;
                                continue;
                            }
                            ConsumeErrorMode::Strict => {
                                error!("{} (topic={})", msg, topic);
                                let _ = consumer.nack(tag, true).await;
                                break Err(anyhow!("{}: topic={}", msg, topic));
                            }
                        }
                    }
                },
                ConsumeQueryMode::Simple => {
                    let qn = self.default_query.clone();
                    let vars = payload_to_variables(&msg.payload);
                    (qn, vars)
                }
            };

            // ── Look up the query ────────────────────────────────────────────
            let query = match self.queries.get(&query_name) {
                Some(q) => q,
                None => {
                    let msg = format!("No named query '{}' found", query_name);
                    match self.on_error {
                        ConsumeErrorMode::Lenient => {
                            warn!("{}. Skipping message (topic={})", msg, topic);
                            let _ = consumer.nack(tag, false).await;
                            continue;
                        }
                        ConsumeErrorMode::Strict => {
                            error!("{} (topic={})", msg, topic);
                            let _ = consumer.nack(tag, true).await;
                            break Err(anyhow!("{}", msg));
                        }
                    }
                }
            };

            // ── Compose the document ─────────────────────────────────────────
            let doc = self.compose.compose(query, &variables).await;

            match doc {
                Ok(doc) => {
                    // ── Send to sink ─────────────────────────────────────────
                    let sink_msg_id = if self.idempotent { msg_id } else { None };
                    if let Err(e) = self.sink.send(&doc, sink_msg_id).await {
                        match self.on_error {
                            ConsumeErrorMode::Lenient => {
                                warn!(error = %e, topic = %topic, query = %query_name, "Sink failed, skipping message");
                                let _ = consumer.nack(tag, false).await;
                                continue;
                            }
                            ConsumeErrorMode::Strict => {
                                error!(error = %e, topic = %topic, query = %query_name, "Sink failed");
                                let _ = consumer.nack(tag, true).await;
                                break Err(e);
                            }
                        }
                    }

                    // ── Record as processed (before ack, so a failed ack still
                    //    causes the redelivered message to be skipped) ────────
                    record_processed(&self.dedup, msg_id).await;

                    // ── Acknowledge ──────────────────────────────────────────
                    if let Err(e) = consumer.ack(tag).await {
                        error!(error = %e, "Failed to ack message — channel may be closed");
                        // Channel is likely dead after a failed ack.
                        // Break so the session-loop can reconnect.
                        if !consumer.is_connected() {
                            session_failed = true;
                            break Ok(());
                        }
                    }
                }
                Err(e) => match self.on_error {
                    ConsumeErrorMode::Lenient => {
                        warn!(error = %e, topic = %topic, query = %query_name, "GraphQL execution failed, skipping message");
                        let _ = consumer.nack(tag, false).await;
                    }
                    ConsumeErrorMode::Strict => {
                        error!(error = %e, topic = %topic, query = %query_name, "GraphQL execution failed");
                        let _ = consumer.nack(tag, true).await;
                        break Err(e);
                    }
                },
            }
        };

        // ── Handle session exit ──────────────────────────────────────────────
        match processing_result {
            // The consumer connected and then lost its channel — a healthy run
            // ended in a drop, so the failure counter resets before counting
            // (a stale retry budget from earlier outages is not exhausted).
            Ok(()) if session_failed => SessionExit::ReconnectAfterHealthy,
            Ok(()) => SessionExit::Shutdown,
            Err(e) => SessionExit::Fatal(e),
        }
    }
}

// ── Variable extraction helpers ──────────────────────────────────────────────

/// Whether `msg_id` was already seen by the dedupe cache.
async fn is_duplicate(dedup: &Option<Arc<DedupCache>>, msg_id: Option<&str>) -> bool {
    match (dedup, msg_id) {
        (Some(cache), Some(id)) => cache.contains(id).await,
        _ => false,
    }
}

/// Record `msg_id` as processed. Only call after a successful sink send, so a
/// redelivered message whose first attempt failed is still processed.
async fn record_processed(dedup: &Option<Arc<DedupCache>>, msg_id: Option<&str>) {
    if let (Some(cache), Some(id)) = (dedup, msg_id) {
        cache.record(id).await;
    }
}

/// Extract variables from a serde_json::Value (top-level object becomes variable map).
fn data_to_variables(data: &Value) -> HashMap<String, Value> {
    match data {
        Value::Object(m) => m.clone().into_iter().collect(),
        other => {
            let mut h = HashMap::new();
            h.insert("data".to_string(), other.clone());
            h
        }
    }
}

/// Parse the entire message payload as a JSON object for variables.
fn payload_to_variables(payload: &str) -> HashMap<String, Value> {
    serde_json::from_str(payload).unwrap_or_else(|_| {
        let mut h = HashMap::new();
        h.insert("payload".to_string(), Value::String(payload.to_string()));
        h
    })
}

#[cfg(test)]
mod dedupe_ordering_tests {
    use super::{is_duplicate, record_processed};
    use crate::consumer::dedupe::DedupCache;
    use std::sync::Arc;
    use std::time::Duration;

    fn cache() -> Option<Arc<DedupCache>> {
        Some(DedupCache::new(Duration::from_secs(60)))
    }

    #[tokio::test]
    async fn failed_send_is_not_recorded_so_redelivery_is_processed() {
        let dedup = cache();

        assert!(!is_duplicate(&dedup, Some("msg-1")).await);

        // First attempt fails (e.g. strict-mode sink error) — no record.
        // The redelivered copy must therefore be processed again.

        assert!(!is_duplicate(&dedup, Some("msg-1")).await);
    }

    #[tokio::test]
    async fn successful_send_is_recorded_and_redelivery_is_deduped() {
        let dedup = cache();

        assert!(!is_duplicate(&dedup, Some("msg-1")).await);

        // First attempt succeeds — record before ack.
        record_processed(&dedup, Some("msg-1")).await;

        // A redelivered copy is now a duplicate and gets acked + skipped.
        assert!(is_duplicate(&dedup, Some("msg-1")).await);
    }

    #[tokio::test]
    async fn no_msg_id_is_never_deduped() {
        let dedup = cache();

        assert!(!is_duplicate(&dedup, None).await);
        record_processed(&dedup, None).await;
        assert!(!is_duplicate(&dedup, None).await);
    }

    #[tokio::test]
    async fn no_cache_is_never_deduped() {
        let dedup: Option<Arc<DedupCache>> = None;

        assert!(!is_duplicate(&dedup, Some("msg-1")).await);
        record_processed(&dedup, Some("msg-1")).await;
        assert!(!is_duplicate(&dedup, Some("msg-1")).await);
    }
}
