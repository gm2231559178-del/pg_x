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
use crate::consumer::r#trait::{BrokerMessage, ConsumeSink, Consumer, DeliveryTag, RecvOutcome};
use crate::downstream::contract::ContractMessage;
use crate::graphql::query::{NamedQuery, QueryLoader};
use crate::utils::session_loop::{SessionExit, Shutdown};

use super::consume::{ConsumeErrorMode, ConsumeQueryMode, ErrorAction, ErrorStage};

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
            let maybe_msg: Option<BrokerMessage> = tokio::select! {
                biased;

                _ = shutdown.wait() => {
                    info!("Signal received, shutting down cleanly");
                    return SessionExit::Shutdown;
                }

                outcome = consumer.recv() => {
                    match outcome {
                        Ok(RecvOutcome::Message(m)) => Some(m),
                        Ok(RecvOutcome::Closed) => {
                            warn!("Consumer ended — no more messages (channel may be closed by broker)");
                            session_failed = true;
                            None
                        }
                        Err(e) => {
                            warn!(error = %e, "Consumer recv failed — escalating to reconnect");
                            session_failed = true;
                            None
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
                    error!(error = %e, "Failed to ack duplicate message — escalating to reconnect");
                    session_failed = true;
                    break Ok(());
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
                        let message =
                            format!("Message is not a valid ContractMessage (topic={})", topic);
                        if let Err(e) = fail(
                            &self.on_error,
                            consumer,
                            tag,
                            ErrorStage::Parse,
                            &message,
                            anyhow!(message.clone()),
                        )
                        .await
                        {
                            break Err(e);
                        }
                        continue;
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
                    let message =
                        format!("No named query '{}' found (topic={})", query_name, topic);
                    if let Err(e) = fail(
                        &self.on_error,
                        consumer,
                        tag,
                        ErrorStage::Lookup,
                        &message,
                        anyhow!(message.clone()),
                    )
                    .await
                    {
                        break Err(e);
                    }
                    continue;
                }
            };

            // ── Compose the document ─────────────────────────────────────────
            let doc = self.compose.compose(query, &variables).await;

            match doc {
                Ok(doc) => {
                    // ── Send to sink ─────────────────────────────────────────
                    let sink_msg_id = if self.idempotent { msg_id } else { None };
                    if let Err(e) = self.sink.send(&doc, sink_msg_id).await {
                        let message =
                            format!("Sink failed (topic={}, query={})", topic, query_name);
                        if let Err(e) =
                            fail(&self.on_error, consumer, tag, ErrorStage::Sink, &message, e).await
                        {
                            break Err(e);
                        }
                        continue;
                    }

                    // ── Record as processed (before ack, so a failed ack still
                    //    causes the redelivered message to be skipped) ────────
                    record_processed(&self.dedup, msg_id).await;

                    // ── Acknowledge ──────────────────────────────────────────
                    if let Err(e) = consumer.ack(tag).await {
                        error!(error = %e, "Failed to ack message — escalating to reconnect");
                        // Channel is likely dead after a failed ack.
                        // Break so the session-loop can reconnect.
                        session_failed = true;
                        break Ok(());
                    }
                }
                Err(e) => {
                    let message = format!(
                        "GraphQL execution failed (topic={}, query={})",
                        topic, query_name
                    );
                    if let Err(e) = fail(
                        &self.on_error,
                        consumer,
                        tag,
                        ErrorStage::Compose,
                        &message,
                        e,
                    )
                    .await
                    {
                        break Err(e);
                    }
                }
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

/// Fail the current message through the error policy: log, settle with the
/// consumer (nack), and return `Err` when the policy aborts the session. The
/// caller resumes the loop on `Ok`.
async fn fail(
    on_error: &ConsumeErrorMode,
    consumer: &dyn Consumer,
    tag: DeliveryTag,
    stage: ErrorStage,
    message: &str,
    err: anyhow::Error,
) -> Result<()> {
    match on_error.handle(stage) {
        ErrorAction::Discard => {
            warn!(error = %err, "{message} — discarded per lenient policy");
            let _ = consumer.nack(tag, false).await;
            Ok(())
        }
        ErrorAction::Requeue => {
            warn!(error = %err, "{message} — requeued per lenient policy");
            let _ = consumer.nack(tag, true).await;
            Ok(())
        }
        ErrorAction::Abort => {
            error!(error = %err, "{message}");
            let _ = consumer.nack(tag, true).await;
            Err(err)
        }
    }
}

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

#[cfg(test)]
mod error_policy_seam_tests {
    // Drives the session through `run()` with a scripted consumer to assert the
    // settle protocol each failure stage × error mode produces: nack with or
    // without requeue, and the resulting session outcome.
    use super::*;
    use crate::graphql::query::{FieldSelection, NamedQuery, QueryLoader};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::sync::watch;

    struct ScriptedConsumer {
        queue: Mutex<VecDeque<BrokerMessage>>,
        nacks: Mutex<Vec<(DeliveryTag, bool)>>,
        acks: Mutex<Vec<DeliveryTag>>,
    }

    #[async_trait]
    impl Consumer for ScriptedConsumer {
        fn name(&self) -> &str {
            "scripted"
        }

        async fn recv(&self) -> anyhow::Result<RecvOutcome> {
            Ok(self
                .queue
                .lock()
                .unwrap()
                .pop_front()
                .map(RecvOutcome::Message)
                .unwrap_or(RecvOutcome::Closed))
        }

        async fn ack(&self, tag: DeliveryTag) -> anyhow::Result<()> {
            self.acks.lock().unwrap().push(tag);
            Ok(())
        }

        async fn nack(&self, tag: DeliveryTag, requeue: bool) -> anyhow::Result<()> {
            self.nacks.lock().unwrap().push((tag, requeue));
            Ok(())
        }
    }

    struct OkSink;

    #[async_trait]
    impl ConsumeSink for OkSink {
        fn name(&self) -> &str {
            "ok"
        }

        async fn send(&self, _doc: &Value, _msg_id: Option<&str>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct FailingSink;

    #[async_trait]
    impl ConsumeSink for FailingSink {
        fn name(&self) -> &str {
            "failing"
        }

        async fn send(&self, _doc: &Value, _msg_id: Option<&str>) -> anyhow::Result<()> {
            Err(anyhow!("sink down"))
        }
    }

    struct OkCompose;

    #[async_trait]
    impl Compose for OkCompose {
        async fn compose(
            &self,
            _query: &NamedQuery,
            _variables: &HashMap<String, Value>,
        ) -> anyhow::Result<Value> {
            Ok(json!({"ok": true}))
        }
    }

    struct FailingCompose;

    #[async_trait]
    impl Compose for FailingCompose {
        async fn compose(
            &self,
            _query: &NamedQuery,
            _variables: &HashMap<String, Value>,
        ) -> anyhow::Result<Value> {
            Err(anyhow!("graphql down"))
        }
    }

    fn loader_with(name: &str) -> Arc<QueryLoader> {
        let mut queries = HashMap::new();
        queries.insert(
            name.to_string(),
            NamedQuery {
                name: name.to_string(),
                operation_name: "Op".to_string(),
                variables: vec![],
                selection: FieldSelection {
                    field_name: "id".to_string(),
                    children: vec![],
                    is_leaf: true,
                },
            },
        );
        Arc::new(QueryLoader { queries })
    }

    fn session(
        query_mode: ConsumeQueryMode,
        on_error: ConsumeErrorMode,
        sink: Arc<dyn ConsumeSink>,
        compose: Arc<dyn Compose>,
    ) -> Arc<ConsumeSession> {
        Arc::new(ConsumeSession::new(
            query_mode,
            on_error,
            false,
            None,
            "default".to_string(),
            loader_with("default"),
            sink,
            compose,
        ))
    }

    fn msg(tag: u64, payload: &str) -> BrokerMessage {
        BrokerMessage {
            topic: "events".to_string(),
            payload: payload.to_string(),
            headers: HashMap::new(),
            message_id: Some("m1".to_string()),
            delivery_tag: DeliveryTag::from_u64(tag),
        }
    }

    fn scripted(messages: Vec<BrokerMessage>) -> ScriptedConsumer {
        ScriptedConsumer {
            queue: Mutex::new(VecDeque::from(messages)),
            nacks: Mutex::new(Vec::new()),
            acks: Mutex::new(Vec::new()),
        }
    }

    async fn run_session(
        sess: Arc<ConsumeSession>,
        consumer: ScriptedConsumer,
    ) -> (SessionExit, Vec<(DeliveryTag, bool)>) {
        let (tx, rx) = watch::channel(false);
        let _tx = tx;
        let mut shutdown = Shutdown::from_receiver(rx);
        let exit = sess.run(&consumer, &mut shutdown).await;
        let nacks = consumer.nacks.lock().unwrap().clone();
        (exit, nacks)
    }

    #[tokio::test]
    async fn lenient_requeues_sink_failures() {
        let sess = session(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Lenient,
            Arc::new(FailingSink),
            Arc::new(OkCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), true)]);
        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
    }

    #[tokio::test]
    async fn strict_sink_failure_aborts() {
        let sess = session(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Strict,
            Arc::new(FailingSink),
            Arc::new(OkCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), true)]);
        assert!(matches!(exit, SessionExit::Fatal(_)));
    }

    #[tokio::test]
    async fn lenient_discards_compose_failures() {
        let sess = session(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Lenient,
            Arc::new(OkSink),
            Arc::new(FailingCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), false)]);
        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
    }

    #[tokio::test]
    async fn strict_compose_failure_aborts() {
        let sess = session(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Strict,
            Arc::new(OkSink),
            Arc::new(FailingCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), true)]);
        assert!(matches!(exit, SessionExit::Fatal(_)));
    }

    #[tokio::test]
    async fn lenient_discards_lookup_failures() {
        // Simple mode + missing query name → Lookup stage.
        let sess = Arc::new(ConsumeSession::new(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Lenient,
            false,
            None,
            "missing".to_string(),
            loader_with("default"),
            Arc::new(OkSink),
            Arc::new(OkCompose),
        ));
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), false)]);
        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
    }

    #[tokio::test]
    async fn strict_lookup_failure_aborts() {
        let sess = Arc::new(ConsumeSession::new(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Strict,
            false,
            None,
            "missing".to_string(),
            loader_with("default"),
            Arc::new(OkSink),
            Arc::new(OkCompose),
        ));
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "{}")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), true)]);
        assert!(matches!(exit, SessionExit::Fatal(_)));
    }

    #[tokio::test]
    async fn lenient_discards_parse_failures() {
        // Contract mode with an unparseable payload → Parse stage.
        let sess = session(
            ConsumeQueryMode::Contract,
            ConsumeErrorMode::Lenient,
            Arc::new(OkSink),
            Arc::new(OkCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "not-json")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), false)]);
        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
    }

    #[tokio::test]
    async fn strict_parse_failure_aborts() {
        let sess = session(
            ConsumeQueryMode::Contract,
            ConsumeErrorMode::Strict,
            Arc::new(OkSink),
            Arc::new(OkCompose),
        );
        let (exit, nacks) = run_session(sess, scripted(vec![msg(7, "not-json")])).await;

        assert_eq!(nacks, vec![(DeliveryTag::from_u64(7), true)]);
        assert!(matches!(exit, SessionExit::Fatal(_)));
    }

    struct FailingRecvConsumer;

    #[async_trait]
    impl Consumer for FailingRecvConsumer {
        fn name(&self) -> &str {
            "failing-recv"
        }

        async fn recv(&self) -> anyhow::Result<RecvOutcome> {
            Err(anyhow!("broker unreachable"))
        }

        async fn ack(&self, _tag: DeliveryTag) -> anyhow::Result<()> {
            Ok(())
        }

        async fn nack(&self, _tag: DeliveryTag, _requeue: bool) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn recv_error_escalates_to_reconnect() {
        let sess = session(
            ConsumeQueryMode::Simple,
            ConsumeErrorMode::Lenient,
            Arc::new(OkSink),
            Arc::new(OkCompose),
        );
        let (tx, rx) = watch::channel(false);
        let _tx = tx;
        let mut shutdown = Shutdown::from_receiver(rx);
        let exit = sess.run(&FailingRecvConsumer, &mut shutdown).await;

        // A broker error reaches the session-loop reconnect path instead of
        // being swallowed by a retry-forever loop (the old is_connected=default-true
        // behavior for consumers that didn't track connection state).
        assert!(matches!(exit, SessionExit::ReconnectAfterHealthy));
    }
}
