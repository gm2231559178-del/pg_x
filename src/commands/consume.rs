use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{Args, ValueEnum};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

use crate::commands::consume_session::{Compose, ConsumeSession};
use crate::consumer::r#trait::{ConsumeSink, Consumer};
use crate::graphql::query::{NamedQuery, QueryLoader};
use crate::graphql::{executor, pool::QueryConn, schema::SchemaRegistry};
use crate::utils::config::{
    Connection, ConsumeConfig, ConsumeSinkKind, ConsumeSourceKind, ResolverConfig,
};
use crate::utils::session_loop::{self, ReconnectConfig, SessionExit};
use crate::utils::signal::shutdown_signal;

// ── CLI args ─────────────────────────────────────────────────────────────────

#[derive(Args)]
pub struct ConsumeArgs {
    /// Source type: rabbitmq or kafka
    #[arg(long, value_enum, default_value_t = ConsumeSourceType::Rabbitmq)]
    pub source: ConsumeSourceType,

    /// Sink type: stdout, elasticsearch, or webhook
    #[arg(long, value_enum, default_value_t = ConsumeSinkType::Stdout)]
    pub sink: ConsumeSinkType,

    // ── Source: RabbitMQ ──
    #[arg(long, env = "AMQP_URL")]
    pub amqp_url: Option<String>,
    #[arg(long)]
    pub queue: Option<String>,
    #[arg(long)]
    pub exchange: Option<String>,
    #[arg(long)]
    pub routing_key: Option<String>,
    /// Max unacknowledged messages per consumer (0 = no limit). Limits how many
    /// messages RabbitMQ delivers before awaiting an ack, preventing an
    /// unbounded unacked backlog when processing is slower than publishing.
    #[arg(long)]
    pub prefetch_count: Option<u16>,

    // ── Source: Kafka ──
    #[arg(long, env = "KAFKA_BROKERS")]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub group_id: Option<String>,

    // ── Query ──
    /// Query mode: contract (name from message event_type) or simple (fixed --query)
    #[arg(long, value_enum)]
    pub query_mode: Option<ConsumeQueryMode>,
    /// Query name (required in simple mode)
    #[arg(long)]
    pub query: Option<String>,
    /// Max resolver recursion depth
    #[arg(long)]
    pub max_depth: Option<u32>,
    /// Schema directory (defaults to ~/.pgx/schema)
    #[arg(long)]
    pub schema_dir: Option<String>,

    // ── Error handling ──
    /// Error mode: lenient (log + continue) or strict (nack + abort)
    #[arg(long, value_enum)]
    pub on_error: Option<ConsumeErrorMode>,

    // ── Idempotence ──
    /// Make redelivered messages harmless: dedupe recently seen message ids and
    /// derive stable sink keys from the message id.
    #[arg(long, default_value_t = false)]
    pub idempotent: bool,
    /// How long (seconds) to remember seen message ids for dedupe (default 900).
    #[arg(long)]
    pub dedup_ttl: Option<u64>,

    // ── Reconnection ──
    /// Max reconnect attempts (0 = infinite)
    #[arg(long, default_value_t = 0)]
    pub max_reconnect_attempts: u32,
    /// Reconnect backoff base in milliseconds
    #[arg(long, default_value_t = 1000)]
    pub reconnect_base_ms: u64,
    /// Reconnect backoff max in milliseconds
    #[arg(long, default_value_t = 30000)]
    pub reconnect_max_ms: u64,

    // ── Sink: Elasticsearch ──
    #[arg(long, env = "ES_URL")]
    pub es_url: Option<String>,
    #[arg(long)]
    pub index: Option<String>,
    #[arg(long)]
    pub id_field: Option<String>,

    // ── Sink: Webhook ──
    #[arg(long, env = "WEBHOOK_URL")]
    pub webhook_url: Option<String>,

    // ── Sink: KV (Redis / Memcached) ──
    /// KV store URL (redis://... or memcached://...)
    #[arg(long, env = "KV_URL")]
    pub kv_url: Option<String>,
    /// Field in the document to use as the cache key
    #[arg(long)]
    pub key_field: Option<String>,
    /// Prefix to prepend to the cache key
    #[arg(long)]
    pub key_prefix: Option<String>,
    /// TTL in seconds (0 = no expiry)
    #[arg(long)]
    pub ttl: Option<u64>,
}

#[derive(Clone, ValueEnum)]
pub enum ConsumeSourceType {
    Rabbitmq,
    Kafka,
}

#[derive(Clone, ValueEnum)]
pub enum ConsumeSinkType {
    Stdout,
    Elasticsearch,
    Webhook,
    /// Key-value store (Redis / Memcached). Requires the 'kv' feature.
    Kv,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum ConsumeQueryMode {
    Simple,
    Contract,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum ConsumeErrorMode {
    Lenient,
    Strict,
}

/// The stage of the per-message pipeline where a failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorStage {
    /// The payload did not parse as a contract message (contract mode only).
    Parse,
    /// No named query matched the message's event type.
    Lookup,
    /// GraphQL execution failed while composing the document.
    Compose,
    /// Delivery to the sink failed.
    Sink,
}

/// The control-flow decision produced by the error policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorAction {
    /// Nack without requeue and continue; the message is dropped.
    Discard,
    /// Nack with requeue and continue; the broker redelivers the message.
    Requeue,
    /// Nack with requeue and stop the session with the caller's error.
    Abort,
}

impl ConsumeErrorMode {
    /// One error policy for the consume session. Stage × mode → decision.
    ///
    /// | stage   | lenient | strict |
    /// |---------|---------|--------|
    /// | Parse   | Discard | Abort  |
    /// | Lookup  | Discard | Abort  |
    /// | Compose | Discard | Abort  |
    /// | Sink    | Requeue | Abort  |
    ///
    /// Per-message failures (parse, lookup, compose) are dropped in lenient
    /// mode: the message itself is unusable, so redelivering it would fail
    /// identically. A sink failure is transient, so lenient mode requeues
    /// rather than silently discarding a durable event. Strict mode aborts
    /// the session on any failure so nothing is dropped unseen.
    pub(crate) fn handle(&self, stage: ErrorStage) -> ErrorAction {
        match (self, stage) {
            (ConsumeErrorMode::Lenient, ErrorStage::Sink) => ErrorAction::Requeue,
            (ConsumeErrorMode::Lenient, _) => ErrorAction::Discard,
            (ConsumeErrorMode::Strict, _) => ErrorAction::Abort,
        }
    }
}

// ── Sink implementations ──────────────────────────────────────────────────────

struct StdoutConsumeSink;

#[async_trait]
impl ConsumeSink for StdoutConsumeSink {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn send(&self, doc: &Value, _msg_id: Option<&str>) -> Result<()> {
        println!("{}", serde_json::to_string_pretty(doc)?);
        Ok(())
    }
}

#[cfg(feature = "elasticsearch")]
struct ElasticsearchConsumeSink {
    index: String,
    id_field: Option<String>,
    es: crate::downstream::delivery::elasticsearch::Elasticsearch,
}

#[cfg(feature = "elasticsearch")]
#[async_trait]
impl ConsumeSink for ElasticsearchConsumeSink {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        let doc_id = crate::downstream::delivery::elasticsearch::Elasticsearch::doc_id(
            self.id_field.as_deref(),
            doc,
            msg_id,
        );

        self.es.push(&self.index, doc_id.as_deref(), doc).await
    }
}

#[cfg(feature = "webhook")]
struct WebhookConsumeSink {
    url: String,
    webhook: crate::downstream::delivery::webhook::Webhook,
}

#[cfg(feature = "webhook")]
#[async_trait]
impl ConsumeSink for WebhookConsumeSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn send(&self, doc: &Value, msg_id: Option<&str>) -> Result<()> {
        // No retry here: the consume session nacks and redelivers via the
        // broker when delivery fails. Idempotency-Key lets the receiver dedupe.
        self.webhook
            .post(&self.url, &HashMap::new(), doc, msg_id)
            .await
    }
}

// ── KV sink (Redis / Memcached) ───────────────────────────────────────────────

#[cfg(feature = "kv")]
use crate::consumer::kv::KvConsumeSink;

// ── Builders ─────────────────────────────────────────────────────────────────

#[allow(unused_variables)]
async fn build_sink(args: &ConsumeArgs) -> Result<Arc<dyn ConsumeSink>> {
    match args.sink {
        ConsumeSinkType::Stdout => Ok(Arc::new(StdoutConsumeSink)),

        #[cfg(feature = "elasticsearch")]
        ConsumeSinkType::Elasticsearch => {
            let es_url = args.es_url.as_deref().unwrap_or("http://localhost:9200");
            let index = args.index.as_deref().unwrap_or("pgx").to_string();
            let es = crate::downstream::delivery::elasticsearch::Elasticsearch::new(es_url)?;
            Ok(Arc::new(ElasticsearchConsumeSink {
                index,
                id_field: args.id_field.clone(),
                es,
            }))
        }

        #[cfg(not(feature = "elasticsearch"))]
        ConsumeSinkType::Elasticsearch => {
            anyhow::bail!("Elasticsearch sink requires the 'elasticsearch' feature")
        }

        #[cfg(feature = "webhook")]
        ConsumeSinkType::Webhook => {
            let url = args.webhook_url.as_deref().unwrap_or_default();
            if url.is_empty() {
                anyhow::bail!(
                    "Webhook URL is required — provide --webhook-url or set WEBHOOK_URL env"
                );
            }
            let webhook = crate::downstream::delivery::webhook::Webhook::with_retries(0);
            Ok(Arc::new(WebhookConsumeSink {
                url: url.to_string(),
                webhook,
            }))
        }

        #[cfg(not(feature = "webhook"))]
        ConsumeSinkType::Webhook => {
            anyhow::bail!("Webhook sink requires the 'webhook' feature")
        }

        #[cfg(feature = "kv")]
        ConsumeSinkType::Kv => {
            let url = args.kv_url.as_deref().unwrap_or("redis://localhost:6379");
            let sink = KvConsumeSink::connect(
                url,
                args.key_prefix.as_deref().unwrap_or("pgx:"),
                args.key_field.clone(),
                args.ttl.unwrap_or(0),
            )
            .await?;
            Ok(Arc::new(sink))
        }

        #[cfg(not(feature = "kv"))]
        ConsumeSinkType::Kv => {
            anyhow::bail!("KV sink requires the 'kv' feature")
        }
    }
}

#[allow(unused_variables)]
async fn build_consumer(args: &ConsumeArgs) -> Result<Arc<dyn Consumer>> {
    match args.source {
        #[cfg(feature = "rabbitmq")]
        ConsumeSourceType::Rabbitmq => {
            let amqp_url = args
                .amqp_url
                .as_deref()
                .unwrap_or("amqp://guest:guest@localhost:5672/%2F");
            let queue = args.queue.as_deref().unwrap_or("pgx-events");
            let exchange = args.exchange.as_deref();
            let routing_key = args.routing_key.as_deref();
            let c = crate::consumer::rabbitmq::rabbitmq::RabbitMqConsumer::connect(
                amqp_url,
                queue,
                exchange,
                routing_key,
                args.prefetch_count.unwrap_or(0),
            )
            .await?;
            Ok(Arc::new(c))
        }

        #[cfg(not(feature = "rabbitmq"))]
        ConsumeSourceType::Rabbitmq => {
            anyhow::bail!("RabbitMQ consumer requires the 'rabbitmq' feature")
        }

        #[cfg(feature = "kafka")]
        ConsumeSourceType::Kafka => {
            let brokers = args.brokers.as_deref().unwrap_or("localhost:9092");
            let topic = args.topic.as_deref().unwrap_or("pgx-events");
            let group_id = args.group_id.as_deref().unwrap_or("pgx-consume");
            let c = crate::consumer::kafka::kafka::KafkaConsumer::connect(brokers, topic, group_id)
                .await?;
            Ok(Arc::new(c))
        }

        #[cfg(not(feature = "kafka"))]
        ConsumeSourceType::Kafka => {
            anyhow::bail!("Kafka consumer requires the 'kafka' feature")
        }
    }
}

// ── Composition ──────────────────────────────────────────────────────────────

/// Production wiring of the [`Compose`] seam: executes a named query against the
/// GraphQL query pool with the configured resolvers and recursion depth.
struct GraphqlCompose {
    pool: Arc<QueryConn>,
    resolvers: Arc<HashMap<String, ResolverConfig>>,
    max_depth: u32,
}

#[async_trait]
impl Compose for GraphqlCompose {
    async fn compose(
        &self,
        query: &NamedQuery,
        variables: &HashMap<String, Value>,
    ) -> Result<Value> {
        executor::execute(
            query,
            variables,
            &self.resolvers,
            self.pool.as_ref(),
            self.max_depth,
        )
        .await
    }
}

// ── Resolve schema dir ──────────────────────────────────────────────────────

fn resolve_schema_dir(override_dir: Option<&str>) -> Result<PathBuf> {
    let home = dirs::home_dir().context("Cannot determine home directory")?;
    if let Some(dir) = override_dir {
        let expanded = dir.replace('~', &home.to_string_lossy());
        return Ok(PathBuf::from(expanded));
    }
    Ok(home.join(".pgx").join("schema"))
}

// ── Effective consume config ─────────────────────────────────────────────────

/// Consume settings after merging CLI flags with connection-level config, then
/// built-in defaults. Produced once by explicit precedence rules (explicit CLI
/// wins over config, which wins over defaults); no code compares against clap
/// defaults, so a default can move without silently breaking the merge.
struct EffectiveConsumeConfig {
    query: Option<String>,
    schema_dir: Option<String>,
    max_depth: u32,
    query_mode: ConsumeQueryMode,
    on_error: ConsumeErrorMode,
    idempotent: bool,
    dedup_ttl: Option<u64>,
}

impl EffectiveConsumeConfig {
    fn merge(cli: &ConsumeArgs, cfg: Option<&ConsumeConfig>) -> Self {
        let max_depth = cli.max_depth.or(cfg.and_then(|c| c.max_depth)).unwrap_or(8);
        let query_mode = cli
            .query_mode
            .clone()
            .or_else(|| {
                cfg.and_then(|c| c.query_mode.as_deref())
                    .and_then(|m| m.parse::<ConsumeQueryMode>().ok())
            })
            .unwrap_or(ConsumeQueryMode::Contract);
        let on_error = cli
            .on_error
            .clone()
            .or_else(|| {
                cfg.and_then(|c| c.on_error.as_deref())
                    .and_then(|m| m.parse::<ConsumeErrorMode>().ok())
            })
            .unwrap_or(ConsumeErrorMode::Lenient);
        Self {
            query: cli
                .query
                .clone()
                .or_else(|| cfg.and_then(|c| c.query.clone())),
            schema_dir: cli
                .schema_dir
                .clone()
                .or_else(|| cfg.and_then(|c| c.schema_dir.clone())),
            max_depth,
            query_mode,
            on_error,
            idempotent: cli.idempotent || cfg.is_some_and(|c| c.idempotent.unwrap_or(false)),
            dedup_ttl: cli.dedup_ttl.or(cfg.and_then(|c| c.dedup_ttl)),
        }
    }
}

// ── Run ─────────────────────────────────────────────────────────────────────

pub async fn run(
    url: String,
    mut args: ConsumeArgs,
    conn: Option<&Connection>,
    use_tls: bool,
    resolvers: &HashMap<String, ResolverConfig>,
) -> Result<()> {
    // ── Merge connection-level defaults into an effective config ────────────
    let cfg = conn.and_then(|c| c.consume.as_ref());
    if let Some(cfg) = cfg {
        // Source defaults
        args.source = match cfg.source {
            ConsumeSourceKind::Rabbitmq { .. } => ConsumeSourceType::Rabbitmq,
            ConsumeSourceKind::Kafka { .. } => ConsumeSourceType::Kafka,
        };
        merge_source_config(&mut args, &cfg.source);
        merge_sink_config(&mut args, &cfg.sink);
    }
    let eff = EffectiveConsumeConfig::merge(&args, cfg);

    // Validate simple mode requires --query
    if matches!(eff.query_mode, ConsumeQueryMode::Simple) && eff.query.is_none() {
        anyhow::bail!("Simple query mode requires --query <name> or consume.query in config");
    }

    // ── Load schema and queries (once, outside reconnection loop) ────────────
    let schema_dir = resolve_schema_dir(eff.schema_dir.as_deref())?;
    let schema = SchemaRegistry::load_from_dir(&schema_dir)?;
    let queries = Arc::new(QueryLoader::load(&schema)?);
    info!(
        "Loaded {} type definitions, {} queries",
        schema.types.len(),
        queries.queries.len()
    );

    // ── Build GraphQL query pool (once) ──────────────────────────────────────
    let pool = Arc::new(QueryConn::connect(&url, use_tls).await?);
    info!("Connected GraphQL query pool to PostgreSQL");

    // ── Resolve default query name (contract mode fallback) ──────────────────
    let default_query = eff.query.clone().unwrap_or_else(|| "default".to_string());

    // ── Build sink (once) ────────────────────────────────────────────────────
    let sink: Arc<dyn ConsumeSink> = build_sink(&args).await?;
    info!("Using {} sink", sink.name());

    // ── Wire the composition seam to the GraphQL executor ────────────────────
    let compose: Arc<dyn Compose> = Arc::new(GraphqlCompose {
        pool: pool.clone(),
        resolvers: Arc::new(resolvers.clone()),
        max_depth: eff.max_depth,
    });

    // ── Build the session (owns the dedupe lifecycle) ────────────────────────
    let session = Arc::new(ConsumeSession::new(
        eff.query_mode.clone(),
        eff.on_error.clone(),
        eff.idempotent,
        eff.dedup_ttl,
        default_query,
        queries,
        sink,
        compose,
    ));

    info!(
        "Starting consume loop (mode={:?}, error={:?})",
        eff.query_mode, eff.on_error
    );

    let reconnect = ReconnectConfig {
        max_attempts: args.max_reconnect_attempts,
        base_ms: args.reconnect_base_ms,
        max_ms: args.reconnect_max_ms,
    };

    let args = Arc::new(args);

    // ── Session-loop: reconnect with backoff until shutdown or a fatal error ─
    session_loop::run(
        move |shutdown| {
            let session = session.clone();
            let args = args.clone();
            async move {
                // ── Build consumer ───────────────────────────────────────────
                let consumer: Arc<dyn Consumer> = match build_consumer(args.as_ref()).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "Failed to connect consumer");
                        return SessionExit::Reconnect;
                    }
                };
                info!("Connected to {} consumer", consumer.name());

                let mut shutdown = shutdown;
                session.run(consumer.as_ref(), &mut shutdown).await
            }
        },
        shutdown_signal(),
        &reconnect,
    )
    .await
}

fn merge_source_config(args: &mut ConsumeArgs, source: &ConsumeSourceKind) {
    match source {
        ConsumeSourceKind::Rabbitmq {
            amqp_url,
            queue,
            exchange,
            routing_key,
            prefetch_count,
        } => {
            if args.amqp_url.is_none() && amqp_url.is_some() {
                args.amqp_url = amqp_url.clone();
            }
            if args.queue.is_none() && queue.is_some() {
                args.queue = queue.clone();
            }
            if args.exchange.is_none() && exchange.is_some() {
                args.exchange = exchange.clone();
            }
            if args.routing_key.is_none() && routing_key.is_some() {
                args.routing_key = routing_key.clone();
            }
            if args.prefetch_count.is_none() && prefetch_count.is_some() {
                args.prefetch_count = *prefetch_count;
            }
        }
        ConsumeSourceKind::Kafka {
            brokers,
            topic,
            group_id,
        } => {
            if args.brokers.is_none() && brokers.is_some() {
                args.brokers = brokers.clone();
            }
            if args.topic.is_none() && topic.is_some() {
                args.topic = topic.clone();
            }
            if args.group_id.is_none() && group_id.is_some() {
                args.group_id = group_id.clone();
            }
        }
    }
}

fn merge_sink_config(args: &mut ConsumeArgs, sink: &ConsumeSinkKind) {
    match sink {
        ConsumeSinkKind::Stdout => {
            args.sink = ConsumeSinkType::Stdout;
        }
        ConsumeSinkKind::Elasticsearch {
            url,
            index,
            id_field,
        } => {
            args.sink = ConsumeSinkType::Elasticsearch;
            if args.es_url.is_none() {
                args.es_url = Some(url.clone());
            }
            if args.index.is_none() {
                args.index = Some(index.clone());
            }
            if args.id_field.is_none() {
                args.id_field = id_field.clone();
            }
        }
        ConsumeSinkKind::Webhook { url, .. } => {
            args.sink = ConsumeSinkType::Webhook;
            if args.webhook_url.is_none() {
                args.webhook_url = Some(url.clone());
            }
        }
        #[cfg(feature = "kv")]
        ConsumeSinkKind::Kv {
            url,
            key_field,
            key_prefix,
            ttl,
        } => {
            args.sink = ConsumeSinkType::Kv;
            if args.kv_url.is_none() {
                args.kv_url = Some(url.clone());
            }
            if args.key_field.is_none() {
                args.key_field = key_field.clone();
            }
            if args.key_prefix.is_none() {
                args.key_prefix = key_prefix.clone();
            }
            if args.ttl.is_none() {
                args.ttl = *ttl;
            }
        }
        #[cfg(not(feature = "kv"))]
        ConsumeSinkKind::Kv { .. } => {
            // Cannot configure KV sink without the 'kv' feature;
            // build_sink will produce a clear error.
        }
    }
}

// ── Parse helpers ───────────────────────────────────────────────────────────

impl std::str::FromStr for ConsumeQueryMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(Self::Simple),
            "contract" => Ok(Self::Contract),
            other => Err(format!(
                "unknown query mode '{other}'; expected simple|contract"
            )),
        }
    }
}

impl std::str::FromStr for ConsumeErrorMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lenient" => Ok(Self::Lenient),
            "strict" => Ok(Self::Strict),
            other => Err(format!(
                "unknown error mode '{other}'; expected lenient|strict"
            )),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "elasticsearch")]
mod tests {
    use crate::downstream::delivery::elasticsearch::Elasticsearch;
    use serde_json::json;

    #[test]
    fn es_id_explicit_field_wins() {
        let doc = json!({"mat_no": "M001", "name": "steel"});
        assert_eq!(
            Elasticsearch::doc_id(Some("mat_no"), &doc, Some("msg-7")),
            Some("M001".into())
        );
    }

    #[test]
    fn es_id_falls_back_to_msg_id() {
        let doc = json!({"name": "steel"});
        assert_eq!(
            Elasticsearch::doc_id(None, &doc, Some("msg-7")),
            Some("msg-7".into())
        );
        assert_eq!(
            Elasticsearch::doc_id(Some("missing"), &doc, Some("msg-7")),
            Some("msg-7".into())
        );
    }

    #[test]
    fn es_id_none_without_idempotence() {
        let doc = json!({"name": "steel"});
        assert_eq!(Elasticsearch::doc_id(None, &doc, None), None);
    }

    #[test]
    fn es_id_ignores_non_string_field() {
        let doc = json!({"mat_no": 42});
        assert_eq!(
            Elasticsearch::doc_id(Some("mat_no"), &doc, Some("msg-7")),
            Some("msg-7".into())
        );
    }
}

#[cfg(test)]
mod error_policy_tests {
    use super::{ConsumeErrorMode, ErrorAction, ErrorStage};

    #[test]
    fn lenient_discards_per_message_failures() {
        for stage in [ErrorStage::Parse, ErrorStage::Lookup, ErrorStage::Compose] {
            assert_eq!(
                ConsumeErrorMode::Lenient.handle(stage),
                ErrorAction::Discard
            );
        }
    }

    #[test]
    fn lenient_requeues_transient_sink_failures() {
        assert_eq!(
            ConsumeErrorMode::Lenient.handle(ErrorStage::Sink),
            ErrorAction::Requeue
        );
    }

    #[test]
    fn strict_aborts_on_every_failure() {
        for stage in [
            ErrorStage::Parse,
            ErrorStage::Lookup,
            ErrorStage::Compose,
            ErrorStage::Sink,
        ] {
            assert_eq!(ConsumeErrorMode::Strict.handle(stage), ErrorAction::Abort);
        }
    }
}

#[cfg(test)]
mod effective_config_tests {
    use super::*;
    use crate::utils::config::{ConsumeConfig, ConsumeSinkKind, ConsumeSourceKind};

    fn cli() -> ConsumeArgs {
        ConsumeArgs {
            source: ConsumeSourceType::Rabbitmq,
            sink: ConsumeSinkType::Stdout,
            amqp_url: None,
            queue: None,
            exchange: None,
            routing_key: None,
            prefetch_count: None,
            brokers: None,
            topic: None,
            group_id: None,
            query_mode: None,
            query: None,
            max_depth: None,
            schema_dir: None,
            on_error: None,
            idempotent: false,
            dedup_ttl: None,
            max_reconnect_attempts: 0,
            reconnect_base_ms: 1000,
            reconnect_max_ms: 30000,
            es_url: None,
            index: None,
            id_field: None,
            webhook_url: None,
            kv_url: None,
            key_field: None,
            key_prefix: None,
            ttl: None,
        }
    }

    fn cfg() -> ConsumeConfig {
        ConsumeConfig {
            source: ConsumeSourceKind::Rabbitmq {
                amqp_url: None,
                queue: None,
                exchange: None,
                routing_key: None,
                prefetch_count: None,
            },
            sink: ConsumeSinkKind::Stdout,
            query_mode: Some("contract".to_string()),
            query: Some("cfg-query".to_string()),
            max_depth: Some(16),
            schema_dir: Some("cfg-schema".to_string()),
            on_error: Some("lenient".to_string()),
            idempotent: Some(true),
            dedup_ttl: Some(60),
        }
    }

    #[test]
    fn explicit_cli_wins_over_config() {
        let mut c = cli();
        c.query_mode = Some(ConsumeQueryMode::Simple);
        c.on_error = Some(ConsumeErrorMode::Strict);
        c.max_depth = Some(3);
        c.query = Some("cli-query".to_string());
        c.schema_dir = Some("cli-schema".to_string());
        c.idempotent = true;
        c.dedup_ttl = Some(5);

        let eff = EffectiveConsumeConfig::merge(&c, Some(&cfg()));
        assert_eq!(eff.query_mode, ConsumeQueryMode::Simple);
        assert_eq!(eff.on_error, ConsumeErrorMode::Strict);
        assert_eq!(eff.max_depth, 3);
        assert_eq!(eff.query.as_deref(), Some("cli-query"));
        assert_eq!(eff.schema_dir.as_deref(), Some("cli-schema"));
        assert!(eff.idempotent);
        assert_eq!(eff.dedup_ttl, Some(5));
    }

    #[test]
    fn config_fills_when_cli_absent() {
        let eff = EffectiveConsumeConfig::merge(&cli(), Some(&cfg()));
        assert_eq!(eff.query_mode, ConsumeQueryMode::Contract);
        assert_eq!(eff.on_error, ConsumeErrorMode::Lenient);
        assert_eq!(eff.max_depth, 16);
        assert_eq!(eff.query.as_deref(), Some("cfg-query"));
        assert_eq!(eff.schema_dir.as_deref(), Some("cfg-schema"));
        assert!(eff.idempotent);
        assert_eq!(eff.dedup_ttl, Some(60));
    }

    #[test]
    fn defaults_apply_when_nothing_provided() {
        let eff = EffectiveConsumeConfig::merge(&cli(), None);
        assert_eq!(eff.query_mode, ConsumeQueryMode::Contract);
        assert_eq!(eff.on_error, ConsumeErrorMode::Lenient);
        assert_eq!(eff.max_depth, 8);
        assert_eq!(eff.query, None);
        assert_eq!(eff.schema_dir, None);
        assert!(!eff.idempotent);
        assert_eq!(eff.dedup_ttl, None);
    }

    #[test]
    fn explicit_cli_default_is_respected_over_config() {
        // The regression this merge fixes: an explicit `--max-depth 8` used to
        // be indistinguishable from the clap default, so config silently won.
        let mut c = cli();
        c.max_depth = Some(8);
        let eff = EffectiveConsumeConfig::merge(&c, Some(&cfg()));
        assert_eq!(eff.max_depth, 8);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_sink_explicit_cli_wins_over_config() {
        let mut c = cli();
        c.key_prefix = Some("cli:".to_string());
        c.ttl = Some(0);
        merge_sink_config(
            &mut c,
            &ConsumeSinkKind::Kv {
                url: "redis://x".to_string(),
                key_field: None,
                key_prefix: Some("cfg:".to_string()),
                ttl: Some(60),
            },
        );
        // Explicit `--ttl 0` / `--key-prefix cli:` are no longer mistaken for
        // the clap defaults and silently overridden by config.
        assert_eq!(c.key_prefix.as_deref(), Some("cli:"));
        assert_eq!(c.ttl, Some(0));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_sink_config_fills_when_cli_absent() {
        let mut c = cli();
        merge_sink_config(
            &mut c,
            &ConsumeSinkKind::Kv {
                url: "redis://x".to_string(),
                key_field: None,
                key_prefix: Some("cfg:".to_string()),
                ttl: Some(60),
            },
        );
        assert_eq!(c.key_prefix.as_deref(), Some("cfg:"));
        assert_eq!(c.ttl, Some(60));
    }
}
