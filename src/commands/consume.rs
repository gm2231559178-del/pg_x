use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{Args, ValueEnum};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::consumer::r#trait::{BrokerMessage, ConsumeSink, Consumer};
use crate::downstream::contract::ContractMessage;
use crate::graphql::{executor, pool::QueryConn, query::QueryLoader, schema::SchemaRegistry};
use crate::utils::config::{Connection, ConsumeSinkKind, ConsumeSourceKind, ResolverConfig};
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

    // ── Source: Kafka ──
    #[arg(long, env = "KAFKA_BROKERS")]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub group_id: Option<String>,

    // ── Query ──
    /// Query mode: contract (name from message event_type) or simple (fixed --query)
    #[arg(long, value_enum, default_value_t = ConsumeQueryMode::Contract)]
    pub query_mode: ConsumeQueryMode,
    /// Query name (required in simple mode)
    #[arg(long)]
    pub query: Option<String>,
    /// Max resolver recursion depth
    #[arg(long, default_value_t = 8)]
    pub max_depth: u32,
    /// Schema directory (defaults to ~/.pgx/schema)
    #[arg(long)]
    pub schema_dir: Option<String>,

    // ── Error handling ──
    /// Error mode: lenient (log + continue) or strict (nack + abort)
    #[arg(long, value_enum, default_value_t = ConsumeErrorMode::Lenient)]
    pub on_error: ConsumeErrorMode,

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
    #[arg(long, default_value = "pgx:")]
    pub key_prefix: String,
    /// TTL in seconds (0 = no expiry)
    #[arg(long, default_value_t = 0)]
    pub ttl: u64,
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

#[derive(Debug, Clone, ValueEnum)]
pub enum ConsumeQueryMode {
    Simple,
    Contract,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ConsumeErrorMode {
    Lenient,
    Strict,
}

// ── Sink implementations ──────────────────────────────────────────────────────

struct StdoutConsumeSink;

#[async_trait]
impl ConsumeSink for StdoutConsumeSink {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn send(&self, doc: &Value) -> Result<()> {
        println!("{}", serde_json::to_string_pretty(doc)?);
        Ok(())
    }
}

#[cfg(feature = "elasticsearch")]
struct ElasticsearchConsumeSink {
    es_url: String,
    index: String,
    id_field: Option<String>,
    client: reqwest::Client,
}

#[cfg(feature = "elasticsearch")]
#[async_trait]
impl ConsumeSink for ElasticsearchConsumeSink {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    async fn send(&self, doc: &Value) -> Result<()> {
        let doc_id = self.id_field.as_ref().and_then(|idf| match doc {
            Value::Object(m) => m.get(idf).and_then(|v| v.as_str().map(|s| s.to_string())),
            _ => None,
        });

        let url = if let Some(ref id) = doc_id {
            format!("{}/{}/_doc/{}", self.es_url, self.index, id)
        } else {
            format!("{}/{}/_doc", self.es_url, self.index)
        };

        let response = self
            .client
            .post(&url)
            .json(doc)
            .send()
            .await
            .with_context(|| format!("ES POST failed to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "ES document index failed (HTTP {}) at {}: {}",
                status,
                url,
                text,
            );
        }

        Ok(())
    }
}

#[cfg(feature = "webhook")]
struct WebhookConsumeSink {
    url: String,
    client: reqwest::Client,
}

#[cfg(feature = "webhook")]
#[async_trait]
impl ConsumeSink for WebhookConsumeSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn send(&self, doc: &Value) -> Result<()> {
        let response = self
            .client
            .post(&self.url)
            .json(doc)
            .send()
            .await
            .with_context(|| format!("Webhook POST failed to {}", self.url))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Webhook failed (HTTP {}) at {}: {}", status, self.url, text);
        }

        Ok(())
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
            let es_url = args
                .es_url
                .as_deref()
                .unwrap_or("http://localhost:9200")
                .trim_end_matches('/')
                .to_string();
            let index = args.index.as_deref().unwrap_or("pgx").to_string();
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?;
            Ok(Arc::new(ElasticsearchConsumeSink {
                es_url,
                index,
                id_field: args.id_field.clone(),
                client,
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
            let client = reqwest::Client::new();
            Ok(Arc::new(WebhookConsumeSink {
                url: url.to_string(),
                client,
            }))
        }

        #[cfg(not(feature = "webhook"))]
        ConsumeSinkType::Webhook => {
            anyhow::bail!("Webhook sink requires the 'webhook' feature")
        }

        #[cfg(feature = "kv")]
        ConsumeSinkType::Kv => {
            let url = args.kv_url.as_deref().unwrap_or("redis://localhost:6379");
            let sink =
                KvConsumeSink::connect(url, &args.key_prefix, args.key_field.clone(), args.ttl)
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

// ── Variable extraction helpers ──────────────────────────────────────────────

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

// ── Resolve schema dir ──────────────────────────────────────────────────────

fn resolve_schema_dir(override_dir: Option<&str>) -> Result<PathBuf> {
    let home = dirs::home_dir().context("Cannot determine home directory")?;
    if let Some(dir) = override_dir {
        let expanded = dir.replace('~', &home.to_string_lossy());
        return Ok(PathBuf::from(expanded));
    }
    Ok(home.join(".pgx").join("schema"))
}

// ── Run ─────────────────────────────────────────────────────────────────────

pub async fn run(
    url: String,
    mut args: ConsumeArgs,
    conn: Option<&Connection>,
    use_tls: bool,
    resolvers: &HashMap<String, ResolverConfig>,
) -> Result<()> {
    // ── Merge connection-level defaults into CLI args ────────────────────────
    if let Some(cfg) = conn.and_then(|c| c.consume.as_ref()) {
        // Source defaults
        args.source = match cfg.source {
            ConsumeSourceKind::Rabbitmq { .. } => ConsumeSourceType::Rabbitmq,
            ConsumeSourceKind::Kafka { .. } => ConsumeSourceType::Kafka,
        };
        merge_source_config(&mut args, &cfg.source);
        merge_sink_config(&mut args, &cfg.sink);

        if args.query.is_none() && cfg.query.is_some() {
            args.query = cfg.query.clone();
        }
        if args.schema_dir.is_none() {
            args.schema_dir = cfg.schema_dir.clone();
        }
        if args.max_depth == 8 {
            if let Some(d) = cfg.max_depth {
                args.max_depth = d;
            }
        }
        if let Some(m) = &cfg.query_mode {
            if let Ok(qm) = m.parse::<ConsumeQueryMode>() {
                args.query_mode = qm;
            }
        }
        if let Some(m) = &cfg.on_error {
            if let Ok(em) = m.parse::<ConsumeErrorMode>() {
                args.on_error = em;
            }
        }
    }

    // Validate simple mode requires --query
    if matches!(args.query_mode, ConsumeQueryMode::Simple) && args.query.is_none() {
        anyhow::bail!("Simple query mode requires --query <name> or consume.query in config");
    }

    // ── Build consumer ───────────────────────────────────────────────────────
    let consumer: Arc<dyn Consumer> = build_consumer(&args).await?;
    info!("Connected to {} consumer", consumer.name());

    // ── Load schema and queries ──────────────────────────────────────────────
    let schema_dir = resolve_schema_dir(args.schema_dir.as_deref())?;
    let schema = SchemaRegistry::load_from_dir(&schema_dir)?;
    let queries = QueryLoader::load(&schema)?;
    info!(
        "Loaded {} type definitions, {} queries",
        schema.types.len(),
        queries.queries.len()
    );

    // ── Build GraphQL query pool ─────────────────────────────────────────────
    let pool = QueryConn::connect(&url, use_tls).await?;
    info!("Connected GraphQL query pool to PostgreSQL");

    // ── Resolve default query name (contract mode fallback) ──────────────────
    let default_query = args.query.clone().unwrap_or_else(|| "default".to_string());

    // ── Build sink ───────────────────────────────────────────────────────────
    let sink: Arc<dyn ConsumeSink> = build_sink(&args).await?;
    info!("Using {} sink", sink.name());

    // ── Consume loop ─────────────────────────────────────────────────────────
    info!(
        "Starting consume loop (mode={:?}, error={:?})",
        args.query_mode, args.on_error
    );

    tokio::pin!(let shutdown = shutdown_signal(););

    loop {
        let msg: BrokerMessage = loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    info!("Signal received, shutting down cleanly");
                    return Ok(());
                }

                maybe_msg = consumer.recv() => {
                    match maybe_msg {
                        Some(m) => break m,
                        None => {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }
        };

        let tag = msg.delivery_tag;
        let topic = msg.topic.clone();

        // ── Resolve query name and variables ─────────────────────────────────
        let (query_name, variables) = match args.query_mode {
            ConsumeQueryMode::Contract => match ContractMessage::try_parse(&msg.payload) {
                Some(contract) => {
                    let qn = contract
                        .meta
                        .event_type
                        .unwrap_or_else(|| default_query.clone());
                    let vars = data_to_variables(&contract.data);
                    (qn, vars)
                }
                None => {
                    let msg = "Message is not a valid ContractMessage";
                    match args.on_error {
                        ConsumeErrorMode::Lenient => {
                            warn!("{}. Skipping message (topic={})", msg, topic);
                            let _ = consumer.nack(tag, false).await;
                            continue;
                        }
                        ConsumeErrorMode::Strict => {
                            error!("{} (topic={})", msg, topic);
                            let _ = consumer.nack(tag, true).await;
                            anyhow::bail!("{}: topic={}", msg, topic);
                        }
                    }
                }
            },
            ConsumeQueryMode::Simple => {
                let qn = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("--query is required in simple mode"))?
                    .to_string();
                let vars = payload_to_variables(&msg.payload);
                (qn, vars)
            }
        };

        // ── Look up the query ────────────────────────────────────────────────
        let query = match queries.get(&query_name) {
            Some(q) => q,
            None => {
                let msg = format!("No named query '{}' found", query_name);
                match args.on_error {
                    ConsumeErrorMode::Lenient => {
                        warn!("{}. Skipping message (topic={})", msg, topic);
                        let _ = consumer.nack(tag, false).await;
                        continue;
                    }
                    ConsumeErrorMode::Strict => {
                        error!("{} (topic={})", msg, topic);
                        let _ = consumer.nack(tag, true).await;
                        anyhow::bail!("{}", msg);
                    }
                }
            }
        };

        // ── Execute GraphQL composition ──────────────────────────────────────
        let doc = executor::execute(query, &variables, resolvers, &pool, args.max_depth).await;

        match doc {
            Ok(doc) => {
                // ── Send to sink ─────────────────────────────────────────────
                if let Err(e) = sink.send(&doc).await {
                    match args.on_error {
                        ConsumeErrorMode::Lenient => {
                            warn!(error = %e, topic = %topic, query = %query_name, "Sink failed, skipping message");
                            let _ = consumer.nack(tag, false).await;
                            continue;
                        }
                        ConsumeErrorMode::Strict => {
                            error!(error = %e, topic = %topic, query = %query_name, "Sink failed");
                            let _ = consumer.nack(tag, true).await;
                            return Err(e);
                        }
                    }
                }

                // ── Acknowledge ──────────────────────────────────────────────
                if let Err(e) = consumer.ack(tag).await {
                    error!(error = %e, "Failed to ack message");
                }
            }
            Err(e) => match args.on_error {
                ConsumeErrorMode::Lenient => {
                    warn!(error = %e, topic = %topic, query = %query_name, "GraphQL execution failed, skipping message");
                    let _ = consumer.nack(tag, false).await;
                }
                ConsumeErrorMode::Strict => {
                    error!(error = %e, topic = %topic, query = %query_name, "GraphQL execution failed");
                    let _ = consumer.nack(tag, true).await;
                    return Err(e);
                }
            },
        }
    }
}

fn merge_source_config(args: &mut ConsumeArgs, source: &ConsumeSourceKind) {
    match source {
        ConsumeSourceKind::Rabbitmq {
            amqp_url,
            queue,
            exchange,
            routing_key,
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
            if args.key_prefix == "pgx:" {
                if let Some(p) = key_prefix {
                    args.key_prefix = p.clone();
                }
            }
            if args.ttl == 0 {
                if let Some(t) = ttl {
                    args.ttl = *t;
                }
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
