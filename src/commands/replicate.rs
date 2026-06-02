//! `pgx replicate` — stream WAL changes via PostgreSQL logical replication.
//!
//! Uses the self-contained replication client (src/replication/client.rs) for
//! the WAL streaming protocol plane, and tokio-postgres for the control plane
//! (slot management, wal_level verification).
//!
//! ## PostgreSQL prerequisites
//!
//! ```sql
//! -- postgresql.conf must have:
//! --   wal_level = logical
//!
//! -- Create a publication (which tables to replicate):
//! CREATE PUBLICATION my_pub FOR TABLE orders, inventory;
//! -- Or for every table:
//! CREATE PUBLICATION my_pub FOR ALL TABLES;
//!
//! -- The user must have the REPLICATION role attribute:
//! ALTER USER myuser REPLICATION;
//! ```

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};

use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::replication::{
    client::{ReplicationClient, ReplicationConfig, ReplicationEvent},
    decoder::{decode_pgoutput, RelationCache},
    event::WalEvent,
    lsn::Lsn,
    slot,
};
use crate::utils::signal::{parse_key_val, shutdown_signal};

// ─────────────────────────────────────────────────────────────────────────────
// CLI argument structs
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Args)]
pub struct ReplicateArgs {
    /// Replication slot name (created automatically if it does not exist).
    #[arg(long, default_value = "pgx_slot")]
    pub slot: String,

    /// Publication name(s) to stream from (repeatable).
    /// Create with: CREATE PUBLICATION name FOR TABLE t1, t2;
    #[arg(long = "publication", required = true)]
    pub publications: Vec<String>,

    /// Only forward events for these tables (schema.table or bare table name).
    /// When omitted, all tables in the publication are forwarded.
    #[arg(long = "table")]
    pub tables: Vec<String>,

    /// Only forward these operation types. Omit to forward all.
    #[arg(long = "op", value_enum)]
    pub ops: Vec<OpFilter>,

    /// Resume streaming from this LSN (format: A/BBCCDDEE).
    /// Omit to continue from the slot's confirmed_flush_lsn.
    #[arg(long)]
    pub start_lsn: Option<String>,

    /// Drop and recreate the replication slot before starting.
    /// WARNING: this loses the acknowledged progress checkpoint.
    #[arg(long)]
    pub reset_slot: bool,

    /// Use a temporary slot (dropped automatically when the session ends).
    #[arg(long)]
    pub temporary: bool,

    /// Also forward BEGIN and COMMIT events to the downstream sink.
    #[arg(long)]
    pub emit_txn_boundaries: bool,

    /// Also forward RELATION (schema) events to the downstream sink.
    #[arg(long)]
    pub emit_schema: bool,

    #[command(subcommand)]
    pub downstream: ReplicateDownstreamCommand,
}

#[derive(Clone, ValueEnum, PartialEq, Eq, Debug)]
pub enum OpFilter {
    Insert,
    Update,
    Delete,
    Truncate,
}

// ─────────────────────────────────────────────────────────────────────────────
// Downstream sub-commands
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Subcommand)]
pub enum ReplicateDownstreamCommand {
    /// Print WAL events as JSON to stdout (great for debugging / piping).
    Stdout(StdoutArgs),

    /// Forward events to a shell command via environment variables.
    Shell(ShellArgs),

    /// Forward events via HTTP webhook (POST).
    #[cfg(feature = "webhook")]
    Webhook(WebhookArgs),

    /// Forward events to RabbitMQ (AMQP).
    #[cfg(feature = "rabbitmq")]
    Rabbitmq(RabbitmqArgs),

    /// Forward events to Apache Kafka.
    #[cfg(feature = "kafka")]
    Kafka(KafkaArgs),
}

#[derive(Args)]
pub struct StdoutArgs {
    /// Pretty-print JSON output (one event per line by default).
    #[arg(long)]
    pub pretty: bool,
}

#[derive(Args)]
pub struct ShellArgs {
    /// Shell command executed via `sh -c`.
    ///
    /// Available environment variables:
    ///   PGX_OP       — insert | update | delete | truncate | begin | commit | relation
    ///   PGX_SCHEMA   — schema name (DML events)
    ///   PGX_TABLE    — table name  (DML events)
    ///   PGX_LSN      — WAL end position (e.g. 0/1A2B3C)
    ///   PGX_XID      — transaction ID (BEGIN events)
    ///   PGX_NEW      — JSON of new row values (INSERT / UPDATE)
    ///   PGX_OLD      — JSON of old row values (UPDATE / DELETE)
    ///   PGX_PAYLOAD  — full event JSON
    #[arg(long)]
    pub command: String,

    /// Extra environment variables to inject (KEY=VALUE, repeatable).
    #[arg(long = "env", value_parser = parse_key_val)]
    pub envs: Vec<(String, String)>,
}

#[cfg(feature = "webhook")]
#[derive(Args)]
pub struct WebhookArgs {
    #[arg(long, env = "WEBHOOK_URL")]
    pub url: String,
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,
}

#[cfg(feature = "rabbitmq")]
#[derive(Args)]
pub struct RabbitmqArgs {
    #[arg(
        long,
        env = "AMQP_URL",
        default_value = "amqp://guest:guest@localhost:5672/%2F"
    )]
    pub amqp_url: String,
    #[arg(long, default_value = "pgx")]
    pub exchange: String,
    #[arg(long, default_value = "pgx.wal")]
    pub routing_key: String,
}

#[cfg(feature = "kafka")]
#[derive(Args)]
pub struct KafkaArgs {
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    pub brokers: String,
    #[arg(long, default_value = "pgx-wal")]
    pub topic: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// WalSink trait
// ─────────────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
trait WalSink: Send + Sync {
    fn name(&self) -> &str;
    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()>;
}

// ── Stdout ────────────────────────────────────────────────────────────────────

struct StdoutSink {
    pretty: bool,
}

#[async_trait::async_trait]
impl WalSink for StdoutSink {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        if self.pretty {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(event_json) {
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                return Ok(());
            }
        }
        println!("{event_json}");
        Ok(())
    }
}

// ── Shell ─────────────────────────────────────────────────────────────────────

struct ShellWalSink {
    command: String,
    base_env: HashMap<String, String>,
}

#[async_trait::async_trait]
impl WalSink for ShellWalSink {
    fn name(&self) -> &str {
        "shell"
    }

    async fn send_wal(&self, event_json: &str, extra_env: &HashMap<String, String>) -> Result<()> {
        let mut env = self.base_env.clone();
        env.extend(extra_env.clone());
        env.insert("PGX_PAYLOAD".to_string(), event_json.to_string());

        let status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .envs(&env)
            .status()
            .await
            .context("Failed to spawn shell command")?;

        if !status.success() {
            anyhow::bail!(
                "Shell command exited with status: {}",
                status.code().unwrap_or(-1)
            );
        }
        Ok(())
    }
}

// ── Webhook ───────────────────────────────────────────────────────────────────

#[cfg(feature = "webhook")]
struct WebhookWalSink {
    client: reqwest::Client,
    url: String,
    default_headers: HashMap<String, String>,
}

#[cfg(feature = "webhook")]
#[async_trait::async_trait]
impl WalSink for WebhookWalSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
        use std::str::FromStr;

        let mut hmap = HeaderMap::new();
        hmap.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        for (k, v) in &self.default_headers {
            if let (Ok(name), Ok(val)) = (HeaderName::from_str(k), HeaderValue::from_str(v)) {
                hmap.insert(name, val);
            }
        }
        self.client
            .post(&self.url)
            .headers(hmap)
            .body(event_json.to_string())
            .send()
            .await
            .context("Webhook POST failed")?
            .error_for_status()
            .context("Webhook returned error status")?;
        Ok(())
    }
}

// ── RabbitMQ ──────────────────────────────────────────────────────────────────

#[cfg(feature = "rabbitmq")]
struct RabbitmqWalSink {
    channel: lapin::Channel,
    exchange: String,
    routing_key: String,
}

#[cfg(feature = "rabbitmq")]
#[async_trait::async_trait]
impl WalSink for RabbitmqWalSink {
    fn name(&self) -> &str {
        "rabbitmq"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        use lapin::{
            options::BasicPublishOptions,
            types::{AMQPValue, FieldTable, ShortString},
            BasicProperties,
        };
        use std::collections::BTreeMap;

        let mut headers: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
        for key in ["PGX_OP", "PGX_SCHEMA", "PGX_TABLE", "PGX_LSN"] {
            if let Some(val) = env.get(key) {
                let header_key = key.to_lowercase().replace('_', "-");
                headers.insert(
                    ShortString::from(header_key.as_str()),
                    AMQPValue::LongString(val.as_str().into()),
                );
            }
        }
        let props = BasicProperties::default().with_headers(FieldTable::from(headers));
        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                event_json.as_bytes(),
                props,
            )
            .await
            .context("RabbitMQ publish failed")?
            .await
            .context("RabbitMQ confirm failed")?;
        Ok(())
    }
}

// ── Kafka ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "kafka")]
struct KafkaWalSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
}

#[cfg(feature = "kafka")]
#[async_trait::async_trait]
impl WalSink for KafkaWalSink {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        use rdkafka::producer::FutureRecord;

        let key = env
            .get("PGX_TABLE")
            .map(|s| s.as_str())
            .unwrap_or("pgx-wal");
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(key).payload(event_json),
                std::time::Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send failed: {e}"))?;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build WalSink from CLI args
// ─────────────────────────────────────────────────────────────────────────────

async fn build_wal_sink(cmd: &ReplicateDownstreamCommand) -> Result<Arc<dyn WalSink>> {
    match cmd {
        ReplicateDownstreamCommand::Stdout(a) => Ok(Arc::new(StdoutSink { pretty: a.pretty })),

        ReplicateDownstreamCommand::Shell(a) => Ok(Arc::new(ShellWalSink {
            command: a.command.clone(),
            base_env: a.envs.iter().cloned().collect(),
        })),

        #[cfg(feature = "webhook")]
        ReplicateDownstreamCommand::Webhook(a) => Ok(Arc::new(WebhookWalSink {
            client: reqwest::Client::new(),
            url: a.url.clone(),
            default_headers: a.headers.iter().cloned().collect(),
        })),

        #[cfg(feature = "rabbitmq")]
        ReplicateDownstreamCommand::Rabbitmq(a) => {
            use lapin::{
                options::ExchangeDeclareOptions, types::FieldTable, Connection,
                ConnectionProperties, ExchangeKind,
            };
            let conn = Connection::connect(&a.amqp_url, ConnectionProperties::default())
                .await
                .context("Failed to connect to RabbitMQ")?;
            let channel = conn
                .create_channel()
                .await
                .context("Failed to open AMQP channel")?;
            channel
                .exchange_declare(
                    &a.exchange,
                    ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .context("Failed to declare exchange")?;
            Ok(Arc::new(RabbitmqWalSink {
                channel,
                exchange: a.exchange.clone(),
                routing_key: a.routing_key.clone(),
            }))
        }

        #[cfg(feature = "kafka")]
        ReplicateDownstreamCommand::Kafka(a) => {
            use rdkafka::config::ClientConfig;
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &a.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .context("Failed to create Kafka producer")?;
            Ok(Arc::new(KafkaWalSink {
                producer,
                topic: a.topic.clone(),
            }))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Parse a postgres:// URL into (host, port, user, password, database)
// ─────────────────────────────────────────────────────────────────────────────

fn parse_postgres_url(url: &str) -> Result<(String, u16, String, String, String)> {
    let parsed = url::Url::parse(url).with_context(|| format!("Invalid database URL: {url}"))?;

    let host = parsed.host_str().unwrap_or("127.0.0.1").to_string();
    let port = parsed.port().unwrap_or(5432);
    let user = parsed.username().to_string();
    let password = parsed.password().unwrap_or("").to_string();
    let database = parsed.path().trim_start_matches('/').to_string();

    Ok((host, port, user, password, database))
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter predicates
// ─────────────────────────────────────────────────────────────────────────────

fn table_matches(schema: &str, table: &str, filter: &[String]) -> bool {
    if filter.is_empty() {
        return true;
    }
    let qualified = format!("{schema}.{table}");
    filter.iter().any(|f| f == table || f == &qualified)
}

fn op_matches(op: &str, filter: &[OpFilter]) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().any(|f| match f {
        OpFilter::Insert => op == "insert",
        OpFilter::Update => op == "update",
        OpFilter::Delete => op == "delete",
        OpFilter::Truncate => op == "truncate",
    })
}

fn should_forward(event: &WalEvent, args: &ReplicateArgs) -> bool {
    match event {
        WalEvent::Insert { schema, table, .. }
        | WalEvent::Update { schema, table, .. }
        | WalEvent::Delete { schema, table, .. } => {
            let op = event.op_label().to_lowercase();
            table_matches(schema, table, &args.tables) && op_matches(&op, &args.ops)
        }
        WalEvent::Truncate { .. } => op_matches("truncate", &args.ops),
        WalEvent::Begin { .. } | WalEvent::Commit { .. } => args.emit_txn_boundaries,
        WalEvent::Relation { .. } => args.emit_schema,
        WalEvent::Keepalive { .. } => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event → env-var map (for shell sinks)
// ─────────────────────────────────────────────────────────────────────────────

fn event_env(event: &WalEvent, lsn_str: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert("PGX_OP".to_string(), event.op_label().to_lowercase());
    env.insert("PGX_LSN".to_string(), lsn_str.to_string());

    match event {
        WalEvent::Insert {
            schema, table, new, ..
        } => {
            env.insert("PGX_SCHEMA".to_string(), schema.clone());
            env.insert("PGX_TABLE".to_string(), table.clone());
            env.insert(
                "PGX_NEW".to_string(),
                serde_json::to_string(new).unwrap_or_default(),
            );
        }
        WalEvent::Update {
            schema,
            table,
            new,
            old,
            ..
        } => {
            env.insert("PGX_SCHEMA".to_string(), schema.clone());
            env.insert("PGX_TABLE".to_string(), table.clone());
            env.insert(
                "PGX_NEW".to_string(),
                serde_json::to_string(new).unwrap_or_default(),
            );
            if let Some(o) = old {
                env.insert(
                    "PGX_OLD".to_string(),
                    serde_json::to_string(o).unwrap_or_default(),
                );
            }
        }
        WalEvent::Delete {
            schema, table, old, ..
        } => {
            env.insert("PGX_SCHEMA".to_string(), schema.clone());
            env.insert("PGX_TABLE".to_string(), table.clone());
            env.insert(
                "PGX_OLD".to_string(),
                serde_json::to_string(old).unwrap_or_default(),
            );
        }
        WalEvent::Truncate { tables, .. } => {
            env.insert("PGX_TABLES".to_string(), tables.join(","));
        }
        WalEvent::Begin { xid, .. } => {
            env.insert("PGX_XID".to_string(), xid.to_string());
        }
        _ => {}
    }
    env
}

// ─────────────────────────────────────────────────────────────────────────────
// Console log helper
// ─────────────────────────────────────────────────────────────────────────────

fn log_event(event: &WalEvent, lsn_str: &str) {
    // Per-row events are logged at debug level — set RUST_LOG=debug to see them.
    // In JSON mode each becomes a structured record; in text mode it is a
    // coloured console line that mirrors the original output.
    match event {
        WalEvent::Insert { schema, table, .. } => debug!(
            op = "insert", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Update { schema, table, .. } => debug!(
            op = "update", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Delete { schema, table, .. } => debug!(
            op = "delete", schema = %schema, table = %table, lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Truncate { tables, .. } => debug!(
            op = "truncate", tables = %tables.join(", "), lsn = %lsn_str, "WAL event"
        ),
        WalEvent::Begin { xid, .. } => debug!(op = "begin", xid, "WAL event"),
        WalEvent::Commit { .. } => debug!(op = "commit", lsn = %lsn_str, "WAL event"),
        WalEvent::Relation {
            schema,
            table,
            columns,
            ..
        } => debug!(
            op = "relation", schema = %schema, table = %table,
            col_count = columns.len(), "WAL schema event"
        ),
        WalEvent::Keepalive { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main entry point
// ─────────────────────────────────────────────────────────────────────────────

pub async fn run(base_url: String, args: ReplicateArgs) -> Result<()> {
    let sink = build_wal_sink(&args.downstream).await?;

    // ── 1. Parse connection URL ───────────────────────────────────────────────
    let (host, port, user, password, database) = parse_postgres_url(&base_url)?;

    // ── 2. One-time control-plane setup ──────────────────────────────────────
    // Slot management only happens once before the retry loop: slots survive
    // across connections, and re-running ensure_slot on every reconnect is
    // harmless but noisy.
    info!("Connecting to PostgreSQL…");

    let (mgmt_client, mgmt_conn) = tokio_postgres::connect(&base_url, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;
    tokio::spawn(async move {
        if let Err(e) = mgmt_conn.await {
            error!(error = %e, "Management connection error");
        }
    });

    // Verify wal_level = logical
    let rows = mgmt_client
        .query("SHOW wal_level", &[])
        .await
        .context("Failed to query wal_level")?;
    let wal_level: &str = rows[0].get(0);
    if wal_level != "logical" {
        bail!(
            "wal_level is '{wal_level}' \u{2014} logical replication requires 'logical'.\n\
             Set `wal_level = logical` in postgresql.conf and restart the server."
        );
    }

    // Slot lifecycle (once, before the retry loop).
    // Temporary slots are created by the replication client itself.
    if args.reset_slot {
        warn!(slot = %args.slot, "Dropping slot (--reset-slot)");
        slot::drop_slot(&mgmt_client, &args.slot).await?;
    }
    if !args.temporary {
        slot::ensure_slot(&mgmt_client, &args.slot, false).await?;
    }
    info!(slot = %args.slot, "Slot ready");

    // ── 3. Build the base ReplicationConfig (cloned per attempt) ─────────────
    let pub_names = args.publications.join(", ");

    let initial_lsn = match &args.start_lsn {
        Some(s) => {
            Lsn::parse(s).map_err(|e| anyhow::anyhow!("Invalid start LSN '{}': {}", s, e))?
        }
        None => Lsn::ZERO,
    };

    let base_cfg = ReplicationConfig {
        host,
        port,
        user,
        password,
        database,
        slot: args.slot.clone(),
        publication: pub_names.clone(),
        start_lsn: initial_lsn,
        temporary: args.temporary,
        ..Default::default()
    };

    // ── 4. Reconnection loop ──────────────────────────────────────────────────
    const BASE_DELAY_MS: u64 = 1_000;
    const MAX_DELAY_MS: u64 = 60_000;
    const MAX_ATTEMPTS: u32 = 10;

    // The confirmed LSN advances each successful session.  It seeds start_lsn
    // on the next connect so we resume from the last durable checkpoint.
    let mut resume_lsn = initial_lsn;
    let mut attempt: u32 = 0;

    // Pin the shutdown future outside the retry loop so a signal cancels
    // both the event loop and any in-progress backoff sleep.
    tokio::pin!(let shutdown = shutdown_signal(););

    loop {
        // ── Backoff sleep (skipped on first attempt) ──────────────────────────
        if attempt > 0 {
            let base = (BASE_DELAY_MS * (1u64 << (attempt - 1).min(6))).min(MAX_DELAY_MS);
            let jitter = base / 5;
            let delay_ms = base - jitter + (rand::random::<u64>() % (jitter * 2 + 1));
            let delay = std::time::Duration::from_millis(delay_ms);

            warn!(
                attempt,
                max_attempts = MAX_ATTEMPTS,
                delay_secs = delay.as_secs_f32(),
                "Reconnecting after connection loss"
            );

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("Signal received, shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        if attempt >= MAX_ATTEMPTS {
            return Err(anyhow::anyhow!(
                "Giving up after {MAX_ATTEMPTS} consecutive connection failures"
            ));
        }

        // ── Open replication stream ───────────────────────────────────────────
        // Resume from the last confirmed LSN so we never re-deliver already-ACKed
        // events and never skip events we didn't confirm.
        let repl_cfg = ReplicationConfig {
            start_lsn: resume_lsn,
            ..base_cfg.clone()
        };

        info!(lsn = %resume_lsn, publications = %pub_names, "Starting replication…");
        info!(sink = sink.name(), "Forwarding events — Ctrl-C to stop");

        let mut repl_client = match ReplicationClient::connect(repl_cfg).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to open replication connection");
                attempt += 1;
                continue;
            }
        };

        // ── 5. Main event loop ────────────────────────────────────────────────
        let mut rel_cache = RelationCache::new();
        let mut clean_exit = false;

        loop {
            let ev = tokio::select! {
                biased;

                // ── Shutdown signal (SIGINT / SIGTERM) ────────────────────────
                _ = &mut shutdown => {
                    info!("Signal received, stopping replication");
                    repl_client.stop();
                    clean_exit = true;
                    break;
                }

                // ── Next event from the replication worker ────────────────────
                result = repl_client.recv() => result,
            };

            match ev {
                // ── Stream closed cleanly ─────────────────────────────────────
                Ok(None) => {
                    clean_exit = true;
                    break;
                }

                // ── Error from the replication worker ────────────────────────
                Err(e) => {
                    error!(error = %e, "Replication error");
                    break;
                }

                Ok(Some(ev)) => match ev {
                    // ── Keepalive: acknowledge so server can reclaim WAL ──────
                    ReplicationEvent::KeepAlive { wal_end } => {
                        repl_client.update_applied_lsn(wal_end);
                    }

                    // ── Transaction boundaries (Begin / Commit) ───────────────
                    //
                    // LSN durability: update_applied_lsn is called AFTER the sink
                    // confirms delivery. If the sink returns an error or the process
                    // crashes before this point, PostgreSQL will not advance its
                    // confirmed_flush_lsn for this slot, so the event will be
                    // re-delivered on reconnect \u2014 no data loss.
                    ReplicationEvent::Begin {
                        final_lsn,
                        xid,
                        commit_time,
                    } => {
                        if args.emit_txn_boundaries {
                            let event = WalEvent::Begin {
                                lsn: final_lsn.to_string(),
                                commit_time,
                                xid,
                            };
                            log_event(&event, &final_lsn.to_string());
                            let env = event_env(&event, &final_lsn.to_string());
                            if let Err(e) = sink.send_wal(&event.to_json(), &env).await {
                                error!(error = %e, "Downstream send failed (Begin); LSN not advanced");
                                continue;
                            }
                        }
                        repl_client.update_applied_lsn(final_lsn);
                    }

                    ReplicationEvent::Commit {
                        lsn,
                        end_lsn,
                        commit_time,
                    } => {
                        if args.emit_txn_boundaries {
                            let event = WalEvent::Commit {
                                lsn: lsn.to_string(),
                                end_lsn: end_lsn.to_string(),
                                commit_time,
                            };
                            log_event(&event, &end_lsn.to_string());
                            let env = event_env(&event, &end_lsn.to_string());
                            if let Err(e) = sink.send_wal(&event.to_json(), &env).await {
                                error!(error = %e, "Downstream send failed (Commit); LSN not advanced");
                                continue;
                            }
                        }
                        repl_client.update_applied_lsn(end_lsn);
                    }

                    // ── XLogData (Insert/Update/Delete/etc.) ──────────────────
                    ReplicationEvent::XLogData { data, wal_end, .. } => {
                        let lsn_str = wal_end.to_string();

                        match decode_pgoutput(&data, &mut rel_cache) {
                            Ok(Some(event)) => {
                                log_event(&event, &lsn_str);
                                if should_forward(&event, &args) {
                                    let env = event_env(&event, &lsn_str);
                                    if let Err(e) = sink.send_wal(&event.to_json(), &env).await {
                                        error!(sink = sink.name(), error = %e, "Downstream send failed; LSN not advanced");
                                        continue;
                                    }
                                }
                                // ACK after confirmed delivery or intentional skip.
                                repl_client.update_applied_lsn(wal_end);
                            }
                            Ok(None) => {
                                // Intentionally skipped message type; safe to ACK.
                                repl_client.update_applied_lsn(wal_end);
                            }
                            Err(e) => {
                                // Decode failure — do not advance LSN.
                                error!(error = %e, "WAL decode error; LSN not advanced");
                            }
                        }
                    }
                },
            }
        }

        // ── Post-loop: capture progress before dropping the client ────────────
        // This is the last durably confirmed LSN from this session.  On the
        // next connect we pass it as start_lsn so the slot resumes exactly
        // from where we left off.
        resume_lsn = repl_client.last_applied_lsn();

        if clean_exit {
            break;
        }

        // Unplanned disconnect \u2014 increment and loop back to backoff + reconnect.
        warn!(attempt, "Connection lost, will retry");
        attempt += 1;
    }

    info!("Replication stream closed");
    Ok(())
}
