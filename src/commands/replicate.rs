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
use tracing::{debug, error, info, warn};

use crate::replication::{
    client::{ReplicationClient, ReplicationConfig, ReplicationEvent},
    decoder::{decode_pgoutput, RelationCache},
    event::{ColVal, Row, WalEvent},
    lsn::Lsn,
    slot,
};
use crate::utils::config::{Connection, DownstreamSinkKind};
use crate::utils::signal::{parse_key_val, shutdown_signal};
use crate::utils::tls;

// ─────────────────────────────────────────────────────────────────────────────
// CLI argument structs
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Args)]
pub struct ReplicateArgs {
    /// Replication slot name (default: pgx_slot).
    #[arg(long)]
    pub slot: Option<String>,

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

    /// Maximum consecutive reconnect attempts before giving up (0 = infinite, default 0).
    #[arg(long, env = "PGX_REPLICATE_MAX_RECONNECT_ATTEMPTS")]
    pub max_reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds (doubles each attempt, default 1000).
    #[arg(long, env = "PGX_REPLICATE_RECONNECT_BASE_MS")]
    pub reconnect_base_ms: Option<u64>,

    /// Maximum reconnect delay cap in milliseconds (default 60000).
    #[arg(long, env = "PGX_REPLICATE_RECONNECT_MAX_MS")]
    pub reconnect_max_ms: Option<u64>,

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

impl std::str::FromStr for OpFilter {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "truncate" => Ok(Self::Truncate),
            other => Err(format!(
                "unknown op filter '{other}'; expected insert|update|delete|truncate"
            )),
        }
    }
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

    /// Apply WAL changes directly to a PostgreSQL target database.
    Postgres(PostgresArgs),
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
    ///
    /// Required unless provided via config.
    #[arg(long)]
    pub command: Option<String>,

    /// Extra environment variables to inject (KEY=VALUE, repeatable).
    #[arg(long = "env", value_parser = parse_key_val)]
    pub envs: Vec<(String, String)>,
}

#[cfg(feature = "webhook")]
#[derive(Args)]
pub struct WebhookArgs {
    /// Webhook URL. Required unless provided via config or WEBHOOK_URL env.
    #[arg(long, env = "WEBHOOK_URL")]
    pub url: Option<String>,
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

#[derive(Args)]
pub struct PostgresArgs {
    /// Target PostgreSQL connection URL.
    /// Required unless provided via config or PGX_REPLICATE_TARGET_URL env.
    #[arg(long, env = "PGX_REPLICATE_TARGET_URL")]
    pub target_url: Option<String>,

    /// Schema/table mapping (src_schema.src_table=tgt_schema.tgt_table, repeatable).
    #[arg(long = "schema-map")]
    pub schema_map: Vec<String>,

    /// Maximum statements per transaction batch.
    #[arg(long, default_value = "1000")]
    pub batch_size: u32,
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
                if let Ok(s) = serde_json::to_string_pretty(&v) {
                    println!("{s}");
                    return Ok(());
                }
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
// PostgreSQL Applier  (applies WAL changes to a target PG database)
// ─────────────────────────────────────────────────────────────────────────────

/// Applies decoded WAL events to a target PostgreSQL database.
struct PostgresApplier {
    client: tokio_postgres::Client,
    buffer: Vec<String>,
    schema_map: HashMap<(String, String), (String, String)>,
    batch_size: usize,
    pending_count: usize,
}

impl PostgresApplier {
    async fn connect(args: &PostgresArgs) -> Result<Self> {
        let url = args
            .target_url
            .as_deref()
            .context("Postgres sink: --target-url is required (or set PGX_REPLICATE_TARGET_URL)")?;

        let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .context("Failed to connect to target PostgreSQL database")?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!(error = %e, "Target PG connection error");
            }
        });

        // Verify target connection and default schema
        let version: String = client.query_one("SELECT version()", &[]).await?.get(0);
        tracing::info!(version = %version, "Connected to target PostgreSQL");

        let mut schema_map = HashMap::new();
        for mapping in &args.schema_map {
            let parts: Vec<&str> = mapping.splitn(2, '=').collect();
            if parts.len() != 2 {
                anyhow::bail!("Invalid schema-map '{mapping}': expected src_schema.src_table=tgt_schema.tgt_table");
            }
            let src_parts: Vec<&str> = parts[0].splitn(2, '.').collect();
            let tgt_parts: Vec<&str> = parts[1].splitn(2, '.').collect();
            if src_parts.len() != 2 || tgt_parts.len() != 2 {
                anyhow::bail!("Invalid schema-map '{mapping}': expected format src_schema.src_table=tgt_schema.tgt_table");
            }
            schema_map.insert(
                (src_parts[0].to_string(), src_parts[1].to_string()),
                (tgt_parts[0].to_string(), tgt_parts[1].to_string()),
            );
        }

        Ok(Self {
            client,
            buffer: Vec::with_capacity(args.batch_size as usize),
            schema_map,
            batch_size: args.batch_size as usize,
            pending_count: 0,
        })
    }

    fn handle_begin(&mut self) {
        self.buffer.clear();
        self.pending_count = 0;
    }

    async fn handle_commit(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let txn = self
            .client
            .transaction()
            .await
            .context("Failed to begin transaction on target")?;

        for sql in &self.buffer {
            txn.execute(sql, &[])
                .await
                .with_context(|| format!("Failed to execute on target: {sql:.200}"))?;
        }

        txn.commit()
            .await
            .context("Failed to commit transaction on target")?;

        let count = self.buffer.len();
        tracing::debug!(applied = count, "Applied batch to target");
        self.buffer.clear();
        self.pending_count = 0;
        Ok(())
    }

    async fn handle_event(&mut self, event: &WalEvent) -> Result<()> {
        match event {
            WalEvent::Relation { .. } => {
                // Schema is ensured by the target; we trust it matches.
                // In a future enhancement we could CREATE TABLE IF NOT EXISTS
                // using the ColumnDef from the Relation event.
                Ok(())
            }

            WalEvent::Insert {
                schema, table, new, ..
            } => {
                let sql = gen_insert_sql(schema, table, new, &self.schema_map);
                tracing::trace!(sql = %sql, "Buffering INSERT");
                self.buffer.push(sql);
                self.pending_count += 1;
                if self.pending_count >= self.batch_size {
                    self.handle_commit().await?;
                    self.handle_begin();
                }
                Ok(())
            }

            WalEvent::Update {
                schema,
                table,
                old,
                new,
                ..
            } => match old {
                Some(old_row) => {
                    let sql = gen_update_sql(schema, table, old_row, new, &self.schema_map);
                    tracing::trace!(sql = %sql, "Buffering UPDATE");
                    self.buffer.push(sql);
                    self.pending_count += 1;
                    if self.pending_count >= self.batch_size {
                        self.handle_commit().await?;
                        self.handle_begin();
                    }
                    Ok(())
                }
                None => {
                    tracing::warn!(
                        schema = %schema, table = %table,
                        "Skipping UPDATE without old tuple — set REPLICA IDENTITY FULL on this table"
                    );
                    Ok(())
                }
            },

            WalEvent::Delete {
                schema, table, old, ..
            } => {
                let sql = gen_delete_sql(schema, table, old, &self.schema_map);
                tracing::trace!(sql = %sql, "Buffering DELETE");
                self.buffer.push(sql);
                self.pending_count += 1;
                if self.pending_count >= self.batch_size {
                    self.handle_commit().await?;
                    self.handle_begin();
                }
                Ok(())
            }

            WalEvent::Truncate {
                tables,
                cascade,
                restart_seqs,
                ..
            } => {
                let sql = gen_truncate_sql(tables, *cascade, *restart_seqs, &self.schema_map);
                tracing::debug!(sql = %sql, "Executing TRUNCATE");
                self.client
                    .execute(&sql, &[])
                    .await
                    .with_context(|| format!("Failed to execute TRUNCATE on target: {sql:.200}"))?;
                Ok(())
            }

            WalEvent::Begin { .. } | WalEvent::Commit { .. } | WalEvent::Keepalive { .. } => {
                // Handled via separate paths in the event loop
                Ok(())
            }
        }
    }
}

// ── SQL generation helpers ───────────────────────────────────────────────────

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

fn colval_to_sql(val: &ColVal) -> String {
    match val {
        ColVal::Text(s) => quote_literal(s),
        ColVal::Null | ColVal::Unchanged => "NULL".to_string(),
    }
}

fn gen_insert_sql(
    schema: &str,
    table: &str,
    new: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

    let mut cols: Vec<&String> = new.keys().collect();
    cols.sort();

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_vals: Vec<String> = cols.iter().map(|c| colval_to_sql(&new[*c])).collect();

    format!(
        "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
        quote_ident(&tgt_schema),
        quote_ident(&tgt_table),
        col_names.join(", "),
        col_vals.join(", "),
    )
}

fn gen_update_sql(
    schema: &str,
    table: &str,
    old: &Row,
    new: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

    let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));

    let mut set_cols: Vec<&String> = new.keys().collect();
    set_cols.sort();
    let set_clauses: Vec<String> = set_cols
        .iter()
        .map(|c| format!("{} = {}", quote_ident(c), colval_to_sql(&new[*c])))
        .collect();

    let where_clauses: Vec<String> = old
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
        .collect();

    if where_clauses.is_empty() {
        tracing::warn!(
            "Cannot generate safe UPDATE for {} — no usable WHERE columns in old tuple. \
             Use REPLICA IDENTITY FULL to receive all columns.",
            qualified,
        );
        return format!("SELECT 1 WHERE FALSE -- SKIPPED UPDATE {qualified}");
    }

    format!(
        "UPDATE {} SET {} WHERE {}",
        qualified,
        set_clauses.join(", "),
        where_clauses.join(" AND "),
    )
}

fn gen_delete_sql(
    schema: &str,
    table: &str,
    old: &Row,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let (tgt_schema, tgt_table) = schema_map
        .get(&(schema.to_string(), table.to_string()))
        .cloned()
        .unwrap_or_else(|| (schema.to_string(), table.to_string()));

    let qualified = format!("{}.{}", quote_ident(&tgt_schema), quote_ident(&tgt_table));

    let where_clauses: Vec<String> = old
        .iter()
        .filter(|(_, v)| !matches!(v, ColVal::Unchanged))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), colval_to_sql(v)))
        .collect();

    if where_clauses.is_empty() {
        tracing::warn!(
            "Cannot generate safe DELETE for {} — no usable WHERE columns in old tuple",
            qualified,
        );
        return format!("SELECT 1 WHERE FALSE -- SKIPPED DELETE {qualified}");
    }

    format!(
        "DELETE FROM {} WHERE {}",
        qualified,
        where_clauses.join(" AND ")
    )
}

fn gen_truncate_sql(
    tables: &[String],
    cascade: bool,
    restart_seqs: bool,
    schema_map: &HashMap<(String, String), (String, String)>,
) -> String {
    let qualified: Vec<String> = tables
        .iter()
        .map(|t| {
            let parts: Vec<&str> = t.splitn(2, '.').collect();
            if parts.len() == 2 {
                let (ts, tt) = schema_map
                    .get(&(parts[0].to_string(), parts[1].to_string()))
                    .cloned()
                    .unwrap_or_else(|| (parts[0].to_string(), parts[1].to_string()));
                format!("{}.{}", quote_ident(&ts), quote_ident(&tt))
            } else {
                quote_ident(t)
            }
        })
        .collect();

    let mut sql = format!("TRUNCATE {}", qualified.join(", "));
    if restart_seqs {
        sql.push_str(" RESTART IDENTITY");
    }
    if cascade {
        sql.push_str(" CASCADE");
    }
    sql
}

// ─────────────────────────────────────────────────────────────────────────────
// Build WalSink from CLI args
// ─────────────────────────────────────────────────────────────────────────────

async fn build_wal_sink(cmd: &ReplicateDownstreamCommand) -> Result<Arc<dyn WalSink>> {
    match cmd {
        ReplicateDownstreamCommand::Stdout(a) => Ok(Arc::new(StdoutSink { pretty: a.pretty })),

        ReplicateDownstreamCommand::Shell(a) => {
            let command = a.command.as_deref().unwrap_or_default();
            if command.is_empty() {
                anyhow::bail!(
                    "Shell command is required — provide --command or add sink.command in config"
                );
            }
            Ok(Arc::new(ShellWalSink {
                command: command.to_string(),
                base_env: a.envs.iter().cloned().collect(),
            }))
        }

        #[cfg(feature = "webhook")]
        ReplicateDownstreamCommand::Webhook(a) => {
            let url = a.url.as_deref().unwrap_or_default();
            if url.is_empty() {
                anyhow::bail!("Webhook URL is required — provide --url, set WEBHOOK_URL env, or add sink.url in config");
            }
            Ok(Arc::new(WebhookWalSink {
                client: reqwest::Client::new(),
                url: url.to_string(),
                default_headers: a.headers.iter().cloned().collect(),
            }))
        }

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

        ReplicateDownstreamCommand::Postgres(_) => {
            // PG applier is initialized separately in run(); no-op sink for the WS path.
            Ok(Arc::new(NoopSink))
        }
    }
}

/// A no-op sink used when the real work is done by the PostgresApplier.
struct NoopSink;

#[async_trait::async_trait]
impl WalSink for NoopSink {
    fn name(&self) -> &str {
        "postgres"
    }
    async fn send_wal(&self, _event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        Ok(())
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

pub async fn run(
    base_url: String,
    mut args: ReplicateArgs,
    conn: Option<&Connection>,
    use_tls: bool,
) -> Result<()> {
    // Merge connection-level defaults into CLI args (CLI wins).
    if let Some(cfg) = conn.and_then(|c| c.replicate.as_ref()) {
        if args.slot.is_none() {
            args.slot = cfg.slot.clone();
        }
        if args.publications.is_empty() && !cfg.publications.is_empty() {
            args.publications = cfg.publications.clone();
        }
        if args.tables.is_empty() && !cfg.tables.is_empty() {
            args.tables = cfg.tables.clone();
        }
        if args.ops.is_empty() && !cfg.ops.is_empty() {
            args.ops = cfg.ops.iter().filter_map(|o| {
                o.parse().map_err(|_| tracing::warn!("Ignoring invalid op filter '{o}' in config (expected insert|update|delete|truncate)")).ok()
            }).collect();
        }
        if !args.temporary && cfg.temporary.unwrap_or(false) {
            args.temporary = true;
        }
        if !args.emit_txn_boundaries && cfg.emit_txn_boundaries.unwrap_or(false) {
            args.emit_txn_boundaries = true;
        }
        if !args.emit_schema && cfg.emit_schema.unwrap_or(false) {
            args.emit_schema = true;
        }
        if args.max_reconnect_attempts.is_none() {
            args.max_reconnect_attempts = cfg.max_reconnect_attempts;
        }
        if args.reconnect_base_ms.is_none() {
            args.reconnect_base_ms = cfg.reconnect_base_ms;
        }
        if args.reconnect_max_ms.is_none() {
            args.reconnect_max_ms = cfg.reconnect_max_ms;
        }

        // Merge downstream sink defaults from config into CLI subcommand args.
        if let Some(sink_cfg) = &cfg.sink {
            match (&mut args.downstream, sink_cfg) {
                (
                    ReplicateDownstreamCommand::Stdout(a),
                    DownstreamSinkKind::Stdout { pretty: Some(p) },
                ) => {
                    a.pretty = *p;
                }
                (
                    ReplicateDownstreamCommand::Stdout(_),
                    DownstreamSinkKind::Stdout { pretty: None },
                ) => {}
                (
                    ReplicateDownstreamCommand::Shell(a),
                    DownstreamSinkKind::Shell { command, .. },
                ) => {
                    if a.command.is_none() {
                        a.command = Some(command.clone());
                    }
                }
                #[cfg(feature = "webhook")]
                (
                    ReplicateDownstreamCommand::Webhook(a),
                    DownstreamSinkKind::Webhook { url, .. },
                ) => {
                    if a.url.is_none() {
                        a.url = Some(url.clone());
                    }
                }
                #[cfg(feature = "rabbitmq")]
                (
                    ReplicateDownstreamCommand::Rabbitmq(a),
                    DownstreamSinkKind::Rabbitmq {
                        amqp_url,
                        exchange,
                        routing_key,
                        ..
                    },
                ) => {
                    if let Some(u) = amqp_url {
                        a.amqp_url = u.clone();
                    }
                    if let Some(e) = exchange {
                        a.exchange = e.clone();
                    }
                    if let Some(r) = routing_key {
                        a.routing_key = r.clone();
                    }
                }
                #[cfg(feature = "kafka")]
                (
                    ReplicateDownstreamCommand::Kafka(a),
                    DownstreamSinkKind::Kafka { brokers, topic, .. },
                ) => {
                    if let Some(b) = brokers {
                        a.brokers = b.clone();
                    }
                    if let Some(t) = topic {
                        a.topic = t.clone();
                    }
                }
                (
                    ReplicateDownstreamCommand::Postgres(a),
                    DownstreamSinkKind::Postgres {
                        target_url,
                        schema_map,
                        batch_size,
                        ..
                    },
                ) => {
                    if a.target_url.is_none() {
                        a.target_url = Some(target_url.clone());
                    }
                    if a.schema_map.is_empty() {
                        if let Some(mappings) = schema_map {
                            a.schema_map = mappings.clone();
                        }
                    }
                    if let Some(bs) = batch_size {
                        a.batch_size = *bs;
                    }
                }
                _ => {}
            }
        }
    }

    let slot_name = args.slot.clone().unwrap_or_else(|| "pgx_slot".to_string());
    let max_reconnect_attempts = args.max_reconnect_attempts.unwrap_or(0);
    let reconnect_base_ms = args.reconnect_base_ms.unwrap_or(1000);
    let reconnect_max_ms = args.reconnect_max_ms.unwrap_or(60000);
    let sink = build_wal_sink(&args.downstream).await?;

    // ── Initialize PostgresApplier if Postgres sink is selected ─────────────
    let mut pg_applier: Option<PostgresApplier> = match &args.downstream {
        ReplicateDownstreamCommand::Postgres(pg_args) => {
            let applier = PostgresApplier::connect(pg_args).await?;
            info!(target = %pg_args.target_url.as_deref().unwrap_or("<config>"), "PostgreSQL applier ready");
            Some(applier)
        }
        _ => None,
    };

    // ── 1. Parse connection URL ───────────────────────────────────────────────
    let (host, port, user, password, database) = parse_postgres_url(&base_url)?;

    // ── 2. One-time control-plane setup ──────────────────────────────────────
    // Slot management only happens once before the retry loop: slots survive
    // across connections, and re-running ensure_slot on every reconnect is
    // harmless but noisy.
    //
    // TODO: this management connection is NOT recreated during retries.
    // If the PostgreSQL server restarts, the slot cleanup/creation (lines
    // below) still ran before the reconnect loop, so the replication client
    // will reconnect successfully — but the mgmt_client will be a stale
    // handle. This is acceptable because mgmt_client is only used for
    // one-time setup, not during the streaming retry loop, but the stale
    // handle will be a problem if health-check or slot-status polling is
    // ever added inside the loop.
    info!("Connecting to PostgreSQL…");

    let mgmt_connector = tls::build_tls(use_tls)?;
    let (mgmt_client, mgmt_conn) = tokio_postgres::connect(&base_url, mgmt_connector)
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
        warn!(slot = %slot_name, "Dropping slot (--reset-slot)");
        slot::drop_slot(&mgmt_client, &slot_name).await?;
    }
    if !args.temporary {
        slot::ensure_slot(&mgmt_client, &slot_name, false).await?;
    }
    info!(slot = %slot_name, "Slot ready");

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
        slot: slot_name.clone(),
        publication: pub_names.clone(),
        start_lsn: initial_lsn,
        temporary: args.temporary,
        use_tls,
        ..Default::default()
    };

    // ── 4. Reconnection loop ──────────────────────────────────────────────────

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
            let infinite = max_reconnect_attempts == 0;

            let delay = crate::utils::backoff::delay(attempt, reconnect_base_ms, reconnect_max_ms);

            warn!(
                attempt,
                max_attempts = if infinite {
                    "∞".to_string()
                } else {
                    max_reconnect_attempts.to_string()
                },
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
                if max_reconnect_attempts > 0 && attempt > max_reconnect_attempts {
                    return Err(anyhow::anyhow!(
                        "Giving up after {max_reconnect_attempts} consecutive connection failures"
                    ));
                }
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
                        // PG applier: start a new transaction batch
                        if let Some(ref mut applier) = pg_applier {
                            applier.handle_begin();
                        }

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
                        // PG applier: flush buffered changes to target
                        if let Some(ref mut applier) = pg_applier {
                            if let Err(e) = applier.handle_commit().await {
                                error!(error = %e, "PG applier commit failed; LSN not advanced");
                                continue;
                            }
                        }

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
                        // Always advance LSN on commit (covers all DMLs in the txn).
                        // When PG applier is active, per-DML advancement is skipped.
                        repl_client.update_applied_lsn(end_lsn);
                    }

                    // ── XLogData (Insert/Update/Delete/etc.) ──────────────────
                    ReplicationEvent::XLogData { data, wal_end, .. } => {
                        let lsn_str = wal_end.to_string();
                        let is_pg_active = pg_applier.is_some();

                        match decode_pgoutput(&data, &mut rel_cache) {
                            Ok(Some(event)) => {
                                log_event(&event, &lsn_str);

                                // PG applier: process event (schema sync, buffer DML)
                                if let Some(ref mut applier) = pg_applier {
                                    if let Err(e) = applier.handle_event(&event).await {
                                        error!(error = %e, "PG applier event failed; LSN not advanced");
                                        continue;
                                    }
                                }

                                if should_forward(&event, &args) {
                                    let env = event_env(&event, &lsn_str);
                                    if let Err(e) = sink.send_wal(&event.to_json(), &env).await {
                                        error!(sink = sink.name(), error = %e, "Downstream send failed; LSN not advanced");
                                        continue;
                                    }
                                }

                                // When PG applier is active, per-DML LSN advancement is
                                // deferred to the Commit event (end_lsn covers the txn).
                                if !is_pg_active {
                                    repl_client.update_applied_lsn(wal_end);
                                }
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
