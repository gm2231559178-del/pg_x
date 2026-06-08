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

    /// Row-level WHERE filters (repeatable).
    /// Format: [schema.table:]expression
    /// Examples:
    ///   --where "public.orders:status = 'active'"
    ///   --where "amount > 100"
    ///   --where "deleted_at IS NULL"
    /// Supported operators: =, !=, <>, >, <, >=, <=, IS NULL, IS NOT NULL
    /// Combinators: AND, OR
    #[arg(long = "where")]
    pub filters: Vec<String>,

    /// Column drop rules (repeatable).
    /// Format: [schema.table:]col1,col2,...
    /// Example: --drop-cols "public.orders:internal_note,secret_flag"
    #[arg(long = "drop-cols")]
    pub drop_cols: Vec<String>,

    /// Column rename rules (repeatable).
    /// Format: [schema.table:]old_name=new_name,...
    /// Example: --rename "public.orders:order_id=id,customer_name=name"
    #[arg(long = "rename")]
    pub rename: Vec<String>,

    /// Additional sinks for fan-out (repeatable).
    /// Format: type:key=val,key=val,...
    /// Types: stdout, shell, webhook, kafka, rabbitmq
    /// Example: --sink "webhook:url=https://hooks.example.com/events"
    ///          --sink "kafka:brokers=localhost:9092,topic=pgx-wal"
    #[arg(long = "sink")]
    pub additional_sinks: Vec<String>,

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

/// Build a single WalSink from a DownstreamSinkKind config.
async fn build_sink_from_kind(kind: &DownstreamSinkKind) -> Result<Arc<dyn WalSink>> {
    match kind {
        DownstreamSinkKind::Stdout { pretty } => {
            Ok(Arc::new(StdoutSink { pretty: pretty.unwrap_or(false) }))
        }
        DownstreamSinkKind::Shell { command, envs, .. } => {
            if command.is_empty() {
                anyhow::bail!("Shell sink requires command");
            }
            let mut base_env = HashMap::new();
            if let Some(env_list) = envs {
                for e in env_list {
                    if let Some((k, v)) = e.split_once('=') {
                        base_env.insert(k.to_string(), v.to_string());
                    }
                }
            }
            Ok(Arc::new(ShellWalSink {
                command: command.clone(),
                base_env,
            }))
        }
        DownstreamSinkKind::Webhook { url, headers, .. } => {
            let mut h = Vec::new();
            if let Some(hdrs) = headers {
                for entry in hdrs {
                    if let Some((k, v)) = entry.split_once('=') {
                        h.push((k.to_string(), v.to_string()));
                    }
                }
            }
            #[cfg(feature = "webhook")]
            {
                Ok(Arc::new(WebhookWalSink {
                    client: reqwest::Client::new(),
                    url: url.clone(),
                    default_headers: h.into_iter().collect(),
                }))
            }
            #[cfg(not(feature = "webhook"))]
            {
                let _ = h;
                anyhow::bail!("Webhook sink requires 'webhook' feature (reqwest)");
            }
        }
        DownstreamSinkKind::Kafka { brokers, topic, .. } => {
            #[cfg(feature = "kafka")]
            {
                use rdkafka::config::ClientConfig;
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.as_deref().unwrap_or("localhost:9092"))
                    .set("message.timeout.ms", "5000")
                    .create()
                    .context("Failed to create Kafka producer")?;
                Ok(Arc::new(KafkaWalSink {
                    producer,
                    topic: topic.clone().unwrap_or_else(|| "pgx-wal".to_string()),
                }))
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (brokers, topic);
                anyhow::bail!("Kafka sink requires 'kafka' feature (rdkafka)");
            }
        }
        DownstreamSinkKind::Rabbitmq { amqp_url, exchange, routing_key, .. } => {
            #[cfg(feature = "rabbitmq")]
            {
                use lapin::{
                    options::ExchangeDeclareOptions, types::FieldTable, Connection,
                    ConnectionProperties, ExchangeKind,
                };
                let url = amqp_url.clone().unwrap_or_else(|| "amqp://guest:guest@localhost:5672/%2F".to_string());
                let conn = Connection::connect(&url, ConnectionProperties::default())
                    .await
                    .context("Failed to connect to RabbitMQ")?;
                let channel = conn
                    .create_channel()
                    .await
                    .context("Failed to open AMQP channel")?;
                let exch = exchange.clone().unwrap_or_else(|| "pgx".to_string());
                channel
                    .exchange_declare(
                        &exch,
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
                    exchange: exch,
                    routing_key: routing_key.clone().unwrap_or_else(|| "pgx.wal".to_string()),
                }))
            }
            #[cfg(not(feature = "rabbitmq"))]
            {
                let _ = (amqp_url, exchange, routing_key);
                anyhow::bail!("RabbitMQ sink requires 'rabbitmq' feature (lapin)");
            }
        }
        DownstreamSinkKind::Elasticsearch { .. } => {
            anyhow::bail!("Elasticsearch sink is not supported for replication; use 'listen elasticsearch' instead");
        }
        DownstreamSinkKind::Postgres { .. } => {
            // PG applier is initialized separately; use no-op for the WS path.
            Ok(Arc::new(NoopSink))
        }
    }
}

/// Build a FanOutSink from the primary downstream command, --sink args, and config sinks.
async fn build_fan_out_sink(args: &ReplicateArgs, config_additional: &[DownstreamSinkKind]) -> Result<Arc<dyn WalSink>> {
    let mut sinks: Vec<Arc<dyn WalSink>> = Vec::new();

    // Primary sink from the subcommand
    let primary = build_wal_sink(&args.downstream).await?;
    sinks.push(primary);

    // Additional sinks from --sink flags (CLI)
    for s in &args.additional_sinks {
        let kind = parse_sink_string(s)?;
        let sink = build_sink_from_kind(&kind).await?;
        sinks.push(sink);
    }

    // Additional sinks from config file
    for kind in config_additional {
        let sink = build_sink_from_kind(kind).await?;
        sinks.push(sink);
    }

    if sinks.len() == 1 {
        Ok(sinks.into_iter().next().unwrap())
    } else {
        Ok(Arc::new(FanOutSink { sinks }))
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
// Fan-out: forward events to multiple sinks simultaneously
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps multiple WalSinks and fans out events to all of them.
struct FanOutSink {
    sinks: Vec<Arc<dyn WalSink>>,
}

#[async_trait::async_trait]
impl WalSink for FanOutSink {
    fn name(&self) -> &str {
        "fan-out"
    }

    async fn send_wal(&self, event_json: &str, env: &HashMap<String, String>) -> Result<()> {
        for sink in &self.sinks {
            sink.send_wal(event_json, env).await?;
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Parse --sink strings into DownstreamSinkKind
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a sink descriptor string into a DownstreamSinkKind.
///
/// Format: `type:key=val,key=val,...`
///
/// Examples:
///   stdout
///   stdout:pretty=true
///   webhook:url=https://hooks.example.com/events
///   kafka:brokers=localhost:9092,topic=pgx-wal
///   rabbitmq:amqp_url=amqp://guest:guest@localhost:5672/%2F
///   shell:command=echo $PGX_OP
fn parse_sink_string(s: &str) -> Result<DownstreamSinkKind> {
    let (sink_type, rest) = match s.split_once(':') {
        Some((t, r)) => (t.to_lowercase(), r),
        None => (s.to_lowercase(), ""),
    };

    let mut params: HashMap<String, String> = HashMap::new();
    if !rest.is_empty() {
        for pair in rest.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(k.to_string(), v.to_string());
            }
        }
    }

    match sink_type.as_str() {
        "stdout" => Ok(DownstreamSinkKind::Stdout {
            pretty: params.get("pretty").map(|v| v == "true"),
        }),
        "shell" => {
            let command = params
                .get("command")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("shell sink requires command=..."))?;
            Ok(DownstreamSinkKind::Shell {
                command,
                envs: params.get("envs").map(|e| e.split(',').map(String::from).collect()),
                mode: params.get("mode").cloned(),
            })
        }
        "webhook" => {
            let url = params
                .get("url")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("webhook sink requires url=..."))?;
            Ok(DownstreamSinkKind::Webhook {
                url,
                headers: params.get("headers").map(|h| h.split(',').map(String::from).collect()),
                mode: params.get("mode").cloned(),
            })
        }
        "kafka" => {
            Ok(DownstreamSinkKind::Kafka {
                brokers: params.get("brokers").cloned(),
                topic: params.get("topic").cloned(),
                mode: params.get("mode").cloned(),
            })
        }
        "rabbitmq" => {
            Ok(DownstreamSinkKind::Rabbitmq {
                amqp_url: params.get("amqp_url").cloned(),
                exchange: params.get("exchange").cloned(),
                routing_key: params.get("routing_key").cloned(),
                mode: params.get("mode").cloned(),
            })
        }
        other => {
            anyhow::bail!("Unknown sink type '{other}'. Supported: stdout, shell, webhook, kafka, rabbitmq");
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
// Filter expression — row-level WHERE for DML events
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum FilterExpr {
    Eq(String, String),
    Neq(String, String),
    Gt(String, f64),
    Lt(String, f64),
    Ge(String, f64),
    Le(String, f64),
    IsNull(String),
    IsNotNull(String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
}

impl FilterExpr {
    pub fn evaluate(&self, row: &Row) -> bool {
        match self {
            FilterExpr::Eq(col, val) => row.get(col).is_some_and(|cv| match cv {
                ColVal::Text(s) => s == val,
                _ => false,
            }),
            FilterExpr::Neq(col, val) => !row.get(col).is_some_and(|cv| match cv {
                ColVal::Text(s) => s == val,
                _ => false,
            }),
            FilterExpr::Gt(col, val) => cmp_numeric(row, col, |a, b| a > b, *val),
            FilterExpr::Lt(col, val) => cmp_numeric(row, col, |a, b| a < b, *val),
            FilterExpr::Ge(col, val) => cmp_numeric(row, col, |a, b| a >= b, *val),
            FilterExpr::Le(col, val) => cmp_numeric(row, col, |a, b| a <= b, *val),
            FilterExpr::IsNull(col) => {
                row.get(col).is_some_and(|cv| matches!(cv, ColVal::Null))
            }
            FilterExpr::IsNotNull(col) => {
                !row.get(col).is_some_and(|cv| matches!(cv, ColVal::Null))
            }
            FilterExpr::And(a, b) => a.evaluate(row) && b.evaluate(row),
            FilterExpr::Or(a, b) => a.evaluate(row) || b.evaluate(row),
        }
    }
}

fn cmp_numeric(row: &Row, col: &str, cmp: fn(f64, f64) -> bool, rhs: f64) -> bool {
    row.get(col).and_then(|cv| match cv {
        ColVal::Text(s) => s.parse::<f64>().ok(),
        _ => None,
    })
    .is_some_and(|lhs| cmp(lhs, rhs))
}

// ─────────────────────────────────────────────────────────────────────────────
// Recursive-descent filter expression parser
// ─────────────────────────────────────────────────────────────────────────────

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.chars.peek() {
            if c.is_ascii_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn expect_word(&mut self) -> Result<String> {
        self.skip_ws();
        let mut s = String::new();
        while let Some(c) = self.chars.peek() {
            if c.is_alphanumeric() || *c == '_' {
                s.push(self.chars.next().unwrap());
            } else {
                break;
            }
        }
        if s.is_empty() {
            bail!("expected identifier");
        }
        Ok(s)
    }

    fn expect_string_literal(&mut self) -> Result<String> {
        self.skip_ws();
        match self.chars.next() {
            Some('\'') => {
                let mut s = String::new();
                loop {
                    match self.chars.next() {
                        Some('\'') => {
                            // Check for escaped single quote ''
                            if self.chars.peek() == Some(&'\'') {
                                self.chars.next();
                                s.push('\'');
                            } else {
                                return Ok(s);
                            }
                        }
                        Some(c) => s.push(c),
                        None => bail!("unterminated string literal"),
                    }
                }
            }
            Some(c) => bail!("expected ' to start string literal, got '{c}'"),
            None => bail!("expected string literal"),
        }
    }

    fn expect_number(&mut self) -> Result<f64> {
        self.skip_ws();
        let mut s = String::new();
        if self.chars.peek() == Some(&'-') {
            s.push(self.chars.next().unwrap());
        }
        let mut has_dot = false;
        while let Some(c) = self.chars.peek() {
            if c.is_ascii_digit() {
                s.push(self.chars.next().unwrap());
            } else if *c == '.' && !has_dot {
                has_dot = true;
                s.push(self.chars.next().unwrap());
            } else {
                break;
            }
        }
        if s.is_empty() || s == "-" {
            bail!("expected number, got '{s}'");
        }
        s.parse::<f64>()
            .with_context(|| format!("invalid number literal: '{s}'"))
    }

    fn parse_literal(&mut self) -> Result<FilterExpr> {
        self.skip_ws();
        if self.chars.peek() == Some(&'\'') {
            // String literal without preceding identifier or operator.
            self.expect_string_literal()?;
            bail!("unexpected string literal without comparison")
        } else {
            // Try number first, then identifier (column name)
            let saved = self.chars.clone();
            match self.expect_number() {
                Ok(_) => {
                    bail!("unexpected number literal without comparison")
                }
                _ => {
                    self.chars = saved;
                    let ident = self.expect_word()?;
                    // Check for IS NULL / IS NOT NULL
                    self.skip_ws();
                    let upper: String = self.chars.clone().take(2).collect::<String>().to_uppercase();
                    if upper == "IS" {
                        self.chars.next(); self.chars.next(); // skip I, S
                        self.skip_ws();
                        let neg = {
                            let next: String = self.chars.clone().take(3).collect::<String>().to_uppercase();
                            if next == "NOT" {
                                self.chars.next(); self.chars.next(); self.chars.next();
                                true
                            } else {
                                false
                            }
                        };
                        self.skip_ws();
                        let null = self.expect_word()?;
                        if null.to_uppercase() != "NULL" {
                            bail!("expected NULL after IS{}", if neg { " NOT" } else { "" });
                        }
                        if neg {
                            Ok(FilterExpr::IsNotNull(ident))
                        } else {
                            Ok(FilterExpr::IsNull(ident))
                        }
                    } else {
                        // Must be followed by an operator
                        Err(anyhow::anyhow!("expected comparison operator after identifier '{ident}'"))
                    }
                }
            }
        }
    }

    fn parse_comparison(&mut self) -> Result<FilterExpr> {
        self.skip_ws();

        let saved = self.chars.clone();
        let ident = self.expect_word()?;
        self.skip_ws();

        let op = self.parse_operator();
        if let Some(op) = op {
            self.skip_ws();
            if self.chars.peek() == Some(&'\'') {
                let val = self.expect_string_literal()?;
                return Ok(match op {
                    "=" => FilterExpr::Eq(ident, val),
                    "!=" | "<>" => FilterExpr::Neq(ident, val),
                    _ => bail!("string comparison does not support operator '{op}'"),
                });
            } else {
                let n = self.expect_number()?;
                return Ok(match op {
                    "=" => FilterExpr::Eq(ident, n.to_string()),
                    "!=" | "<>" => FilterExpr::Neq(ident, n.to_string()),
                    ">" => FilterExpr::Gt(ident, n),
                    "<" => FilterExpr::Lt(ident, n),
                    ">=" => FilterExpr::Ge(ident, n),
                    "<=" => FilterExpr::Le(ident, n),
                    _ => bail!("unknown operator '{op}'"),
                });
            }
        }

        // No operator after identifier — likely IS NULL/IS NOT NULL (handled by parse_literal)
        self.chars = saved;
        self.parse_literal()
    }

    fn parse_operator(&mut self) -> Option<&'static str> {
        self.skip_ws();
        let mut two = String::new();
        two.push(*self.chars.peek()?);
        let c2 = {
            let mut it = self.chars.clone();
            it.next();
            it.peek().copied()
        };
        if let Some(c2) = c2 {
            two.push(c2);
        }
        match two.as_str() {
            ">=" => { self.chars.next(); self.chars.next(); Some(">=") }
            "<=" => { self.chars.next(); self.chars.next(); Some("<=") }
            "<>" => { self.chars.next(); self.chars.next(); Some("<>") }
            "!=" => { self.chars.next(); self.chars.next(); Some("!=") }
            _ => {
                let one = self.chars.peek().copied()?;
                match one {
                    '=' => { self.chars.next(); Some("=") }
                    '>' => { self.chars.next(); Some(">") }
                    '<' => { self.chars.next(); Some("<") }
                    _ => None,
                }
            }
        }
    }

    fn parse_expression(&mut self) -> Result<FilterExpr> {
        let mut left = self.parse_comparison()?;
        loop {
            self.skip_ws();
            let peek: String = self.chars.clone().take(4).collect::<String>().to_uppercase();
            if peek.starts_with("AND") {
                self.chars.next(); self.chars.next(); self.chars.next();
                let right = self.parse_comparison()?;
                left = FilterExpr::And(Box::new(left), Box::new(right));
            } else if peek.starts_with("OR") {
                self.chars.next(); self.chars.next();
                let right = self.parse_comparison()?;
                left = FilterExpr::Or(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }
}

/// Parse a single filter expression string (without table prefix).
pub fn parse_filter_expr(input: &str) -> Result<FilterExpr> {
    let mut parser = Parser::new(input);
    let expr = parser.parse_expression()?;
    parser.skip_ws();
    if parser.chars.peek().is_some() {
        bail!(
            "trailing characters after filter expression: '{}'",
            parser.chars.collect::<String>()
        );
    }
    Ok(expr)
}

// ─────────────────────────────────────────────────────────────────────────────
// RowFilter — collection of table-scoped WHERE expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A set of row-level filters keyed optionally by table.
///
/// - `None` key  → filter applies to every table.
/// - `Some((schema, table))` → filter applies only to that table.
/// - A DML event passes through only if every matching filter evaluates to true.
/// - Events for tables with no matching filter always pass through.
#[derive(Debug, Clone)]
pub struct RowFilter {
    filters: Vec<(Option<(String, String)>, FilterExpr)>,
}

impl RowFilter {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Add a filter: `None` for global, `Some((schema, table))` for table-specific.
    pub fn add(&mut self, table_key: Option<(String, String)>, expr: FilterExpr) {
        self.filters.push((table_key, expr));
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Build from CLI `--where` args (format: `[schema.table:]expr`).
    pub fn from_cli_args(filters: &[String]) -> Result<Self> {
        let mut rf = RowFilter::new();
        for arg in filters {
            let (table_key, expr) = parse_where_arg(arg)?;
            rf.add(table_key, expr);
        }
        Ok(rf)
    }

    /// Returns `true` if the event should be forwarded.
    pub fn should_forward(&self, event: &WalEvent) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let (schema, table, row_option) = match event {
            WalEvent::Insert { schema, table, new, .. } => (schema, table, Some(new)),
            WalEvent::Update { schema, table, new, .. } => (schema, table, Some(new)),
            WalEvent::Delete { schema, table, old, .. } => (schema, table, Some(old)),
            _ => return true,
        };
        let row = match row_option {
            Some(r) => r,
            None => return true,
        };
        // Check all matching filters — event passes if all evaluate to true
        for (key, expr) in &self.filters {
            let applies = match key {
                Some((s, t)) => s == schema && t == table,
                None => true,
            };
            if !applies {
                continue;
            }
            if !expr.evaluate(row) {
                return false;
            }
        }
        true
    }
}

/// Parse a `--where` arg into an optional table key and filter expression.
///
/// Input formats:
///     `schema.table:expression` → `Some(("schema", "table")), expr`
///     `expression`              → `None, expr`
pub fn parse_where_arg(arg: &str) -> Result<(TableKey, FilterExpr)> {
    let colon_pos = arg.find(':');
    // Reject bare colon (empty prefix) — likely a typo.
    if colon_pos == Some(0) {
        bail!("filter expression starts with ':' but no table prefix before it — \
               use 'schema.table:expression' or omit the colon for global filters");
    }
    let table_key = match colon_pos {
        Some(pos) => {
            let prefix = &arg[..pos];
            if let Some(dot) = prefix.find('.') {
                // schema.table:expr
                Some((prefix[..dot].to_string(), prefix[dot + 1..].to_string()))
            } else {
                // plain table name — no dot, treat as schema-less name? Actually
                // for safety, require schema-qualified prefix for table filtering.
                // A bare word before ':' could conflict with expressions like "x > 5".
                // We treat a single word before : as a table name filter.
                // But that's ambiguous with expressions. Let's just require schema.table.
                return Err(anyhow::anyhow!(
                    "filter prefix must be schema-qualified (e.g. public.orders:expression), \
                     got '{prefix}' — use 'public.{prefix}:expression' or omit the prefix for global filters"
                ));
            }
        }
        _ => None,
    };
    let expr_str = match colon_pos {
        Some(pos) => arg[pos + 1..].trim(),
        None => arg.trim(),
    };
    if expr_str.is_empty() {
        bail!("empty filter expression");
    }
    let expr = parse_filter_expr(expr_str)?;
    Ok((table_key, expr))
}

// ─────────────────────────────────────────────────────────────────────────────
// Column transforms — drop / rename columns before forwarding
// ─────────────────────────────────────────────────────────────────────────────

/// Per-table column transforms.
#[derive(Debug, Clone, Default)]
pub struct TableTransform {
    pub drop_cols: Vec<String>,
    pub renames: Vec<(String, String)>,
}

/// Collection of column transforms scoped by table.
#[derive(Debug, Clone, Default)]
pub struct ColumnTransforms {
    /// key = `"schema.table"`, `None` = global
    entries: Vec<(Option<(String, String)>, TableTransform)>,
}

impl ColumnTransforms {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(|(_, t)| t.drop_cols.is_empty() && t.renames.is_empty())
    }

    /// Apply applicable transforms to a WalEvent in-place.
    pub fn apply(&self, event: &mut WalEvent) {
        let tn = event.table_name().map(|(s, t)| (s.to_string(), t.to_string()));
        let (schema, table) = match tn {
            Some(ref p) => p,
            None => return,
        };
        for (key, transform) in &self.entries {
            let applies = match key {
                Some((ref s, ref t)) => s == schema && t == table,
                None => true,
            };
            if applies {
                event.apply_transforms(&transform.drop_cols, &transform.renames);
            }
        }
    }
}

type TableKey = Option<(String, String)>;

/// Parse a `--drop-cols` argument: `[schema.table:]col1,col2,...`
pub fn parse_drop_cols_arg(arg: &str) -> Result<(TableKey, Vec<String>)> {
    let (table_key, rest) = parse_table_prefix(arg)?;
    let cols: Vec<String> = rest.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    if cols.is_empty() {
        bail!("drop-cols: no columns specified in '{arg}'");
    }
    Ok((table_key, cols))
}

/// Parse a `--rename` argument: `[schema.table:]old=new,old2=new2,...`
pub fn parse_rename_arg(arg: &str) -> Result<(TableKey, Vec<(String, String)>)> {
    let (table_key, rest) = parse_table_prefix(arg)?;
    let mut pairs = Vec::new();
    for part in rest.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut eq_split = part.splitn(2, '=');
        let old = eq_split.next().unwrap().trim().to_string();
        let new = eq_split.next().ok_or_else(|| {
            anyhow::anyhow!("rename: expected 'old=new' format, got '{part}'")
        })?.trim().to_string();
        if old.is_empty() || new.is_empty() {
            bail!("rename: empty name in rename pair '{part}'");
        }
        pairs.push((old, new));
    }
    if pairs.is_empty() {
        bail!("rename: no rename pairs specified in '{arg}'");
    }
    Ok((table_key, pairs))
}

/// Extract optional `schema.table:` prefix from an argument string.
/// Returns `(table_key, rest_of_string)`.
fn parse_table_prefix(arg: &str) -> Result<(TableKey, &str)> {
    if let Some(pos) = arg.find(':') {
        let prefix = &arg[..pos];
        if prefix.is_empty() {
            bail!("empty table prefix before ':'");
        }
        let table_key = if let Some(dot) = prefix.find('.') {
            Some((prefix[..dot].to_string(), prefix[dot + 1..].to_string()))
        } else {
            return Err(anyhow::anyhow!(
                "prefix must be schema-qualified (e.g. public.orders:...), \
                 got '{prefix}' — use 'public.{prefix}:...' or omit the prefix for global rules"
            ));
        };
        Ok((table_key, &arg[pos + 1..]))
    } else {
        Ok((None, arg))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter helpers (table name, op)
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

fn should_forward(event: &WalEvent, args: &ReplicateArgs, row_filter: &RowFilter) -> bool {
    match event {
        WalEvent::Insert { schema, table, .. }
        | WalEvent::Update { schema, table, .. }
        | WalEvent::Delete { schema, table, .. } => {
            let op = event.op_label().to_lowercase();
            table_matches(schema, table, &args.tables)
                && op_matches(&op, &args.ops)
                && row_filter.should_forward(event)
        }
        WalEvent::Truncate { tables, .. } => {
            op_matches("truncate", &args.ops)
                && (args.tables.is_empty()
                    || tables.iter().any(|t| args.tables.iter().any(|f| f == t)))
        }
        WalEvent::Begin { .. } | WalEvent::Commit { .. } => args.emit_txn_boundaries,
        WalEvent::Relation { .. } => args.emit_schema,
        WalEvent::Keepalive { .. } => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event → env-var map (for shell sinks)
// ─────────────────────────────────────────────────────────────────────────────

fn json_or_dash(v: &impl serde::Serialize) -> String {
    match serde_json::to_string(v) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "Failed to serialize row for event_env");
            String::new()
        }
    }
}

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
            env.insert("PGX_NEW".to_string(), json_or_dash(new));
        }
        WalEvent::Update {
            schema, table, new, old, ..
        } => {
            env.insert("PGX_SCHEMA".to_string(), schema.clone());
            env.insert("PGX_TABLE".to_string(), table.clone());
            env.insert("PGX_NEW".to_string(), json_or_dash(new));
            if let Some(o) = old {
                env.insert("PGX_OLD".to_string(), json_or_dash(o));
            }
        }
        WalEvent::Delete {
            schema, table, old, ..
        } => {
            env.insert("PGX_SCHEMA".to_string(), schema.clone());
            env.insert("PGX_TABLE".to_string(), table.clone());
            env.insert("PGX_OLD".to_string(), json_or_dash(old));
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
    let mut config_additional_sinks: Vec<DownstreamSinkKind> = Vec::new();
    let mut config_filters: Vec<String> = Vec::new();
    let mut config_drop_cols: Vec<String> = Vec::new();
    let mut config_rename: Vec<String> = Vec::new();
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

        config_additional_sinks = cfg.additional_sinks.clone();
        config_filters = cfg.filters.clone();
        config_drop_cols = cfg.drop_cols.clone();
        config_rename = cfg.rename.clone();
    }

    let row_filter = {
        let mut all: Vec<String> = Vec::new();
        all.extend(config_filters);
        all.extend(args.filters.clone());
        RowFilter::from_cli_args(&all)?
    };
    if !row_filter.is_empty() {
        info!("Row-level WHERE filters active");
    }

    let transforms = {
        let mut t = ColumnTransforms::new();
        for arg in config_drop_cols.iter().chain(args.drop_cols.iter()) {
            let (key, cols) = parse_drop_cols_arg(arg)?;
            let entry = t.entries.iter_mut().find(|(k, _)| k == &key);
            match entry {
                Some((_, tt)) => tt.drop_cols.extend(cols),
                None => t.entries.push((key, TableTransform { drop_cols: cols, renames: Vec::new() })),
            }
        }
        for arg in config_rename.iter().chain(args.rename.iter()) {
            let (key, pairs) = parse_rename_arg(arg)?;
            let entry = t.entries.iter_mut().find(|(k, _)| k == &key);
            match entry {
                Some((_, tt)) => tt.renames.extend(pairs),
                None => t.entries.push((key, TableTransform { drop_cols: Vec::new(), renames: pairs })),
            }
        }
        t
    };
    if !transforms.is_empty() {
        info!("Column transforms active");
    }

    let slot_name = args.slot.clone().unwrap_or_else(|| "pgx_slot".to_string());
    let max_reconnect_attempts = args.max_reconnect_attempts.unwrap_or(0);
    let reconnect_base_ms = args.reconnect_base_ms.unwrap_or(1000);
    let reconnect_max_ms = args.reconnect_max_ms.unwrap_or(60000);
    let sink = build_fan_out_sink(&args, &config_additional_sinks).await?;

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

        const RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

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

                // ── Connection stall guard ────────────────────────────────────
                // If the server stops sending data or TCP silently drops,
                // break to trigger a reconnect instead of hanging forever.
                _ = tokio::time::sleep(RECV_TIMEOUT) => {
                    warn!("Replication stream idle for 60s, reconnecting");
                    repl_client.stop();
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
                            Ok(Some(mut event)) => {
                                // Evaluate row filter BEFORE transforms so WHERE expressions
                                // use the original (pre-rename) column names.
                                let forward = should_forward(&event, &args, &row_filter);

                                // Apply column transforms for all downstreams.
                                transforms.apply(&mut event);

                                log_event(&event, &lsn_str);

                                // PG applier: process event (schema sync, buffer DML)
                                if let Some(ref mut applier) = pg_applier {
                                    if let Err(e) = applier.handle_event(&event).await {
                                        error!(error = %e, "PG applier event failed; LSN not advanced");
                                        continue;
                                    }
                                }

                                if forward {
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
