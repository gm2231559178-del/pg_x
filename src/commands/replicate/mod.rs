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
//!
//! ## CLI examples
//!
//! ### Stream decoded WAL to stdout
//! ```bash
//! pgx replicate --publication my_pub stdout
//! ```
//!
//! ### Filter by table and operation
//! ```bash
//! pgx replicate --publication my_pub --table public.orders --op insert --op update stdout
//! ```
//!
//! ### Row-level WHERE filter
//! ```bash
//! pgx replicate --publication my_pub --table public.orders \
//!   --where "public.orders:status = 'active' AND amount > 100" stdout
//! ```
//!
//! ### Apply changes to another PostgreSQL
//! ```bash
//! pgx replicate --publication my_pub \
//!   postgres --target-url "postgres://user:pass@replica:5432/db" \
//!   --schema-map "public.orders=public.orders_archive" --batch-size 500
//! ```
//!
//! ### Write WAL events to Apache Parquet files
//! ```bash
//! pgx replicate --publication my_pub parquet --output-dir ./wal_archive
//! ```
//!
//! ### Apply to PG + archive to Parquet simultaneously
//! ```bash
//! pgx replicate --publication my_pub \
//!   postgres --target-url "postgres://user:pass@replica:5432/db" \
//!   --sink "parquet:output_dir=./wal_archive,compression=zstd"
//! ```
//!
//! ### Full pipeline — filter, rename, drop, fan-out
//! ```bash
//! pgx replicate --publication my_pub --table public.orders \
//!   --drop-cols "public.orders:ssn,credit_card" \
//!   --rename "public.orders:order_id=id,customer_name=name" \
//!   --where "public.orders:status = 'active'" \
//!   stdout --pretty \
//!   --sink "webhook:url=https://hooks.example.com/orders"
//! ```

mod applier;
mod filter;
#[cfg(feature = "iceberg")]
mod iceberg;
#[cfg(feature = "parquet")]
mod parquet;
mod sinks;
mod transforms;

pub(crate) use replicate_session::{PGX_LSN, PGX_OP, PGX_PAYLOAD, PGX_SCHEMA, PGX_TABLE};

mod replicate_session;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};

use std::sync::Arc;
use tracing::{error, info, warn};

use crate::replication::{
    client::{ReplicationClient, ReplicationConfig},
    lsn::Lsn,
    slot,
};
use crate::utils::config::{merge_bool, merge_opt, merge_vec, Connection, DownstreamSinkKind};
use crate::utils::session_loop::{self, ReconnectConfig, SessionExit};
use crate::utils::signal::{parse_key_val, shutdown_signal};
use crate::utils::tls;

use self::applier::PostgresApplier;
use self::filter::RowFilter;
use self::replicate_session::{Applier, ReplicateSession, ReplicateSessionConfig};
use self::sinks::build_fan_out_sink;
use self::transforms::{parse_drop_cols_arg, parse_rename_arg, ColumnTransforms, TableTransform};

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

    /// Write WAL events to Apache Parquet files.
    #[cfg(feature = "parquet")]
    Parquet(parquet::ParquetArgs),

    /// Write WAL events to Apache Iceberg tables (COW).
    #[cfg(feature = "iceberg")]
    Iceberg(iceberg::IcebergArgs),
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
        merge_opt(&mut args.slot, &cfg.slot);
        merge_vec(&mut args.publications, &cfg.publications);
        merge_vec(&mut args.tables, &cfg.tables);
        if args.ops.is_empty() && !cfg.ops.is_empty() {
            args.ops = cfg.ops.iter().filter_map(|o| {
                o.parse().map_err(|_| tracing::warn!("Ignoring invalid op filter '{o}' in config (expected insert|update|delete|truncate)")).ok()
            }).collect();
        }
        merge_bool(&mut args.temporary, cfg.temporary);
        merge_bool(&mut args.emit_txn_boundaries, cfg.emit_txn_boundaries);
        merge_bool(&mut args.emit_schema, cfg.emit_schema);
        merge_opt(
            &mut args.max_reconnect_attempts,
            &cfg.max_reconnect_attempts,
        );
        merge_opt(&mut args.reconnect_base_ms, &cfg.reconnect_base_ms);
        merge_opt(&mut args.reconnect_max_ms, &cfg.reconnect_max_ms);

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
                    merge_opt(&mut a.command, &Some(command.clone()));
                }
                #[cfg(feature = "webhook")]
                (
                    ReplicateDownstreamCommand::Webhook(a),
                    DownstreamSinkKind::Webhook { url, .. },
                ) => {
                    merge_opt(&mut a.url, &Some(url.clone()));
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
                    merge_opt(&mut a.target_url, &Some(target_url.clone()));
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
                None => t.entries.push((
                    key,
                    TableTransform {
                        drop_cols: cols,
                        renames: Vec::new(),
                    },
                )),
            }
        }
        for arg in config_rename.iter().chain(args.rename.iter()) {
            let (key, pairs) = parse_rename_arg(arg)?;
            let entry = t.entries.iter_mut().find(|(k, _)| k == &key);
            match entry {
                Some((_, tt)) => tt.renames.extend(pairs),
                None => t.entries.push((
                    key,
                    TableTransform {
                        drop_cols: Vec::new(),
                        renames: pairs,
                    },
                )),
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
    let pg_applier: Option<PostgresApplier> = match &args.downstream {
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

    let session_config = ReplicateSessionConfig {
        emit_txn_boundaries: args.emit_txn_boundaries,
        emit_schema: args.emit_schema,
        tables: args.tables.clone(),
        ops: args.ops.clone(),
    };
    let resume_lsn = Arc::new(tokio::sync::Mutex::new(initial_lsn));
    let pg_applier: Arc<tokio::sync::Mutex<Option<Box<dyn Applier>>>> = Arc::new(
        tokio::sync::Mutex::new(pg_applier.map(|a| Box::new(a) as Box<dyn Applier>)),
    );

    let reconnect = ReconnectConfig {
        max_attempts: max_reconnect_attempts,
        base_ms: reconnect_base_ms,
        max_ms: reconnect_max_ms,
    };

    let sink_for_session = sink.clone();

    session_loop::run(
        move |shutdown| {
            let session_config = session_config.clone();
            let sink = sink_for_session.clone();
            let base_cfg = base_cfg.clone();
            let row_filter = row_filter.clone();
            let transforms = transforms.clone();
            let pub_names = pub_names.clone();
            let resume_lsn = resume_lsn.clone();
            let pg_applier = pg_applier.clone();

            async move {
                let mut shutdown = shutdown;

                let start_lsn = *resume_lsn.lock().await;
                let repl_cfg = ReplicationConfig {
                    start_lsn,
                    ..base_cfg
                };

                info!(lsn = %start_lsn, publications = %pub_names, "Starting replication…");
                info!(sink = sink.name(), "Forwarding events — Ctrl-C to stop");

                let mut repl_client = match ReplicationClient::connect(repl_cfg).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "Failed to open replication connection");
                        return SessionExit::Reconnect;
                    }
                };

                // ── 5. Run one session (decode → filter → transform → deliver) ──
                let mut session = ReplicateSession::new(
                    session_config,
                    row_filter,
                    transforms,
                    sink,
                    pg_applier,
                    resume_lsn,
                );
                session.run(&mut repl_client, &mut shutdown).await
            }
        },
        shutdown_signal(),
        &reconnect,
    )
    .await?;

    if let Err(e) = sink.flush().await {
        warn!(error = %e, "Failed to flush sink on shutdown");
    }

    info!("Replication stream closed");
    Ok(())
}
