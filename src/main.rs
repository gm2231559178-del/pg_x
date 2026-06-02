mod commands;
mod downstream;
mod replication;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{doctor, export, info, listen, profiles, psql, query, replicate};
use utils::config::Config;

/// pgx — PostgreSQL power CLI (beyond psql & pg_*)
#[derive(Parser)]
#[command(
    name = "pgx",
    version,
    about = "A feature-rich PostgreSQL CLI tool",
    long_about = None,
    arg_required_else_help = true,
)]
struct Cli {
    /// PostgreSQL connection URL
    /// e.g. postgres://user:pass@localhost:5432/mydb
    #[arg(short = 'U', long = "url", env = "DATABASE_URL", global = true)]
    url: Option<String>,

    /// Named connection from ~/.pgx/config.toml
    #[arg(short = 'c', long = "conn", global = true)]
    connection: Option<String>,

    /// Emit logs as newline-delimited JSON (useful for log aggregators).
    /// Can also be set with PGX_LOG_JSON=1.
    #[arg(
        long = "log-json",
        env = "PGX_LOG_JSON",
        global = true,
        default_value_t = false
    )]
    log_json: bool,

    /// Enable TLS for the PostgreSQL connection.
    /// Requires building with --features tls.
    #[arg(long = "tls", global = true, default_value_t = false)]
    tls: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export SQL query results to Excel (.xlsx), CSV, or JSON
    Export(export::ExportArgs),

    /// Run a SQL query and display results in the terminal
    Query(query::QueryArgs),

    /// Show database / server information
    Info(info::InfoArgs),

    /// Subscribe to PostgreSQL NOTIFY channels and forward events to a downstream sink
    Listen(listen::ListenArgs),

    /// Stream WAL changes via PostgreSQL logical replication (INSERT/UPDATE/DELETE)
    Replicate(replicate::ReplicateArgs),

    /// Open an interactive psql session (or run a command via psql)
    Psql(psql::PsqlArgs),

    /// Diagnose your pgx installation and environment
    Doctor(doctor::DoctorArgs),

    /// Manage pgx connection profiles
    Profiles(profiles::ProfilesArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Initialise structured logging ─────────────────────────────────────────
    // Log level is controlled by RUST_LOG (defaults to "info").
    // JSON format is activated with --log-json or PGX_LOG_JSON=1.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if cli.log_json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_current_span(false)
            .with_span_list(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .init();
    }

    // Resolve connection URL: flag > named connection > config default.
    // The (optional) connection name is used to look up sink defaults.
    // Doctor handles its own URL internally (no URL required for diagnosis).
    let cfg = Config::load()?;

    // ── Commands that don't need a URL ──────────────────────────────────────
    match &cli.command {
        Commands::Doctor(d) => return doctor::run(d, cli.url.clone(), cli.connection.clone()).await,
        Commands::Profiles(p) => return profiles::run(p),
        _ => {}
    }

    // ── Resolve connection URL and dispatch ──────────────────────────────────
    let (url, conn_name) = cfg.resolve_from(cli.url, cli.connection)?;
    let conn = conn_name.as_ref().and_then(|name| cfg.get(name));

    match cli.command {
        Commands::Export(args) => export::run(url, args, cli.tls).await,
        Commands::Query(args) => query::run(url, args, cli.tls).await,
        Commands::Info(args) => info::run(url, args, cli.tls).await,
        Commands::Listen(args) => listen::run(url, args, conn, cli.tls).await,
        Commands::Replicate(args) => replicate::run(url, args, conn, cli.tls).await,
        Commands::Psql(args) => psql::run(url, args),
        _ => unreachable!(),
    }
}
