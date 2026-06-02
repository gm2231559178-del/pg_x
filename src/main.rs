mod commands;
mod downstream;
mod replication;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{export, info, listen, query, replicate};
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

    // Resolve connection URL: flag > named connection > config default
    let url = resolve_url(cli.url, cli.connection)?;

    // Build TLS connector if requested
    let _tls = utils::tls::build_tls(cli.tls)?;

    match cli.command {
        Commands::Export(args) => export::run(url, args).await,
        Commands::Query(args) => query::run(url, args).await,
        Commands::Info(args) => info::run(url, args).await,
        Commands::Listen(args) => listen::run(url, args).await,
        Commands::Replicate(args) => replicate::run(url, args).await,
    }
}

fn resolve_url(flag: Option<String>, conn_name: Option<String>) -> Result<String> {
    if let Some(u) = flag {
        return Ok(u);
    }

    let cfg = Config::load()?;

    if let Some(name) = conn_name {
        return cfg
            .connection(&name)
            .ok_or_else(|| anyhow::anyhow!("No connection named '{}' in config", name));
    }

    cfg.default_url().ok_or_else(|| {
        anyhow::anyhow!(
            "No database URL supplied.\n\
             Use -U <url>, set DATABASE_URL, or add a default in ~/.pgx/config.toml"
        )
    })
}
