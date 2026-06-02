use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::replication::slot::list_slots;
use crate::utils::db::connect;

#[derive(Args, Debug)]
pub struct InfoArgs {
    /// Show active connections
    #[arg(long)]
    pub connections: bool,

    /// Show all databases
    #[arg(long)]
    pub databases: bool,

    /// Show tables in current database
    #[arg(long)]
    pub tables: bool,

    /// Show server version and settings
    #[arg(long, default_value_t = true)]
    pub version: bool,

    /// Show logical replication slots
    #[arg(long)]
    pub slots: bool,

    /// Show publications and their tables
    #[arg(long)]
    pub publications: bool,

    /// Show installed extensions
    #[arg(long)]
    pub extensions: bool,

    /// Show indexes in current database
    #[arg(long)]
    pub indexes: bool,

    /// Show user-defined functions/procedures
    #[arg(long)]
    pub functions: bool,

    /// Show PostgreSQL configuration settings
    #[arg(long)]
    pub settings: bool,
}

pub async fn run(url: String, args: InfoArgs, use_tls: bool) -> Result<()> {
    let client = connect(&url, use_tls).await?;

    if args.version {
        let row = client.query_one("SELECT version()", &[]).await?;
        let v: String = row.get(0);
        println!("{} {}", "PostgreSQL:".cyan().bold(), v);
    }

    if args.databases {
        println!("\n{}", "── Databases ──".cyan().bold());
        let rows = client
            .query(
                "SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size \
                 FROM pg_database WHERE datistemplate = false ORDER BY datname",
                &[],
            )
            .await?;
        for r in &rows {
            let name: String = r.get(0);
            let size: String = r.get(1);
            println!("  {:<30} {}", name.yellow(), size.dimmed());
        }
    }

    if args.tables {
        println!("\n{}", "── Tables ──".cyan().bold());
        let rows = client
            .query(
                "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) \
                 FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') \
                 ORDER BY schemaname, tablename",
                &[],
            )
            .await?;
        for r in &rows {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            let size: String = r.get(2);
            println!(
                "  {}.{:<40} {}",
                schema.dimmed(),
                table.yellow(),
                size.dimmed()
            );
        }
    }

    if args.connections {
        println!("\n{}", "── Active Connections ──".cyan().bold());
        let rows = client
            .query(
                "SELECT pid, usename, application_name, client_addr, state, query_start \
                 FROM pg_stat_activity WHERE state IS NOT NULL ORDER BY query_start DESC NULLS LAST",
                &[],
            )
            .await?;
        for r in &rows {
            let pid: i32 = r.get(0);
            let user: Option<String> = r.get(1);
            let app: Option<String> = r.get(2);
            let state: Option<String> = r.get(4);
            println!(
                "  pid={} user={} app={} state={}",
                pid.to_string().yellow(),
                user.unwrap_or_default().cyan(),
                app.unwrap_or_default().dimmed(),
                state.unwrap_or_default().green(),
            );
        }
    }

    if args.slots {
        println!("\n{}", "── Replication Slots ──".cyan().bold());
        let slots = list_slots(&client).await?;
        if slots.is_empty() {
            println!("  {}", "(no logical replication slots)".dimmed());
        } else {
            for s in &slots {
                let status = if s.active {
                    "active".green()
                } else {
                    "inactive".dimmed()
                };
                println!(
                    "  {:<30}  plugin={:<12}  db={:<12}  {}  flush={}  restart={}",
                    s.name.yellow(),
                    s.plugin,
                    s.database.as_deref().unwrap_or("-"),
                    status,
                    s.confirmed_flush_lsn.as_deref().unwrap_or("-").dimmed(),
                    s.restart_lsn.as_deref().unwrap_or("-").dimmed(),
                );
            }
        }
    }

    if args.publications {
        println!("\n{}", "── Publications ──".cyan().bold());
        let rows = client
            .query(
                "SELECT p.pubname, COALESCE(puballtables, false), \
                 (SELECT string_agg(n.nspname || '.' || c.relname, ', ' ORDER BY n.nspname, c.relname) \
                  FROM pg_publication_rel pr \
                  JOIN pg_class c ON c.oid = pr.prrelid \
                  JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE pr.prpubid = p.oid) AS tables \
                 FROM pg_publication p ORDER BY p.pubname",
                &[],
            )
            .await?;
        if rows.is_empty() {
            println!("  {}", "(no publications)".dimmed());
        } else {
            for r in &rows {
                let name: String = r.get(0);
                let all_tables: bool = r.get(1);
                let tables: Option<String> = r.get(2);
                let info = if all_tables {
                    "ALL TABLES".green().to_string()
                } else if let Some(t) = tables {
                    t.dimmed().to_string()
                } else {
                    "(no tables)".dimmed().to_string()
                };
                println!("  {:<30}  {}", name.yellow(), info);
            }
        }
    }

    if args.extensions {
        println!("\n{}", "── Extensions ──".cyan().bold());
        let rows = client
            .query(
                "SELECT extname, extversion, n.nspname \
                 FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace \
                 ORDER BY extname",
                &[],
            )
            .await?;
        if rows.is_empty() {
            println!("  {}", "(no extensions)".dimmed());
        } else {
            for r in &rows {
                let name: String = r.get(0);
                let version: String = r.get(1);
                let schema: String = r.get(2);
                println!(
                    "  {:<30}  v{:<12}  schema: {}",
                    name.yellow(),
                    version,
                    schema.dimmed(),
                );
            }
        }
    }

    if args.settings {
        println!("\n{}", "── Settings ──".cyan().bold());
        let rows = client
            .query(
                "SELECT name, setting, unit, context \
                 FROM pg_settings ORDER BY name",
                &[],
            )
            .await?;
        for r in &rows {
            let name: String = r.get(0);
            let setting: String = r.get(1);
            let unit: Option<String> = r.get(2);
            let context: String = r.get(3);
            let val = match unit {
                Some(u) => format!("{} {}", setting, u),
                None => setting,
            };
            println!(
                "  {:<40} {}  [{}]",
                name.yellow(),
                val.cyan(),
                context.dimmed()
            );
        }
    }

    if args.indexes {
        println!("\n{}", "── Indexes ──".cyan().bold());
        let rows = client
            .query(
                "SELECT schemaname, tablename, indexname, pg_size_pretty(pg_relation_size(indexrelid)) \
                 FROM pg_indexes \
                 WHERE schemaname NOT IN ('pg_catalog','information_schema') \
                 ORDER BY schemaname, tablename, indexname",
                &[],
            )
            .await?;
        if rows.is_empty() {
            println!("  {}", "(no indexes)".dimmed());
        } else {
            for r in &rows {
                let schema: String = r.get(0);
                let table: String = r.get(1);
                let index: String = r.get(2);
                let size: String = r.get(3);
                println!(
                    "  {}.{}  →  {}  [{}]",
                    schema.dimmed(),
                    table,
                    index.yellow(),
                    size.dimmed(),
                );
            }
        }
    }

    if args.functions {
        println!("\n{}", "── Functions ──".cyan().bold());
        let rows = client
            .query(
                "SELECT n.nspname, p.proname, pg_get_function_arguments(p.oid), \
                        CASE WHEN p.prorettype = 0 THEN 'trigger' \
                             ELSE format_type(p.prorettype, NULL) END AS return_type \
                 FROM pg_proc p \
                 JOIN pg_namespace n ON n.oid = p.pronamespace \
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema') \
                   AND p.prokind IN ('f', 'p') \
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?;
        if rows.is_empty() {
            println!("  {}", "(no user functions)".dimmed());
        } else {
            for r in &rows {
                let schema: String = r.get(0);
                let name: String = r.get(1);
                let args: String = r.get(2);
                let ret: String = r.get(3);
                println!(
                    "  {}.{}({})  → {}",
                    schema.dimmed(),
                    name.yellow(),
                    args.dimmed(),
                    ret,
                );
            }
        }
    }

    Ok(())
}
