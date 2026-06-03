use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::graphql::{executor, pool::QueryPool, query::QueryLoader, schema::SchemaRegistry};
use crate::utils::config::{Config, ResolverConfig};
use crate::utils::db::connect;

#[derive(Args)]
pub struct GraphqlArgs {
    #[command(subcommand)]
    pub command: GraphqlCommand,
}

#[derive(Subcommand)]
pub enum GraphqlCommand {
    /// Dry-run validation of schema, resolvers, and query files
    Validate(ValidateArgs),
    /// Execute a named query and print the assembled JSON
    Run(RunArgs),
}

#[derive(Args)]
pub struct ValidateArgs {
    /// Path to a directory with .graphql type definition files
    #[arg(long, default_value = "~/.pgx/schema")]
    pub schema_dir: Option<String>,
}

#[derive(Args)]
pub struct RunArgs {
    /// Name of the query to run (without .graphql extension)
    pub query_name: String,
    /// Variables in KEY=VALUE format
    #[arg(short = 'V', long = "var", value_parser = parse_var)]
    pub vars: Vec<(String, String)>,
    /// Print compact JSON (no pretty-print)
    #[arg(long, default_value_t = false)]
    pub compact: bool,
    /// Output JSON to file instead of stdout
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,
}

fn parse_var(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!("Invalid variable format: use KEY=VALUE"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

pub async fn run(
    url: String,
    args: GraphqlArgs,
    conn_name: Option<&str>,
    use_tls: bool,
) -> Result<()> {
    match args.command {
        GraphqlCommand::Validate(a) => validate(url, a, conn_name, use_tls).await,
        GraphqlCommand::Run(a) => run_query(url, a, conn_name, use_tls).await,
    }
}

async fn validate(
    url: String,
    args: ValidateArgs,
    _conn_name: Option<&str>,
    use_tls: bool,
) -> Result<()> {
    let schema_dir = resolve_schema_dir(args.schema_dir.as_deref())?;
    let config = Config::load()?;

    // 1. Load schema type definitions
    println!("Loading schema from: {}", schema_dir.display());
    let schema = SchemaRegistry::load_from_dir(&schema_dir)?;
    println!("  Found {} type definitions", schema.types.len());

    // 2. Load resolver config
    let resolvers = &config.resolvers;
    println!("  Found {} resolver configurations", resolvers.len());

    // 3. Load query files
    let queries = QueryLoader::load(&schema)?;
    println!("  Found {} named query files", queries.queries.len());

    // 4. Verify every selected field has a resolver
    for (qname, query) in &queries.queries {
        println!("  Validating query '{}'...", qname);
        validate_selection(&query.selection, resolvers, &schema, qname)?;
    }

    // 5. Verify resolver SQL is valid by connecting
    let _client = connect(&url, use_tls).await?;
    for (rname, resolver) in resolvers {
        // Try to EXPLAIN the SQL to verify syntax
        match _client
            .query(&format!("EXPLAIN {}", resolver.sql), &[])
            .await
        {
            Ok(_) => {}
            Err(e) => {
                anyhow::bail!(
                    "Resolver '{}' has invalid SQL: {}\n  SQL: {}",
                    rname,
                    e,
                    resolver.sql
                );
            }
        }
    }

    println!("✓ All validations passed");
    Ok(())
}

fn validate_selection(
    selection: &crate::graphql::query::FieldSelection,
    resolvers: &HashMap<String, ResolverConfig>,
    _schema: &SchemaRegistry,
    qname: &str,
) -> Result<()> {
    for field in &selection.children {
        let field_name = field
            .field_name
            .split('(')
            .next()
            .unwrap_or(&field.field_name);
        if !field.is_leaf && !field.children.is_empty() && !resolvers.contains_key(field_name) {
            anyhow::bail!(
                "Query '{}' selects field '{}' which has no resolver configured",
                qname,
                field_name
            );
        }
        if !field.children.is_empty() {
            validate_selection(field, resolvers, _schema, qname)?;
        }
    }
    Ok(())
}

async fn run_query(
    url: String,
    args: RunArgs,
    _conn_name: Option<&str>,
    use_tls: bool,
) -> Result<()> {
    let config = Config::load()?;

    // Load schema
    let schema_dir = resolve_schema_dir(None)?;
    let schema = SchemaRegistry::load_from_dir(&schema_dir)?;

    // Load queries
    let queries = QueryLoader::load(&schema)?;
    let query = queries
        .get(&args.query_name)
        .with_context(|| format!("No query named '{}' found", args.query_name))?;

    // Build variables map
    let mut variables = HashMap::new();
    for (k, v) in &args.vars {
        variables.insert(k.clone(), serde_json::Value::String(v.clone()));
    }

    // Connect and execute
    let pool: QueryPool = QueryPool::connect(&url, use_tls).await?;
    let resolvers: &HashMap<String, crate::utils::config::ResolverConfig> = &config.resolvers;

    let result: serde_json::Value = executor::execute(query, &variables, resolvers, &pool).await?;

    // Output
    let json_str = if args.compact {
        serde_json::to_string(&result)?
    } else {
        serde_json::to_string_pretty(&result)?
    };

    if let Some(path) = &args.output {
        std::fs::write(path, &json_str)
            .with_context(|| format!("Cannot write output to: {}", path.display()))?;
        println!("Wrote {} to {}", human_size(json_str.len()), path.display());
    } else {
        println!("{}", json_str);
    }

    Ok(())
}

fn resolve_schema_dir(override_dir: Option<&str>) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        let dir = dir.replace('~', &dirs::home_dir().unwrap().to_string_lossy());
        return Ok(PathBuf::from(dir));
    }
    let home = dirs::home_dir().context("Cannot determine home directory")?;
    Ok(home.join(".pgx").join("schema"))
}

fn human_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}
