use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Instant;

#[cfg(feature = "excel")]
use crate::utils::excel::write_excel;
use crate::utils::{csv::write_csv, db::connect, format::RowSet, json::write_json};

/// A parsed query block with an optional sheet name.
struct QueryBlock {
    sheet: String,
    sql: String,
}

/// Output format
#[derive(Clone, ValueEnum, Debug, Default)]
pub enum OutputFormat {
    #[cfg(feature = "excel")]
    Excel,
    #[default]
    Csv,
    Json,
}

#[derive(Args, Debug)]
pub struct ExportArgs {
    /// SQL query to execute (or path to a .sql file)
    #[arg(short = 'q', long, required_unless_present = "file")]
    pub query: Option<String>,

    /// Path to a .sql file to execute
    #[arg(short = 'f', long, conflicts_with = "query")]
    pub file: Option<PathBuf>,

    /// Output file path (default: ./export_<timestamp>.xlsx)
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Output format
    #[arg(short = 'm', long, value_enum, default_value = "excel")]
    pub format: OutputFormat,

    /// Sheet name (Excel only)
    #[arg(long, default_value = "Query Result")]
    pub sheet: String,

    /// Freeze the header row (Excel only)
    #[arg(long, default_value_t = true)]
    pub freeze_header: bool,

    /// Auto-fit column widths (Excel only)
    #[arg(long, default_value_t = true)]
    pub autofit: bool,

    /// Apply alternating row colours (Excel only)
    #[arg(long, default_value_t = true)]
    pub stripe: bool,

    /// Max rows to export (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub limit: usize,

    /// Print progress bar
    #[arg(long, default_value_t = true)]
    pub progress: bool,
}

pub async fn run(url: String, args: ExportArgs, use_tls: bool) -> Result<()> {
    // ── 1. Read query blocks ─────────────────────────────────────────────────
    let blocks = read_queries(&args)?;
    let multi = blocks.len() > 1;

    for (i, block) in blocks.iter().enumerate() {
        if multi {
            println!("{} {} «{}»", "▶ Query".cyan().bold(), i + 1, &block.sheet);
        }
        println!("{} {}", "▶ SQL:".cyan().bold(), block.sql.trim().dimmed());
    }

    // ── 2. Connect ───────────────────────────────────────────────────────────
    let client = connect(&url, use_tls).await?;

    // ── 3. Execute all queries ───────────────────────────────────────────────
    let mut results: Vec<(String, RowSet)> = Vec::with_capacity(blocks.len());

    for block in &blocks {
        let spinner = if args.progress {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            pb.set_message(format!("Executing «{}»…", block.sheet));
            pb.enable_steady_tick(std::time::Duration::from_millis(80));
            Some(pb)
        } else {
            None
        };

        let t0 = Instant::now();
        let rows = client
            .query(block.sql.as_str(), &[])
            .await
            .with_context(|| format!("Query «{}» failed", block.sheet))?;

        let elapsed = t0.elapsed();

        if let Some(pb) = &spinner {
            pb.finish_and_clear();
        }

        if rows.is_empty() {
            println!("  {} «{}» — no rows returned", "⚠".yellow(), block.sheet);
            continue;
        }

        let rowset = RowSet::from_pg_rows(&rows, args.limit)?;
        println!(
            "  {} {} rows in {:.3}s  columns: {}",
            "✔ Fetched".green().bold(),
            rowset.rows.len().to_string().yellow(),
            elapsed.as_secs_f64(),
            rowset.columns.len().to_string().cyan(),
        );

        results.push((block.sheet.clone(), rowset));
    }

    if results.is_empty() {
        println!("{}", "No data returned — nothing to export.".yellow());
        return Ok(());
    }

    // ── 4. Output ────────────────────────────────────────────────────────────
    let out_path = resolve_output_path(&args)?;

    match args.format {
        #[cfg(feature = "excel")]
        OutputFormat::Excel => {
            let refs: Vec<(&str, &RowSet)> = results.iter().map(|(n, r)| (n.as_str(), r)).collect();
            write_excel(
                &refs,
                &out_path,
                args.freeze_header,
                args.autofit,
                args.stripe,
            )?;
        }
        OutputFormat::Csv => {
            let (_, rowset) = &results[0];
            write_csv(rowset, &out_path)?;
        }
        OutputFormat::Json => {
            let (_, rowset) = &results[0];
            write_json(rowset, &out_path)?;
        }
    }

    if multi && results.len() > 1 {
        println!(
            "{} {} queries, {} sheets",
            "✔ Saved:".green().bold(),
            results.len(),
            results.len(),
        );
    }
    println!("  {}", out_path.display().to_string().underline());

    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Parse `--query` or `--file` into a list of `(sheet_name, sql)` blocks.
fn read_queries(args: &ExportArgs) -> Result<Vec<QueryBlock>> {
    if let Some(ref q) = args.query {
        return Ok(vec![QueryBlock {
            sheet: args.sheet.clone(),
            sql: q.clone(),
        }]);
    }
    if let Some(ref path) = args.file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read SQL file: {}", path.display()))?;
        return Ok(parse_sql_file(&content, &args.sheet));
    }
    anyhow::bail!("Provide --query or --file");
}

/// Split SQL file content by `-- sheet:` annotation lines.
/// Lines matching `-- sheet: Name` start a new query block.
/// Content before the first annotation uses the default sheet name.
fn parse_sql_file(content: &str, default_sheet: &str) -> Vec<QueryBlock> {
    let mut blocks: Vec<QueryBlock> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- sheet:") {
            let name = rest.trim();
            let sheet = if name.is_empty() {
                default_sheet.to_owned()
            } else {
                name.to_owned()
            };
            blocks.push(QueryBlock {
                sheet,
                sql: String::new(),
            });
        } else if let Some(last) = blocks.last_mut() {
            if !last.sql.is_empty() {
                last.sql.push('\n');
            }
            last.sql.push_str(line);
        }
    }

    // If no annotation was found, treat the whole file as one block
    if blocks.is_empty() {
        blocks.push(QueryBlock {
            sheet: default_sheet.to_owned(),
            sql: content.to_owned(),
        });
    }

    // Discard empty blocks
    blocks.retain(|b| !b.sql.trim().is_empty());

    blocks
}

fn resolve_output_path(args: &ExportArgs) -> Result<PathBuf> {
    if let Some(ref p) = args.output {
        return Ok(p.clone());
    }

    let ext = match args.format {
        #[cfg(feature = "excel")]
        OutputFormat::Excel => "xlsx",
        OutputFormat::Csv => "csv",
        OutputFormat::Json => "json",
    };

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    Ok(PathBuf::from(format!("export_{ts}.{ext}")))
}
