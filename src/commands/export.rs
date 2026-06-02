use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Instant;

#[cfg(feature = "excel")]
use crate::utils::excel::write_excel;
use crate::utils::{csv::write_csv, db::connect, format::RowSet, json::write_json};

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

pub async fn run(url: String, args: ExportArgs) -> Result<()> {
    // ── 1. Read SQL ──────────────────────────────────────────────────────────
    let sql = read_sql(&args)?;
    println!("{} {}", "▶ SQL:".cyan().bold(), sql.trim().dimmed());

    // ── 2. Connect ───────────────────────────────────────────────────────────
    let client = connect(&url).await?;

    // ── 3. Execute ───────────────────────────────────────────────────────────
    let spinner = if args.progress {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        pb.set_message("Executing query…");
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        Some(pb)
    } else {
        None
    };

    let t0 = Instant::now();
    let rows = client
        .query(sql.as_str(), &[])
        .await
        .context("Query execution failed")?;

    let elapsed = t0.elapsed();

    if let Some(pb) = &spinner {
        pb.finish_and_clear();
    }

    println!(
        "{} {} rows in {:.3}s",
        "✔ Fetched".green().bold(),
        rows.len().to_string().yellow(),
        elapsed.as_secs_f64(),
    );

    if rows.is_empty() {
        println!("{}", "No rows returned — nothing to export.".yellow());
        return Ok(());
    }

    // ── 4. Convert to RowSet ─────────────────────────────────────────────────
    let rowset = RowSet::from_pg_rows(&rows, args.limit)?;
    println!(
        "  columns: {}  rows exported: {}",
        rowset.columns.len().to_string().cyan(),
        rowset.rows.len().to_string().cyan(),
    );

    // ── 5. Output ────────────────────────────────────────────────────────────
    let out_path = resolve_output_path(&args)?;

    match args.format {
        #[cfg(feature = "excel")]
        OutputFormat::Excel => {
            write_excel(
                &rowset,
                &out_path,
                &args.sheet,
                args.freeze_header,
                args.autofit,
                args.stripe,
            )?;
        }
        OutputFormat::Csv => {
            write_csv(&rowset, &out_path)?;
        }
        OutputFormat::Json => {
            write_json(&rowset, &out_path)?;
        }
    }

    println!(
        "{} {}",
        "✔ Saved:".green().bold(),
        out_path.display().to_string().underline()
    );

    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn read_sql(args: &ExportArgs) -> Result<String> {
    if let Some(ref q) = args.query {
        return Ok(q.clone());
    }
    if let Some(ref path) = args.file {
        let sql = std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read SQL file: {}", path.display()))?;
        return Ok(sql);
    }
    anyhow::bail!("Provide --query or --file");
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
