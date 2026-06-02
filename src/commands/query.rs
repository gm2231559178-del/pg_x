use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;
use comfy_table::{
    presets::UTF8_FULL_CONDENSED, Attribute, Cell, Color, ContentArrangement, Table,
};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::utils::{db::connect, format::RowSet};

#[derive(Args, Debug)]
pub struct QueryArgs {
    /// SQL to execute
    #[arg(short = 'q', long, required_unless_present = "file")]
    pub query: Option<String>,

    /// Read SQL from a file
    #[arg(short = 'f', long, conflicts_with = "query")]
    pub file: Option<PathBuf>,

    /// Max rows to display (0 = unlimited)
    #[arg(short = 'n', long, default_value_t = 500)]
    pub limit: usize,

    /// Output as JSON instead of table
    #[arg(long)]
    pub json: bool,

    /// Output as CSV instead of table
    #[arg(long)]
    pub csv: bool,

    /// Watch mode: re-run the query every N seconds (like `watch -n N`)
    #[arg(short = 'w', long, default_value_t = 0)]
    pub watch: u64,
}

pub async fn run(url: String, args: QueryArgs, use_tls: bool) -> Result<()> {
    let sql = resolve_sql(&args)?;

    loop {
        let client = connect(&url, use_tls).await?;

        let t0 = Instant::now();
        let rows = client.query(sql.as_str(), &[]).await?;
        let elapsed = t0.elapsed();

        if args.watch > 0 {
            print!("\u{1b}[2J\u{1b}[H"); // clear screen
            std::io::stdout().flush()?;
            println!(
                "{} {}  (every {}s)  {}",
                "⟳".cyan().bold(),
                sql.dimmed(),
                args.watch,
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            );
            println!();
        }

        if rows.is_empty() {
            println!("{}", "(no rows)".dimmed());
        } else {
            let rowset = RowSet::from_pg_rows(&rows, args.limit)?;

            if args.csv {
                let mut wtr = csv::Writer::from_writer(std::io::stdout());
                wtr.write_record(&rowset.columns)
                    .context("Write CSV header")?;
                for row in &rowset.rows {
                    wtr.write_record(row).context("Write CSV row")?;
                }
                wtr.flush().context("Flush CSV")?;
            } else if args.json {
                let out = serde_json::to_string_pretty(&rowset.to_json_value())?;
                println!("{out}");
            } else {
                print_table(&rowset);
            }

            if args.watch == 0 {
                let summary = format!(
                    "\n{} {} {} in {:.3}s",
                    "✔".green().bold(),
                    rows.len().to_string().yellow(),
                    "rows",
                    elapsed.as_secs_f64(),
                );
                if args.csv {
                    eprintln!("{summary}");
                } else {
                    println!("{summary}");
                }
            }
        }

        if args.watch == 0 {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_secs(args.watch)).await;
    }
}

fn resolve_sql(args: &QueryArgs) -> Result<String> {
    if let Some(ref q) = args.query {
        return Ok(q.clone());
    }
    if let Some(ref path) = args.file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read SQL file: {}", path.display()));
    }
    anyhow::bail!("Provide --query or --file");
}

fn print_table(rowset: &RowSet) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);

    // Header row
    let header: Vec<Cell> = rowset
        .columns
        .iter()
        .map(|c| Cell::new(c).add_attribute(Attribute::Bold).fg(Color::Cyan))
        .collect();
    table.set_header(header);

    // Data rows
    for (i, row) in rowset.rows.iter().enumerate() {
        let cells: Vec<Cell> = row
            .iter()
            .map(|v| {
                let cell = Cell::new(v);
                if i % 2 == 1 {
                    cell.fg(Color::Grey)
                } else {
                    cell
                }
            })
            .collect();
        table.add_row(cells);
    }

    println!("{table}");
}
