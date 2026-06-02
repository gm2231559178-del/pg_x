use anyhow::Result;
use clap::Args;
use colored::Colorize;
use comfy_table::{
    presets::UTF8_FULL_CONDENSED, Attribute, Cell, Color, ContentArrangement, Table,
};
use std::time::Instant;

use crate::utils::{db::connect, format::RowSet};

#[derive(Args, Debug)]
pub struct QueryArgs {
    /// SQL to execute
    #[arg(short = 'q', long)]
    pub query: String,

    /// Max rows to display (0 = unlimited)
    #[arg(short = 'n', long, default_value_t = 500)]
    pub limit: usize,

    /// Output as JSON instead of table
    #[arg(long)]
    pub json: bool,
}

pub async fn run(url: String, args: QueryArgs, use_tls: bool) -> Result<()> {
    let client = connect(&url, use_tls).await?;

    let t0 = Instant::now();
    let rows = client.query(args.query.as_str(), &[]).await?;
    let elapsed = t0.elapsed();

    if rows.is_empty() {
        println!("{}", "(no rows)".dimmed());
        return Ok(());
    }

    let rowset = RowSet::from_pg_rows(&rows, args.limit)?;

    if args.json {
        let out = serde_json::to_string_pretty(&rowset.to_json_value())?;
        println!("{out}");
    } else {
        print_table(&rowset);
    }

    println!(
        "\n{} {} row(s) in {:.3}s",
        "✔".green().bold(),
        rows.len().to_string().yellow(),
        elapsed.as_secs_f64(),
    );

    Ok(())
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
