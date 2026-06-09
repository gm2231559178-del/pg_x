use std::path::PathBuf;
#[cfg(feature = "iceberg")]
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};

#[cfg(feature = "excel")]
use crate::utils::excel::write_excel;
use crate::utils::{csv::write_csv, db::connect, format::RowSet, json::write_json};
#[cfg(feature = "iceberg")]
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
#[cfg(feature = "iceberg")]
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
#[cfg(feature = "iceberg")]
use arrow::record_batch::RecordBatch;

/// A parsed query block with an optional sheet name.
struct QueryBlock {
    sheet: String,
    sql: String,
}

/// Iceberg write mode
#[cfg(feature = "iceberg")]
#[derive(Clone, ValueEnum, Debug)]
pub enum IcebergMode {
    Create,
    Append,
    Replace,
}

/// Output format
#[derive(Clone, ValueEnum, Debug, Default)]
pub enum OutputFormat {
    #[cfg(feature = "excel")]
    Excel,
    #[default]
    Csv,
    Json,
    #[cfg(feature = "iceberg")]
    Iceberg,
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

    #[cfg(feature = "iceberg")]
    #[arg(long)]
    pub iceberg_table: Option<String>,

    #[cfg(feature = "iceberg")]
    #[arg(long, default_value = "./iceberg_warehouse")]
    pub warehouse_path: String,

    #[cfg(feature = "iceberg")]
    #[arg(long, default_value = "default")]
    pub namespace: String,

    #[cfg(feature = "iceberg")]
    #[arg(long, value_enum, default_value_t = IcebergMode::Create)]
    pub iceberg_mode: IcebergMode,

    #[cfg(feature = "iceberg")]
    #[arg(long, default_value = "snappy")]
    pub compression: String,
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

    // ── 3. Iceberg export path (early return) ─────────────────────────────────
    #[cfg(feature = "iceberg")]
    if matches!(args.format, OutputFormat::Iceberg) {
        if blocks.len() > 1 {
            anyhow::bail!("Iceberg export supports only a single query; use --query or a .sql file with one statement");
        }
        let table_name = args
            .iceberg_table
            .as_deref()
            .context("--iceberg-table is required for iceberg format")?;
        let block = &blocks[0];
        let rows = client
            .query(block.sql.as_str(), &[])
            .await
            .with_context(|| format!("Query «{}» failed", block.sheet))?;
        if rows.is_empty() {
            match args.iceberg_mode {
                IcebergMode::Append => {
                    println!(
                        "  {} Query returned no rows — nothing to append",
                        "ℹ".yellow()
                    );
                    return Ok(());
                }
                _ => anyhow::bail!("Query returned no rows — cannot create empty Iceberg table"),
            }
        }
        let row_count = rows.len();
        let col_count = rows[0].columns().len();
        println!(
            "  {} {} rows, {} columns",
            "✔ Fetched".green().bold(),
            row_count,
            col_count
        );
        export_to_iceberg(
            &rows,
            table_name,
            &args.namespace,
            &args.warehouse_path,
            &args.iceberg_mode,
            &args.compression,
        )
        .await?;
        println!(
            "  {} Written to Iceberg table «{}»",
            "✔".green().bold(),
            table_name
        );
        return Ok(());
    }

    // ── 4. Execute all queries (file formats) ─────────────────────────────────
    let mut results: Vec<(String, RowSet)> = Vec::with_capacity(blocks.len());

    for block in &blocks {
        let spinner = if args.progress {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner:.cyan} {msg}")
                    .expect("static template string always valid")
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
        #[cfg(feature = "iceberg")]
        OutputFormat::Iceberg => unreachable!(), // handled before output
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

// ── Iceberg export ───────────────────────────────────────────────────────────

#[cfg(feature = "iceberg")]
fn pg_type_to_arrow(col_type: &str) -> DataType {
    match col_type {
        "bool" => DataType::Boolean,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" | "oid" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "numeric" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        _ => DataType::Utf8,
    }
}

#[cfg(feature = "iceberg")]
fn make_builder(dt: &DataType, num_rows: usize) -> Box<dyn ArrayBuilder> {
    let capacity = num_rows.max(1);
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(..) => Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

#[cfg(feature = "iceberg")]
fn append_cell(
    builder: &mut Box<dyn ArrayBuilder>,
    col_type: &str,
    row: &tokio_postgres::Row,
    idx: usize,
) {
    use chrono::Datelike;

    macro_rules! get {
        ($builder:ident, $rust:ty, $arrow:ty) => {{
            let b = builder
                .as_any_mut()
                .downcast_mut::<$arrow>()
                .expect("builder type matches pg_type_to_arrow");
            match row.try_get::<_, Option<$rust>>(idx) {
                Ok(Some(v)) => b.append_value(v),
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type, idx, error = %e, "Column decode failed — storing null");
                    b.append_null();
                }
            }
        }};
    }

    match col_type {
        "bool" => get!(builder, bool, BooleanBuilder),
        "int2" => get!(builder, i16, Int16Builder),
        "int4" => get!(builder, i32, Int32Builder),
        "int8" => get!(builder, i64, Int64Builder),
        "oid" => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("oid maps to Int64 in pg_type_to_arrow");
            match row.try_get::<_, Option<u32>>(idx) {
                Ok(Some(v)) => b.append_value(v as i64),
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type = "oid", idx, error = %e, "Column decode failed — storing null");
                    b.append_null();
                }
            }
        }
        "float4" => get!(builder, f32, Float32Builder),
        "float8" => get!(builder, f64, Float64Builder),
        "numeric" => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("numeric maps to Utf8 in pg_type_to_arrow");
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => b.append_value(&v),
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type = "numeric", idx, error = %e, "Failed to decode numeric — storing null");
                    b.append_null();
                }
            }
        }
        "date" => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .expect("date maps to Date32 in pg_type_to_arrow");
            match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
                Ok(Some(v)) => {
                    let days = v.num_days_from_ce() - 719_163;
                    b.append_value(days);
                }
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type = "date", idx, error = %e, "Column decode failed — storing null");
                    b.append_null();
                }
            }
        }
        "timestamp" => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .expect("timestamp maps to TimestampMicrosecond in pg_type_to_arrow");
            match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
                Ok(Some(v)) => {
                    let micros = v.and_utc().timestamp_micros();
                    b.append_value(micros);
                }
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::debug!(col_type = "timestamp", idx, error = %e, "NaiveDateTime failed, trying timestamptz fallback");
                    match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                        Ok(Some(v)) => b.append_value(v.timestamp_micros()),
                        Ok(None) => b.append_null(),
                        Err(e2) => {
                            tracing::warn!(col_type = "timestamp", idx, error = %e2, "Both timestamp decodings failed — storing null");
                            b.append_null();
                        }
                    }
                }
            }
        }
        "timestamptz" => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .expect("timestamptz maps to TimestampMicrosecond in pg_type_to_arrow");
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => b.append_value(v.timestamp_micros()),
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type = "timestamptz", idx, error = %e, "Column decode failed — storing null");
                    b.append_null();
                }
            }
        }
        _ => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("fallback type maps to Utf8 in pg_type_to_arrow");
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => b.append_value(&v),
                Ok(None) => b.append_null(),
                Err(e) => {
                    tracing::warn!(col_type, idx, error = %e, "Column decode failed — storing null");
                    b.append_null();
                }
            }
        }
    }
}

#[cfg(feature = "iceberg")]
async fn export_to_iceberg(
    rows: &[tokio_postgres::Row],
    table_name: &str,
    namespace: &str,
    warehouse_path: &str,
    mode: &IcebergMode,
    compression: &str,
) -> Result<()> {
    use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::{DataFileFormat, FormatVersion};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{Catalog, CatalogBuilder, TableCreation, TableIdent};
    use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let num_rows = rows.len();

    // 1. Extract column names and PG types from the first row.
    let columns: Vec<(String, String)> = rows[0]
        .columns()
        .iter()
        .map(|c| (c.name().to_owned(), c.type_().name().to_owned()))
        .collect();

    // 2. Build Arrow schema.
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, pg_type)| Field::new(name, pg_type_to_arrow(pg_type), true))
        .collect();
    let arrow_schema = Schema::new(fields);

    // 3. Convert to Iceberg schema (auto-assigns field IDs) and back,
    //    which embeds PARQUET:field_id metadata required by the writer pipeline.
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
        .context("Failed to convert Arrow schema to Iceberg schema")?;
    let id_arrow_schema = Arc::new(
        schema_to_arrow_schema(&iceberg_schema)
            .context("Failed to convert Iceberg schema back to Arrow schema")?,
    );

    // 4. Build typed Arrow arrays from rows.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = columns
        .iter()
        .map(|(_, pg_type)| make_builder(&pg_type_to_arrow(pg_type), num_rows))
        .collect();

    for row in rows {
        for (i, (_, pg_type)) in columns.iter().enumerate() {
            append_cell(&mut builders[i], pg_type, row, i);
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();

    let batch = RecordBatch::try_new(Arc::clone(&id_arrow_schema), arrays)
        .context("Failed to create RecordBatch")?;

    // 5. Iceberg catalog.
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "pgx",
            std::collections::HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse_path),
            )]),
        )
        .await
        .context("Failed to create Iceberg catalog")?;

    let table_ident = TableIdent::from_strs([namespace, table_name])?;
    let ns_ident = table_ident.namespace().clone();

    // 6. Create or load table based on mode.
    if !catalog.namespace_exists(&ns_ident).await? {
        catalog
            .create_namespace(&ns_ident, std::collections::HashMap::new())
            .await
            .context("Failed to create namespace")?;
    }

    let table_exists = catalog.table_exists(&table_ident).await?;

    let iceberg_table = match mode {
        IcebergMode::Create => {
            if table_exists {
                anyhow::bail!("Iceberg table «{namespace}.{table_name}» already exists");
            }
            let table_creation = TableCreation {
                name: table_name.to_string(),
                location: None,
                schema: iceberg_schema,
                partition_spec: None,
                sort_order: None,
                properties: std::collections::HashMap::new(),
                format_version: FormatVersion::V2,
            };
            catalog
                .create_table(&ns_ident, table_creation)
                .await
                .context("Failed to create Iceberg table")?
        }
        IcebergMode::Append => {
            if table_exists {
                catalog.load_table(&table_ident).await?
            } else {
                let table_creation = TableCreation {
                    name: table_name.to_string(),
                    location: None,
                    schema: iceberg_schema,
                    partition_spec: None,
                    sort_order: None,
                    properties: std::collections::HashMap::new(),
                    format_version: FormatVersion::V2,
                };
                catalog
                    .create_table(&ns_ident, table_creation)
                    .await
                    .context("Failed to create Iceberg table")?
            }
        }
        IcebergMode::Replace => {
            if table_exists {
                catalog.drop_table(&table_ident).await?;
            }
            let table_creation = TableCreation {
                name: table_name.to_string(),
                location: None,
                schema: iceberg_schema,
                partition_spec: None,
                sort_order: None,
                properties: std::collections::HashMap::new(),
                format_version: FormatVersion::V2,
            };
            catalog
                .create_table(&ns_ident, table_creation)
                .await
                .context("Failed to create Iceberg table")?
        }
    };

    // 7. Build writer pipeline.
    let location_generator = DefaultLocationGenerator::new(iceberg_table.metadata().clone())
        .context("Failed to create location generator")?;
    let file_name_generator =
        DefaultFileNameGenerator::new(table_name.to_string(), None, DataFileFormat::Parquet);

    let writer_props = match compression {
        "zstd" => WriterProperties::builder()
            .set_compression(ParquetCompression::ZSTD(
                ZstdLevel::try_new(3).unwrap_or_default(),
            ))
            .build(),
        "none" | "uncompressed" => WriterProperties::builder()
            .set_compression(ParquetCompression::UNCOMPRESSED)
            .build(),
        _ => WriterProperties::builder()
            .set_compression(ParquetCompression::SNAPPY)
            .build(),
    };

    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_props,
        iceberg_table.metadata().current_schema().clone(),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        iceberg_table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut writer = data_file_writer_builder.build(None).await?;
    writer.write(batch).await?;
    let data_files = writer.close().await?;

    // 8. Commit.
    let tx = Transaction::new(&iceberg_table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    let _ = tx.commit(&catalog).await?;

    Ok(())
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
        #[cfg(feature = "iceberg")]
        OutputFormat::Iceberg => unreachable!(), // handled before output
    };

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    Ok(PathBuf::from(format!("export_{ts}.{ext}")))
}
