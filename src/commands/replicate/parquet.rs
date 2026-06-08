use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, error};
use uuid::Uuid;

use crate::replication::event::{ColVal, WalEvent};
use super::sinks::WalSink;

#[derive(Debug, Clone, clap::Args)]
pub(crate) struct ParquetArgs {
    #[arg(long, default_value = "./parquet_output")]
    pub output_dir: String,
    #[arg(long, default_value = "100000")]
    pub max_rows: usize,
    #[arg(long, default_value = "300")]
    pub flush_interval: u64,
    #[arg(long, default_value = "snappy")]
    pub compression: String,
}

struct AccumulatedEvent {
    lsn: String,
    event: WalEvent,
}

pub(crate) struct ParquetSink {
    inner: Mutex<ParquetSinkInner>,
    max_rows: usize,
    flush_interval: Duration,
    compression: String,
    output_dir: PathBuf,
}

struct ParquetSinkInner {
    buffers: HashMap<(String, String), Vec<AccumulatedEvent>>,
    last_flush: Instant,
}

impl ParquetSink {
    pub(crate) fn new(args: &ParquetArgs) -> Self {
        ParquetSink {
            inner: Mutex::new(ParquetSinkInner {
                buffers: HashMap::new(),
                last_flush: Instant::now(),
            }),
            max_rows: args.max_rows,
            flush_interval: Duration::from_secs(args.flush_interval),
            compression: args.compression.clone(),
            output_dir: PathBuf::from(&args.output_dir),
        }
    }

    fn accumulate(&self, event: WalEvent, lsn: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = match &event {
            WalEvent::Insert { schema, table, .. }
            | WalEvent::Update { schema, table, .. }
            | WalEvent::Delete { schema, table, .. } => (schema.clone(), table.clone()),
            _ => return Ok(()),
        };
        inner
            .buffers
            .entry(key)
            .or_default()
            .push(AccumulatedEvent { lsn, event });
        Ok(())
    }

    fn maybe_flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let elapsed = inner.last_flush.elapsed();
        let total_rows: usize = inner.buffers.values().map(|v| v.len()).sum();
        if total_rows == 0 {
            return Ok(());
        }
        let any_full = inner
            .buffers
            .values()
            .any(|v| v.len() >= self.max_rows);
        if !any_full && elapsed < self.flush_interval {
            return Ok(());
        }
        let buffers = std::mem::take(&mut inner.buffers);
        inner.last_flush = Instant::now();
        drop(inner);
        for ((schema, table), events) in buffers {
            if let Err(e) = flush_table(
                &self.output_dir,
                &schema,
                &table,
                &events,
                &self.compression,
            ) {
                error!(error = %e, schema = %schema, table = %table, "Failed to flush parquet table");
                return Err(e);
            }
        }
        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let buffers = std::mem::take(&mut inner.buffers);
        inner.last_flush = Instant::now();
        drop(inner);
        for ((schema, table), events) in buffers {
            flush_table(&self.output_dir, &schema, &table, &events, &self.compression)
                .with_context(|| format!("Failed to flush {schema}.{table}"))?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WalSink for ParquetSink {
    fn name(&self) -> &str {
        "parquet"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        let event: WalEvent = serde_json::from_str(event_json)
            .with_context(|| "Failed to parse WAL event JSON in parquet sink")?;
        let lsn = _env
            .get("PGX_LSN")
            .cloned()
            .unwrap_or_default();
        self.accumulate(event, lsn)?;
        self.maybe_flush()
    }

    async fn flush(&self) -> Result<()> {
        self.flush_all()
    }
}

fn flush_table(
    base_dir: &Path,
    schema: &str,
    table: &str,
    events: &[AccumulatedEvent],
    compression: &str,
) -> Result<()> {
    #[cfg(feature = "parquet")]
    {
        use std::fs::{self, File};
        use std::sync::Arc;

        use arrow::array::{ArrayRef, StringBuilder};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
        use parquet::file::properties::WriterProperties;

        let now = Utc::now();
        let part_dir = base_dir
            .join(schema)
            .join(table)
            .join(format!("year={}", now.format("%Y")))
            .join(format!("month={}", now.format("%m")))
            .join(format!("day={}", now.format("%d")));
        fs::create_dir_all(&part_dir)
            .with_context(|| format!("Failed to create part dir: {}", part_dir.display()))?;

        // Collect all column names from the events, preserving order of first event.
        let mut columns: Vec<String> = Vec::new();
        for ae in events {
            let row = match &ae.event {
                WalEvent::Insert { new, .. } | WalEvent::Update { new, .. } => Some(new),
                WalEvent::Delete { old, .. } => Some(old),
                _ => None,
            };
            if let Some(r) = row {
                if columns.is_empty() {
                    let mut keys: Vec<&String> = r.keys().collect();
                    keys.sort();
                    columns = keys.into_iter().cloned().collect();
                }
            }
        }

        // Build columns: metadata + user columns
        let mut fields: Vec<Field> = Vec::with_capacity(columns.len() + 3);
        let field_meta = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
        fields.push(field_meta("_pgx_op", false));
        fields.push(field_meta("_pgx_lsn", false));
        fields.push(field_meta("_pgx_old", true));
        for col in &columns {
            fields.push(field_meta(col, true));
        }
        let num_fields = fields.len();
        let num_events = events.len();
        let schema = Arc::new(Schema::new(fields));

        let mut op_builder = StringBuilder::with_capacity(num_events, num_events * 8);
        let mut lsn_builder = StringBuilder::with_capacity(num_events, num_events * 16);
        let mut old_builder = StringBuilder::with_capacity(num_events, num_events * 32);
        let mut col_builders: Vec<StringBuilder> = (0..columns.len())
            .map(|_| StringBuilder::with_capacity(num_events, num_events * 16))
            .collect();

        for ae in events {
            let (op, new_row, old_row) = match &ae.event {
                WalEvent::Insert { new, .. } => ("insert", Some(new), None),
                WalEvent::Update { new, old, .. } => {
                    ("update", Some(new), old.as_ref())
                }
                WalEvent::Delete { old, .. } => ("delete", None, Some(old)),
                _ => unreachable!(),
            };

            op_builder.append_value(op);
            lsn_builder.append_value(&ae.lsn);

            if let Some(old) = old_row {
                let old_json = serde_json::to_string(old).unwrap_or_default();
                old_builder.append_value(&old_json);
            } else {
                old_builder.append_null();
            }

            for (i, col) in columns.iter().enumerate() {
                if let Some(new_row) = new_row {
                    match new_row.get(col) {
                        Some(ColVal::Text(v)) => col_builders[i].append_value(v),
                        _ => col_builders[i].append_null(),
                    }
                } else if let Some(old_row) = old_row {
                    match old_row.get(col) {
                        Some(ColVal::Text(v)) => col_builders[i].append_value(v),
                        _ => col_builders[i].append_null(),
                    }
                } else {
                    col_builders[i].append_null();
                }
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_fields);
        arrays.push(Arc::new(op_builder.finish()));
        arrays.push(Arc::new(lsn_builder.finish()));
        arrays.push(Arc::new(old_builder.finish()));
        for mut builder in col_builders {
            arrays.push(Arc::new(builder.finish()));
        }

        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .context("Failed to create RecordBatch")?;

        let file_name = format!(
            "part-{}-{}.parquet",
            now.format("%Y%m%d%H%M%S%3f"),
            Uuid::new_v4().to_string().split('-').next().unwrap()
        );
        let file_path = part_dir.join(&file_name);
        let file = File::create(&file_path)
            .with_context(|| format!("Failed to create parquet file: {}", file_path.display()))?;

        let parquet_compression = match compression {
            "zstd" => ParquetCompression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
            "none" | "uncompressed" => ParquetCompression::UNCOMPRESSED,
            _ => ParquetCompression::SNAPPY,
        };
        let props = WriterProperties::builder()
            .set_compression(parquet_compression)
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
            .context("Failed to create ArrowWriter")?;
        writer
            .write(&batch)
            .context("Failed to write RecordBatch to parquet")?;
        writer
            .close()
            .context("Failed to close parquet writer")?;

        let row_count = events.len();
        debug!(path = %file_path.display(), rows = row_count, schema = %schema, table = %table, "Wrote parquet file");

        Ok(())
    }

    #[cfg(not(feature = "parquet"))]
    {
        let _ = (base_dir, schema, table, events, compression);
        anyhow::bail!("Parquet sink requires 'parquet' feature");
    }
}
