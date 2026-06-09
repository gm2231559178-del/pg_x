use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::{debug, error};

use super::sinks::WalSink;
use crate::replication::event::{ColVal, WalEvent};

#[derive(Debug, Clone, clap::Args)]
pub(crate) struct IcebergArgs {
    #[arg(long, default_value = "./iceberg_warehouse")]
    pub warehouse_path: String,
    #[arg(long, default_value = "default")]
    pub namespace: String,
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

pub(crate) struct IcebergSink {
    pub(crate) catalog: iceberg::memory::MemoryCatalog,
    inner: Mutex<IcebergSinkInner>,
    max_rows: usize,
    flush_interval: Duration,
    compression: String,
    namespace: String,
}

struct IcebergSinkInner {
    buffers: HashMap<(String, String), Vec<AccumulatedEvent>>,
    last_flush: Instant,
}

impl IcebergSink {
    pub(crate) async fn new(args: &IcebergArgs) -> Result<Self> {
        use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
        use iceberg::CatalogBuilder;

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "pgx",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", args.warehouse_path),
                )]),
            )
            .await
            .context("Failed to create memory catalog")?;

        Ok(IcebergSink {
            catalog,
            inner: Mutex::new(IcebergSinkInner {
                buffers: HashMap::new(),
                last_flush: Instant::now(),
            }),
            max_rows: args.max_rows,
            flush_interval: Duration::from_secs(args.flush_interval),
            compression: args.compression.clone(),
            namespace: args.namespace.clone(),
        })
    }

    fn accumulate(&self, event: WalEvent, lsn: String) {
        let mut inner = self.inner.lock().unwrap();
        let key = match &event {
            WalEvent::Insert { schema, table, .. }
            | WalEvent::Update { schema, table, .. }
            | WalEvent::Delete { schema, table, .. } => (schema.clone(), table.clone()),
            _ => return,
        };
        inner
            .buffers
            .entry(key)
            .or_default()
            .push(AccumulatedEvent { lsn, event });
    }

    fn should_flush(&self, inner: &IcebergSinkInner) -> bool {
        if inner.buffers.is_empty() {
            return false;
        }
        let any_full = inner.buffers.values().any(|v| v.len() >= self.max_rows);
        let elapsed = inner.last_flush.elapsed() >= self.flush_interval;
        any_full || elapsed
    }

    async fn do_flush(&self) -> Result<()> {
        let buffers = {
            let mut inner = self.inner.lock().unwrap();
            if !self.should_flush(&inner) {
                return Ok(());
            }
            let buffers = std::mem::take(&mut inner.buffers);
            inner.last_flush = Instant::now();
            buffers
        };

        for ((schema, table), events) in buffers {
            let full_name = format!("{schema}.{table}");
            if let Err(e) = flush_table_to_iceberg(
                &self.catalog,
                &schema,
                &full_name,
                &events,
                &self.namespace,
                &self.compression,
            )
            .await
            {
                error!(
                    error = %e,
                    schema = %schema,
                    table = %full_name,
                    "Failed to flush iceberg table"
                );
                return Err(e);
            }
        }
        Ok(())
    }

    async fn flush_all(&self) -> Result<()> {
        let buffers = {
            let mut inner = self.inner.lock().unwrap();
            let buffers = std::mem::take(&mut inner.buffers);
            inner.last_flush = Instant::now();
            buffers
        };

        for ((schema, table), events) in buffers {
            let full_name = format!("{schema}.{table}");
            flush_table_to_iceberg(
                &self.catalog,
                &schema,
                &full_name,
                &events,
                &self.namespace,
                &self.compression,
            )
            .await
            .with_context(|| format!("Failed to flush iceberg {full_name}"))?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WalSink for IcebergSink {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn send_wal(&self, event_json: &str, _env: &HashMap<String, String>) -> Result<()> {
        let event: WalEvent = serde_json::from_str(event_json)
            .with_context(|| "Failed to parse WAL event JSON in iceberg sink")?;
        let lsn = _env.get("PGX_LSN").cloned().unwrap_or_default();
        self.accumulate(event, lsn);
        self.do_flush().await
    }

    async fn flush(&self) -> Result<()> {
        self.flush_all().await
    }
}

#[cfg(feature = "iceberg")]
async fn flush_table_to_iceberg(
    catalog: &iceberg::memory::MemoryCatalog,
    schema: &str,
    table: &str,
    events: &[AccumulatedEvent],
    namespace: &str,
    compression: &str,
) -> Result<()> {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
    use iceberg::spec::{DataFileFormat, FormatVersion};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{Catalog, TableCreation, TableIdent};
    use parquet::file::properties::WriterProperties;

    // Collect column names from the events.
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

    // Build Arrow schema fields.
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len() + 3);
    let meta_field = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
    fields.push(meta_field("_pgx_op", false));
    fields.push(meta_field("_pgx_lsn", false));
    fields.push(meta_field("_pgx_old", true));
    for col in &columns {
        fields.push(meta_field(col, true));
    }
    let num_fields = fields.len();
    let schema_no_ids = Schema::new(fields);

    // Convert to Iceberg schema (auto-assigns field IDs) and back to Arrow schema,
    // which embeds PARQUET:field_id metadata — required by the Iceberg writer pipeline.
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema_no_ids)
        .context("Failed to convert Arrow schema to Iceberg schema")?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(&iceberg_schema)
            .context("Failed to convert Iceberg schema back to Arrow schema")?,
    );

    // Build RecordBatch using the ID-annotated Arrow schema.
    let num_events = events.len();
    let mut op_builder = StringBuilder::with_capacity(num_events, num_events * 8);
    let mut lsn_builder = StringBuilder::with_capacity(num_events, num_events * 16);
    let mut old_builder = StringBuilder::with_capacity(num_events, num_events * 32);
    let mut col_builders: Vec<StringBuilder> = (0..columns.len())
        .map(|_| StringBuilder::with_capacity(num_events, num_events * 16))
        .collect();

    for ae in events {
        let (op, new_row, old_row) = match &ae.event {
            WalEvent::Insert { new, .. } => ("insert", Some(new), None),
            WalEvent::Update { new, old, .. } => ("update", Some(new), old.as_ref()),
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

    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), arrays)
        .context("Failed to create RecordBatch")?;

    // Load or create the Iceberg table.
    let table_ident = TableIdent::from_strs([namespace, table])?;
    let ns_ident = table_ident.namespace().clone();

    if !catalog.namespace_exists(&ns_ident).await? {
        catalog
            .create_namespace(&ns_ident, HashMap::new())
            .await
            .context("Failed to create namespace")?;
    }

    let iceberg_table = if catalog.table_exists(&table_ident).await? {
        catalog.load_table(&table_ident).await?
    } else {
        let table_creation = TableCreation {
            name: table.to_string(),
            location: None,
            schema: iceberg_schema,
            partition_spec: None,
            sort_order: None,
            properties: HashMap::new(),
            format_version: FormatVersion::V2,
        };
        catalog
            .create_table(&ns_ident, table_creation)
            .await
            .context("Failed to create table")?
    };

    // Build writer pipeline.
    let location_generator = DefaultLocationGenerator::new(iceberg_table.metadata().clone())
        .context("Failed to create location generator")?;
    let file_name_generator =
        DefaultFileNameGenerator::new(format!("{schema}.{table}"), None, DataFileFormat::Parquet);

    let writer_props = if compression == "zstd" {
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
            ))
            .build()
    } else if compression == "none" || compression == "uncompressed" {
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build()
    } else {
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build()
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

    // Commit.
    let tx = Transaction::new(&iceberg_table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    let _ = tx.commit(catalog).await?;

    debug!(
        schema = %schema,
        table = %table,
        rows = num_events,
        "Committed iceberg snapshot"
    );

    Ok(())
}

#[cfg(not(feature = "iceberg"))]
async fn flush_table_to_iceberg(
    _catalog: &iceberg::memory::MemoryCatalog,
    _schema: &str,
    _table: &str,
    _events: &[AccumulatedEvent],
    _namespace: &str,
    _compression: &str,
) -> Result<()> {
    anyhow::bail!("Iceberg sink requires 'iceberg' feature");
}

#[cfg(all(test, feature = "iceberg"))]
mod tests {
    use std::collections::HashMap;

    use anyhow::{Context, Result};
    use arrow::array::StringArray;
    use futures::TryStreamExt;
    use iceberg::Catalog;

    use crate::replication::event::{ColVal, Row, WalEvent};

    use super::*;

    fn make_insert(table: &str, cols: &[(&str, &str)]) -> String {
        let mut row = Row::new();
        for (k, v) in cols {
            row.insert(k.to_string(), ColVal::Text(v.to_string()));
        }
        WalEvent::Insert {
            rel_id: 1,
            schema: "public".to_string(),
            table: table.to_string(),
            new: row,
        }
        .to_json()
    }

    fn make_update(table: &str, old_cols: &[(&str, &str)], new_cols: &[(&str, &str)]) -> String {
        let mut old = Row::new();
        for (k, v) in old_cols {
            old.insert(k.to_string(), ColVal::Text(v.to_string()));
        }
        let mut new = Row::new();
        for (k, v) in new_cols {
            new.insert(k.to_string(), ColVal::Text(v.to_string()));
        }
        WalEvent::Update {
            rel_id: 1,
            schema: "public".to_string(),
            table: table.to_string(),
            old: Some(old),
            new,
        }
        .to_json()
    }

    fn make_delete(table: &str, cols: &[(&str, &str)]) -> String {
        let mut row = Row::new();
        for (k, v) in cols {
            row.insert(k.to_string(), ColVal::Text(v.to_string()));
        }
        WalEvent::Delete {
            rel_id: 1,
            schema: "public".to_string(),
            table: table.to_string(),
            old: row,
        }
        .to_json()
    }

    #[tokio::test]
    async fn test_iceberg_sink_insert_read_back() -> Result<()> {
        let dir = tempfile::tempdir().context("Failed to create temp dir")?;

        let args = IcebergArgs {
            warehouse_path: dir.path().to_str().unwrap().to_string(),
            namespace: "test".to_string(),
            max_rows: 100,
            flush_interval: 3600,
            compression: "none".to_string(),
        };

        let sink = IcebergSink::new(&args).await?;

        let mut env = HashMap::new();
        env.insert("PGX_LSN".into(), "0/12345".into());

        sink.send_wal(
            &make_insert("users", &[("name", "Alice"), ("city", "NYC")]),
            &env,
        )
        .await?;
        sink.send_wal(
            &make_insert("users", &[("name", "Bob"), ("city", "SF")]),
            &env,
        )
        .await?;
        sink.send_wal(
            &make_update(
                "users",
                &[("name", "Alice"), ("city", "NYC")],
                &[("name", "Alice"), ("city", "LA")],
            ),
            &env,
        )
        .await?;
        sink.send_wal(
            &make_delete("users", &[("name", "Bob"), ("city", "SF")]),
            &env,
        )
        .await?;

        sink.flush().await?;

        // Read back via scan
        let table_ident = iceberg::TableIdent::from_strs(["test", "public.users"])?;
        let table = sink.catalog.load_table(&table_ident).await?;
        let stream = table
            .scan()
            .select_all()
            .build()
            .context("Failed to build scan")?
            .to_arrow()
            .await?;
        let batches: Vec<arrow::record_batch::RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 4,
            "expected 4 rows (2 inserts + 1 update + 1 delete)"
        );

        // Verify schema has expected columns
        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"_pgx_op"));
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"city"));

        // Check ops are in order
        let ops = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "insert");
        assert_eq!(ops.value(1), "insert");
        assert_eq!(ops.value(2), "update");
        assert_eq!(ops.value(3), "delete");

        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_sink_multiple_tables() -> Result<()> {
        let dir = tempfile::tempdir().context("Failed to create temp dir")?;

        let args = IcebergArgs {
            warehouse_path: dir.path().to_str().unwrap().to_string(),
            namespace: "test".to_string(),
            max_rows: 100,
            flush_interval: 3600,
            compression: "snappy".to_string(),
        };

        let sink = IcebergSink::new(&args).await?;

        let mut env = HashMap::new();
        env.insert("PGX_LSN".into(), "0/AAA".into());

        sink.send_wal(
            &make_insert("orders", &[("id", "1"), ("amount", "100")]),
            &env,
        )
        .await?;
        sink.send_wal(
            &make_insert("products", &[("sku", "P1"), ("price", "50")]),
            &env,
        )
        .await?;
        sink.send_wal(
            &make_insert("orders", &[("id", "2"), ("amount", "200")]),
            &env,
        )
        .await?;

        sink.flush().await?;

        // Verify orders table has 2 rows, products has 1
        let orders_ident = iceberg::TableIdent::from_strs(["test", "public.orders"])?;
        let orders_table = sink.catalog.load_table(&orders_ident).await?;
        let orders_stream = orders_table.scan().select_all().build()?.to_arrow().await?;
        let orders_batches: Vec<_> = orders_stream.try_collect().await?;
        let orders_rows: usize = orders_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(orders_rows, 2);

        let products_ident = iceberg::TableIdent::from_strs(["test", "public.products"])?;
        let products_table = sink.catalog.load_table(&products_ident).await?;
        let products_stream = products_table
            .scan()
            .select_all()
            .build()?
            .to_arrow()
            .await?;
        let products_batches: Vec<_> = products_stream.try_collect().await?;
        let products_rows: usize = products_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(products_rows, 1);

        Ok(())
    }
}
