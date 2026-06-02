use anyhow::Result;
use serde_json::Value;
use tokio_postgres::Row;

/// A serializable, format-agnostic result set.
#[derive(Debug)]
pub struct RowSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// Whether each column is numeric (for JSON type preservation).
    pub col_is_numeric: Vec<bool>,
}

impl RowSet {
    /// Convert raw `tokio_postgres::Row` slice into a `RowSet`.
    /// `limit = 0` means no limit.
    pub fn from_pg_rows(rows: &[Row], limit: usize) -> Result<Self> {
        if rows.is_empty() {
            return Ok(RowSet {
                columns: vec![],
                rows: vec![],
                col_is_numeric: vec![],
            });
        }

        // Column names from the first row's columns()
        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect();

        let take = if limit == 0 {
            rows.len()
        } else {
            limit.min(rows.len())
        };

        let mut col_is_numeric: Vec<bool> = Vec::with_capacity(columns.len());
        let mut result_rows: Vec<Vec<String>> = Vec::with_capacity(take);

        for (ri, row) in rows.iter().enumerate() {
            if result_rows.len() >= take {
                break;
            }
            let cells: Vec<String> = (0..columns.len())
                .map(|i| pg_cell_to_string(row, i))
                .collect();
            if ri == 0 {
                col_is_numeric = (0..columns.len())
                    .map(|i| {
                        let ct = row.columns()[i].type_().name();
                        matches!(ct, "int2" | "int4" | "int8" | "oid" | "float4" | "float8" | "numeric")
                    })
                    .collect();
            }
            result_rows.push(cells);
        }

        Ok(RowSet {
            columns,
            rows: result_rows,
            col_is_numeric,
        })
    }

    /// Convert to a `serde_json::Value` array of objects.
    /// Numeric columns are serialized as JSON numbers, not strings.
    pub fn to_json_value(&self) -> Value {
        let objects: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, Value> = self
                    .columns
                    .iter()
                    .zip(row.iter())
                    .enumerate()
                    .map(|(ci, (k, v))| {
                        let val = if v == "\0NULL" {
                            Value::Null
                        } else if self.col_is_numeric.get(ci).copied().unwrap_or(false) {
                            v.parse::<f64>()
                                .map(Value::from)
                                .unwrap_or_else(|_| Value::String(v.clone()))
                        } else {
                            Value::String(v.clone())
                        };
                        (k.clone(), val)
                    })
                    .collect();
                Value::Object(obj)
            })
            .collect();
        Value::Array(objects)
    }
}

/// Try to extract a human-readable string for any supported Postgres column type.
fn pg_cell_to_string(row: &Row, idx: usize) -> String {
    let col_type = row.columns()[idx].type_().name();

    // Sentinel used to distinguish SQL NULL from the literal string "NULL".
    // The leading NUL character (\0) cannot appear in a real Postgres text value.
    const NULL_SENTINEL: &str = "\0NULL";

    macro_rules! try_get {
        ($t:ty) => {
            match row.try_get::<_, Option<$t>>(idx) {
                Ok(Some(v)) => return v.to_string(),
                Ok(None) => return NULL_SENTINEL.to_owned(),
                Err(e) => tracing::debug!(error = %e, col_type, "try_get failed"),
            }
        };
    }

    match col_type {
        "bool" => {
            try_get!(bool);
        }
        "int2" => {
            try_get!(i16);
        }
        "int4" => {
            try_get!(i32);
        }
        "int8" | "oid" => {
            try_get!(i64);
        }
        "float4" => {
            try_get!(f32);
        }
        "float8" | "numeric" => {
            try_get!(f64);
        }
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => {
            try_get!(String);
        }
        "json" | "jsonb" => {
            match row.try_get::<_, Option<serde_json::Value>>(idx) {
                Ok(Some(v)) => return v.to_string(),
                Ok(None) => return "null".to_owned(),
                Err(e) => tracing::debug!(error = %e, col_type, "try_get failed"),
            }
        }
        "uuid" => {
            try_get!(uuid::Uuid);
        }
        "timestamp" | "timestamptz" => {
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => return v.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                Ok(None) => return NULL_SENTINEL.to_owned(),
                Err(e) => tracing::debug!(error = %e, col_type, "try_get failed for timestamptz"),
            }
            match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
                Ok(Some(v)) => return v.format("%Y-%m-%d %H:%M:%S").to_string(),
                Ok(None) => return NULL_SENTINEL.to_owned(),
                Err(e) => tracing::debug!(error = %e, col_type, "try_get failed for timestamp"),
            }
        }
        "date" => {
            match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
                Ok(Some(v)) => return v.to_string(),
                Ok(None) => return NULL_SENTINEL.to_owned(),
                Err(e) => tracing::debug!(error = %e, col_type, "try_get failed"),
            }
        }
        _ => {}
    }

    // Generic fallback: try String first, then format unknown
    match row.try_get::<_, Option<String>>(idx) {
        Ok(Some(v)) => return v,
        Ok(None) => return NULL_SENTINEL.to_owned(),
        Err(e) => tracing::debug!(error = %e, col_type, "try_get failed for String fallback"),
    }

    format!("<{col_type}>")
}
