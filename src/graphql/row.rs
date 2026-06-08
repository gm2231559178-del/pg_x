use anyhow::Result;
use serde_json::Value;
use tokio_postgres::Row;

use crate::utils::format::pg_cell_to_json;

/// Convert a `tokio_postgres::Row` into a `serde_json::Value::Object`.
/// NULL columns map to `Value::Null`.
pub fn row_to_json_value(row: &Row) -> Result<Value> {
    let columns = row.columns();
    let mut map = serde_json::Map::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let name = col.name();
        let val = pg_cell_to_json(row, i);
        map.insert(name.to_string(), val);
    }

    Ok(Value::Object(map))
}

/// Extract a column value as a string (used for DataLoader key matching).
pub fn cell_as_string(row: &Row, col_name: &str) -> Option<String> {
    let columns = row.columns();
    for (i, col) in columns.iter().enumerate() {
        if col.name() == col_name {
            return row
                .try_get::<_, Option<String>>(i)
                .ok()
                .flatten()
                .or_else(|| {
                    // Fallback: try to_json_string for non-text types
                    let val = pg_cell_to_json(row, i);
                    match val {
                        Value::Null => None,
                        Value::String(s) => Some(s),
                        other => Some(other.to_string()),
                    }
                });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    // Row-to-JSON tests require a live Postgres or mock connection.
    // These are integration-level tests.
}
