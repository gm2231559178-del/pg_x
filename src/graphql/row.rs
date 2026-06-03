use anyhow::Result;
use serde_json::Value;
use tokio_postgres::Row;

/// Convert a `tokio_postgres::Row` into a `serde_json::Value::Object`.
/// NULL columns map to `Value::Null`.
pub fn row_to_json_value(row: &Row) -> Result<Value> {
    let columns = row.columns();
    let mut map = serde_json::Map::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let name = col.name();
        let val = cell_to_json(row, i);
        map.insert(name.to_string(), val);
    }

    Ok(Value::Object(map))
}

/// Convert a specific cell to a serde_json::Value.
fn cell_to_json(row: &Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_().name();

    macro_rules! get {
        ($t:ty) => {
            match row.try_get::<_, Option<$t>>(idx) {
                Ok(Some(v)) => return Value::from(v),
                Ok(None) => return Value::Null,
                Err(_) => {}
            }
        };
    }

    match col_type {
        "bool" => {
            get!(bool);
        }
        "int2" => {
            get!(i16);
        }
        "int4" => {
            get!(i32);
        }
        "int8" | "oid" => {
            get!(i64);
        }
        "float4" => {
            get!(f32);
        }
        "float8" | "numeric" => {
            get!(f64);
        }
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => {
            get!(String);
        }
        "json" | "jsonb" => match row.try_get::<_, Option<Value>>(idx) {
            Ok(Some(v)) => return v,
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        "uuid" => match row.try_get::<_, Option<uuid::Uuid>>(idx) {
            Ok(Some(v)) => return Value::String(v.to_string()),
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        "timestamp" | "timestamptz" => {
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => return Value::String(v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
                Ok(None) => return Value::Null,
                Err(_) => {}
            }
            match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
                Ok(Some(v)) => return Value::String(v.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                Ok(None) => return Value::Null,
                Err(_) => {}
            }
        }
        "date" => match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
            Ok(Some(v)) => return Value::String(v.to_string()),
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        _ => {}
    }

    // Fallback: try String
    match row.try_get::<_, Option<String>>(idx) {
        Ok(Some(v)) => Value::String(v),
        Ok(None) => Value::Null,
        Err(_) => Value::String(format!("<{}>", col_type)),
    }
}

/// Extract a column value as a string (used for DataLoader key matching).
pub fn cell_as_string(row: &Row, col_name: &str) -> Option<String> {
    let columns = row.columns();
    for (i, col) in columns.iter().enumerate() {
        if col.name() == col_name {
            let col_type = col.type_().name();
            return match col_type {
                "bool" => row
                    .try_get::<_, Option<bool>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "int2" => row
                    .try_get::<_, Option<i16>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "int4" => row
                    .try_get::<_, Option<i32>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "int8" | "oid" => row
                    .try_get::<_, Option<i64>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "float4" => row
                    .try_get::<_, Option<f32>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "float8" | "numeric" => row
                    .try_get::<_, Option<f64>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => {
                    row.try_get::<_, Option<String>>(i).ok().flatten()
                }
                "uuid" => row
                    .try_get::<_, Option<uuid::Uuid>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "json" | "jsonb" => row
                    .try_get::<_, Option<serde_json::Value>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                "timestamp" | "timestamptz" => {
                    if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                    {
                        Some(v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string())
                    } else if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(i) {
                        Some(v.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                    } else {
                        row.try_get::<_, Option<String>>(i).ok().flatten()
                    }
                }
                "date" => row
                    .try_get::<_, Option<chrono::NaiveDate>>(i)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string()),
                _ => row.try_get::<_, Option<String>>(i).ok().flatten(),
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    // Row-to-JSON tests require a live Postgres or mock connection.
    // These are integration-level tests.
}
