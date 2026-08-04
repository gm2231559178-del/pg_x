use anyhow::Result;
use serde_json::Value;
use tokio_postgres::types::{FromSql, Type};
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
                        matches!(
                            ct,
                            "int2" | "int4" | "int8" | "oid" | "float4" | "float8" | "numeric"
                        )
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

/// A PostgreSQL `numeric` column decoded from its binary wire format into an
/// exact decimal string. `postgres-types` ships no `FromSql` impl for the
/// NUMERIC oid — its `f64` only accepts DOUBLE PRECISION and its `String` only
/// accepts text-like types — so without this the generic fallback would emit a
/// literal `<numeric>` for every such column.
#[derive(Debug, Clone, PartialEq)]
pub struct Numeric(pub String);

impl<'a> FromSql<'a> for Numeric {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        pg_numeric_to_string(raw)
            .map(Numeric)
            .ok_or_else(|| "cannot decode Postgres numeric".into())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

/// Decode PostgreSQL's binary `numeric` wire format (see `numeric_send` in
/// backend/utils/adt/numeric.c): int16 `ndigits`, int16 `weight`, int16
/// `sign`, int16 `dscale`, then `ndigits` base-10000 digits (int16 each, most
/// significant first). The value is `sum(digits[i] * 10000^(weight - i))`, and
/// `dscale` is the display scale used to pad or trim the fractional part.
fn pg_numeric_to_string(raw: &[u8]) -> Option<String> {
    if raw.len() < 8 {
        return None;
    }
    let read_i16 = |off: usize| -> Option<i16> {
        Some(i16::from_be_bytes(raw.get(off..off + 2)?.try_into().ok()?))
    };

    let ndigits = read_i16(0)?;
    let weight = read_i16(2)?;
    let sign = read_i16(4)?;
    let dscale = read_i16(6)?;

    if ndigits < 0 || dscale < 0 || raw.len() < 8 + ndigits as usize * 2 {
        return None;
    }

    // Sign codes (NUMERIC_POS/NEG/NAN/PINF/NINF) do not fit in i16, so compare
    // the u16 view of the wire bytes.
    let sign = sign as u16;
    match sign {
        0xC000 => return Some("NaN".to_string()),
        0xD000 => return Some("Infinity".to_string()),
        0xF000 => return Some("-Infinity".to_string()),
        0x0000 | 0x4000 => {}
        _ => return None,
    }

    let mut digits = String::with_capacity(ndigits as usize * 4);
    for i in 0..ndigits as usize {
        digits.push_str(&format!("{:04}", read_i16(8 + i * 2)?));
    }

    // Decimal digits before the point; negative means leading fractional zeros.
    let int_chars = (weight as i32 + 1) * 4;
    let (mut integer, mut frac) = if int_chars <= 0 {
        let mut frac = "0".repeat((-int_chars) as usize);
        frac.push_str(&digits);
        ("0".to_string(), frac)
    } else if (int_chars as usize) >= digits.len() {
        // Trailing integer zeros are omitted from the digit array.
        let mut ip = digits;
        ip.push_str(&"0".repeat(int_chars as usize - ip.len()));
        (ip.trim_start_matches('0').to_string(), String::new())
    } else {
        let (ip, fp) = digits.split_at(int_chars as usize);
        (ip.trim_start_matches('0').to_string(), fp.to_string())
    };
    if integer.is_empty() {
        integer = "0".to_string();
    }

    let dscale = dscale as usize;
    if frac.len() > dscale {
        frac.truncate(dscale);
    } else if frac.len() < dscale {
        frac.push_str(&"0".repeat(dscale - frac.len()));
    }

    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&integer);
    if dscale > 0 {
        out.push('.');
        out.push_str(&frac);
    }
    Some(out)
}

/// Convert a Postgres cell to a `serde_json::Value`, preserving type information.
/// Numeric types become JSON numbers, NULL becomes `Value::Null`.
pub fn pg_cell_to_json(row: &Row, idx: usize) -> Value {
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
        "bool" => get!(bool),
        "int2" => get!(i16),
        "int4" => get!(i32),
        "int8" | "oid" => get!(i64),
        "float4" => get!(f32),
        "float8" => get!(f64),
        "numeric" => match row.try_get::<_, Option<Numeric>>(idx) {
            Ok(Some(v)) => match v.0.parse::<f64>() {
                Ok(n) if n.is_finite() => return Value::from(n),
                _ => return Value::String(v.0),
            },
            Ok(None) => return Value::Null,
            Err(e) => tracing::debug!(error = %e, col_type, "try_get failed for numeric"),
        },
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => get!(String),
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
        Err(_) => Value::String(format!("<{col_type}>")),
    }
}

/// Try to extract a human-readable string for any supported Postgres column type.
pub fn pg_cell_to_string(row: &Row, idx: usize) -> String {
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
        "float8" => {
            try_get!(f64);
        }
        "numeric" => match row.try_get::<_, Option<Numeric>>(idx) {
            Ok(Some(v)) => return v.0,
            Ok(None) => return NULL_SENTINEL.to_owned(),
            Err(e) => tracing::debug!(error = %e, col_type, "try_get failed for numeric"),
        },
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext" => {
            try_get!(String);
        }
        "json" | "jsonb" => match row.try_get::<_, Option<serde_json::Value>>(idx) {
            Ok(Some(v)) => return v.to_string(),
            Ok(None) => return "null".to_owned(),
            Err(e) => tracing::debug!(error = %e, col_type, "try_get failed"),
        },
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
        "date" => match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
            Ok(Some(v)) => return v.to_string(),
            Ok(None) => return NULL_SENTINEL.to_owned(),
            Err(e) => tracing::debug!(error = %e, col_type, "try_get failed"),
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_json_value_empty() {
        let rs = RowSet {
            columns: vec![],
            rows: vec![],
            col_is_numeric: vec![],
        };
        assert_eq!(rs.to_json_value(), serde_json::json!([]));
    }

    #[test]
    fn to_json_value_string_columns() {
        let rs = RowSet {
            columns: vec!["name".into(), "email".into()],
            rows: vec![
                vec!["Alice".into(), "a@test.com".into()],
                vec!["Bob".into(), "b@test.com".into()],
            ],
            col_is_numeric: vec![false, false],
        };
        let expected = serde_json::json!([
            {"name": "Alice", "email": "a@test.com"},
            {"name": "Bob", "email": "b@test.com"},
        ]);
        assert_eq!(rs.to_json_value(), expected);
    }

    #[test]
    fn to_json_value_numeric_columns() {
        let rs = RowSet {
            columns: vec!["id".into(), "score".into()],
            rows: vec![
                vec!["1".into(), "95.5".into()],
                vec!["2".into(), "87.0".into()],
            ],
            col_is_numeric: vec![true, true],
        };
        let v = rs.to_json_value();
        assert_eq!(v[0]["id"], serde_json::json!(1.0));
        assert_eq!(v[0]["score"], serde_json::json!(95.5));
    }

    #[test]
    fn to_json_value_null_sentinel() {
        let rs = RowSet {
            columns: vec!["val".into()],
            rows: vec![vec!["\0NULL".into()]],
            col_is_numeric: vec![false],
        };
        let v = rs.to_json_value();
        assert_eq!(v[0]["val"], serde_json::Value::Null);
    }

    /// Build a `numeric` binary payload from its logical fields.
    fn numeric_raw(ndigits: i16, weight: i16, sign: i16, dscale: i16, digits: &[i16]) -> Vec<u8> {
        let mut raw = Vec::new();
        for v in [ndigits, weight, sign, dscale] {
            raw.extend_from_slice(&v.to_be_bytes());
        }
        for d in digits {
            raw.extend_from_slice(&d.to_be_bytes());
        }
        raw
    }

    #[test]
    fn pg_numeric_decodes_integer_and_fractional() {
        // 123.45 -> digits [123, 4500], weight 0, dscale 2
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(2, 0, 0x0000, 2, &[123, 4500])).as_deref(),
            Some("123.45")
        );
        // 1000
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(1, 0, 0x0000, 0, &[1000])).as_deref(),
            Some("1000")
        );
        // 0.00123
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(2, -1, 0x0000, 5, &[12, 3000])).as_deref(),
            Some("0.00123")
        );
        // 1,000,000 -> digits [100], weight 1 (trailing integer zeros implied)
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(1, 1, 0x0000, 0, &[100])).as_deref(),
            Some("1000000")
        );
        // -0.5
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(1, -1, 0x4000, 1, &[5000])).as_deref(),
            Some("-0.5")
        );
        // 123.456 with dscale 3
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(2, 0, 0x0000, 3, &[123, 4560])).as_deref(),
            Some("123.456")
        );
    }

    #[test]
    fn pg_numeric_decodes_zero_and_specials() {
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(0, 0, 0x0000, 0, &[])).as_deref(),
            Some("0")
        );
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(0, 0, 0x0000, 2, &[])).as_deref(),
            Some("0.00")
        );
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(0, 0, 0xC000u16 as i16, 0, &[])).as_deref(),
            Some("NaN")
        );
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(0, 0, 0xD000u16 as i16, 0, &[])).as_deref(),
            Some("Infinity")
        );
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(0, 0, 0xF000u16 as i16, 0, &[])).as_deref(),
            Some("-Infinity")
        );
    }

    #[test]
    fn pg_numeric_rejects_malformed_payloads() {
        assert_eq!(pg_numeric_to_string(&[0, 1, 0]), None);
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(-1, 0, 0x0000, 0, &[])),
            None
        );
        assert_eq!(
            pg_numeric_to_string(&numeric_raw(3, 0, 0x0000, 0, &[1, 2])),
            None,
            "digit array shorter than ndigits"
        );
    }
}
