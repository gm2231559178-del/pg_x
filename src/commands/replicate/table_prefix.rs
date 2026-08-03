//! The shared `schema.table:` prefix grammar used by row filters and column
//! transforms. One helper owns the split so both consumers parse prefixes
//! identically.

use anyhow::{bail, Result};

/// An optional `schema.table` key. `None` marks a global rule applying to
/// every table; `Some((schema, table))` restricts it to one table.
pub(crate) type TableKey = Option<(String, String)>;

/// Split an argument of the form `[schema.table:]rest` into the optional table
/// key and the remainder.
///
/// A prefix must be schema-qualified; a bare `table:` or a leading `:` is
/// rejected so a global rule is always written without a prefix.
pub(crate) fn split_table_prefix(arg: &str) -> Result<(TableKey, &str)> {
    let Some(pos) = arg.find(':') else {
        return Ok((None, arg));
    };
    let prefix = &arg[..pos];
    if prefix.is_empty() {
        bail!(
            "empty table prefix before ':' — use 'schema.table:...' or omit the prefix for global rules"
        );
    }
    let table_key = if let Some(dot) = prefix.find('.') {
        Some((prefix[..dot].to_string(), prefix[dot + 1..].to_string()))
    } else {
        bail!(
            "table prefix must be schema-qualified (e.g. public.orders:...), got '{prefix}' \
             — use 'public.{prefix}:...' or omit the prefix for global rules"
        );
    };
    Ok((table_key, &arg[pos + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_prefix_is_a_global_rule() {
        let (key, rest) = split_table_prefix("status = 'active'").unwrap();
        assert_eq!(key, None);
        assert_eq!(rest, "status = 'active'");
    }

    #[test]
    fn schema_qualified_prefix_is_split() {
        let (key, rest) = split_table_prefix("public.orders:a=1").unwrap();
        assert_eq!(key, Some(("public".to_string(), "orders".to_string())));
        assert_eq!(rest, "a=1");
    }

    #[test]
    fn leading_colon_is_rejected() {
        let err = split_table_prefix(":a=1").unwrap_err().to_string();
        assert!(err.contains("empty table prefix"), "{err}");
    }

    #[test]
    fn bare_table_prefix_is_rejected() {
        let err = split_table_prefix("orders:a=1").unwrap_err().to_string();
        assert!(err.contains("schema-qualified"), "{err}");
    }
}
