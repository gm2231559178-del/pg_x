//! The batch-key codec: the single serialization shared by the DataLoader and
//! the result lookup. Both sides must encode the same logical key identically,
//! or numeric batch keys silently stop matching.

use serde_json::Value;

/// Serialize a batch parameter value (string, numeric, boolean) to the string
/// key used for `ANY($1)` batching and for grouping results by key.
pub fn batch_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_pass_through_unchanged() {
        assert_eq!(batch_key(&Value::String("42".into())), "42");
        assert_eq!(batch_key(&Value::String("abc".into())), "abc");
    }

    #[test]
    fn numbers_encode_as_their_decimal_notation() {
        assert_eq!(batch_key(&Value::Number(42.into())), "42");
        assert_eq!(
            batch_key(&Value::Number(serde_json::Number::from_f64(42.5).unwrap())),
            "42.5"
        );
    }

    #[test]
    fn booleans_encode_lowercase() {
        assert_eq!(batch_key(&Value::Bool(true)), "true");
        assert_eq!(batch_key(&Value::Bool(false)), "false");
    }

    #[test]
    fn null_encodes_to_empty_string() {
        assert_eq!(batch_key(&Value::Null), "");
    }

    /// The symmetry invariant that fixes the numeric-key bug: the parent-side
    /// value (a JSON number from a row or variable) and the child-side value
    /// (a JSON string, number, or bool from the result row) of the same logical
    /// key must encode identically.
    #[test]
    fn load_and_lookup_sides_are_symmetric() {
        assert_eq!(
            batch_key(&Value::Number(7.into())),
            batch_key(&Value::String("7".into()))
        );
        assert_eq!(
            batch_key(&Value::Number(7.into())),
            batch_key(&Value::Number(7.into()))
        );
        assert_eq!(
            batch_key(&Value::Bool(true)),
            batch_key(&Value::String("true".into()))
        );
    }

    #[test]
    fn distinct_values_keep_distinct_keys() {
        assert_ne!(
            batch_key(&Value::Number(42.into())),
            batch_key(&Value::Number(43.into()))
        );
        assert_ne!(
            batch_key(&Value::String("a".into())),
            batch_key(&Value::String("b".into()))
        );
    }
}
