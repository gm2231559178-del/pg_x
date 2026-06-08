use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// Column value — three distinct states from pgoutput
// ─────────────────────────────────────────────────────────────────────────────

/// A column value as received from the pgoutput logical replication stream.
///
/// PostgreSQL's tuple format distinguishes three states that `Option<String>`
/// cannot represent unambiguously:
///
/// | pgoutput tag | Meaning                                    | Serialises as       |
/// |--------------|--------------------------------------------|---------------------|
/// | `'t'`        | Text value (even if the text is "NULL")    | `"some text"`       |
/// | `'n'`        | SQL NULL                                   | `null`              |
/// | `'u'`        | Unchanged / not sent (TOAST or non-key)    | `{"$unchanged": true}` |
///
/// The `'u'` case appears in:
/// - UPDATE old-tuples under `REPLICA IDENTITY DEFAULT/INDEX` for non-key columns
/// - DELETE old-tuples under `REPLICA IDENTITY DEFAULT` for non-key columns
///
/// Run `ALTER TABLE t REPLICA IDENTITY FULL` to receive all column values.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum ColVal {
    /// A text-encoded SQL value (may be any non-null SQL type).
    Text(String),
    /// SQL NULL.
    Null,
    /// Column not sent by the server (unchanged TOAST or non-replica-identity column).
    Unchanged,
}

#[allow(dead_code)]
impl ColVal {
    /// Returns `true` if the column was not sent by the server.
    pub fn is_unchanged(&self) -> bool {
        matches!(self, ColVal::Unchanged)
    }

    /// Returns the text value if present.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ColVal::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

impl Serialize for ColVal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ColVal::Text(v) => s.serialize_str(v),
            ColVal::Null => s.serialize_none(),
            // Use a structured marker so consumers can tell "not sent" from NULL
            // without needing to know a magic string.
            ColVal::Unchanged => {
                let mut map = s.serialize_map(Some(1))?;
                map.serialize_entry("$unchanged", &true)?;
                map.end()
            }
        }
    }
}

pub type Row = HashMap<String, ColVal>;

// ─────────────────────────────────────────────────────────────────────────────
// WAL event enum
// ─────────────────────────────────────────────────────────────────────────────

/// A decoded WAL event from the pgoutput logical replication protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WalEvent {
    /// Marks the start of a transaction.
    Begin {
        lsn: String,
        commit_time: i64,
        xid: u32,
    },

    /// Marks the successful commit of a transaction.
    Commit {
        lsn: String,
        end_lsn: String,
        commit_time: i64,
    },

    /// Describes a relation (table) — emitted before the first DML on that table.
    Relation {
        rel_id: u32,
        schema: String,
        table: String,
        columns: Vec<ColumnDef>,
    },

    /// A row was inserted.
    Insert {
        rel_id: u32,
        schema: String,
        table: String,
        /// New row — all columns are always `Text` or `Null` for inserts.
        new: Row,
    },

    /// A row was updated.
    Update {
        rel_id: u32,
        schema: String,
        table: String,
        /// Old row values.
        ///
        /// - `None` — server sent no old tuple (update did not touch replica-identity cols)
        /// - `Some(row)` — old values; non-key columns may be `Unchanged` under DEFAULT identity
        ///
        /// To always receive full old rows: `ALTER TABLE t REPLICA IDENTITY FULL`
        old: Option<Row>,
        /// New (post-update) row — all columns present.
        new: Row,
    },

    /// A row was deleted.
    Delete {
        rel_id: u32,
        schema: String,
        table: String,
        /// Old row at time of deletion.
        ///
        /// Under `REPLICA IDENTITY DEFAULT` only key columns are `Text`/`Null`;
        /// all other columns will be `Unchanged`.
        ///
        /// To always receive full old rows: `ALTER TABLE t REPLICA IDENTITY FULL`
        old: Row,
    },

    /// One or more tables were truncated.
    Truncate {
        rel_ids: Vec<u32>,
        tables: Vec<String>,
        cascade: bool,
        restart_seqs: bool,
    },

    /// A server keepalive — handled internally, not normally forwarded.
    Keepalive {
        wal_end: String,
        reply_requested: bool,
    },
}

/// Column metadata from a Relation message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// Whether this column is part of the replica identity.
    pub is_key: bool,
    /// PostgreSQL type OID.
    pub type_id: u32,
    /// Type modifier (atttypmod).
    pub type_modifier: i32,
}

#[allow(dead_code)]
impl WalEvent {
    pub fn op_label(&self) -> &'static str {
        match self {
            WalEvent::Begin { .. } => "BEGIN",
            WalEvent::Commit { .. } => "COMMIT",
            WalEvent::Relation { .. } => "RELATION",
            WalEvent::Insert { .. } => "INSERT",
            WalEvent::Update { .. } => "UPDATE",
            WalEvent::Delete { .. } => "DELETE",
            WalEvent::Truncate { .. } => "TRUNCATE",
            WalEvent::Keepalive { .. } => "KEEPALIVE",
        }
    }

    pub fn table_name(&self) -> Option<(&str, &str)> {
        match self {
            WalEvent::Insert { schema, table, .. }
            | WalEvent::Update { schema, table, .. }
            | WalEvent::Delete { schema, table, .. } => Some((schema, table)),
            _ => None,
        }
    }

    pub fn to_json(&self) -> String {
        match serde_json::to_string(self) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "Failed to serialize WAL event to JSON");
                "{}".to_string()
            }
        }
    }

    /// Apply column transforms (drop columns, rename columns) in-place.
    ///
    /// Renames first remove all old keys, then insert under new names,
    /// so `a→b, b→a` correctly swaps values without data loss.
    pub fn apply_transforms(&mut self, drop_cols: &[String], renames: &[(String, String)]) {
        let mut rows = match self {
            WalEvent::Insert { new, .. } => vec![new],
            WalEvent::Update { old, new, .. } => {
                let mut v = vec![new];
                if let Some(ref mut o) = old {
                    v.push(o);
                }
                v
            }
            WalEvent::Delete { old, .. } => vec![old],
            _ => return,
        };
        for row in &mut rows {
            for col in drop_cols {
                row.remove(col);
            }
            // Two-phase rename: first remove all old keys, then insert new ones.
            // This prevents data loss when keys collide (e.g. a→b, b→a).
            let mut pending: Vec<(String, ColVal)> = Vec::new();
            for (old_name, new_name) in renames {
                if let Some(val) = row.remove(old_name) {
                    pending.push((new_name.clone(), val));
                }
            }
            for (new_key, val) in pending {
                row.insert(new_key, val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ColVal ───────────────────────────────────────────────────────────────

    #[test]
    fn colval_is_unchanged() {
        assert!(ColVal::Unchanged.is_unchanged());
        assert!(!ColVal::Text("x".into()).is_unchanged());
        assert!(!ColVal::Null.is_unchanged());
    }

    #[test]
    fn colval_as_str() {
        assert_eq!(ColVal::Text("hello".into()).as_str(), Some("hello"));
        assert_eq!(ColVal::Null.as_str(), None);
        assert_eq!(ColVal::Unchanged.as_str(), None);
    }

    #[test]
    fn colval_serialize_text() {
        let v = serde_json::to_value(ColVal::Text("foo".into())).unwrap();
        assert_eq!(v, serde_json::json!("foo"));
    }

    #[test]
    fn colval_serialize_null() {
        let v = serde_json::to_value(&ColVal::Null).unwrap();
        assert_eq!(v, serde_json::Value::Null);
    }

    #[test]
    fn colval_serialize_unchanged() {
        let v = serde_json::to_value(&ColVal::Unchanged).unwrap();
        assert_eq!(v, serde_json::json!({"$unchanged": true}));
    }

    // ── WalEvent ─────────────────────────────────────────────────────────────

    #[test]
    fn op_label_all_variants() {
        assert_eq!(
            WalEvent::Begin {
                lsn: "0/0".into(),
                commit_time: 0,
                xid: 0
            }
            .op_label(),
            "BEGIN"
        );
        assert_eq!(
            WalEvent::Commit {
                lsn: "0/0".into(),
                end_lsn: "0/0".into(),
                commit_time: 0
            }
            .op_label(),
            "COMMIT"
        );
        assert_eq!(
            WalEvent::Relation {
                rel_id: 0,
                schema: "s".into(),
                table: "t".into(),
                columns: vec![]
            }
            .op_label(),
            "RELATION"
        );
        assert_eq!(
            WalEvent::Insert {
                rel_id: 0,
                schema: "s".into(),
                table: "t".into(),
                new: Row::new()
            }
            .op_label(),
            "INSERT"
        );
        assert_eq!(
            WalEvent::Update {
                rel_id: 0,
                schema: "s".into(),
                table: "t".into(),
                old: None,
                new: Row::new()
            }
            .op_label(),
            "UPDATE"
        );
        assert_eq!(
            WalEvent::Delete {
                rel_id: 0,
                schema: "s".into(),
                table: "t".into(),
                old: Row::new()
            }
            .op_label(),
            "DELETE"
        );
        assert_eq!(
            WalEvent::Truncate {
                rel_ids: vec![],
                tables: vec![],
                cascade: false,
                restart_seqs: false
            }
            .op_label(),
            "TRUNCATE"
        );
        assert_eq!(
            WalEvent::Keepalive {
                wal_end: "0/0".into(),
                reply_requested: false
            }
            .op_label(),
            "KEEPALIVE"
        );
    }

    #[test]
    fn table_name_insert_update_delete() {
        let insert = WalEvent::Insert {
            rel_id: 1,
            schema: "public".into(),
            table: "users".into(),
            new: Row::new(),
        };
        assert_eq!(insert.table_name(), Some(("public", "users")));

        let update = WalEvent::Update {
            rel_id: 1,
            schema: "public".into(),
            table: "users".into(),
            old: None,
            new: Row::new(),
        };
        assert_eq!(update.table_name(), Some(("public", "users")));

        let delete = WalEvent::Delete {
            rel_id: 1,
            schema: "public".into(),
            table: "users".into(),
            old: Row::new(),
        };
        assert_eq!(delete.table_name(), Some(("public", "users")));
    }

    #[test]
    fn table_name_other_variants() {
        assert_eq!(
            WalEvent::Begin {
                lsn: "0/0".into(),
                commit_time: 0,
                xid: 0
            }
            .table_name(),
            None
        );
        assert_eq!(
            WalEvent::Commit {
                lsn: "0/0".into(),
                end_lsn: "0/0".into(),
                commit_time: 0
            }
            .table_name(),
            None
        );
    }

    #[test]
    fn to_json_insert() {
        let event = WalEvent::Insert {
            rel_id: 42,
            schema: "public".into(),
            table: "users".into(),
            new: {
                let mut r = Row::new();
                r.insert("name".into(), ColVal::Text("Alice".into()));
                r
            },
        };
        let json = event.to_json();
        assert!(json.contains("insert"), "JSON: {json}");
        assert!(json.contains("Alice"), "JSON: {json}");
        assert!(json.contains("public"), "JSON: {json}");
        assert!(json.contains("users"), "JSON: {json}");
    }
}
