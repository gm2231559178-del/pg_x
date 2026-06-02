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
/// | `'u'`        | Unchanged / not sent (TOAST or non-key)    | `"__pgx_unchanged"` |
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
            // Use a sentinel string so consumers can tell "not sent" from NULL
            ColVal::Unchanged => s.serialize_str("__pgx_unchanged"),
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
}
