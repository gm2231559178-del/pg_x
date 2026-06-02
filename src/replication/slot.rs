//! Helpers for managing PostgreSQL logical replication slots.

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;

/// Information about an existing replication slot.
#[derive(Debug)]
pub struct SlotInfo {
    pub name: String,
    pub plugin: String,
    pub database: Option<String>,
    pub active: bool,
    pub confirmed_flush_lsn: Option<String>,
    pub restart_lsn: Option<String>,
}

/// Validate a replication slot name.
///
/// PostgreSQL slot names must be valid identifiers: start with a letter or
/// underscore, followed by letters, digits, or underscores.
fn validate_slot_name(name: &str) -> Result<()> {
    let first = name.chars().next().unwrap_or('\0');
    if !first.is_ascii_alphabetic() && first != '_' {
        bail!("Invalid slot name '{name}': must start with a letter or underscore");
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("Invalid slot name '{name}': only letters, digits, and underscores are allowed");
    }
    Ok(())
}

/// Ensure a logical replication slot with the given name exists.
///
/// If the slot already exists this is a no-op (returns the existing slot info).
/// If it does not exist it is created with the `pgoutput` plugin.
///
/// `temporary` — create a temporary slot that is dropped when the session ends.
pub async fn ensure_slot(client: &Client, slot_name: &str, temporary: bool) -> Result<()> {
    validate_slot_name(slot_name)?;

    // Check if the slot already exists.
    let rows = client
        .query(
            "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("Failed to query pg_replication_slots")?;

    if let Some(row) = rows.first() {
        let active: bool = row.get("active");
        if active {
            bail!(
                "Replication slot '{}' already exists and is currently active (in use by another process). \
                 Stop the other consumer or choose a different slot name.",
                slot_name
            );
        }
        // Slot exists and is not active — we can reuse it.
        return Ok(());
    }

    // Use the SQL function (works from any connection).
    let sql = if temporary {
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput', true)"
    } else {
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false)"
    };

    client
        .query(sql, &[&slot_name])
        .await
        .with_context(|| format!("Failed to create replication slot '{slot_name}'"))?;

    Ok(())
}

/// Drop a replication slot by name. Does nothing if the slot doesn't exist.
pub async fn drop_slot(client: &Client, slot_name: &str) -> Result<()> {
    validate_slot_name(slot_name)?;

    let rows = client
        .query(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("Failed to query pg_replication_slots")?;

    if rows.is_empty() {
        return Ok(());
    }

    client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
        .with_context(|| format!("Failed to drop replication slot '{slot_name}'"))?;

    Ok(())
}

/// List all logical replication slots on the server.
pub async fn list_slots(client: &Client) -> Result<Vec<SlotInfo>> {
    let rows = client
        .query(
            "SELECT slot_name, plugin, database, active, \
                    confirmed_flush_lsn::text, restart_lsn::text \
             FROM pg_replication_slots \
             WHERE slot_type = 'logical' \
             ORDER BY slot_name",
            &[],
        )
        .await
        .context("Failed to list replication slots")?;

    Ok(rows
        .into_iter()
        .map(|row| SlotInfo {
            name: row.get("slot_name"),
            plugin: row.get("plugin"),
            database: row.get("database"),
            active: row.get("active"),
            confirmed_flush_lsn: row.get("confirmed_flush_lsn"),
            restart_lsn: row.get("restart_lsn"),
        })
        .collect())
}
