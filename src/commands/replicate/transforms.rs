use anyhow::{bail, Result};

use crate::replication::event::WalEvent;
use super::filter::TableKey;

#[derive(Debug, Clone, Default)]
pub(crate) struct TableTransform {
    pub(crate) drop_cols: Vec<String>,
    pub(crate) renames: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ColumnTransforms {
    pub(crate) entries: Vec<(Option<(String, String)>, TableTransform)>,
}

impl ColumnTransforms {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries
            .iter()
            .all(|(_, t)| t.drop_cols.is_empty() && t.renames.is_empty())
    }

    pub(crate) fn apply(&self, event: &mut WalEvent) {
        let tn = event
            .table_name()
            .map(|(s, t)| (s.to_string(), t.to_string()));
        let (schema, table) = match tn {
            Some(ref p) => p,
            None => return,
        };
        for (key, transform) in &self.entries {
            let applies = match key {
                Some((ref s, ref t)) => s == schema && t == table,
                None => true,
            };
            if applies {
                event.apply_transforms(&transform.drop_cols, &transform.renames);
            }
        }
    }
}

pub(crate) fn parse_drop_cols_arg(arg: &str) -> Result<(TableKey, Vec<String>)> {
    let (table_key, rest) = parse_table_prefix(arg)?;
    let cols: Vec<String> = rest
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if cols.is_empty() {
        bail!("drop-cols: no columns specified in '{arg}'");
    }
    Ok((table_key, cols))
}

pub(crate) fn parse_rename_arg(arg: &str) -> Result<(TableKey, Vec<(String, String)>)> {
    let (table_key, rest) = parse_table_prefix(arg)?;
    let mut pairs = Vec::new();
    for part in rest.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut eq_split = part.splitn(2, '=');
        let old = eq_split.next().unwrap().trim().to_string();
        let new = eq_split
            .next()
            .ok_or_else(|| anyhow::anyhow!("rename: expected 'old=new' format, got '{part}'"))?
            .trim()
            .to_string();
        if old.is_empty() || new.is_empty() {
            bail!("rename: empty name in rename pair '{part}'");
        }
        pairs.push((old, new));
    }
    if pairs.is_empty() {
        bail!("rename: no rename pairs specified in '{arg}'");
    }
    Ok((table_key, pairs))
}

fn parse_table_prefix(arg: &str) -> Result<(TableKey, &str)> {
    if let Some(pos) = arg.find(':') {
        let prefix = &arg[..pos];
        if prefix.is_empty() {
            bail!("empty table prefix before ':'");
        }
        let table_key = if let Some(dot) = prefix.find('.') {
            Some((prefix[..dot].to_string(), prefix[dot + 1..].to_string()))
        } else {
            return Err(anyhow::anyhow!(
                "prefix must be schema-qualified (e.g. public.orders:...), \
                 got '{prefix}' — use 'public.{prefix}:...' or omit the prefix for global rules"
            ));
        };
        Ok((table_key, &arg[pos + 1..]))
    } else {
        Ok((None, arg))
    }
}
