use anyhow::{bail, Result};

use super::table_prefix::{split_table_prefix, TableKey};
use crate::replication::event::WalEvent;

#[derive(Debug, Clone, Default)]
pub(crate) struct TableTransform {
    pub(crate) drop_cols: Vec<String>,
    pub(crate) renames: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ColumnTransforms {
    pub(crate) entries: Vec<(TableKey, TableTransform)>,
}

impl ColumnTransforms {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Assemble the column transforms from config then CLI sources.
    ///
    /// Deterministic ordering: config rules are merged first, CLI rules second.
    /// A global rule (`None` key) and a table-specific rule (`Some` key) stay
    /// distinct entries and both apply, with drops and renames accumulated in
    /// first-seen order.
    pub(crate) fn from_sources(
        config_drop_cols: &[String],
        config_rename: &[String],
        cli_drop_cols: &[String],
        cli_rename: &[String],
    ) -> Result<Self> {
        let mut t = ColumnTransforms::new();
        for arg in config_drop_cols.iter().chain(cli_drop_cols) {
            let (key, cols) = parse_drop_cols_arg(arg)?;
            t.merge(key, cols, Vec::new());
        }
        for arg in config_rename.iter().chain(cli_rename) {
            let (key, pairs) = parse_rename_arg(arg)?;
            t.merge(key, Vec::new(), pairs);
        }
        Ok(t)
    }

    fn merge(&mut self, key: TableKey, drop_cols: Vec<String>, renames: Vec<(String, String)>) {
        match self.entries.iter_mut().find(|(k, _)| k == &key) {
            Some((_, tt)) => {
                tt.drop_cols.extend(drop_cols);
                tt.renames.extend(renames);
            }
            None => self
                .entries
                .push((key, TableTransform { drop_cols, renames })),
        }
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
    let (table_key, rest) = split_table_prefix(arg)?;
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
    let (table_key, rest) = split_table_prefix(arg)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn from_sources_accumulates_drops_for_the_same_key() {
        let t = ColumnTransforms::from_sources(
            &[s("public.orders:a,b")],
            &[],
            &[s("public.orders:c")],
            &[],
        )
        .unwrap();
        assert_eq!(t.entries.len(), 1);
        let (key, tt) = &t.entries[0];
        assert_eq!(key, &Some((s("public"), s("orders"))));
        assert_eq!(tt.drop_cols, vec![s("a"), s("b"), s("c")]);
        assert!(tt.renames.is_empty());
    }

    #[test]
    fn from_sources_merges_drop_and_rename_for_the_same_key() {
        let t = ColumnTransforms::from_sources(
            &[s("public.orders:a")],
            &[s("public.orders:x=y")],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(t.entries.len(), 1);
        let (_, tt) = &t.entries[0];
        assert_eq!(tt.drop_cols, vec![s("a")]);
        assert_eq!(tt.renames, vec![(s("x"), s("y"))]);
    }

    #[test]
    fn global_and_specific_rules_stay_distinct_and_in_order() {
        let t = ColumnTransforms::from_sources(&[s("public.orders:a")], &[], &[s("secret")], &[])
            .unwrap();
        assert_eq!(t.entries.len(), 2);
        assert_eq!(
            t.entries[0].0,
            Some((s("public"), s("orders"))),
            "config first"
        );
        assert_eq!(t.entries[1].0, None, "CLI second");
    }

    #[test]
    fn cli_drops_come_after_config_drops() {
        let t = ColumnTransforms::from_sources(
            &[s("public.orders:config_col")],
            &[],
            &[s("public.orders:cli_col")],
            &[],
        )
        .unwrap();
        assert_eq!(
            t.entries[0].1.drop_cols,
            vec![s("config_col"), s("cli_col")]
        );
    }

    #[test]
    fn from_sources_rejects_bad_args() {
        assert!(
            ColumnTransforms::from_sources(&[s("orders:a")], &[], &[], &[]).is_err(),
            "bare table prefix rejected"
        );
        assert!(
            ColumnTransforms::from_sources(&[], &[], &[s(":a")], &[]).is_err(),
            "empty prefix rejected"
        );
        assert!(
            ColumnTransforms::from_sources(&[], &[], &[s(",")], &[]).is_err(),
            "empty cols rejected"
        );
        assert!(
            ColumnTransforms::from_sources(&[], &[], &[], &[s("no-equals")]).is_err(),
            "missing '=' rejected"
        );
    }
}
