use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;

use super::batch::batch_key;
use super::dataloader::DataLoader;
use super::query::{FieldSelection, NamedQuery};
use super::runner::QueryRunner;
use crate::utils::config::ResolverConfig;

/// Execute a named query with given variables and return the assembled JSON document.
/// Uses DataLoader batching for child resolvers when `batch_by` is configured.
pub async fn execute(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
) -> Result<Value> {
    execute_batched(query, variables, resolvers, runner, max_depth).await
}

/// Recursively resolve child fields for a single parent row.
/// `depth` tracks the recursion level to prevent stack overflow on circular references.
async fn resolve_children(
    parent_obj: &mut Value,
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
) -> Result<()> {
    resolve_children_with_depth(parent_obj, child_fields, resolvers, runner, max_depth, 0).await
}

/// Resolve sibling child fields concurrently.
/// Sibling fields have no data dependencies, so they can run in parallel.
async fn resolve_children_with_depth(
    parent_obj: &mut Value,
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
    depth: u32,
) -> Result<()> {
    if depth > max_depth {
        anyhow::bail!(
            "Max resolver recursion depth ({}) exceeded — possible circular reference",
            max_depth
        );
    }

    // Phase 1: collect task parameters (immutable borrow of parent_obj)
    let parents = std::slice::from_ref(parent_obj);
    let tasks: Vec<_> = child_fields
        .iter()
        .filter_map(|field| {
            let field_name = field
                .field_name
                .split('(')
                .next()
                .unwrap_or(&field.field_name)
                .to_string();

            let resolver = match resolvers.get(&field_name) {
                Some(r) => r,
                None => {
                    if !field.is_leaf && !field.children.is_empty() {
                        tracing::warn!("No resolver for child field '{}'", field_name);
                    }
                    return None;
                }
            };

            if field.is_leaf || field.children.is_empty() {
                return None;
            }

            if resolver.batch_by.is_none() && depth > 0 {
                tracing::warn!(
                    field = %field_name,
                    "to-many resolver lacks batch_by — this causes N+1 queries. Add batch_by to the resolver config for this field.",
                );
            }

            let field = field.clone();
            let resolver = resolver.clone();
            Some(async move {
                let per_parent = resolve_field_for_parents(
                    &field,
                    &resolver,
                    parents,
                    resolvers,
                    runner,
                    max_depth,
                    depth + 1,
                )
                .await?;
                Ok::<_, anyhow::Error>((field_name, resolver, per_parent))
            })
        })
        .collect();

    // Phase 2: run all sibling queries concurrently
    let results = futures::future::join_all(tasks).await;

    // Phase 3: apply results to parent_obj (mutable borrow)
    for result in results {
        let (field_name, resolver, per_parent) = result?;
        let children = per_parent.into_iter().next().unwrap_or_default();
        if let Value::Object(ref mut m) = parent_obj {
            let name = field_name;
            let val = if children.len() == 1 && !is_to_many(&resolver) {
                children.into_iter().next().ok_or_else(|| {
                    anyhow::anyhow!("expected 1 child, got 0 for field '{}'", name)
                })?
            } else {
                Value::Array(children)
            };
            m.insert(name, val);
        }
    }

    Ok(())
}

/// Extract an argument value from a field like "material(mat_no: $mat_no)".
/// Given param_name "mat_no", returns "$mat_no" -> "mat_no" (the variable name).
fn extract_arg(field_expr: &str, param_name: &str) -> Option<String> {
    if let Some(args_start) = field_expr.find('(') {
        let args = &field_expr[args_start + 1..];
        if let Some(args_end) = args.rfind(')') {
            let args_str = &args[..args_end];
            for arg in args_str.split(',') {
                let arg = arg.trim();
                if let Some((name, val)) = arg.split_once(':') {
                    let name = name.trim();
                    if name == param_name {
                        let val = val.trim();
                        return Some(val.strip_prefix('$').unwrap_or(val).to_string());
                    }
                }
            }
        }
    }
    None
}

/// Heuristic: if resolver has `batch_by` set, it's a to-many relation.
fn is_to_many(resolver: &ResolverConfig) -> bool {
    resolver.batch_by.is_some()
}

/// The single collapsed resolver dispatch, parameterized by batch mode. Runs
/// one field for every parent in `parents` and returns the resolved children
/// per parent, in parent order. `next_depth` is the depth handed to child
/// recursion.
#[allow(clippy::too_many_arguments)]
async fn resolve_field_for_parents(
    field: &FieldSelection,
    resolver: &ResolverConfig,
    parents: &[Value],
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
    next_depth: u32,
) -> Result<Vec<Vec<Value>>> {
    let field_name = field
        .field_name
        .split('(')
        .next()
        .unwrap_or(&field.field_name)
        .to_string();
    let param_name = resolver.param.as_deref().unwrap_or(&field_name);
    let children = &field.children;

    if let Some(batch_by) = &resolver.batch_by {
        // Batched mode: one query for all parents via the DataLoader.
        let mut loader = DataLoader::new(&resolver.sql, batch_by);
        if let Some(cache) = runner.global_cache() {
            loader = loader.with_global_cache(cache);
        }
        for obj in parents {
            if let Some(key) = obj.get(param_name) {
                loader.add_key(key);
            }
        }
        loader.execute(runner).await?;

        let mut per_parent = Vec::with_capacity(parents.len());
        for obj in parents {
            let key = obj.get(param_name).map(batch_key).unwrap_or_default();
            let mut resolved = Vec::new();
            for mut child in loader.get_children(&key) {
                if !children.is_empty() {
                    resolve_children_with_depth(
                        &mut child, children, resolvers, runner, max_depth, next_depth,
                    )
                    .await?;
                }
                resolved.push(child);
            }
            per_parent.push(resolved);
        }
        Ok(per_parent)
    } else {
        // Non-batched mode: one query per parent, run concurrently.
        let child_fields = children.clone();
        let tasks: Vec<_> = parents
            .iter()
            .map(|obj| {
                let param_value = obj.get(param_name).cloned().unwrap_or(Value::Null);
                let field_name = field_name.clone();
                let children = child_fields.clone();
                async move {
                    let rows = runner
                        .run_rows_array(&resolver.sql, &[batch_key(&param_value)])
                        .await
                        .with_context(|| {
                            format!("Child resolver SQL failed for '{}'", field_name)
                        })?;

                    let mut resolved = Vec::new();
                    for mut child in rows {
                        if !children.is_empty() {
                            resolve_children_with_depth(
                                &mut child, &children, resolvers, runner, max_depth, next_depth,
                            )
                            .await?;
                        }
                        resolved.push(child);
                    }
                    Ok::<_, anyhow::Error>(resolved)
                }
            })
            .collect();

        let all = futures::future::join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(all)
    }
}

/// Execute a named query with DataLoader batching for child resolvers.
async fn execute_batched(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
) -> Result<Value> {
    let root_selection = &query.selection;
    let root_fields = &root_selection.children;

    let mut root_values = Vec::new();

    for field in root_fields {
        let field_name = field
            .field_name
            .split('(')
            .next()
            .unwrap_or(&field.field_name)
            .to_string();

        let resolver = resolvers
            .get(&field_name)
            .with_context(|| format!("No resolver configured for '{}'", field_name))?;

        let param_value = if let Some(param_name) = &resolver.param {
            variables
                .get(param_name)
                .cloned()
                .or_else(|| {
                    extract_arg(&field.field_name, param_name)
                        .and_then(|var_name| variables.get(&var_name).cloned())
                })
                .with_context(|| {
                    format!(
                        "Missing required variable '{}' for resolver '{}'",
                        param_name, field_name
                    )
                })?
        } else {
            Value::Null
        };

        let rows = runner
            .run_rows(&resolver.sql, &[batch_key(&param_value)])
            .await
            .with_context(|| format!("Resolver SQL failed for '{}'", field_name))?;

        for obj in rows {
            root_values.push(obj);
        }

        // Resolve child fields with batching using DataLoader
        if !field.children.is_empty() && root_values.len() > 1 {
            resolve_children_batched(
                &mut root_values,
                &field.children,
                resolvers,
                runner,
                max_depth,
            )
            .await?;
        } else if !field.children.is_empty() {
            // Single root row — use direct resolver (no batching needed)
            for root_val in &mut root_values {
                resolve_children(root_val, &field.children, resolvers, runner, max_depth).await?;
            }
        }
    }

    if root_values.len() == 1 {
        Ok(root_values
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("expected 1 root value, got 0"))?)
    } else {
        Ok(Value::Array(root_values))
    }
}

/// Resolve child fields across many parent rows. Batched resolvers run one
/// query for all parents; non-batched resolvers run one query per parent.
async fn resolve_children_batched(
    parent_objs: &mut [Value],
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    runner: &dyn QueryRunner,
    max_depth: u32,
) -> Result<()> {
    for field in child_fields {
        let field_name = field
            .field_name
            .split('(')
            .next()
            .unwrap_or(&field.field_name);

        let resolver = match resolvers.get(field_name) {
            Some(r) => r,
            None => {
                if !field.is_leaf && !field.children.is_empty() {
                    tracing::warn!("No resolver for child field '{}'", field_name);
                }
                continue;
            }
        };

        if field.is_leaf || field.children.is_empty() {
            continue;
        }

        if resolver.batch_by.is_none() {
            tracing::warn!(
                field = %field_name,
                parent_count = parent_objs.len(),
                "to-many resolver lacks batch_by — N+1 queries executed. Add batch_by to the resolver config.",
            );
        }

        let per_parent = resolve_field_for_parents(
            field,
            resolver,
            parent_objs,
            resolvers,
            runner,
            max_depth,
            0,
        )
        .await?;

        for (obj, children) in parent_objs.iter_mut().zip(per_parent) {
            if let Value::Object(ref mut m) = obj {
                m.insert(field_name.to_string(), Value::Array(children));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;

    use crate::graphql::pool::GlobalDataCache;

    const MATERIALS_SQL: &str = "SELECT * FROM materials";
    const SIZES_SQL: &str = "SELECT * FROM sizes WHERE mat_no = ANY($1)";

    type Respond = Box<dyn Fn(&str, &[String]) -> Result<Vec<Value>> + Send + Sync>;

    struct FakeRunner {
        respond: Respond,
    }

    impl FakeRunner {
        fn new(
            respond: impl Fn(&str, &[String]) -> Result<Vec<Value>> + Send + Sync + 'static,
        ) -> Self {
            Self {
                respond: Box::new(respond),
            }
        }
    }

    #[async_trait]
    impl QueryRunner for FakeRunner {
        async fn run_rows(&self, sql: &str, params: &[String]) -> Result<Vec<Value>> {
            (self.respond)(sql, params)
        }

        async fn run_rows_array(&self, sql: &str, values: &[String]) -> Result<Vec<Value>> {
            (self.respond)(sql, values)
        }

        fn global_cache(&self) -> Option<Arc<GlobalDataCache>> {
            None
        }
    }

    fn fixture_query(child_field: &str) -> NamedQuery {
        NamedQuery {
            name: "test".to_string(),
            operation_name: "Full".to_string(),
            variables: Vec::new(),
            selection: FieldSelection {
                field_name: "__root".to_string(),
                children: vec![FieldSelection {
                    field_name: "materials".to_string(),
                    children: vec![
                        FieldSelection {
                            field_name: "mat_no".to_string(),
                            children: Vec::new(),
                            is_leaf: true,
                        },
                        FieldSelection {
                            field_name: child_field.to_string(),
                            children: vec![FieldSelection {
                                field_name: "size_code".to_string(),
                                children: Vec::new(),
                                is_leaf: true,
                            }],
                            is_leaf: false,
                        },
                    ],
                    is_leaf: false,
                }],
                is_leaf: false,
            },
        }
    }

    fn resolvers(sizes_batch_by: Option<&str>) -> HashMap<String, ResolverConfig> {
        let mut m = HashMap::new();
        m.insert(
            "materials".to_string(),
            ResolverConfig {
                sql: MATERIALS_SQL.to_string(),
                param: None,
                batch_by: None,
                connection: None,
            },
        );
        m.insert(
            "sizes".to_string(),
            ResolverConfig {
                sql: SIZES_SQL.to_string(),
                param: Some("mat_no".to_string()),
                batch_by: sizes_batch_by.map(|s| s.to_string()),
                connection: None,
            },
        );
        m
    }

    fn tables(materials: &[Value], sizes: &[Value]) -> HashMap<String, Vec<Value>> {
        let mut t = HashMap::new();
        t.insert(MATERIALS_SQL.to_string(), materials.to_vec());
        t.insert(SIZES_SQL.to_string(), sizes.to_vec());
        t
    }

    /// A fake that filters child rows by the batch key, treating an empty param
    /// (the null encoding) as "no filter" for parameterless root queries.
    fn runner_for(tables: HashMap<String, Vec<Value>>) -> FakeRunner {
        FakeRunner::new(move |sql, params| {
            let all = tables.get(sql).cloned().unwrap_or_default();
            if params.is_empty() || params.iter().any(|p| p.is_empty()) {
                return Ok(all);
            }
            Ok(all
                .into_iter()
                .filter(|row| {
                    row.get("mat_no")
                        .map(batch_key)
                        .is_some_and(|k| params.contains(&k))
                })
                .collect())
        })
    }

    #[tokio::test]
    async fn batched_and_non_batched_resolvers_produce_equivalent_documents() {
        for batch_by in [None, Some("mat_no")] {
            let query = fixture_query("sizes");
            let resolvers = resolvers(batch_by);
            let runner = runner_for(tables(
                &[json!({"mat_no": 1}), json!({"mat_no": 2})],
                &[
                    json!({"mat_no": 1, "size_code": "S"}),
                    json!({"mat_no": 2, "size_code": "M"}),
                    json!({"mat_no": 2, "size_code": "L"}),
                ],
            ));

            let doc = execute(&query, &HashMap::new(), &resolvers, &runner, 10)
                .await
                .unwrap();

            assert_eq!(
                doc,
                json!([
                    {"mat_no": 1, "sizes": [{"mat_no": 1, "size_code": "S"}]},
                    {"mat_no": 2, "sizes": [{"mat_no": 2, "size_code": "M"}, {"mat_no": 2, "size_code": "L"}]},
                ]),
                "batch_by = {batch_by:?}"
            );
        }
    }

    #[tokio::test]
    async fn missing_child_resolver_is_warned_not_fatal() {
        let mut query = fixture_query("sizes");
        query.selection.children[0].children.push(FieldSelection {
            field_name: "ghost".to_string(),
            children: vec![FieldSelection {
                field_name: "x".to_string(),
                children: Vec::new(),
                is_leaf: true,
            }],
            is_leaf: false,
        });

        let resolvers = resolvers(Some("mat_no"));
        let runner = runner_for(tables(
            &[json!({"mat_no": 1}), json!({"mat_no": 2})],
            &[
                json!({"mat_no": 1, "size_code": "S"}),
                json!({"mat_no": 2, "size_code": "M"}),
                json!({"mat_no": 2, "size_code": "L"}),
            ],
        ));

        let doc = execute(&query, &HashMap::new(), &resolvers, &runner, 10)
            .await
            .unwrap();

        assert_eq!(
            doc,
            json!([
                {"mat_no": 1, "sizes": [{"mat_no": 1, "size_code": "S"}]},
                {"mat_no": 2, "sizes": [{"mat_no": 2, "size_code": "M"}, {"mat_no": 2, "size_code": "L"}]},
            ]),
            "the unresolved 'ghost' field is omitted without failing the query"
        );
    }

    #[tokio::test]
    async fn single_root_row_is_shaped_as_object_with_to_one_folding() {
        let query = fixture_query("sizes");
        let resolvers = resolvers(None);
        let runner = runner_for(tables(
            &[json!({"mat_no": 1})],
            &[json!({"mat_no": 1, "size_code": "S"})],
        ));

        let doc = execute(&query, &HashMap::new(), &resolvers, &runner, 10)
            .await
            .unwrap();

        assert_eq!(
            doc,
            json!({"mat_no": 1, "sizes": {"mat_no": 1, "size_code": "S"}}),
            "a single root row returns an object and a single non-batched child is folded"
        );
    }
}
