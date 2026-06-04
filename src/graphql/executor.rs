use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;

use super::dataloader::DataLoader;
use super::pool::QueryPool;
use super::query::{FieldSelection, NamedQuery};
use super::row::row_to_json_value;
use crate::utils::config::ResolverConfig;

/// Convert a serde_json::Value to a string suitable for SQL $1 binding.
fn value_to_param(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Execute a named query with given variables and return the assembled JSON document.
/// Uses DataLoader batching for child resolvers when `batch_by` is configured.
pub async fn execute(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
    max_depth: u32,
) -> Result<Value> {
    execute_batched(query, variables, resolvers, pool, max_depth).await
}

/// Recursively resolve child fields for a parent row.
/// `depth` tracks the recursion level to prevent stack overflow on circular references.
async fn resolve_children(
    parent_obj: &mut Value,
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
    max_depth: u32,
) -> Result<()> {
    resolve_children_with_depth(parent_obj, child_fields, resolvers, pool, max_depth, 0).await
}

async fn resolve_children_with_depth(
    parent_obj: &mut Value,
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
    max_depth: u32,
    depth: u32,
) -> Result<()> {
    if depth > max_depth {
        anyhow::bail!(
            "Max resolver recursion depth ({}) exceeded — possible circular reference",
            max_depth
        );
    }

    for field in child_fields {
        let field_name = field
            .field_name
            .split('(')
            .next()
            .unwrap_or(&field.field_name);

        let resolver = match resolvers.get(field_name) {
            Some(r) => r,
            None => {
                if field.is_leaf {
                    continue;
                }
                tracing::warn!("No resolver for child field '{}'", field_name);
                continue;
            }
        };

        if field.is_leaf || field.children.is_empty() {
            continue;
        }

        let param_name = resolver.param.as_deref().unwrap_or(field_name);
        let param_value = match parent_obj {
            Value::Object(ref m) => m.get(param_name).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };

        let client = pool.get().await?;
        let param_str = value_to_param(&param_value);
        // Single-string param — wrap in vec for ANY($1) compatibility
        let param_vec = vec![param_str];
        let rows = client
            .query(&resolver.sql, &[&param_vec])
            .await
            .with_context(|| format!("Child resolver SQL failed for '{}'", field_name))?;

        let mut children = Vec::new();
        for row in &rows {
            let mut child_obj = row_to_json_value(row)?;
            if !field.children.is_empty() {
                Box::pin(resolve_children_with_depth(
                    &mut child_obj,
                    &field.children,
                    resolvers,
                    pool,
                    max_depth,
                    depth + 1,
                ))
                .await?;
            }
            children.push(child_obj);
        }

        if let Value::Object(ref mut m) = parent_obj {
            m.insert(
                field_name.to_string(),
                if children.len() == 1 && !is_to_many(resolver) {
                    let val = children.into_iter().next()
                        .ok_or_else(|| anyhow::anyhow!("expected 1 child, got 0 for field '{}'", field_name))?;
                    val
                } else {
                    Value::Array(children)
                },
            );
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

/// Execute a named query with DataLoader batching for child resolvers.
async fn execute_batched(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
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

        let client = pool.get().await?;
        let param_str = value_to_param(&param_value);
        let rows = client
            .query(&resolver.sql, &[&param_str])
            .await
            .with_context(|| format!("Resolver SQL failed for '{}'", field_name))?;

        for row in &rows {
            let obj = row_to_json_value(row)?;
            root_values.push(obj);
        }

        // Resolve child fields with batching using DataLoader
        if !field.children.is_empty() && root_values.len() > 1 {
            resolve_children_batched(
                &mut root_values,
                &field.children,
                resolvers,
                pool,
                max_depth,
            )
            .await?;
        } else if !field.children.is_empty() {
            // Single root row — use direct resolver (no batching needed)
            for root_val in &mut root_values {
                resolve_children(root_val, &field.children, resolvers, pool, max_depth).await?;
            }
        }
    }

    if root_values.len() == 1 {
        Ok(root_values.into_iter().next()
            .ok_or_else(|| anyhow::anyhow!("expected 1 root value, got 0"))?)
    } else {
        Ok(Value::Array(root_values))
    }
}

/// Resolve child fields using DataLoader batching.
async fn resolve_children_batched(
    parent_objs: &mut [Value],
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
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
                if field.is_leaf {
                    continue;
                }
                tracing::warn!("No resolver for child field '{}'", field_name);
                continue;
            }
        };

        if field.is_leaf || field.children.is_empty() {
            continue;
        }

        // If resolver has batch_by, use DataLoader
        if let Some(batch_by) = &resolver.batch_by {
            let param_name = resolver.param.as_deref().unwrap_or(field_name);
            let mut loader = DataLoader::new(&resolver.sql, batch_by);

            // Collect keys from parent objects
            for obj in parent_objs.iter() {
                if let Some(key) = obj.get(param_name) {
                    loader.add_key(key);
                }
            }

            loader.execute(pool).await?;

            // Assign children to each parent
            for obj in parent_objs.iter_mut() {
                let key = obj
                    .get(param_name)
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let children = key
                    .as_ref()
                    .map(|k| loader.get_children(k))
                    .unwrap_or_default();

                let mut resolved_children = Vec::new();
                for mut child in children {
                    if !field.children.is_empty() {
                        resolve_children_with_depth(
                            &mut child,
                            &field.children,
                            resolvers,
                            pool,
                            max_depth,
                            0,
                        )
                        .await?;
                    }
                    resolved_children.push(child);
                }

                if let Value::Object(ref mut m) = obj {
                    m.insert(field_name.to_string(), Value::Array(resolved_children));
                }
            }
        } else {
            // No batching — use single-param resolution per parent
            let param_name = resolver.param.as_deref().unwrap_or(field_name);
            for obj in parent_objs.iter_mut() {
                let param_value = obj.get(param_name).cloned().unwrap_or(Value::Null);

                let client = pool.get().await?;
                let param_str = value_to_param(&param_value);
                let param_vec = vec![param_str];
                let rows = client
                    .query(&resolver.sql, &[&param_vec])
                    .await
                    .with_context(|| format!("Child resolver SQL failed for '{}'", field_name))?;

                let mut children = Vec::new();
                for row in &rows {
                    let mut child_obj = row_to_json_value(row)?;
                    if !field.children.is_empty() {
                        resolve_children_with_depth(
                            &mut child_obj,
                            &field.children,
                            resolvers,
                            pool,
                            max_depth,
                            0,
                        )
                        .await?;
                    }
                    children.push(child_obj);
                }

                if let Value::Object(ref mut m) = obj {
                    m.insert(field_name.to_string(), Value::Array(children));
                }
            }
        }
    }

    Ok(())
}
