use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;

use super::dataloader::DataLoader;
use super::pool::QueryPool;
use super::query::{FieldSelection, NamedQuery};
use super::row::row_to_json_value;
use crate::utils::config::ResolverConfig;

/// Execute a named query with given variables and return the assembled JSON document.
pub async fn execute(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
) -> Result<Value> {
    let root_selection = &query.selection;
    let root_fields = &root_selection.children;

    // Find the root resolver (top-level field like "material")
    let mut root_values = Vec::new();

    for field in root_fields {
        // Extract the field name without arguments: "material(mat_no: $mat_no)" -> "material"
        let field_name = field
            .field_name
            .split('(')
            .next()
            .unwrap_or(&field.field_name)
            .to_string();

        let resolver = resolvers
            .get(&field_name)
            .with_context(|| format!("No resolver configured for '{}'", field_name))?;

        // Bind root-level parameters from variables
        let param_value = if let Some(param_name) = &resolver.param {
            variables
                .get(param_name)
                .cloned()
                .or_else(|| {
                    // Try extracting from argument: e.g. material(mat_no: $mat_no)
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
        let rows = client
            .query(&resolver.sql, &[&param_value])
            .await
            .with_context(|| format!("Resolver SQL failed for '{}'", field_name))?;

        for row in &rows {
            let mut obj = row_to_json_value(row)?;
            // Resolve child fields
            if !field.children.is_empty() {
                resolve_children(&mut obj, &field.children, resolvers, pool).await?;
            }
            root_values.push(obj);
        }
    }

    // If the root has a single field, return the object(s) directly
    if root_values.len() == 1 {
        Ok(root_values.into_iter().next().unwrap())
    } else {
        Ok(Value::Array(root_values))
    }
}

/// Recursively resolve child fields for a parent row.
async fn resolve_children(
    parent_obj: &mut Value,
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
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

        // Extract param value from parent before any mutable borrow
        let param_name = resolver.param.as_deref().unwrap_or(field_name);
        let param_value = match parent_obj {
            Value::Object(ref m) => m.get(param_name).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };

        let client = pool.get().await?;
        let rows = client
            .query(&resolver.sql, &[&param_value])
            .await
            .with_context(|| format!("Child resolver SQL failed for '{}'", field_name))?;

        let mut children = Vec::new();
        for row in &rows {
            let mut child_obj = row_to_json_value(row)?;
            if !field.children.is_empty() {
                Box::pin(resolve_children(
                    &mut child_obj,
                    &field.children,
                    resolvers,
                    pool,
                ))
                .await?;
            }
            children.push(child_obj);
        }

        if let Value::Object(ref mut m) = parent_obj {
            m.insert(
                field_name.to_string(),
                if children.len() == 1 && !is_to_many(resolver) {
                    children.into_iter().next().unwrap()
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
#[allow(dead_code)]
pub async fn execute_batched(
    query: &NamedQuery,
    variables: &HashMap<String, Value>,
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
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
        let rows = client
            .query(&resolver.sql, &[&param_value])
            .await
            .with_context(|| format!("Resolver SQL failed for '{}'", field_name))?;

        for row in &rows {
            let obj = row_to_json_value(row)?;
            root_values.push(obj);
        }

        // Resolve child fields with batching using DataLoader
        if !field.children.is_empty() && root_values.len() > 1 {
            resolve_children_batched(&mut root_values, &field.children, resolvers, pool).await?;
        } else if !field.children.is_empty() {
            // Single root row — use direct resolver (no batching needed)
            for root_val in &mut root_values {
                resolve_children(root_val, &field.children, resolvers, pool).await?;
            }
        }
    }

    if root_values.len() == 1 {
        Ok(root_values.into_iter().next().unwrap())
    } else {
        Ok(Value::Array(root_values))
    }
}

/// Resolve child fields using DataLoader batching.
#[allow(dead_code)]
async fn resolve_children_batched(
    parent_objs: &mut [Value],
    child_fields: &[FieldSelection],
    resolvers: &HashMap<String, ResolverConfig>,
    pool: &QueryPool,
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
            let keys: Vec<String> = parent_objs
                .iter()
                .filter_map(|obj| {
                    obj.get(param_name)
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                })
                .collect();

            for key in &keys {
                loader.add_key(key.clone());
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
                        // Recursively resolve nested children (single-row path)
                        resolve_children(&mut child, &field.children, resolvers, pool).await?;
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
                let rows = client
                    .query(&resolver.sql, &[&param_value])
                    .await
                    .with_context(|| format!("Child resolver SQL failed for '{}'", field_name))?;

                let mut children = Vec::new();
                for row in &rows {
                    let mut child_obj = row_to_json_value(row)?;
                    if !field.children.is_empty() {
                        resolve_children(&mut child_obj, &field.children, resolvers, pool).await?;
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
