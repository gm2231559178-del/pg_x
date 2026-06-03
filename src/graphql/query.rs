use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use super::schema::SchemaRegistry;

/// A loaded named query ready for execution.
/// The selection set is parsed into an execution plan at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedQuery {
    /// Query name (file stem)
    pub name: String,
    /// The root operation name (e.g. "MaterialFull")
    pub operation_name: String,
    /// Variable definitions (e.g. `$mat_no: String!`)
    pub variables: Vec<VariableDef>,
    /// The root selection set
    pub selection: FieldSelection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableDef {
    pub name: String,
    pub var_type: String,
    pub required: bool,
}

/// A field selection node in the execution plan tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSelection {
    pub field_name: String,
    /// If this field is a relation (has sub-fields), the sub-selections
    pub children: Vec<FieldSelection>,
    /// If true, this is a leaf (scalar) field
    pub is_leaf: bool,
}

/// Loader for named GraphQL query files.
pub struct QueryLoader {
    /// Queries keyed by query name (file stem)
    pub queries: HashMap<String, NamedQuery>,
}

impl QueryLoader {
    /// Load all `.graphql` query files from `~/.pgx/queries/`.
    pub fn load(schema: &SchemaRegistry) -> Result<Self> {
        let queries_dir = Self::queries_dir()?;
        let mut queries = HashMap::new();

        if !queries_dir.exists() {
            return Ok(Self { queries });
        }

        for entry in std::fs::read_dir(&queries_dir).context("Cannot read queries directory")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "graphql") {
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("Cannot read query file: {}", path.display()))?;
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let query = parse_query(&name, &content, schema)?;
                queries.insert(query.name.clone(), query);
            }
        }

        Ok(Self { queries })
    }

    fn queries_dir() -> Result<PathBuf> {
        let home = dirs::home_dir().context("Cannot determine home directory")?;
        Ok(home.join(".pgx").join("queries"))
    }

    pub fn get(&self, name: &str) -> Option<&NamedQuery> {
        self.queries.get(name)
    }
}

/// Parse a single GraphQL query document into a NamedQuery.
/// This handles the minimal subset: `query Name($var: Type!) { ... }`
fn parse_query(name: &str, content: &str, schema: &SchemaRegistry) -> Result<NamedQuery> {
    let content = content.trim();

    // Extract operation name and variable definitions
    let (operation_name, variables) = if let Some(after_query) = content.strip_prefix("query ") {
        let end_of_header = after_query
            .find('{')
            .context("Query must contain a selection set '{ ... }'")?;
        let header = &after_query[..end_of_header].trim();

        let (op_name, var_str) = if let Some(open_paren) = header.find('(') {
            let close_paren = header
                .rfind(')')
                .context("Unclosed parenthesis in query header")?;
            let name_part = &header[..open_paren].trim();
            let vars_part = &header[open_paren + 1..close_paren];
            (name_part.to_string(), Some(vars_part))
        } else {
            (header.to_string(), None)
        };

        let mut var_defs = Vec::new();
        if let Some(vars) = var_str {
            for var_def in vars.split(',') {
                let var_def = var_def.trim();
                if let Some(after_dollar) = var_def.strip_prefix('$') {
                    if let Some((var_name, var_type)) = after_dollar.split_once(':') {
                        let var_name = var_name.trim().to_string();
                        let var_type = var_type.trim().to_string();
                        let required = var_type.ends_with('!');
                        var_defs.push(VariableDef {
                            name: var_name,
                            var_type: var_type.trim_end_matches('!').to_string(),
                            required,
                        });
                    }
                }
            }
        }

        (op_name, var_defs)
    } else {
        ("Query".to_string(), Vec::new())
    };

    // Find the first selection set
    let selection_start = content
        .find('{')
        .context("Query must contain a selection set")?;
    let selection_content = &content[selection_start + 1..];
    let selection = parse_selection_set(selection_content, content, schema)?;

    Ok(NamedQuery {
        name: name.to_string(),
        operation_name,
        variables,
        selection,
    })
}

/// Parse a single selection set (content between { and }).
/// Returns the list of FieldSelection nodes.
fn parse_selection_set(
    content: &str,
    full_content: &str,
    schema: &SchemaRegistry,
) -> Result<FieldSelection> {
    // Dummy root to collect top-level fields
    let mut root = FieldSelection {
        field_name: "__root".to_string(),
        children: Vec::new(),
        is_leaf: false,
    };
    root.children = parse_fields(content, full_content, schema)?;
    Ok(root)
}

/// Parse fields from selection set content.
fn parse_fields(
    content: &str,
    full_content: &str,
    schema: &SchemaRegistry,
) -> Result<Vec<FieldSelection>> {
    let mut fields = Vec::new();
    let mut depth = 0;
    let mut current = String::new();

    for ch in content.chars() {
        match ch {
            '{' => {
                depth += 1;
                current.push(ch);
            }
            '}' => {
                if depth == 0 {
                    // We've reached the end of the current selection set
                    break;
                }
                depth -= 1;
                current.push(ch);
            }
            '\n' | '\r' => {
                if depth == 0 {
                    if let Some(field) = parse_field_line(current.trim(), full_content, schema)? {
                        fields.push(field);
                    }
                    current.clear();
                } else {
                    current.push('\n');
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    // Handle last field
    if depth == 0 && !current.trim().is_empty() {
        if let Some(field) = parse_field_line(current.trim(), full_content, schema)? {
            fields.push(field);
        }
    }

    Ok(fields)
}

/// Parse a single field line into a FieldSelection.
/// A line can be:
///   - `field_name` (scalar leaf)
///   - `relation_name { sub_field1 sub_field2 }` (relation with children)
fn parse_field_line(
    line: &str,
    _full_content: &str,
    _schema: &SchemaRegistry,
) -> Result<Option<FieldSelection>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Ok(None);
    }

    // Check if this is a relation with sub-fields
    if let Some(open_brace) = line.find('{') {
        let field_name = line[..open_brace].trim().to_string();
        let inner = &line[open_brace + 1..];
        // Find matching close brace
        let close_brace = inner.rfind('}').unwrap_or(inner.len());
        let child_content = &inner[..close_brace];

        let children = parse_fields(child_content.trim(), "", _schema)?;

        Ok(Some(FieldSelection {
            field_name,
            children,
            is_leaf: false,
        }))
    } else {
        // Simple scalar field (or alias, which we don't support yet)
        let field_name = line.to_string();
        Ok(Some(FieldSelection {
            field_name,
            children: Vec::new(),
            is_leaf: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let mut schema = SchemaRegistry::new();
        schema
            .parse_sdl("type Material { mat_no: String! name: String }")
            .unwrap();

        let content = r#"
query MaterialFull($mat_no: String!) {
  material(mat_no: $mat_no) {
    mat_no
    name
  }
}
"#;
        let query = parse_query("test", content, &schema).unwrap();
        assert_eq!(query.operation_name, "MaterialFull");
        assert_eq!(query.variables.len(), 1);
        assert_eq!(query.variables[0].name, "mat_no");
        assert!(query.variables[0].required);
    }

    #[test]
    fn test_parse_nested_query() {
        let mut schema = SchemaRegistry::new();
        schema.parse_sdl("type Material { mat_no: String! sizes: [Size!]! } type Size { size_code: String! }").unwrap();

        let content = r#"
query Full($mat_no: String!) {
  material(mat_no: $mat_no) {
    mat_no
    sizes { size_code }
  }
}
"#;
        let query = parse_query("test", content, &schema).unwrap();
        assert_eq!(query.selection.children.len(), 1);
        let mat = &query.selection.children[0];
        assert_eq!(mat.field_name, "material(mat_no: $mat_no)");
        assert_eq!(mat.children.len(), 2);
        assert!(mat.children[0].is_leaf); // mat_no
        assert!(!mat.children[1].is_leaf); // sizes
        assert_eq!(mat.children[1].children[0].field_name, "size_code");
    }
}
