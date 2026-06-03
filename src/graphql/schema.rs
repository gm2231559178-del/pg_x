use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A parsed GraphQL type system representation.
/// This is NOT a full GraphQL schema parser — it handles the subset needed
/// for document composition: object types with scalar fields and list relations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistry {
    /// Object types keyed by name (e.g. "Material", "Size", "Colorway")
    pub types: HashMap<String, ObjectType>,
}

/// An object type or a list-of-objects relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectType {
    pub name: String,
    /// Scalar fields (leaf values)
    pub fields: Vec<FieldDef>,
    /// Relations to other object types (non-leaf selections)
    pub relations: Vec<RelationDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub scalar_type: ScalarType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    Int,
    Float,
    Boolean,
    ID,
}

/// A relation field that references another object type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationDef {
    pub name: String,
    /// The type name this relation resolves to (must exist in the registry)
    pub type_name: String,
    /// Whether this is a list (to-many) relation
    pub is_list: bool,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    /// Parse all `.graphql` type definition files from a directory.
    /// Each file is expected to contain type definitions like:
    ///
    /// ```graphql
    /// type Material {
    ///   mat_no: String!
    ///   name: String!
    ///   status: String
    ///   sizes: [Size!]!
    ///   colorways: [Colorway!]!
    /// }
    /// ```
    pub fn load_from_dir(dir: &Path) -> Result<Self> {
        let mut registry = SchemaRegistry::new();

        if !dir.exists() {
            return Ok(registry);
        }

        for entry in std::fs::read_dir(dir).context("Cannot read schema directory")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "graphql") {
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("Cannot read schema file: {}", path.display()))?;
                registry.parse_sdl(&content)?;
            }
        }

        Ok(registry)
    }

    /// Parse inline SDL (Schema Definition Language) content.
    /// This is a minimal parser that handles `type Name { field: Type }` blocks.
    pub fn parse_sdl(&mut self, sdl: &str) -> Result<()> {
        let mut lines = sdl.lines().peekable();
        // Two-pass: first collect all type names, then parse and cross-validate
        let mut parsed_types: Vec<(String, Vec<FieldDef>, Vec<RelationDef>)> = Vec::new();
        let mut type_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Pass 1: collect type names for forward references
        let raw = sdl.to_string();
        for mut rest in raw.lines().map(|l| l.trim()) {
            loop {
                rest = rest.trim_start();
                if let Some(after_type) = rest.strip_prefix("type ") {
                    if let Some(name) = after_type.split('{').next() {
                        type_names.insert(name.trim().to_string());
                    }
                    if let Some(close_brace) = after_type.find('}') {
                        rest = &after_type[close_brace + 1..];
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        // Pass 2: parse type definitions
        while let Some(line) = lines.peek() {
            let trimmed = line.trim();
            if trimmed.starts_with("type ") {
                let type_name = trimmed
                    .strip_prefix("type ")
                    .and_then(|s| s.split('{').next())
                    .map(|s| s.trim())
                    .context("Invalid type definition syntax")?;

                // Check for inline open brace: type Foo { ... }
                if trimmed.contains('{') && !trimmed.ends_with('{') {
                    // inline form: type Material { mat_no: String! }
                    let body_start = trimmed.find('{').unwrap();
                    let after_brace = &trimmed[body_start + 1..];
                    // Walk character by character to find matching close brace
                    let mut depth = 1i32;
                    let mut close_pos = None;
                    for (i, ch) in after_brace.char_indices() {
                        match ch {
                            '{' => depth += 1,
                            '}' => {
                                depth -= 1;
                                if depth == 0 {
                                    close_pos = Some(i);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    let body_end = close_pos.unwrap_or(after_brace.len());
                    let body = &after_brace[..body_end];
                    let (fields, relations) = parse_fields_and_relations(body, &type_names)?;
                    parsed_types.push((type_name.to_string(), fields, relations));
                    lines.next();
                    continue;
                }

                lines.next(); // consume "type Name {"

                let mut body = String::new();
                let mut brace_depth = 1;
                for l in &mut lines {
                    let t = l.trim();
                    if t.contains('{') {
                        brace_depth += 1;
                    }
                    if t.contains('}') {
                        brace_depth -= 1;
                        if brace_depth == 0 {
                            break;
                        }
                    }
                    body.push_str(l);
                    body.push('\n');
                }

                let (fields, relations) = parse_fields_and_relations(&body, &type_names)?;
                parsed_types.push((type_name.to_string(), fields, relations));
            } else {
                lines.next();
            }
        }

        // Insert all parsed types into the registry
        for (name, fields, relations) in parsed_types {
            self.types.insert(
                name.clone(),
                ObjectType {
                    name,
                    fields,
                    relations,
                },
            );
        }

        Ok(())
    }

    fn is_scalar(s: &str) -> bool {
        matches!(s, "String" | "Int" | "Float" | "Boolean" | "ID")
    }

    fn scalar_kind(s: &str) -> ScalarType {
        match s {
            "String" | "ID" => ScalarType::String,
            "Int" => ScalarType::Int,
            "Float" => ScalarType::Float,
            "Boolean" => ScalarType::Boolean,
            _ => ScalarType::String,
        }
    }

    #[allow(dead_code)]
    pub fn get_type(&self, name: &str) -> Option<&ObjectType> {
        self.types.get(name)
    }

    #[allow(dead_code)]
    pub fn has_type(&self, name: &str) -> bool {
        self.types.contains_key(name)
    }
}

/// Parse field and relation definitions from a type body (content between braces).
fn parse_fields_and_relations(
    body: &str,
    type_names: &std::collections::HashSet<String>,
) -> Result<(Vec<FieldDef>, Vec<RelationDef>)> {
    let mut fields = Vec::new();
    let mut relations = Vec::new();

    for line in body.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') || t == "}" || t.starts_with("type ") {
            continue;
        }
        // Split into individual field:type tokens handling space-separated fields on one line.
        // Tokenize by finding patterns: `name: type` separated by whitespace.
        let tokens = tokenize_fields(t);
        for token in tokens {
            if let Some((name, type_str)) = token.split_once(':') {
                let name = name.trim().to_string();
                let raw_type = type_str.trim();
                let base_type = raw_type.trim_end_matches('!');

                if base_type.starts_with('[') {
                    let inner = base_type
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .trim_end_matches('!');
                    if !SchemaRegistry::is_scalar(inner) && !type_names.contains(inner) {
                        anyhow::bail!(
                            "Type '{}' referenced in list field '{}' is not defined",
                            inner,
                            name
                        );
                    }
                    if SchemaRegistry::is_scalar(inner) {
                        fields.push(FieldDef {
                            name,
                            scalar_type: SchemaRegistry::scalar_kind(inner),
                        });
                    } else {
                        relations.push(RelationDef {
                            name,
                            type_name: inner.to_string(),
                            is_list: true,
                        });
                    }
                } else if SchemaRegistry::is_scalar(base_type) {
                    fields.push(FieldDef {
                        name,
                        scalar_type: SchemaRegistry::scalar_kind(base_type),
                    });
                } else if type_names.contains(base_type) {
                    relations.push(RelationDef {
                        name,
                        type_name: base_type.to_string(),
                        is_list: false,
                    });
                } else {
                    anyhow::bail!(
                        "Unknown type '{}' for field '{}' — not a scalar or defined type",
                        base_type,
                        name
                    );
                }
            }
        }
    }

    Ok((fields, relations))
}

/// Split a line of field definitions into individual `name: type` tokens.
/// Handles both single-field-per-line and multi-field inline definitions.
fn tokenize_fields(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_bracket = false;

    for ch in line.chars() {
        match ch {
            '[' => {
                in_bracket = true;
                current.push(ch);
            }
            ']' => {
                in_bracket = false;
                current.push(ch);
            }
            ' ' if !in_bracket => {
                if !current.trim().is_empty() {
                    tokens.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.trim().is_empty() {
        tokens.push(current.trim().to_string());
    }

    // Rejoin tokens that belong together (name: type)
    let mut merged = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if i + 1 < tokens.len() && tokens[i].ends_with(':') {
            // name: is followed by its type on the next token
            merged.push(format!("{} {}", tokens[i], tokens[i + 1]));
            i += 2;
        } else if tokens[i].contains(':') {
            // Already has colon (name:Type without space)
            merged.push(tokens[i].clone());
            i += 1;
        } else {
            i += 1;
        }
    }

    merged
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
