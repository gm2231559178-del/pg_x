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

        while let Some(line) = lines.peek() {
            let trimmed = line.trim();
            if trimmed.starts_with("type ") && trimmed.ends_with('{') {
                let type_name = trimmed
                    .strip_prefix("type ")
                    .and_then(|s| s.strip_suffix('{'))
                    .map(|s| s.trim())
                    .context("Invalid type definition syntax")?;
                lines.next(); // consume type line

                let mut fields = Vec::new();
                let mut relations = Vec::new();

                loop {
                    match lines.next() {
                        None => break,
                        Some(l) => {
                            let t = l.trim();
                            if t == "}" || t.starts_with("type ") {
                                break;
                            }
                            if t.is_empty() || t.starts_with('#') {
                                continue;
                            }
                            if let Some((name, type_str)) = t.split_once(':') {
                                let name = name.trim().to_string();
                                let type_str = type_str.trim().trim_end_matches('!');

                                if type_str.starts_with('[') {
                                    // list relation: [Type!]
                                    let inner = type_str
                                        .trim_start_matches('[')
                                        .trim_end_matches(']')
                                        .trim_end_matches('!');
                                    relations.push(RelationDef {
                                        name,
                                        type_name: inner.to_string(),
                                        is_list: true,
                                    });
                                } else if Self::is_scalar(type_str) {
                                    fields.push(FieldDef {
                                        name,
                                        scalar_type: Self::scalar_kind(type_str),
                                    });
                                } else {
                                    // single-object relation (non-list)
                                    relations.push(RelationDef {
                                        name,
                                        type_name: type_str.to_string(),
                                        is_list: false,
                                    });
                                }
                            }
                        }
                    }
                }

                self.types.insert(
                    type_name.to_string(),
                    ObjectType {
                        name: type_name.to_string(),
                        fields,
                        relations,
                    },
                );
            } else if trimmed.starts_with("type ") {
                // one-liner: type Foo { ... } without newline before {
                // Not supporting this for simplicity; skip
                lines.next();
            } else {
                lines.next();
            }
        }

        Ok(())
    }

    fn is_scalar(s: &str) -> bool {
        matches!(
            s,
            "String"
                | "Int"
                | "Float"
                | "Boolean"
                | "ID"
                | "String!"
                | "Int!"
                | "Float!"
                | "Boolean!"
                | "ID!"
        )
    }

    fn scalar_kind(s: &str) -> ScalarType {
        match s.trim_end_matches('!') {
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

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
