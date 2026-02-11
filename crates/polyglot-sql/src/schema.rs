//! Schema management for SQL queries
//!
//! This module provides functionality for:
//! - Representing database schemas (tables, columns, types)
//! - Looking up column types for query optimization
//! - Normalizing identifiers per dialect
//!
//! Based on the Python implementation in `sqlglot/schema.py`.

use crate::dialects::DialectType;
use crate::expressions::DataType;
use crate::trie::{Trie, TrieResult};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Errors that can occur during schema operations
#[derive(Debug, Error, Clone)]
pub enum SchemaError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Ambiguous table: {table} matches multiple tables: {matches}")]
    AmbiguousTable { table: String, matches: String },

    #[error("Column not found: {column} in table {table}")]
    ColumnNotFound { table: String, column: String },

    #[error("Schema nesting depth mismatch: expected {expected}, got {actual}")]
    DepthMismatch { expected: usize, actual: usize },

    #[error("Invalid schema structure: {0}")]
    InvalidStructure(String),
}

/// Result type for schema operations
pub type SchemaResult<T> = Result<T, SchemaError>;

/// Supported table argument names
pub const TABLE_PARTS: &[&str] = &["this", "db", "catalog"];

/// Abstract trait for database schemas
pub trait Schema {
    /// Get the dialect associated with this schema (if any)
    fn dialect(&self) -> Option<DialectType>;

    /// Add or update a table in the schema
    fn add_table(
        &mut self,
        table: &str,
        columns: &[(String, DataType)],
        dialect: Option<DialectType>,
    ) -> SchemaResult<()>;

    /// Get column names for a table
    fn column_names(&self, table: &str) -> SchemaResult<Vec<String>>;

    /// Get the type of a column in a table
    fn get_column_type(&self, table: &str, column: &str) -> SchemaResult<DataType>;

    /// Check if a column exists in a table
    fn has_column(&self, table: &str, column: &str) -> bool;

    /// Get supported table argument levels
    fn supported_table_args(&self) -> &[&str];

    /// Check if the schema is empty
    fn is_empty(&self) -> bool;

    /// Get the nesting depth of the schema
    fn depth(&self) -> usize;
}

/// A column with its type and visibility
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub data_type: DataType,
    pub visible: bool,
}

impl ColumnInfo {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            visible: true,
        }
    }

    pub fn with_visibility(data_type: DataType, visible: bool) -> Self {
        Self { data_type, visible }
    }
}

/// A mapping-based schema implementation
///
/// Supports nested schemas with different levels:
/// - Level 1: `{table: {col: type}}`
/// - Level 2: `{db: {table: {col: type}}}`
/// - Level 3: `{catalog: {db: {table: {col: type}}}}`
#[derive(Debug, Clone)]
pub struct MappingSchema {
    /// The actual schema data
    mapping: HashMap<String, SchemaNode>,
    /// Trie for efficient table lookup
    mapping_trie: Trie<()>,
    /// The dialect for this schema
    dialect: Option<DialectType>,
    /// Whether to normalize identifiers
    normalize: bool,
    /// Visible columns per table
    visible: HashMap<String, HashSet<String>>,
    /// Cached depth
    cached_depth: usize,
}

/// A node in the schema tree
#[derive(Debug, Clone)]
pub enum SchemaNode {
    /// Intermediate node (database or catalog)
    Namespace(HashMap<String, SchemaNode>),
    /// Leaf node (table with columns)
    Table(HashMap<String, ColumnInfo>),
}

impl Default for MappingSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl MappingSchema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
            mapping_trie: Trie::new(),
            dialect: None,
            normalize: true,
            visible: HashMap::new(),
            cached_depth: 0,
        }
    }

    /// Create a schema with a specific dialect
    pub fn with_dialect(dialect: DialectType) -> Self {
        Self {
            dialect: Some(dialect),
            ..Self::new()
        }
    }

    /// Create a schema with normalization disabled
    pub fn without_normalization(mut self) -> Self {
        self.normalize = false;
        self
    }

    /// Set visibility for columns in a table
    pub fn set_visible_columns(&mut self, table: &str, columns: &[&str]) {
        let key = self.normalize_name(table, true);
        let cols: HashSet<String> = columns
            .iter()
            .map(|c| self.normalize_name(c, false))
            .collect();
        self.visible.insert(key, cols);
    }

    /// Normalize an identifier name based on dialect
    fn normalize_name(&self, name: &str, is_table: bool) -> String {
        if !self.normalize {
            return name.to_string();
        }

        // Default normalization: lowercase
        // Different dialects may have different rules
        match self.dialect {
            Some(DialectType::BigQuery) if is_table => {
                // BigQuery preserves case for tables
                name.to_string()
            }
            Some(DialectType::Snowflake) => {
                // Snowflake uppercases by default
                name.to_uppercase()
            }
            _ => {
                // Most dialects lowercase
                name.to_lowercase()
            }
        }
    }

    /// Parse a qualified table name into parts
    fn parse_table_parts(&self, table: &str) -> Vec<String> {
        table
            .split('.')
            .map(|s| self.normalize_name(s.trim(), true))
            .collect()
    }

    /// Get the column mapping for a table
    fn find_table(&self, table: &str) -> SchemaResult<&HashMap<String, ColumnInfo>> {
        let parts = self.parse_table_parts(table);

        // Use trie to find table
        let reversed_parts: Vec<_> = parts.iter().rev().map(|s| s.as_str()).collect();
        let key: String = reversed_parts.join(".");

        let (result, _) = self.mapping_trie.in_trie(&key);

        match result {
            TrieResult::Failed => Err(SchemaError::TableNotFound(table.to_string())),
            TrieResult::Prefix => {
                // Ambiguous - multiple tables match
                Err(SchemaError::AmbiguousTable {
                    table: table.to_string(),
                    matches: "multiple matches".to_string(),
                })
            }
            TrieResult::Exists => {
                // Navigate to the table
                self.navigate_to_table(&parts)
            }
        }
    }

    /// Navigate the schema tree to find a table's columns
    fn navigate_to_table(&self, parts: &[String]) -> SchemaResult<&HashMap<String, ColumnInfo>> {
        let mut current = &self.mapping;

        for (i, part) in parts.iter().enumerate() {
            match current.get(part) {
                Some(SchemaNode::Namespace(inner)) => {
                    current = inner;
                }
                Some(SchemaNode::Table(cols)) => {
                    if i == parts.len() - 1 {
                        return Ok(cols);
                    } else {
                        return Err(SchemaError::InvalidStructure(format!(
                            "Found table at {} but expected more levels",
                            parts[..=i].join(".")
                        )));
                    }
                }
                None => {
                    return Err(SchemaError::TableNotFound(parts.join(".")));
                }
            }
        }

        // We've exhausted parts but didn't find a table
        Err(SchemaError::TableNotFound(parts.join(".")))
    }

    /// Add a table to the schema
    fn add_table_internal(
        &mut self,
        parts: &[String],
        columns: HashMap<String, ColumnInfo>,
    ) -> SchemaResult<()> {
        if parts.is_empty() {
            return Err(SchemaError::InvalidStructure(
                "Table name cannot be empty".to_string(),
            ));
        }

        // Build trie key (reversed parts)
        let trie_key: String = parts.iter().rev().cloned().collect::<Vec<_>>().join(".");
        self.mapping_trie.insert(&trie_key, ());

        // Navigate/create path to table
        let mut current = &mut self.mapping;

        for (i, part) in parts.iter().enumerate() {
            let is_last = i == parts.len() - 1;

            if is_last {
                // Insert table
                current.insert(part.clone(), SchemaNode::Table(columns));
                return Ok(());
            } else {
                // Navigate or create namespace
                let entry = current
                    .entry(part.clone())
                    .or_insert_with(|| SchemaNode::Namespace(HashMap::new()));

                match entry {
                    SchemaNode::Namespace(inner) => {
                        current = inner;
                    }
                    SchemaNode::Table(_) => {
                        return Err(SchemaError::InvalidStructure(format!(
                            "Expected namespace at {} but found table",
                            parts[..=i].join(".")
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Update cached depth
    fn update_depth(&mut self) {
        self.cached_depth = self.calculate_depth(&self.mapping);
    }

    fn calculate_depth(&self, mapping: &HashMap<String, SchemaNode>) -> usize {
        if mapping.is_empty() {
            return 0;
        }

        let mut max_depth = 1;
        for node in mapping.values() {
            match node {
                SchemaNode::Namespace(inner) => {
                    let d = 1 + self.calculate_depth(inner);
                    if d > max_depth {
                        max_depth = d;
                    }
                }
                SchemaNode::Table(_) => {
                    // Tables don't add to depth beyond their level
                }
            }
        }
        max_depth
    }
}

impl Schema for MappingSchema {
    fn dialect(&self) -> Option<DialectType> {
        self.dialect
    }

    fn add_table(
        &mut self,
        table: &str,
        columns: &[(String, DataType)],
        _dialect: Option<DialectType>,
    ) -> SchemaResult<()> {
        let parts = self.parse_table_parts(table);

        let cols: HashMap<String, ColumnInfo> = columns
            .iter()
            .map(|(name, dtype)| {
                let normalized_name = self.normalize_name(name, false);
                (normalized_name, ColumnInfo::new(dtype.clone()))
            })
            .collect();

        self.add_table_internal(&parts, cols)?;
        self.update_depth();
        Ok(())
    }

    fn column_names(&self, table: &str) -> SchemaResult<Vec<String>> {
        let cols = self.find_table(table)?;
        let table_key = self.normalize_name(table, true);

        // Check visibility
        if let Some(visible_cols) = self.visible.get(&table_key) {
            Ok(cols
                .keys()
                .filter(|k| visible_cols.contains(*k))
                .cloned()
                .collect())
        } else {
            Ok(cols.keys().cloned().collect())
        }
    }

    fn get_column_type(&self, table: &str, column: &str) -> SchemaResult<DataType> {
        let cols = self.find_table(table)?;
        let normalized_col = self.normalize_name(column, false);

        cols.get(&normalized_col)
            .map(|info| info.data_type.clone())
            .ok_or_else(|| SchemaError::ColumnNotFound {
                table: table.to_string(),
                column: column.to_string(),
            })
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.get_column_type(table, column).is_ok()
    }

    fn supported_table_args(&self) -> &[&str] {
        let depth = self.depth();
        if depth == 0 {
            &[]
        } else if depth <= 3 {
            &TABLE_PARTS[..depth]
        } else {
            TABLE_PARTS
        }
    }

    fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    fn depth(&self) -> usize {
        self.cached_depth
    }
}

/// Normalize a table or column name according to dialect rules
pub fn normalize_name(
    name: &str,
    dialect: Option<DialectType>,
    is_table: bool,
    normalize: bool,
) -> String {
    if !normalize {
        return name.to_string();
    }

    match dialect {
        Some(DialectType::BigQuery) if is_table => name.to_string(),
        Some(DialectType::Snowflake) => name.to_uppercase(),
        _ => name.to_lowercase(),
    }
}

/// Ensure we have a schema instance
pub fn ensure_schema(schema: Option<MappingSchema>) -> MappingSchema {
    schema.unwrap_or_default()
}

/// Helper to build a schema from a simple map
///
/// # Example
///
/// ```
/// use polyglot_sql::schema::{MappingSchema, Schema, from_simple_map};
/// use polyglot_sql::expressions::DataType;
///
/// let schema = from_simple_map(&[
///     ("users", &[("id", DataType::Int { length: None, integer_spelling: false }), ("name", DataType::VarChar { length: Some(255), parenthesized_length: false })]),
///     ("orders", &[("id", DataType::Int { length: None, integer_spelling: false }), ("user_id", DataType::Int { length: None, integer_spelling: false })]),
/// ]);
///
/// assert_eq!(schema.column_names("users").unwrap().len(), 2);
/// ```
pub fn from_simple_map(tables: &[(&str, &[(&str, DataType)])]) -> MappingSchema {
    let mut schema = MappingSchema::new();

    for (table_name, columns) in tables {
        let cols: Vec<(String, DataType)> = columns
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect();

        schema.add_table(table_name, &cols, None).ok();
    }

    schema
}

/// Flatten a nested schema to get all table paths
pub fn flatten_schema_paths(schema: &MappingSchema) -> Vec<Vec<String>> {
    let mut paths = Vec::new();
    flatten_schema_paths_recursive(&schema.mapping, Vec::new(), &mut paths);
    paths
}

fn flatten_schema_paths_recursive(
    mapping: &HashMap<String, SchemaNode>,
    prefix: Vec<String>,
    paths: &mut Vec<Vec<String>>,
) {
    for (key, node) in mapping {
        let mut path = prefix.clone();
        path.push(key.clone());

        match node {
            SchemaNode::Namespace(inner) => {
                flatten_schema_paths_recursive(inner, path, paths);
            }
            SchemaNode::Table(_) => {
                paths.push(path);
            }
        }
    }
}

/// Set a value in a nested dictionary-like structure
pub fn nested_set<V: Clone>(
    map: &mut HashMap<String, HashMap<String, V>>,
    keys: &[String],
    value: V,
) {
    if keys.is_empty() {
        return;
    }

    if keys.len() == 1 {
        // Can't set at single level - need at least 2 keys
        return;
    }

    let outer_key = &keys[0];
    let inner_key = &keys[1];

    map.entry(outer_key.clone())
        .or_insert_with(HashMap::new)
        .insert(inner_key.clone(), value);
}

/// Get a value from a nested dictionary-like structure
pub fn nested_get<'a, V>(
    map: &'a HashMap<String, HashMap<String, V>>,
    keys: &[String],
) -> Option<&'a V> {
    if keys.len() != 2 {
        return None;
    }

    map.get(&keys[0])?.get(&keys[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_schema() {
        let schema = MappingSchema::new();
        assert!(schema.is_empty());
        assert_eq!(schema.depth(), 0);
    }

    #[test]
    fn test_add_table() {
        let mut schema = MappingSchema::new();
        let columns = vec![
            ("id".to_string(), DataType::Int { length: None, integer_spelling: false }),
            (
                "name".to_string(),
                DataType::VarChar { length: Some(255), parenthesized_length: false },
            ),
        ];

        schema.add_table("users", &columns, None).unwrap();

        assert!(!schema.is_empty());
        assert_eq!(schema.depth(), 1);
        assert!(schema.has_column("users", "id"));
        assert!(schema.has_column("users", "name"));
        assert!(!schema.has_column("users", "email"));
    }

    #[test]
    fn test_qualified_table_names() {
        let mut schema = MappingSchema::new();
        let columns = vec![("id".to_string(), DataType::Int { length: None, integer_spelling: false })];

        schema.add_table("mydb.users", &columns, None).unwrap();

        assert!(schema.has_column("mydb.users", "id"));
        assert_eq!(schema.depth(), 2);
    }

    #[test]
    fn test_catalog_db_table() {
        let mut schema = MappingSchema::new();
        let columns = vec![("id".to_string(), DataType::Int { length: None, integer_spelling: false })];

        schema
            .add_table("catalog.mydb.users", &columns, None)
            .unwrap();

        assert!(schema.has_column("catalog.mydb.users", "id"));
        assert_eq!(schema.depth(), 3);
    }

    #[test]
    fn test_get_column_type() {
        let mut schema = MappingSchema::new();
        let columns = vec![
            ("id".to_string(), DataType::Int { length: None, integer_spelling: false }),
            (
                "name".to_string(),
                DataType::VarChar { length: Some(255), parenthesized_length: false },
            ),
        ];

        schema.add_table("users", &columns, None).unwrap();

        let id_type = schema.get_column_type("users", "id").unwrap();
        assert!(matches!(id_type, DataType::Int { .. }));

        let name_type = schema.get_column_type("users", "name").unwrap();
        assert!(matches!(name_type, DataType::VarChar { length: Some(255), parenthesized_length: false }));
    }

    #[test]
    fn test_column_names() {
        let mut schema = MappingSchema::new();
        let columns = vec![
            ("id".to_string(), DataType::Int { length: None, integer_spelling: false }),
            ("name".to_string(), DataType::VarChar { length: None, parenthesized_length: false }),
        ];

        schema.add_table("users", &columns, None).unwrap();

        let names = schema.column_names("users").unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
    }

    #[test]
    fn test_table_not_found() {
        let schema = MappingSchema::new();
        let result = schema.column_names("nonexistent");
        assert!(matches!(result, Err(SchemaError::TableNotFound(_))));
    }

    #[test]
    fn test_column_not_found() {
        let mut schema = MappingSchema::new();
        let columns = vec![("id".to_string(), DataType::Int { length: None, integer_spelling: false })];
        schema.add_table("users", &columns, None).unwrap();

        let result = schema.get_column_type("users", "nonexistent");
        assert!(matches!(result, Err(SchemaError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_normalize_name_default() {
        let name = normalize_name("MyTable", None, true, true);
        assert_eq!(name, "mytable");
    }

    #[test]
    fn test_normalize_name_snowflake() {
        let name = normalize_name("MyTable", Some(DialectType::Snowflake), true, true);
        assert_eq!(name, "MYTABLE");
    }

    #[test]
    fn test_normalize_disabled() {
        let name = normalize_name("MyTable", None, true, false);
        assert_eq!(name, "MyTable");
    }

    #[test]
    fn test_from_simple_map() {
        let schema = from_simple_map(&[
            (
                "users",
                &[
                    ("id", DataType::Int { length: None, integer_spelling: false }),
                    ("name", DataType::VarChar { length: None, parenthesized_length: false }),
                ],
            ),
            (
                "orders",
                &[
                    ("id", DataType::Int { length: None, integer_spelling: false }),
                    ("user_id", DataType::Int { length: None, integer_spelling: false }),
                ],
            ),
        ]);

        assert!(schema.has_column("users", "id"));
        assert!(schema.has_column("users", "name"));
        assert!(schema.has_column("orders", "id"));
        assert!(schema.has_column("orders", "user_id"));
    }

    #[test]
    fn test_flatten_schema_paths() {
        let mut schema = MappingSchema::new();
        schema
            .add_table("db1.table1", &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })], None)
            .unwrap();
        schema
            .add_table("db1.table2", &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })], None)
            .unwrap();
        schema
            .add_table("db2.table1", &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })], None)
            .unwrap();

        let paths = flatten_schema_paths(&schema);
        assert_eq!(paths.len(), 3);
    }

    #[test]
    fn test_visible_columns() {
        let mut schema = MappingSchema::new();
        let columns = vec![
            ("id".to_string(), DataType::Int { length: None, integer_spelling: false }),
            ("name".to_string(), DataType::VarChar { length: None, parenthesized_length: false }),
            ("password".to_string(), DataType::VarChar { length: None, parenthesized_length: false }),
        ];
        schema.add_table("users", &columns, None).unwrap();
        schema.set_visible_columns("users", &["id", "name"]);

        let names = schema.column_names("users").unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
        assert!(!names.contains(&"password".to_string()));
    }
}
