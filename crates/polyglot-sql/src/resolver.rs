//! Column Resolver Module
//!
//! This module provides functionality for resolving column references to their
//! source tables. It handles:
//! - Finding which table a column belongs to
//! - Resolving ambiguous column references
//! - Handling join context for disambiguation
//! - Supporting set operations (UNION, INTERSECT, EXCEPT)
//!
//! Based on the Python implementation in `sqlglot/optimizer/resolver.py`.

use crate::dialects::DialectType;
use crate::expressions::{Expression, Identifier};
use crate::schema::Schema;
use crate::scope::{Scope, SourceInfo};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Errors that can occur during column resolution
#[derive(Debug, Error, Clone)]
pub enum ResolverError {
    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Ambiguous column: {column} appears in multiple sources: {sources}")]
    AmbiguousColumn { column: String, sources: String },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Unknown set operation: {0}")]
    UnknownSetOperation(String),
}

/// Result type for resolver operations
pub type ResolverResult<T> = Result<T, ResolverError>;

/// Helper for resolving columns to their source tables.
///
/// This is a struct so we can lazily load some things and easily share
/// them across functions.
pub struct Resolver<'a> {
    /// The scope being analyzed
    pub scope: &'a Scope,
    /// The schema for table/column information
    schema: &'a dyn Schema,
    /// The dialect being used
    pub dialect: Option<DialectType>,
    /// Whether to infer schema from context
    infer_schema: bool,
    /// Cached source columns: source_name -> column names
    source_columns_cache: HashMap<String, Vec<String>>,
    /// Cached unambiguous columns: column_name -> source_name
    unambiguous_columns_cache: Option<HashMap<String, String>>,
    /// Cached set of all available columns
    all_columns_cache: Option<HashSet<String>>,
}

impl<'a> Resolver<'a> {
    /// Create a new resolver for a scope
    pub fn new(scope: &'a Scope, schema: &'a dyn Schema, infer_schema: bool) -> Self {
        Self {
            scope,
            schema,
            dialect: schema.dialect(),
            infer_schema,
            source_columns_cache: HashMap::new(),
            unambiguous_columns_cache: None,
            all_columns_cache: None,
        }
    }

    /// Get the table for a column name.
    ///
    /// Returns the table name if it can be found/inferred.
    pub fn get_table(&mut self, column_name: &str) -> Option<String> {
        // Try to find table from all sources (unambiguous lookup)
        let table_name = self.get_table_name_from_sources(column_name, None);

        // If we found a table, return it
        if table_name.is_some() {
            return table_name;
        }

        // If schema inference is enabled and exactly one source has no schema,
        // assume the column belongs to that source
        if self.infer_schema {
            let sources_without_schema: Vec<_> = self
                .get_all_source_columns()
                .iter()
                .filter(|(_, columns)| columns.is_empty() || columns.contains(&"*".to_string()))
                .map(|(name, _)| name.clone())
                .collect();

            if sources_without_schema.len() == 1 {
                return Some(sources_without_schema[0].clone());
            }
        }

        None
    }

    /// Get the table for a column, returning an Identifier
    pub fn get_table_identifier(&mut self, column_name: &str) -> Option<Identifier> {
        self.get_table(column_name).map(Identifier::new)
    }

    /// Get all available columns across all sources in this scope
    pub fn all_columns(&mut self) -> &HashSet<String> {
        if self.all_columns_cache.is_none() {
            let mut all = HashSet::new();
            for columns in self.get_all_source_columns().values() {
                all.extend(columns.iter().cloned());
            }
            self.all_columns_cache = Some(all);
        }
        self.all_columns_cache
            .as_ref()
            .expect("cache populated above")
    }

    /// Get column names for a source.
    ///
    /// Returns the list of column names available from the given source.
    pub fn get_source_columns(&mut self, source_name: &str) -> ResolverResult<Vec<String>> {
        // Check cache first
        if let Some(columns) = self.source_columns_cache.get(source_name) {
            return Ok(columns.clone());
        }

        // Get the source info
        let source_info = self
            .scope
            .sources
            .get(source_name)
            .ok_or_else(|| ResolverError::UnknownTable(source_name.to_string()))?;

        let columns = self.extract_columns_from_source(source_info)?;

        // Cache the result
        self.source_columns_cache
            .insert(source_name.to_string(), columns.clone());

        Ok(columns)
    }

    /// Extract column names from a source expression
    fn extract_columns_from_source(&self, source_info: &SourceInfo) -> ResolverResult<Vec<String>> {
        let columns = match &source_info.expression {
            Expression::Table(table) => {
                // For tables, try to get columns from schema
                let table_name = table.name.name.clone();
                match self.schema.column_names(&table_name) {
                    Ok(cols) => cols,
                    Err(_) => Vec::new(), // Schema might not have this table
                }
            }
            Expression::Subquery(subquery) => {
                // For subqueries, get named_selects from the inner query
                self.get_named_selects(&subquery.this)
            }
            Expression::Select(select) => {
                // For derived tables that are SELECT expressions
                self.get_select_column_names(select)
            }
            Expression::Union(union) => {
                // For UNION, columns come from the set operation
                self.get_source_columns_from_set_op(&Expression::Union(union.clone()))?
            }
            Expression::Intersect(intersect) => {
                self.get_source_columns_from_set_op(&Expression::Intersect(intersect.clone()))?
            }
            Expression::Except(except) => {
                self.get_source_columns_from_set_op(&Expression::Except(except.clone()))?
            }
            Expression::Cte(cte) => {
                if !cte.columns.is_empty() {
                    cte.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    self.get_named_selects(&cte.this)
                }
            }
            _ => Vec::new(),
        };

        Ok(columns)
    }

    /// Get named selects (column names) from an expression
    fn get_named_selects(&self, expr: &Expression) -> Vec<String> {
        match expr {
            Expression::Select(select) => self.get_select_column_names(select),
            Expression::Union(union) => {
                // For unions, use the left side's columns
                self.get_named_selects(&union.left)
            }
            Expression::Intersect(intersect) => self.get_named_selects(&intersect.left),
            Expression::Except(except) => self.get_named_selects(&except.left),
            Expression::Subquery(subquery) => self.get_named_selects(&subquery.this),
            _ => Vec::new(),
        }
    }

    /// Get column names from a SELECT expression
    fn get_select_column_names(&self, select: &crate::expressions::Select) -> Vec<String> {
        select
            .expressions
            .iter()
            .filter_map(|expr| self.get_expression_alias(expr))
            .collect()
    }

    /// Get the alias or name for a select expression
    fn get_expression_alias(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::Alias(alias) => Some(alias.alias.name.clone()),
            Expression::Column(col) => Some(col.name.name.clone()),
            Expression::Star(_) => Some("*".to_string()),
            Expression::Identifier(id) => Some(id.name.clone()),
            _ => None,
        }
    }

    /// Get columns from a set operation (UNION, INTERSECT, EXCEPT)
    pub fn get_source_columns_from_set_op(
        &self,
        expression: &Expression,
    ) -> ResolverResult<Vec<String>> {
        match expression {
            Expression::Select(select) => Ok(self.get_select_column_names(select)),
            Expression::Subquery(subquery) => {
                if matches!(
                    &subquery.this,
                    Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
                ) {
                    self.get_source_columns_from_set_op(&subquery.this)
                } else {
                    Ok(self.get_named_selects(&subquery.this))
                }
            }
            Expression::Union(union) => {
                // Standard UNION: columns come from the left side
                self.get_source_columns_from_set_op(&union.left)
            }
            Expression::Intersect(intersect) => {
                self.get_source_columns_from_set_op(&intersect.left)
            }
            Expression::Except(except) => self.get_source_columns_from_set_op(&except.left),
            _ => Err(ResolverError::UnknownSetOperation(format!(
                "{:?}",
                expression
            ))),
        }
    }

    /// Get all source columns for all sources in the scope
    fn get_all_source_columns(&mut self) -> HashMap<String, Vec<String>> {
        let source_names: Vec<_> = self.scope.sources.keys().cloned().collect();

        let mut result = HashMap::new();
        for source_name in source_names {
            if let Ok(columns) = self.get_source_columns(&source_name) {
                result.insert(source_name, columns);
            }
        }
        result
    }

    /// Get the table name for a column from the sources
    fn get_table_name_from_sources(
        &mut self,
        column_name: &str,
        source_columns: Option<&HashMap<String, Vec<String>>>,
    ) -> Option<String> {
        let unambiguous = match source_columns {
            Some(cols) => self.compute_unambiguous_columns(cols),
            None => {
                if self.unambiguous_columns_cache.is_none() {
                    let all_source_columns = self.get_all_source_columns();
                    self.unambiguous_columns_cache =
                        Some(self.compute_unambiguous_columns(&all_source_columns));
                }
                self.unambiguous_columns_cache
                    .clone()
                    .expect("cache populated above")
            }
        };

        unambiguous.get(column_name).cloned()
    }

    /// Compute unambiguous columns mapping
    ///
    /// A column is unambiguous if it appears in exactly one source.
    fn compute_unambiguous_columns(
        &self,
        source_columns: &HashMap<String, Vec<String>>,
    ) -> HashMap<String, String> {
        if source_columns.is_empty() {
            return HashMap::new();
        }

        let mut column_to_sources: HashMap<String, Vec<String>> = HashMap::new();

        for (source_name, columns) in source_columns {
            for column in columns {
                column_to_sources
                    .entry(column.clone())
                    .or_default()
                    .push(source_name.clone());
            }
        }

        // Keep only columns that appear in exactly one source
        column_to_sources
            .into_iter()
            .filter(|(_, sources)| sources.len() == 1)
            .map(|(column, sources)| (column, sources.into_iter().next().unwrap()))
            .collect()
    }

    /// Check if a column is ambiguous (appears in multiple sources)
    pub fn is_ambiguous(&mut self, column_name: &str) -> bool {
        let all_source_columns = self.get_all_source_columns();
        let sources_with_column: Vec<_> = all_source_columns
            .iter()
            .filter(|(_, columns)| columns.contains(&column_name.to_string()))
            .map(|(name, _)| name.clone())
            .collect();

        sources_with_column.len() > 1
    }

    /// Get all sources that contain a given column
    pub fn sources_for_column(&mut self, column_name: &str) -> Vec<String> {
        let all_source_columns = self.get_all_source_columns();
        all_source_columns
            .iter()
            .filter(|(_, columns)| columns.contains(&column_name.to_string()))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Try to disambiguate a column based on join context
    ///
    /// In join conditions, a column can sometimes be disambiguated based on
    /// which tables have been joined up to that point.
    pub fn disambiguate_in_join_context(
        &mut self,
        column_name: &str,
        available_sources: &[String],
    ) -> Option<String> {
        let mut matching_sources = Vec::new();

        for source_name in available_sources {
            if let Ok(columns) = self.get_source_columns(source_name) {
                if columns.contains(&column_name.to_string()) {
                    matching_sources.push(source_name.clone());
                }
            }
        }

        if matching_sources.len() == 1 {
            Some(matching_sources.remove(0))
        } else {
            None
        }
    }
}

/// Resolve a column to its source table.
///
/// This is a convenience function that creates a Resolver and calls get_table.
pub fn resolve_column(
    scope: &Scope,
    schema: &dyn Schema,
    column_name: &str,
    infer_schema: bool,
) -> Option<String> {
    let mut resolver = Resolver::new(scope, schema, infer_schema);
    resolver.get_table(column_name)
}

/// Check if a column is ambiguous in the given scope.
pub fn is_column_ambiguous(scope: &Scope, schema: &dyn Schema, column_name: &str) -> bool {
    let mut resolver = Resolver::new(scope, schema, true);
    resolver.is_ambiguous(column_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::DataType;
    use crate::parser::Parser;
    use crate::schema::MappingSchema;
    use crate::scope::build_scope;

    fn create_test_schema() -> MappingSchema {
        let mut schema = MappingSchema::new();
        // Add tables with columns
        schema
            .add_table(
                "users",
                &[
                    (
                        "id".to_string(),
                        DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    ),
                    ("name".to_string(), DataType::Text),
                    ("email".to_string(), DataType::Text),
                ],
                None,
            )
            .unwrap();
        schema
            .add_table(
                "orders",
                &[
                    (
                        "id".to_string(),
                        DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    ),
                    (
                        "user_id".to_string(),
                        DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    ),
                    (
                        "amount".to_string(),
                        DataType::Double {
                            precision: None,
                            scale: None,
                        },
                    ),
                ],
                None,
            )
            .unwrap();
        schema
    }

    #[test]
    fn test_resolver_basic() {
        let ast = Parser::parse_sql("SELECT id, name FROM users").expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        // 'name' should resolve to 'users' since it's the only source
        let table = resolver.get_table("name");
        assert_eq!(table, Some("users".to_string()));
    }

    #[test]
    fn test_resolver_ambiguous_column() {
        let ast =
            Parser::parse_sql("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
                .expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        // 'id' appears in both tables, so it's ambiguous
        assert!(resolver.is_ambiguous("id"));

        // 'name' only appears in users
        assert!(!resolver.is_ambiguous("name"));

        // 'amount' only appears in orders
        assert!(!resolver.is_ambiguous("amount"));
    }

    #[test]
    fn test_resolver_unambiguous_column() {
        let ast = Parser::parse_sql(
            "SELECT name, amount FROM users JOIN orders ON users.id = orders.user_id",
        )
        .expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        // 'name' should resolve to 'users'
        let table = resolver.get_table("name");
        assert_eq!(table, Some("users".to_string()));

        // 'amount' should resolve to 'orders'
        let table = resolver.get_table("amount");
        assert_eq!(table, Some("orders".to_string()));
    }

    #[test]
    fn test_resolver_with_alias() {
        let ast = Parser::parse_sql("SELECT u.id FROM users AS u").expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let _resolver = Resolver::new(&scope, &schema, true);

        // Source should be indexed by alias 'u'
        assert!(scope.sources.contains_key("u"));
    }

    #[test]
    fn test_sources_for_column() {
        let ast = Parser::parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
            .expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        // 'id' should be in both users and orders
        let sources = resolver.sources_for_column("id");
        assert!(sources.contains(&"users".to_string()));
        assert!(sources.contains(&"orders".to_string()));

        // 'email' should only be in users
        let sources = resolver.sources_for_column("email");
        assert_eq!(sources, vec!["users".to_string()]);
    }

    #[test]
    fn test_all_columns() {
        let ast = Parser::parse_sql("SELECT * FROM users").expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        let all = resolver.all_columns();
        assert!(all.contains("id"));
        assert!(all.contains("name"));
        assert!(all.contains("email"));
    }

    #[test]
    fn test_resolver_cte_projected_alias_column() {
        let ast = Parser::parse_sql(
            "WITH my_cte AS (SELECT id AS emp_id FROM users) SELECT emp_id FROM my_cte",
        )
        .expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();
        let mut resolver = Resolver::new(&scope, &schema, true);

        let table = resolver.get_table("emp_id");
        assert_eq!(table, Some("my_cte".to_string()));
    }

    #[test]
    fn test_resolve_column_helper() {
        let ast = Parser::parse_sql("SELECT name FROM users").expect("Failed to parse");
        let scope = build_scope(&ast[0]);
        let schema = create_test_schema();

        let table = resolve_column(&scope, &schema, "name", true);
        assert_eq!(table, Some("users".to_string()));
    }
}
