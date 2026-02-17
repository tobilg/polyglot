//! Polyglot Core - SQL parsing and dialect translation library
//!
//! This library provides the core functionality for parsing SQL statements,
//! building an abstract syntax tree (AST), and generating SQL in different dialects.
//!
//! # Architecture
//!
//! The library follows a pipeline architecture:
//! 1. **Tokenizer** - Converts SQL string to token stream
//! 2. **Parser** - Builds AST from tokens
//! 3. **Generator** - Converts AST back to SQL string
//!
//! Each stage can be customized per dialect.

pub mod ast_transforms;
pub mod builder;
pub mod dialects;
pub mod diff;
pub mod error;
pub mod expressions;
pub mod generator;
pub mod helper;
pub mod lineage;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod resolver;
pub mod schema;
pub mod scope;
pub mod time;
pub mod tokens;
pub mod transforms;
pub mod traversal;
pub mod trie;

pub use dialects::{Dialect, DialectType, CustomDialectBuilder, unregister_custom_dialect};
pub use error::{Error, Result, ValidationError, ValidationResult, ValidationSeverity};
pub use expressions::Expression;
pub use generator::Generator;
pub use helper::{
    csv, find_new_name, is_date_unit, is_float, is_int, is_iso_date, is_iso_datetime,
    merge_ranges, name_sequence, seq_get, split_num_words, tsort, while_changing, DATE_UNITS,
};
pub use parser::Parser;
pub use resolver::{is_column_ambiguous, resolve_column, Resolver, ResolverError, ResolverResult};
pub use schema::{ensure_schema, from_simple_map, normalize_name, MappingSchema, Schema, SchemaError};
pub use scope::{
    build_scope, find_all_in_scope, find_in_scope, traverse_scope, walk_in_scope, ColumnRef, Scope,
    ScopeType, SourceInfo,
};
pub use time::{format_time, is_valid_timezone, subsecond_precision, TIMEZONES};
pub use tokens::{Token, TokenType, Tokenizer};
pub use traversal::{
    contains_aggregate, contains_subquery, contains_window_function, find_ancestor, find_parent,
    get_columns, get_tables, is_aggregate, is_column, is_function, is_literal, is_select,
    is_subquery, is_window_function, transform, transform_map, BfsIter, DfsIter, ExpressionWalk,
    ParentInfo, TreeContext,
    // Extended type predicates
    is_insert, is_update, is_delete, is_union, is_intersect, is_except,
    is_boolean, is_null_literal, is_star, is_identifier, is_table,
    is_eq, is_neq, is_lt, is_lte, is_gt, is_gte, is_like, is_ilike,
    is_add, is_sub, is_mul, is_div, is_mod, is_concat,
    is_and, is_or, is_not,
    is_in, is_between, is_is_null, is_exists,
    is_count, is_sum, is_avg, is_min_func, is_max_func, is_coalesce, is_null_if,
    is_cast, is_try_cast, is_safe_cast, is_case,
    is_from, is_join, is_where, is_group_by, is_having, is_order_by, is_limit, is_offset,
    is_with, is_cte, is_alias, is_paren, is_ordered,
    is_create_table, is_drop_table, is_alter_table, is_create_index, is_drop_index,
    is_create_view, is_drop_view,
    // Composite predicates
    is_query, is_set_operation, is_comparison, is_arithmetic, is_logical, is_ddl,
};
pub use ast_transforms::{
    add_select_columns, remove_select_columns, set_distinct,
    add_where, remove_where,
    set_limit, set_offset, remove_limit_offset,
    rename_columns, rename_tables, qualify_columns,
    replace_nodes, replace_by_type, remove_nodes,
    get_column_names, get_table_names, get_identifiers, get_functions, get_literals,
    get_subqueries, get_aggregate_functions, get_window_functions, node_count,
};
pub use trie::{new_trie, new_trie_from_keys, Trie, TrieResult};
pub use optimizer::{annotate_types, TypeAnnotator, TypeCoercionClass};

/// Transpile SQL from one dialect to another.
///
/// # Arguments
/// * `sql` - The SQL string to transpile
/// * `read` - The source dialect to parse with
/// * `write` - The target dialect to generate
///
/// # Returns
/// A vector of transpiled SQL statements
///
/// # Example
/// ```
/// use polyglot_sql::{transpile, DialectType};
///
/// let result = transpile(
///     "SELECT EPOCH_MS(1618088028295)",
///     DialectType::DuckDB,
///     DialectType::Hive
/// );
/// ```
pub fn transpile(sql: &str, read: DialectType, write: DialectType) -> Result<Vec<String>> {
    let read_dialect = Dialect::get(read);
    let write_dialect = Dialect::get(write);

    let expressions = read_dialect.parse(sql)?;

    expressions
        .into_iter()
        .map(|expr| {
            let transformed = write_dialect.transform(expr)?;
            write_dialect.generate(&transformed)
        })
        .collect()
}

/// Parse SQL into an AST.
///
/// # Arguments
/// * `sql` - The SQL string to parse
/// * `dialect` - The dialect to use for parsing
///
/// # Returns
/// A vector of parsed expressions
pub fn parse(sql: &str, dialect: DialectType) -> Result<Vec<Expression>> {
    let d = Dialect::get(dialect);
    d.parse(sql)
}

/// Parse a single SQL statement.
///
/// # Arguments
/// * `sql` - The SQL string containing a single statement
/// * `dialect` - The dialect to use for parsing
///
/// # Returns
/// The parsed expression, or an error if multiple statements found
pub fn parse_one(sql: &str, dialect: DialectType) -> Result<Expression> {
    let mut expressions = parse(sql, dialect)?;

    if expressions.len() != 1 {
        return Err(Error::Parse(format!(
            "Expected 1 statement, found {}",
            expressions.len()
        )));
    }

    Ok(expressions.remove(0))
}

/// Generate SQL from an AST.
///
/// # Arguments
/// * `expression` - The expression to generate SQL from
/// * `dialect` - The target dialect
///
/// # Returns
/// The generated SQL string
pub fn generate(expression: &Expression, dialect: DialectType) -> Result<String> {
    let d = Dialect::get(dialect);
    d.generate(expression)
}

/// Validate SQL syntax.
///
/// # Arguments
/// * `sql` - The SQL string to validate
/// * `dialect` - The dialect to use for validation
///
/// # Returns
/// A validation result with any errors found
pub fn validate(sql: &str, dialect: DialectType) -> ValidationResult {
    let d = Dialect::get(dialect);
    match d.parse(sql) {
        Ok(expressions) => {
            // Reject bare expressions that aren't valid SQL statements.
            // The parser accepts any expression at the top level, but bare identifiers,
            // literals, function calls, etc. are not valid statements.
            for expr in &expressions {
                if !expr.is_statement() {
                    let msg = format!("Invalid expression / Unexpected token");
                    return ValidationResult::with_errors(vec![
                        ValidationError::error(msg, "E004"),
                    ]);
                }
            }
            ValidationResult::success()
        }
        Err(e) => {
            let error = match &e {
                Error::Syntax {
                    message,
                    line,
                    column,
                } => ValidationError::error(message.clone(), "E001").with_location(*line, *column),
                Error::Tokenize {
                    message,
                    line,
                    column,
                } => ValidationError::error(message.clone(), "E002").with_location(*line, *column),
                Error::Parse(msg) => ValidationError::error(msg.clone(), "E003"),
                _ => ValidationError::error(e.to_string(), "E000"),
            };
            ValidationResult::with_errors(vec![error])
        }
    }
}

/// Transpile SQL from one dialect to another, using string dialect names.
///
/// This supports both built-in dialect names (e.g., "postgresql", "mysql") and
/// custom dialects registered via [`CustomDialectBuilder`].
///
/// # Arguments
/// * `sql` - The SQL string to transpile
/// * `read` - The source dialect name
/// * `write` - The target dialect name
///
/// # Returns
/// A vector of transpiled SQL statements, or an error if a dialect name is unknown.
pub fn transpile_by_name(sql: &str, read: &str, write: &str) -> Result<Vec<String>> {
    let read_dialect = Dialect::get_by_name(read)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", read)))?;
    let write_dialect = Dialect::get_by_name(write)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", write)))?;

    let expressions = read_dialect.parse(sql)?;

    expressions
        .into_iter()
        .map(|expr| {
            let transformed = write_dialect.transform(expr)?;
            write_dialect.generate(&transformed)
        })
        .collect()
}

/// Parse SQL into an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
pub fn parse_by_name(sql: &str, dialect: &str) -> Result<Vec<Expression>> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect)))?;
    d.parse(sql)
}

/// Generate SQL from an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
pub fn generate_by_name(expression: &Expression, dialect: &str) -> Result<String> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect)))?;
    d.generate(expression)
}
