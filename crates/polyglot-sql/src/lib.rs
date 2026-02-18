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
pub mod validation;

pub use ast_transforms::{
    add_select_columns, add_where, get_aggregate_functions, get_column_names, get_functions,
    get_identifiers, get_literals, get_subqueries, get_table_names, get_window_functions,
    node_count, qualify_columns, remove_limit_offset, remove_nodes, remove_select_columns,
    remove_where, rename_columns, rename_tables, replace_by_type, replace_nodes, set_distinct,
    set_limit, set_offset,
};
pub use dialects::{unregister_custom_dialect, CustomDialectBuilder, Dialect, DialectType};
pub use error::{Error, Result, ValidationError, ValidationResult, ValidationSeverity};
pub use expressions::Expression;
pub use generator::Generator;
pub use helper::{
    csv, find_new_name, is_date_unit, is_float, is_int, is_iso_date, is_iso_datetime, merge_ranges,
    name_sequence, seq_get, split_num_words, tsort, while_changing, DATE_UNITS,
};
pub use optimizer::{annotate_types, TypeAnnotator, TypeCoercionClass};
pub use parser::Parser;
pub use resolver::{is_column_ambiguous, resolve_column, Resolver, ResolverError, ResolverResult};
pub use schema::{
    ensure_schema, from_simple_map, normalize_name, MappingSchema, Schema, SchemaError,
};
pub use scope::{
    build_scope, find_all_in_scope, find_in_scope, traverse_scope, walk_in_scope, ColumnRef, Scope,
    ScopeType, SourceInfo,
};
pub use time::{format_time, is_valid_timezone, subsecond_precision, TIMEZONES};
pub use tokens::{Token, TokenType, Tokenizer};
pub use traversal::{
    contains_aggregate,
    contains_subquery,
    contains_window_function,
    find_ancestor,
    find_parent,
    get_columns,
    get_tables,
    is_add,
    is_aggregate,
    is_alias,
    is_alter_table,
    is_and,
    is_arithmetic,
    is_avg,
    is_between,
    is_boolean,
    is_case,
    is_cast,
    is_coalesce,
    is_column,
    is_comparison,
    is_concat,
    is_count,
    is_create_index,
    is_create_table,
    is_create_view,
    is_cte,
    is_ddl,
    is_delete,
    is_div,
    is_drop_index,
    is_drop_table,
    is_drop_view,
    is_eq,
    is_except,
    is_exists,
    is_from,
    is_function,
    is_group_by,
    is_gt,
    is_gte,
    is_having,
    is_identifier,
    is_ilike,
    is_in,
    // Extended type predicates
    is_insert,
    is_intersect,
    is_is_null,
    is_join,
    is_like,
    is_limit,
    is_literal,
    is_logical,
    is_lt,
    is_lte,
    is_max_func,
    is_min_func,
    is_mod,
    is_mul,
    is_neq,
    is_not,
    is_null_if,
    is_null_literal,
    is_offset,
    is_or,
    is_order_by,
    is_ordered,
    is_paren,
    // Composite predicates
    is_query,
    is_safe_cast,
    is_select,
    is_set_operation,
    is_star,
    is_sub,
    is_subquery,
    is_sum,
    is_table,
    is_try_cast,
    is_union,
    is_update,
    is_where,
    is_window_function,
    is_with,
    transform,
    transform_map,
    BfsIter,
    DfsIter,
    ExpressionWalk,
    ParentInfo,
    TreeContext,
};
pub use trie::{new_trie, new_trie_from_keys, Trie, TrieResult};
pub use validation::{FunctionCatalog, FunctionSignature};

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
                    return ValidationResult::with_errors(vec![ValidationError::error(
                        msg, "E004",
                    )]);
                }
            }

            // Dialect-specific function validation (non-blocking warnings)
            let mut result = ValidationResult::success();
            let function_warnings =
                validation::dialect_validate_functions(&expressions, dialect);
            for w in function_warnings {
                result.add_error(w);
            }
            result
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
