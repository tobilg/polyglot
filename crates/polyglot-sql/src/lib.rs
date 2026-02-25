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
pub mod function_catalog;
mod function_registry;
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

use serde::{Deserialize, Serialize};

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
pub use function_catalog::{
    FunctionCatalog, FunctionNameCase, FunctionSignature, HashMapFunctionCatalog,
};
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
pub use validation::{
    validate_with_schema, SchemaColumn, SchemaColumnReference, SchemaForeignKey, SchemaTable,
    SchemaTableReference, SchemaValidationOptions, ValidationSchema,
};

const DEFAULT_FORMAT_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
const DEFAULT_FORMAT_MAX_TOKENS: usize = 1_000_000;
const DEFAULT_FORMAT_MAX_AST_NODES: usize = 1_000_000;

fn default_format_max_input_bytes() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_INPUT_BYTES)
}

fn default_format_max_tokens() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_TOKENS)
}

fn default_format_max_ast_nodes() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_AST_NODES)
}

/// Guard options for SQL pretty-formatting.
///
/// These limits protect against extremely large/complex queries that can cause
/// high memory pressure in constrained runtimes (for example browser WASM).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormatGuardOptions {
    /// Maximum allowed SQL input size in bytes.
    /// `None` disables this check.
    #[serde(default = "default_format_max_input_bytes")]
    pub max_input_bytes: Option<usize>,
    /// Maximum allowed number of tokens after tokenization.
    /// `None` disables this check.
    #[serde(default = "default_format_max_tokens")]
    pub max_tokens: Option<usize>,
    /// Maximum allowed AST node count after parsing.
    /// `None` disables this check.
    #[serde(default = "default_format_max_ast_nodes")]
    pub max_ast_nodes: Option<usize>,
}

impl Default for FormatGuardOptions {
    fn default() -> Self {
        Self {
            max_input_bytes: default_format_max_input_bytes(),
            max_tokens: default_format_max_tokens(),
            max_ast_nodes: default_format_max_ast_nodes(),
        }
    }
}

fn format_guard_error(code: &str, actual: usize, limit: usize) -> Error {
    Error::generate(format!(
        "{code}: value {actual} exceeds configured limit {limit}"
    ))
}

fn enforce_input_guard(sql: &str, options: &FormatGuardOptions) -> Result<()> {
    if let Some(max) = options.max_input_bytes {
        let input_bytes = sql.len();
        if input_bytes > max {
            return Err(format_guard_error(
                "E_GUARD_INPUT_TOO_LARGE",
                input_bytes,
                max,
            ));
        }
    }
    Ok(())
}

fn parse_with_token_guard(
    sql: &str,
    dialect: &Dialect,
    options: &FormatGuardOptions,
) -> Result<Vec<Expression>> {
    let tokens = dialect.tokenize(sql)?;
    if let Some(max) = options.max_tokens {
        let token_count = tokens.len();
        if token_count > max {
            return Err(format_guard_error(
                "E_GUARD_TOKEN_BUDGET_EXCEEDED",
                token_count,
                max,
            ));
        }
    }

    let config = crate::parser::ParserConfig {
        dialect: Some(dialect.dialect_type()),
        ..Default::default()
    };
    let mut parser = Parser::with_source(tokens, config, sql.to_string());
    parser.parse()
}

fn enforce_ast_guard(expressions: &[Expression], options: &FormatGuardOptions) -> Result<()> {
    if let Some(max) = options.max_ast_nodes {
        let ast_nodes: usize = expressions.iter().map(node_count).sum();
        if ast_nodes > max {
            return Err(format_guard_error(
                "E_GUARD_AST_BUDGET_EXCEEDED",
                ast_nodes,
                max,
            ));
        }
    }
    Ok(())
}

fn format_with_dialect(
    sql: &str,
    dialect: &Dialect,
    options: &FormatGuardOptions,
) -> Result<Vec<String>> {
    enforce_input_guard(sql, options)?;
    let expressions = parse_with_token_guard(sql, dialect, options)?;
    enforce_ast_guard(&expressions, options)?;

    expressions
        .iter()
        .map(|expr| dialect.generate_pretty(expr))
        .collect()
}

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
    let generic_identity = read == DialectType::Generic && write == DialectType::Generic;

    let expressions = read_dialect.parse(sql)?;

    expressions
        .into_iter()
        .map(|expr| {
            if generic_identity {
                write_dialect.generate_with_source(&expr, read)
            } else {
                let transformed = write_dialect.transform(expr)?;
                write_dialect.generate_with_source(&transformed, read)
            }
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
        return Err(Error::parse(
            format!("Expected 1 statement, found {}", expressions.len()),
            0,
            0,
        ));
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

/// Format/pretty-print SQL statements.
///
/// Uses [`FormatGuardOptions::default`] guards.
pub fn format(sql: &str, dialect: DialectType) -> Result<Vec<String>> {
    format_with_options(sql, dialect, &FormatGuardOptions::default())
}

/// Format/pretty-print SQL statements with configurable guard limits.
pub fn format_with_options(
    sql: &str,
    dialect: DialectType,
    options: &FormatGuardOptions,
) -> Result<Vec<String>> {
    let d = Dialect::get(dialect);
    format_with_dialect(sql, &d, options)
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
    validate_with_options(sql, dialect, &ValidationOptions::default())
}

/// Options for syntax validation behavior.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationOptions {
    /// When enabled, validation rejects non-canonical trailing commas that the parser
    /// would otherwise accept for compatibility (e.g. `SELECT a, FROM t`).
    #[serde(default)]
    pub strict_syntax: bool,
}

/// Validate SQL syntax with additional validation options.
pub fn validate_with_options(
    sql: &str,
    dialect: DialectType,
    options: &ValidationOptions,
) -> ValidationResult {
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
            if options.strict_syntax {
                if let Some(error) = strict_syntax_error(sql, &d) {
                    return ValidationResult::with_errors(vec![error]);
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
                Error::Parse {
                    message,
                    line,
                    column,
                } => ValidationError::error(message.clone(), "E003").with_location(*line, *column),
                _ => ValidationError::error(e.to_string(), "E000"),
            };
            ValidationResult::with_errors(vec![error])
        }
    }
}

fn strict_syntax_error(sql: &str, dialect: &Dialect) -> Option<ValidationError> {
    let tokens = dialect.tokenize(sql).ok()?;

    for (idx, token) in tokens.iter().enumerate() {
        if token.token_type != TokenType::Comma {
            continue;
        }

        let next = tokens.get(idx + 1);
        let (is_boundary, boundary_name) = match next.map(|t| t.token_type) {
            Some(TokenType::From) => (true, "FROM"),
            Some(TokenType::Where) => (true, "WHERE"),
            Some(TokenType::GroupBy) => (true, "GROUP BY"),
            Some(TokenType::Having) => (true, "HAVING"),
            Some(TokenType::Order) | Some(TokenType::OrderBy) => (true, "ORDER BY"),
            Some(TokenType::Limit) => (true, "LIMIT"),
            Some(TokenType::Offset) => (true, "OFFSET"),
            Some(TokenType::Union) => (true, "UNION"),
            Some(TokenType::Intersect) => (true, "INTERSECT"),
            Some(TokenType::Except) => (true, "EXCEPT"),
            Some(TokenType::Qualify) => (true, "QUALIFY"),
            Some(TokenType::Window) => (true, "WINDOW"),
            Some(TokenType::Semicolon) | None => (true, "end of statement"),
            _ => (false, ""),
        };

        if is_boundary {
            let message = format!(
                "Trailing comma before {} is not allowed in strict syntax mode",
                boundary_name
            );
            return Some(
                ValidationError::error(message, "E005")
                    .with_location(token.span.line, token.span.column),
            );
        }
    }

    None
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
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", read), 0, 0))?;
    let write_dialect = Dialect::get_by_name(write)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", write), 0, 0))?;
    let generic_identity = read_dialect.dialect_type() == DialectType::Generic
        && write_dialect.dialect_type() == DialectType::Generic;

    let expressions = read_dialect.parse(sql)?;

    expressions
        .into_iter()
        .map(|expr| {
            if generic_identity {
                write_dialect.generate_with_source(&expr, read_dialect.dialect_type())
            } else {
                let transformed = write_dialect.transform(expr)?;
                write_dialect.generate_with_source(&transformed, read_dialect.dialect_type())
            }
        })
        .collect()
}

/// Parse SQL into an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
pub fn parse_by_name(sql: &str, dialect: &str) -> Result<Vec<Expression>> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0))?;
    d.parse(sql)
}

/// Generate SQL from an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
pub fn generate_by_name(expression: &Expression, dialect: &str) -> Result<String> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0))?;
    d.generate(expression)
}

/// Format SQL using a string dialect name.
///
/// Uses [`FormatGuardOptions::default`] guards.
pub fn format_by_name(sql: &str, dialect: &str) -> Result<Vec<String>> {
    format_with_options_by_name(sql, dialect, &FormatGuardOptions::default())
}

/// Format SQL using a string dialect name with configurable guard limits.
pub fn format_with_options_by_name(
    sql: &str,
    dialect: &str,
    options: &FormatGuardOptions,
) -> Result<Vec<String>> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0))?;
    format_with_dialect(sql, &d, options)
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn validate_is_permissive_by_default_for_trailing_commas() {
        let result = validate("SELECT name, FROM employees", DialectType::Generic);
        assert!(result.valid, "Result: {:?}", result.errors);
    }

    #[test]
    fn validate_with_options_rejects_trailing_comma_before_from() {
        let options = ValidationOptions {
            strict_syntax: true,
        };
        let result = validate_with_options(
            "SELECT name, FROM employees",
            DialectType::Generic,
            &options,
        );
        assert!(!result.valid, "Result should be invalid");
        assert!(
            result.errors.iter().any(|e| e.code == "E005"),
            "Expected E005, got: {:?}",
            result.errors
        );
    }

    #[test]
    fn validate_with_options_rejects_trailing_comma_before_where() {
        let options = ValidationOptions {
            strict_syntax: true,
        };
        let result = validate_with_options(
            "SELECT name FROM employees, WHERE salary > 10",
            DialectType::Generic,
            &options,
        );
        assert!(!result.valid, "Result should be invalid");
        assert!(
            result.errors.iter().any(|e| e.code == "E005"),
            "Expected E005, got: {:?}",
            result.errors
        );
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;

    #[test]
    fn format_basic_query() {
        let result = format("SELECT a,b FROM t", DialectType::Generic).expect("format failed");
        assert_eq!(result.len(), 1);
        assert!(result[0].contains('\n'));
    }

    #[test]
    fn format_guard_rejects_large_input() {
        let options = FormatGuardOptions {
            max_input_bytes: Some(7),
            max_tokens: None,
            max_ast_nodes: None,
        };
        let err = format_with_options("SELECT 1", DialectType::Generic, &options)
            .expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_INPUT_TOO_LARGE"));
    }

    #[test]
    fn format_guard_rejects_token_budget() {
        let options = FormatGuardOptions {
            max_input_bytes: None,
            max_tokens: Some(1),
            max_ast_nodes: None,
        };
        let err = format_with_options("SELECT 1", DialectType::Generic, &options)
            .expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_TOKEN_BUDGET_EXCEEDED"));
    }

    #[test]
    fn format_guard_rejects_ast_budget() {
        let options = FormatGuardOptions {
            max_input_bytes: None,
            max_tokens: None,
            max_ast_nodes: Some(1),
        };
        let err = format_with_options("SELECT 1", DialectType::Generic, &options)
            .expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_AST_BUDGET_EXCEEDED"));
    }
}
