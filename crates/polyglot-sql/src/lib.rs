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

mod ast_children;
pub mod ast_json;
#[cfg(any(feature = "ast-tools", feature = "generate", feature = "semantic"))]
pub mod ast_transforms;
#[cfg(feature = "builder")]
pub mod builder;
pub mod dialects;
#[cfg(feature = "diff")]
pub mod diff;
pub mod error;
pub mod expressions;
#[cfg(any(test, feature = "dialect-tsql"))]
mod format_tokens;
#[cfg(feature = "semantic")]
pub mod function_catalog;
mod function_registry;
#[cfg(feature = "generate")]
pub mod generator;
pub mod guard;
#[cfg(feature = "semantic")]
pub mod helper;
#[cfg(feature = "semantic")]
pub mod lineage;
#[cfg(feature = "openlineage")]
pub mod openlineage;
#[cfg(feature = "semantic")]
pub mod optimizer;
pub mod parser;
#[cfg(feature = "planner")]
pub mod planner;
#[cfg(all(feature = "semantic", feature = "generate"))]
pub mod query_analysis;
#[cfg(feature = "semantic")]
pub mod resolver;
#[cfg(feature = "semantic")]
pub mod schema;
#[cfg(feature = "semantic")]
pub mod scope;
#[cfg(feature = "time")]
pub mod time;
pub mod tokens;
#[cfg(feature = "transpile")]
pub mod transforms;
#[cfg(any(feature = "ast-tools", feature = "generate", feature = "semantic"))]
pub mod traversal;
#[cfg(not(any(feature = "ast-tools", feature = "generate", feature = "semantic")))]
mod traversal;
#[cfg(any(feature = "semantic", feature = "time"))]
pub mod trie;
#[cfg(feature = "semantic")]
pub mod validation;

#[cfg(any(feature = "generate", feature = "semantic"))]
use serde::{Deserialize, Serialize};

#[cfg(feature = "ast-tools")]
pub use ast_transforms::{
    add_select_columns, add_where, get_aggregate_functions, get_column_names, get_functions,
    get_identifiers, get_literals, get_output_column_names, get_subqueries, get_table_names,
    get_window_functions, node_count, qualify_columns, remove_limit_offset, remove_nodes,
    remove_select_columns, remove_where, rename_columns, rename_tables, rename_tables_with_options,
    replace_by_type, replace_nodes, set_distinct, set_limit, set_limit_expr, set_offset,
    set_offset_expr, set_order_by, RenameTablesOptions,
};
pub use dialects::{unregister_custom_dialect, CustomDialectBuilder, Dialect, DialectType};
#[cfg(feature = "transpile")]
pub use dialects::{TranspileOptions, TranspileTarget};
pub use error::{Error, Result};
#[cfg(feature = "semantic")]
pub use error::{ValidationError, ValidationResult, ValidationSeverity};
pub use expressions::{DataType, Expression};
#[cfg(feature = "semantic")]
pub use function_catalog::{
    FunctionCatalog, FunctionNameCase, FunctionSignature, HashMapFunctionCatalog,
};
#[cfg(feature = "generate")]
pub use generator::{Generator, UnsupportedLevel};
pub use guard::ComplexityGuardOptions;
#[cfg(feature = "semantic")]
pub use helper::{
    csv, find_new_name, is_date_unit, is_float, is_int, is_iso_date, is_iso_datetime, merge_ranges,
    name_sequence, seq_get, split_num_words, tsort, while_changing, DATE_UNITS,
};
#[cfg(feature = "semantic")]
pub use optimizer::{
    annotate_types, qualify_tables, QualifyTablesOptions, TypeAnnotator, TypeCoercionClass,
};
pub use parser::Parser;
#[cfg(all(feature = "semantic", feature = "generate"))]
pub use query_analysis::{
    analyze_query, AnalyzeQueryOptions, ColumnReferenceFact, CteFact, ProjectionFact,
    ProjectionNullability, QueryAnalysis, QueryShape, ReferenceConfidence, RelationFact,
    SetOperationBranchFact, SetOperationFact, StarProjectionFact, TransformFunctionFact,
    TransformKind,
};
#[cfg(feature = "semantic")]
pub use resolver::{is_column_ambiguous, resolve_column, Resolver, ResolverError, ResolverResult};
#[cfg(feature = "semantic")]
pub use schema::{
    ensure_schema, from_simple_map, normalize_name, MappingSchema, Schema, SchemaError,
};
#[cfg(feature = "semantic")]
pub use scope::{
    build_scope, find_all_in_scope, find_in_scope, traverse_scope, walk_in_scope, ColumnRef, Scope,
    ScopeType, SourceInfo,
};
#[cfg(feature = "time")]
pub use time::{format_time, is_valid_timezone, subsecond_precision, TIMEZONES};
pub use tokens::{Token, TokenType, Tokenizer};
#[cfg(feature = "ast-tools")]
pub use traversal::{
    contains_aggregate,
    contains_subquery,
    contains_window_function,
    find_ancestor,
    find_parent,
    get_all_tables,
    get_columns,
    get_merge_source,
    get_merge_target,
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
    is_merge,
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
#[cfg(any(feature = "semantic", feature = "time"))]
pub use trie::{new_trie, new_trie_from_keys, Trie, TrieResult};
#[cfg(feature = "semantic")]
pub use validation::{
    mapping_schema_from_validation_schema, validate_with_schema, SchemaColumn,
    SchemaColumnReference, SchemaForeignKey, SchemaTable, SchemaTableReference,
    SchemaValidationOptions, ValidationSchema,
};

#[cfg(feature = "generate")]
const DEFAULT_FORMAT_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
#[cfg(feature = "generate")]
const DEFAULT_FORMAT_MAX_TOKENS: usize = 1_000_000;
#[cfg(feature = "generate")]
const DEFAULT_FORMAT_MAX_AST_NODES: usize = 1_000_000;
#[cfg(feature = "generate")]
const DEFAULT_FORMAT_MAX_SET_OP_CHAIN: usize = 256;

#[cfg(feature = "generate")]
fn default_format_max_input_bytes() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_INPUT_BYTES)
}

#[cfg(feature = "generate")]
fn default_format_max_tokens() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_TOKENS)
}

#[cfg(feature = "generate")]
fn default_format_max_ast_nodes() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_AST_NODES)
}

#[cfg(feature = "generate")]
fn default_format_max_set_op_chain() -> Option<usize> {
    Some(DEFAULT_FORMAT_MAX_SET_OP_CHAIN)
}

/// Guard options for SQL pretty-formatting.
///
/// These limits protect against extremely large/complex queries that can cause
/// high memory pressure in constrained runtimes (for example browser WASM).
#[cfg(feature = "generate")]
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
    /// Maximum allowed count of set-operation operators (`UNION`/`INTERSECT`/`EXCEPT`)
    /// observed in a statement before parsing.
    ///
    /// `None` disables this check.
    #[serde(default = "default_format_max_set_op_chain")]
    pub max_set_op_chain: Option<usize>,
}

#[cfg(feature = "generate")]
impl Default for FormatGuardOptions {
    fn default() -> Self {
        Self {
            max_input_bytes: default_format_max_input_bytes(),
            max_tokens: default_format_max_tokens(),
            max_ast_nodes: default_format_max_ast_nodes(),
            max_set_op_chain: default_format_max_set_op_chain(),
        }
    }
}

#[cfg(feature = "generate")]
fn format_guard_error(code: &str, actual: usize, limit: usize) -> Error {
    Error::generate(format!(
        "{code}: value {actual} exceeds configured limit {limit}"
    ))
}

#[cfg(feature = "generate")]
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

#[cfg(feature = "generate")]
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
    enforce_set_op_chain_guard(&tokens, options)?;

    let complexity_guard = ComplexityGuardOptions {
        max_input_bytes: options.max_input_bytes,
        max_tokens: options.max_tokens,
        max_ast_nodes: options.max_ast_nodes,
        ..Default::default()
    };
    let config = crate::parser::ParserConfig {
        dialect: Some(dialect.dialect_type()),
        complexity_guard,
        ..Default::default()
    };
    let mut parser = Parser::with_source(tokens, config, sql.to_string());
    parser.parse()
}

#[cfg(feature = "generate")]
fn is_trivia_token(token_type: TokenType) -> bool {
    matches!(
        token_type,
        TokenType::Space | TokenType::Break | TokenType::LineComment | TokenType::BlockComment
    )
}

#[cfg(feature = "generate")]
fn next_significant_token(tokens: &[Token], start: usize) -> Option<&Token> {
    tokens
        .iter()
        .skip(start)
        .find(|token| !is_trivia_token(token.token_type))
}

#[cfg(feature = "generate")]
fn is_set_operation_token(tokens: &[Token], idx: usize) -> bool {
    let token = &tokens[idx];
    match token.token_type {
        TokenType::Union | TokenType::Intersect => true,
        TokenType::Except => {
            // MINUS is aliased to EXCEPT in the tokenizer, but in ClickHouse minus(...)
            // is a function call rather than a set operation.
            if token.text.eq_ignore_ascii_case("minus")
                && matches!(
                    next_significant_token(tokens, idx + 1).map(|t| t.token_type),
                    Some(TokenType::LParen)
                )
            {
                return false;
            }
            true
        }
        _ => false,
    }
}

#[cfg(feature = "generate")]
fn enforce_set_op_chain_guard(tokens: &[Token], options: &FormatGuardOptions) -> Result<()> {
    let Some(max) = options.max_set_op_chain else {
        return Ok(());
    };

    let mut set_op_count = 0usize;
    for (idx, token) in tokens.iter().enumerate() {
        if token.token_type == TokenType::Semicolon {
            set_op_count = 0;
            continue;
        }

        if is_set_operation_token(tokens, idx) {
            set_op_count += 1;
            if set_op_count > max {
                return Err(format_guard_error(
                    "E_GUARD_SET_OP_CHAIN_EXCEEDED",
                    set_op_count,
                    max,
                ));
            }
        }
    }

    Ok(())
}

#[cfg(feature = "generate")]
fn enforce_ast_guard(expressions: &[Expression], options: &FormatGuardOptions) -> Result<()> {
    if let Some(max) = options.max_ast_nodes {
        let ast_nodes: usize = expressions
            .iter()
            .map(crate::ast_transforms::node_count)
            .sum();
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

#[cfg(feature = "generate")]
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
#[cfg(feature = "transpile")]
pub fn transpile(sql: &str, read: DialectType, write: DialectType) -> Result<Vec<String>> {
    // Delegate to Dialect::transpile so that the full cross-dialect rewrite
    // pipeline (source+target-aware normalization in `cross_dialect_normalize`)
    // runs here as well. This keeps Rust crate users on the same code path as
    // the WASM/FFI/Python bindings and the playground.
    Dialect::get(read).transpile(sql, write)
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
            0,
            0,
        ));
    }

    Ok(expressions.remove(0))
}

/// Parse a standalone SQL data type.
///
/// # Arguments
/// * `sql` - The data type string to parse, e.g. `DECIMAL(10, 2)`
/// * `dialect` - The dialect to use for parsing
///
/// # Returns
/// The parsed data type
pub fn parse_data_type(sql: &str, dialect: DialectType) -> Result<DataType> {
    Dialect::get(dialect).parse_data_type(sql)
}

/// Generate SQL from a standalone data type.
///
/// # Arguments
/// * `data_type` - The data type to render
/// * `dialect` - The target dialect
///
/// # Returns
/// The generated type SQL string
#[cfg(feature = "generate")]
pub fn generate_data_type(data_type: &DataType, dialect: DialectType) -> Result<String> {
    Dialect::get(dialect).generate(&Expression::DataType(data_type.clone()))
}

/// Generate SQL from an AST.
///
/// # Arguments
/// * `expression` - The expression to generate SQL from
/// * `dialect` - The target dialect
///
/// # Returns
/// The generated SQL string
#[cfg(feature = "generate")]
pub fn generate(expression: &Expression, dialect: DialectType) -> Result<String> {
    let d = Dialect::get(dialect);
    d.generate(expression)
}

/// Format/pretty-print SQL statements.
///
/// Uses [`FormatGuardOptions::default`] guards.
#[cfg(feature = "generate")]
pub fn format(sql: &str, dialect: DialectType) -> Result<Vec<String>> {
    format_with_options(sql, dialect, &FormatGuardOptions::default())
}

/// Format/pretty-print SQL statements with configurable guard limits.
#[cfg(feature = "generate")]
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
#[cfg(feature = "semantic")]
pub fn validate(sql: &str, dialect: DialectType) -> ValidationResult {
    validate_with_options(sql, dialect, &ValidationOptions::default())
}

/// Options for syntax validation behavior.
#[cfg(feature = "semantic")]
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationOptions {
    /// When enabled, validation rejects non-canonical trailing commas that the parser
    /// would otherwise accept for compatibility (e.g. `SELECT a, FROM t`).
    #[serde(default)]
    pub strict_syntax: bool,
    /// When enabled, validation reports query-quality warnings W001 through W004.
    #[serde(default)]
    pub semantic: bool,
}

/// Validate SQL syntax and optional query-quality semantic warnings.
#[cfg(feature = "semantic")]
pub fn validate_with_options(
    sql: &str,
    dialect: DialectType,
    options: &ValidationOptions,
) -> ValidationResult {
    let d = Dialect::get(dialect);
    validate_with_dialect(sql, &d, options)
}

/// Validate SQL using an already-resolved dialect.
///
/// This is useful for wrappers and custom dialect consumers that should retain
/// the exact tokenizer and parser configuration of a [`Dialect`] handle.
#[cfg(feature = "semantic")]
pub fn validate_with_dialect(
    sql: &str,
    dialect: &Dialect,
    options: &ValidationOptions,
) -> ValidationResult {
    match dialect.parse(sql) {
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
                if let Some(error) = strict_syntax_error(sql, dialect) {
                    return ValidationResult::with_errors(vec![error]);
                }
            }
            let mut errors = Vec::new();
            if options.semantic {
                for expression in &expressions {
                    errors.extend(validation::check_semantics(expression));
                }
            }
            ValidationResult::with_errors(errors)
        }
        Err(e) => {
            let error = match &e {
                Error::Syntax {
                    message,
                    line,
                    column,
                    start,
                    end,
                } => ValidationError::error(message.clone(), "E001")
                    .with_location(*line, *column)
                    .with_span(Some(*start), Some(*end)),
                Error::Tokenize {
                    message,
                    line,
                    column,
                    start,
                    end,
                } => ValidationError::error(message.clone(), "E002")
                    .with_location(*line, *column)
                    .with_span(Some(*start), Some(*end)),
                Error::Parse {
                    message,
                    line,
                    column,
                    start,
                    end,
                } => ValidationError::error(message.clone(), "E003")
                    .with_location(*line, *column)
                    .with_span(Some(*start), Some(*end)),
                _ => ValidationError::error(e.to_string(), "E000"),
            };
            ValidationResult::with_errors(vec![error])
        }
    }
}

#[cfg(feature = "semantic")]
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
#[cfg(feature = "transpile")]
pub fn transpile_by_name(sql: &str, read: &str, write: &str) -> Result<Vec<String>> {
    transpile_with_by_name(sql, read, write, &TranspileOptions::default())
}

/// Transpile SQL with configurable [`TranspileOptions`], using string dialect names.
///
/// Same as [`transpile_by_name`] but accepts options (e.g., pretty-printing).
#[cfg(feature = "transpile")]
pub fn transpile_with_by_name(
    sql: &str,
    read: &str,
    write: &str,
    opts: &TranspileOptions,
) -> Result<Vec<String>> {
    let read_dialect = Dialect::get_by_name(read)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", read), 0, 0, 0, 0))?;
    let write_dialect = Dialect::get_by_name(write)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", write), 0, 0, 0, 0))?;
    read_dialect.transpile_with(sql, &write_dialect, opts.clone())
}

/// Parse SQL into an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
pub fn parse_by_name(sql: &str, dialect: &str) -> Result<Vec<Expression>> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0, 0, 0))?;
    d.parse(sql)
}

/// Generate SQL from an AST using a string dialect name.
///
/// Supports both built-in and custom dialect names.
#[cfg(feature = "generate")]
pub fn generate_by_name(expression: &Expression, dialect: &str) -> Result<String> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0, 0, 0))?;
    d.generate(expression)
}

/// Format SQL using a string dialect name.
///
/// Uses [`FormatGuardOptions::default`] guards.
#[cfg(feature = "generate")]
pub fn format_by_name(sql: &str, dialect: &str) -> Result<Vec<String>> {
    format_with_options_by_name(sql, dialect, &FormatGuardOptions::default())
}

/// Format SQL using a string dialect name with configurable guard limits.
#[cfg(feature = "generate")]
pub fn format_with_options_by_name(
    sql: &str,
    dialect: &str,
    options: &FormatGuardOptions,
) -> Result<Vec<String>> {
    let d = Dialect::get_by_name(dialect)
        .ok_or_else(|| Error::parse(format!("Unknown dialect: {}", dialect), 0, 0, 0, 0))?;
    format_with_dialect(sql, &d, options)
}

#[cfg(test)]
mod api_contract_tests {
    use super::*;
    use serde_json::Value;
    use std::collections::{BTreeMap, BTreeSet};

    macro_rules! exported_symbols {
        ($($capability:literal => { $($name:literal => $symbol:expr),+ $(,)? }),+ $(,)?) => {{
            let mut capabilities = BTreeMap::new();
            $(
                $(let _ = $symbol;)+
                capabilities.insert(
                    $capability,
                    BTreeSet::from([$($name),+]),
                );
            )+
            capabilities
        }};
    }

    #[test]
    fn public_api_matches_capability_contract() {
        let Ok(path) = std::env::var("POLYGLOT_API_CONTRACT") else {
            return;
        };
        let contract: Value = serde_json::from_str(
            &std::fs::read_to_string(path).expect("read API capability contract"),
        )
        .expect("parse API capability contract");

        let actual = exported_symbols! {
            "dialects" => { "Dialect.get" => Dialect::get },
            "transpile" => { "transpile" => transpile },
            "parse" => { "parse" => parse, "parse_one" => parse_one },
            "data_types" => { "parse_data_type" => parse_data_type, "generate_data_type" => generate_data_type },
            "generate" => { "generate" => generate },
            "format" => { "format" => format, "format_with_options" => format_with_options },
            "validate" => {
                "validate" => validate,
                "validate_with_options" => validate_with_options,
                "validate_with_dialect" => validate_with_dialect
            },
            "validate_schema" => { "validation.validate_with_schema" => validation::validate_with_schema },
            "optimize" => { "optimizer.optimize" => optimizer::optimize },
            "tokenize" => { "Tokenizer.tokenize" => Tokenizer::tokenize },
            "annotate_types" => { "annotate_types" => annotate_types },
            "diff" => { "diff.diff" => diff::diff },
            "ast_transforms" => {
                "rename_tables" => rename_tables,
                "set_limit" => set_limit,
                "set_offset" => set_offset,
                "set_order_by" => set_order_by
            },
            "lineage" => {
                "lineage.lineage" => lineage::lineage,
                "lineage.lineage_with_schema" => lineage::lineage_with_schema,
                "lineage.get_source_tables" => lineage::get_source_tables
            },
            "openlineage" => {
                "openlineage.openlineage_column_lineage" => openlineage::openlineage_column_lineage,
                "openlineage.openlineage_job_event" => openlineage::openlineage_job_event,
                "openlineage.openlineage_run_event" => openlineage::openlineage_run_event
            },
            "analyze_query" => { "analyze_query" => analyze_query },
            "planner" => { "planner.Plan.from_expression" => planner::Plan::from_expression },
            "builders" => { "builder.col" => builder::col },
            "visitors" => {
                "traversal.transform" => traversal::transform::<fn(Expression) -> Result<Option<Expression>>>,
                "traversal.get_columns" => traversal::get_columns
            },
        };

        assert_layer_contract(&contract, "rust", &actual);
    }

    fn assert_layer_contract(
        contract: &Value,
        layer: &str,
        actual: &BTreeMap<&str, BTreeSet<&str>>,
    ) {
        let capabilities = contract["capabilities"]
            .as_array()
            .expect("capabilities must be an array");
        let mut declared_available = BTreeSet::new();

        for capability in capabilities {
            let id = capability["id"].as_str().expect("capability id");
            let entry = &capability["layers"][layer];
            let status = entry["status"].as_str().expect("capability status");
            assert!(matches!(status, "supported" | "partial" | "unavailable"));
            if status != "supported" {
                assert!(entry["notes"].as_str().is_some_and(|note| !note.is_empty()));
            }
            if status == "unavailable" {
                assert!(!actual.contains_key(id), "{id} is declared unavailable");
                continue;
            }

            declared_available.insert(id);
            let expected = entry["symbols"]
                .as_array()
                .expect("symbols must be an array")
                .iter()
                .map(|symbol| symbol.as_str().expect("symbol must be a string"))
                .collect::<BTreeSet<_>>();
            assert_eq!(actual.get(id), Some(&expected), "capability {id}");
        }

        assert_eq!(
            actual.keys().copied().collect::<BTreeSet<_>>(),
            declared_available
        );
    }
}

#[cfg(all(test, feature = "semantic"))]
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
            ..Default::default()
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
            ..Default::default()
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

    #[test]
    fn validate_with_options_reports_semantic_warnings() {
        let options = ValidationOptions {
            semantic: true,
            ..Default::default()
        };
        let result = validate_with_options(
            "SELECT *, category, COUNT(*) FROM products LIMIT 10",
            DialectType::Generic,
            &options,
        );

        assert!(result.valid, "Warnings must not invalidate SQL");
        assert!(result.errors.iter().any(|error| error.code == "W001"));
        assert!(result.errors.iter().any(|error| error.code == "W002"));
        assert!(result.errors.iter().any(|error| error.code == "W004"));
    }

    #[test]
    fn validate_with_dialect_combines_strict_and_semantic_options() {
        let options = ValidationOptions {
            strict_syntax: true,
            semantic: true,
        };
        let dialect = Dialect::get_by_name("generic").expect("generic dialect");
        let result = validate_with_dialect("SELECT *, FROM products", &dialect, &options);

        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].code, "E005");
    }
}

#[cfg(all(test, feature = "generate"))]
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
            max_set_op_chain: None,
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
            max_set_op_chain: None,
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
            max_set_op_chain: None,
        };
        let err = format_with_options("SELECT 1", DialectType::Generic, &options)
            .expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_AST_BUDGET_EXCEEDED"));
    }

    #[test]
    fn format_guard_rejects_set_op_chain_budget() {
        let options = FormatGuardOptions {
            max_input_bytes: None,
            max_tokens: None,
            max_ast_nodes: None,
            max_set_op_chain: Some(1),
        };
        let err = format_with_options(
            "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
            DialectType::Generic,
            &options,
        )
        .expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_SET_OP_CHAIN_EXCEEDED"));
    }

    #[test]
    fn format_guard_does_not_treat_clickhouse_minus_function_as_set_op() {
        let options = FormatGuardOptions {
            max_input_bytes: None,
            max_tokens: None,
            max_ast_nodes: None,
            max_set_op_chain: Some(0),
        };
        let result = format_with_options("SELECT minus(3, 2)", DialectType::ClickHouse, &options);
        assert!(result.is_ok(), "Result: {:?}", result);
    }

    #[test]
    fn issue57_invalid_ternary_returns_error() {
        // https://github.com/tobilg/polyglot/issues/57
        // Invalid SQL with ternary operator should return an error, not garbled output.
        let sql = "SELECT x > 0 ? 1 : 0 FROM t";

        let parse_result = parse(sql, DialectType::PostgreSQL);
        assert!(
            parse_result.is_err(),
            "Expected parse error for invalid ternary SQL, got: {:?}",
            parse_result
        );

        let format_result = format(sql, DialectType::PostgreSQL);
        assert!(
            format_result.is_err(),
            "Expected format error for invalid ternary SQL, got: {:?}",
            format_result
        );

        let transpile_result = transpile(sql, DialectType::PostgreSQL, DialectType::PostgreSQL);
        assert!(
            transpile_result.is_err(),
            "Expected transpile error for invalid ternary SQL, got: {:?}",
            transpile_result
        );
    }

    /// Regression guard: `lib::transpile()` must apply the full cross-dialect
    /// rewrite pipeline (same as `Dialect::transpile()`). If these two paths
    /// diverge again, Rust crate users silently get under-transformed SQL that
    /// differs from what WASM/FFI/Python bindings produce.
    #[test]
    fn transpile_applies_cross_dialect_rewrites() {
        // DuckDB to_timestamp → Trino FROM_UNIXTIME (different input semantics).
        let out = transpile(
            "SELECT to_timestamp(col) FROM t",
            DialectType::DuckDB,
            DialectType::Trino,
        )
        .expect("transpile failed");
        assert_eq!(out[0], "SELECT FROM_UNIXTIME(col) FROM t");

        // DuckDB CAST(x AS JSON) → Trino JSON_PARSE(x) (different CAST semantics).
        let out = transpile(
            "SELECT CAST(col AS JSON) FROM t",
            DialectType::DuckDB,
            DialectType::Trino,
        )
        .expect("transpile failed");
        assert_eq!(out[0], "SELECT JSON_PARSE(col) FROM t");
    }

    /// Regression guard: all three transpile entry points (lib::transpile,
    /// lib::transpile_by_name, Dialect::transpile) must produce identical
    /// output. transpile_by_name is the one used by Python and C FFI bindings.
    #[test]
    fn transpile_matches_dialect_method() {
        let cases: &[(DialectType, DialectType, &str, &str, &str)] = &[
            (
                DialectType::DuckDB,
                DialectType::Trino,
                "duckdb",
                "trino",
                "SELECT to_timestamp(col) FROM t",
            ),
            (
                DialectType::DuckDB,
                DialectType::Trino,
                "duckdb",
                "trino",
                "SELECT CAST(col AS JSON) FROM t",
            ),
            (
                DialectType::DuckDB,
                DialectType::Trino,
                "duckdb",
                "trino",
                "SELECT json_valid(col) FROM t",
            ),
            (
                DialectType::Snowflake,
                DialectType::DuckDB,
                "snowflake",
                "duckdb",
                "SELECT DATEDIFF(day, a, b) FROM t",
            ),
            (
                DialectType::BigQuery,
                DialectType::DuckDB,
                "bigquery",
                "duckdb",
                "SELECT DATE_DIFF(a, b, DAY) FROM t",
            ),
            (
                DialectType::Generic,
                DialectType::Generic,
                "generic",
                "generic",
                "SELECT 1",
            ),
        ];
        for (read, write, read_name, write_name, sql) in cases {
            let via_lib = transpile(sql, *read, *write).expect("lib::transpile failed");
            let via_name = transpile_by_name(sql, read_name, write_name)
                .expect("lib::transpile_by_name failed");
            let via_dialect = Dialect::get(*read)
                .transpile(sql, *write)
                .expect("Dialect::transpile failed");
            assert_eq!(
                via_lib, via_dialect,
                "lib::transpile / Dialect::transpile diverged for {:?} -> {:?}: {sql}",
                read, write
            );
            assert_eq!(
                via_name, via_dialect,
                "lib::transpile_by_name / Dialect::transpile diverged for {read_name} -> {write_name}: {sql}"
            );
        }
    }

    #[test]
    fn format_default_guard_rejects_deep_union_chain_before_parse() {
        let base = "SELECT col0, col1 FROM t";
        let mut sql = base.to_string();
        for _ in 0..1100 {
            sql.push_str(" UNION ALL ");
            sql.push_str(base);
        }

        let err = format(&sql, DialectType::Athena).expect_err("expected guard error");
        assert!(err.to_string().contains("E_GUARD_SET_OP_CHAIN_EXCEEDED"));
    }
}
