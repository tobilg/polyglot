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

pub use dialects::{Dialect, DialectType};
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
        Ok(_) => ValidationResult::success(),
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
