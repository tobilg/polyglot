//! WebAssembly bindings for Polyglot SQL Translator
//!
//! This crate provides WASM bindings for the polyglot-sql library,
//! allowing SQL dialect translation in the browser.

pub mod builders;

use polyglot_sql::{
    ast_transforms,
    dialects::{Dialect, DialectType},
    diff::{diff_with_config, DiffConfig, Edit},
    expressions::Expression,
    generator::Generator,
    lineage::{self, LineageNode},
    planner::{Plan, Step},
    validate_with_schema as core_validate_with_schema,
    SchemaValidationOptions as CoreSchemaValidationOptions,
    ValidationOptions as CoreValidationOptions, ValidationResult as CoreValidationResult,
    ValidationSchema as CoreValidationSchema,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

/// Initialize panic hook for better error messages in WASM
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Result type for WASM operations
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranspileResult {
    pub success: bool,
    pub sql: Option<Vec<String>>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_column: Option<usize>,
}

/// Result type for parse operations
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParseResult {
    pub success: bool,
    pub ast: Option<String>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_column: Option<usize>,
}

/// Result type for parse operations with structured AST values.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParseValueResult {
    pub success: bool,
    pub ast: Option<Vec<Expression>>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_column: Option<usize>,
}

fn serialize_result<T>(result: &T) -> String
where
    T: Serialize,
{
    serde_json::to_string(result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn serialize_result_value<T>(result: &T) -> JsValue
where
    T: Serialize,
{
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    result
        .serialize(&serializer)
        .unwrap_or_else(|e| JsValue::from_str(&format!("Serialization error: {}", e)))
}

/// Transpile SQL from one dialect to another.
///
/// # Arguments
/// * `sql` - The SQL string to transpile
/// * `read_dialect` - The source dialect (e.g., "duckdb", "postgres")
/// * `write_dialect` - The target dialect (e.g., "hive", "bigquery")
///
/// # Returns
/// A JSON string containing the TranspileResult
#[wasm_bindgen]
pub fn transpile(sql: &str, read_dialect: &str, write_dialect: &str) -> String {
    set_panic_hook();

    let result = transpile_internal(sql, read_dialect, write_dialect);
    serialize_result(&result)
}

/// Transpile SQL and return a structured JS object instead of a JSON string.
#[wasm_bindgen]
pub fn transpile_value(sql: &str, read_dialect: &str, write_dialect: &str) -> JsValue {
    set_panic_hook();

    let result = transpile_internal(sql, read_dialect, write_dialect);
    serialize_result_value(&result)
}

fn transpile_internal(sql: &str, read_dialect: &str, write_dialect: &str) -> TranspileResult {
    let read = match read_dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid read dialect: {}", e)),
                error_line: e.line(),
                error_column: e.column(),
            };
        }
    };

    let write = match write_dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid write dialect: {}", e)),
                error_line: e.line(),
                error_column: e.column(),
            };
        }
    };

    let dialect = Dialect::get(read);
    match dialect.transpile_to(sql, write) {
        Ok(results) => TranspileResult {
            success: true,
            sql: Some(results),
            error: None,
            error_line: None,
            error_column: None,
        },
        Err(e) => TranspileResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
            error_line: e.line(),
            error_column: e.column(),
        },
    }
}

/// Parse SQL into an AST (returned as JSON).
///
/// # Arguments
/// * `sql` - The SQL string to parse
/// * `dialect` - The dialect to use for parsing
///
/// # Returns
/// A JSON string containing the ParseResult with AST
#[wasm_bindgen]
pub fn parse(sql: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = parse_internal(sql, dialect);
    serialize_result(&result)
}

fn parse_internal(sql: &str, dialect: &str) -> ParseResult {
    let result = parse_value_internal(sql, dialect);
    match result.ast {
        Some(expressions) => match serde_json::to_string(&expressions) {
            Ok(ast) => ParseResult {
                success: true,
                ast: Some(ast),
                error: None,
                error_line: None,
                error_column: None,
            },
            Err(e) => ParseResult {
                success: false,
                ast: None,
                error: Some(format!("Failed to serialize AST: {}", e)),
                error_line: None,
                error_column: None,
            },
        },
        None => ParseResult {
            success: result.success,
            ast: None,
            error: result.error,
            error_line: result.error_line,
            error_column: result.error_column,
        },
    }
}

/// Parse SQL and return a structured JS object with AST values.
#[wasm_bindgen]
pub fn parse_value(sql: &str, dialect: &str) -> JsValue {
    set_panic_hook();

    let result = parse_value_internal(sql, dialect);
    serialize_result_value(&result)
}

fn parse_value_internal(sql: &str, dialect: &str) -> ParseValueResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return ParseValueResult {
                success: false,
                ast: None,
                error: Some(format!("Invalid dialect: {}", e)),
                error_line: e.line(),
                error_column: e.column(),
            };
        }
    };

    let d = Dialect::get(dialect_type);
    match d.parse(sql) {
        Ok(expressions) => ParseValueResult {
            success: true,
            ast: Some(expressions),
            error: None,
            error_line: None,
            error_column: None,
        },
        Err(e) => ParseValueResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
            error_line: e.line(),
            error_column: e.column(),
        },
    }
}

/// Generate SQL from an AST (provided as JSON).
///
/// # Arguments
/// * `ast_json` - The AST as a JSON string
/// * `dialect` - The dialect to use for generation
///
/// # Returns
/// A JSON string containing the result
#[wasm_bindgen]
pub fn generate(ast_json: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = generate_internal(ast_json, dialect);
    serialize_result(&result)
}

/// Generate SQL from an AST represented as a structured JS value.
#[wasm_bindgen]
pub fn generate_value(ast: JsValue, dialect: &str) -> JsValue {
    set_panic_hook();

    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return serialize_result_value(&TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid dialect: {}", e)),
                error_line: e.line(),
                error_column: e.column(),
            });
        }
    };

    let expressions: Vec<Expression> = match serde_wasm_bindgen::from_value(ast) {
        Ok(e) => e,
        Err(e) => {
            return serialize_result_value(&TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid AST value: {}", e)),
                error_line: None,
                error_column: None,
            });
        }
    };

    let result = generate_from_expressions_with_dialect(expressions, dialect_type);
    serialize_result_value(&result)
}

fn generate_internal(ast_json: &str, dialect: &str) -> TranspileResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid dialect: {}", e)),
                error_line: e.line(),
                error_column: e.column(),
            };
        }
    };

    let expressions: Vec<Expression> = match serde_json::from_str(ast_json) {
        Ok(e) => e,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid AST JSON: {}", e)),
                error_line: None,
                error_column: None,
            };
        }
    };

    generate_from_expressions_with_dialect(expressions, dialect_type)
}

fn generate_from_expressions_with_dialect(
    expressions: Vec<Expression>,
    dialect_type: DialectType,
) -> TranspileResult {
    let d = Dialect::get(dialect_type);
    let results: Result<Vec<String>, _> = expressions.iter().map(|expr| d.generate(expr)).collect();

    match results {
        Ok(sql) => TranspileResult {
            success: true,
            sql: Some(sql),
            error: None,
            error_line: None,
            error_column: None,
        },
        Err(e) => TranspileResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
            error_line: e.line(),
            error_column: e.column(),
        },
    }
}

/// Get a list of supported dialects.
///
/// # Returns
/// A JSON array of dialect names
#[wasm_bindgen]
pub fn get_dialects() -> String {
    serialize_result(&get_dialects_internal())
}

/// Get a list of supported dialects as a structured JS array.
#[wasm_bindgen]
pub fn get_dialects_value() -> JsValue {
    serialize_result_value(&get_dialects_internal())
}

fn get_dialects_internal() -> Vec<&'static str> {
    let mut dialects = vec!["generic"];
    #[cfg(feature = "dialect-postgresql")]
    dialects.push("postgresql");
    #[cfg(feature = "dialect-mysql")]
    dialects.push("mysql");
    #[cfg(feature = "dialect-bigquery")]
    dialects.push("bigquery");
    #[cfg(feature = "dialect-snowflake")]
    dialects.push("snowflake");
    #[cfg(feature = "dialect-duckdb")]
    dialects.push("duckdb");
    #[cfg(feature = "dialect-sqlite")]
    dialects.push("sqlite");
    #[cfg(feature = "dialect-hive")]
    dialects.push("hive");
    #[cfg(feature = "dialect-spark")]
    dialects.push("spark");
    #[cfg(feature = "dialect-trino")]
    dialects.push("trino");
    #[cfg(feature = "dialect-presto")]
    dialects.push("presto");
    #[cfg(feature = "dialect-redshift")]
    dialects.push("redshift");
    #[cfg(feature = "dialect-tsql")]
    dialects.push("tsql");
    #[cfg(feature = "dialect-oracle")]
    dialects.push("oracle");
    #[cfg(feature = "dialect-clickhouse")]
    dialects.push("clickhouse");
    #[cfg(feature = "dialect-databricks")]
    dialects.push("databricks");
    #[cfg(feature = "dialect-athena")]
    dialects.push("athena");
    #[cfg(feature = "dialect-teradata")]
    dialects.push("teradata");
    #[cfg(feature = "dialect-doris")]
    dialects.push("doris");
    #[cfg(feature = "dialect-starrocks")]
    dialects.push("starrocks");
    #[cfg(feature = "dialect-materialize")]
    dialects.push("materialize");
    #[cfg(feature = "dialect-risingwave")]
    dialects.push("risingwave");
    #[cfg(feature = "dialect-singlestore")]
    dialects.push("singlestore");
    #[cfg(feature = "dialect-cockroachdb")]
    dialects.push("cockroachdb");
    #[cfg(feature = "dialect-tidb")]
    dialects.push("tidb");
    #[cfg(feature = "dialect-druid")]
    dialects.push("druid");
    #[cfg(feature = "dialect-solr")]
    dialects.push("solr");
    #[cfg(feature = "dialect-tableau")]
    dialects.push("tableau");
    #[cfg(feature = "dialect-dune")]
    dialects.push("dune");
    #[cfg(feature = "dialect-fabric")]
    dialects.push("fabric");
    #[cfg(feature = "dialect-drill")]
    dialects.push("drill");
    #[cfg(feature = "dialect-dremio")]
    dialects.push("dremio");
    #[cfg(feature = "dialect-exasol")]
    dialects.push("exasol");
    #[cfg(feature = "dialect-datafusion")]
    dialects.push("datafusion");
    dialects
}

/// Format/pretty-print SQL.
///
/// # Arguments
/// * `sql` - The SQL string to format
/// * `dialect` - The dialect to use
///
/// # Returns
/// The formatted SQL string
#[wasm_bindgen]
pub fn format_sql(sql: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = format_sql_internal(sql, dialect);
    serialize_result(&result)
}

/// Format SQL and return a structured JS object instead of JSON text.
#[wasm_bindgen]
pub fn format_sql_value(sql: &str, dialect: &str) -> JsValue {
    set_panic_hook();

    let result = format_sql_internal(sql, dialect);
    serialize_result_value(&result)
}

fn format_sql_internal(sql: &str, dialect: &str) -> TranspileResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(e.to_string()),
                error_line: e.line(),
                error_column: e.column(),
            };
        }
    };

    let d = Dialect::get(dialect_type);
    match d.parse(sql) {
        Ok(expressions) => {
            let formatted: Result<Vec<String>, _> =
                expressions.iter().map(Generator::pretty_sql).collect();

            match formatted {
                Ok(sql) => TranspileResult {
                    success: true,
                    sql: Some(sql),
                    error: None,
                    error_line: None,
                    error_column: None,
                },
                Err(e) => TranspileResult {
                    success: false,
                    sql: None,
                    error: Some(e.to_string()),
                    error_line: e.line(),
                    error_column: e.column(),
                },
            }
        }
        Err(e) => TranspileResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
            error_line: e.line(),
            error_column: e.column(),
        },
    }
}

/// Validate SQL syntax.
///
/// # Arguments
/// * `sql` - The SQL string to validate
/// * `dialect` - The dialect to use for validation
///
/// # Returns
/// A JSON string containing the ValidationResult
#[wasm_bindgen]
pub fn validate(sql: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = validate_internal(sql, dialect);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"valid":false,"errors":[{{"message":"Serialization error: {}","severity":"error","code":"E000"}}]}}"#,
            e
        )
    })
}

fn validate_internal(sql: &str, dialect: &str) -> CoreValidationResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return CoreValidationResult::with_errors(vec![polyglot_sql::ValidationError::error(
                format!("Invalid dialect: {}", e),
                "E000",
            )]);
        }
    };

    polyglot_sql::validate(sql, dialect_type)
}

/// Validate SQL syntax with additional options.
///
/// # Arguments
/// * `sql` - The SQL string to validate
/// * `dialect` - The dialect to use for validation
/// * `options_json` - Options JSON matching ValidationOptions
///
/// # Returns
/// A JSON string containing the ValidationResult
#[wasm_bindgen]
pub fn validate_with_options(sql: &str, dialect: &str, options_json: &str) -> String {
    set_panic_hook();

    let result = validate_with_options_internal(sql, dialect, options_json);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"valid":false,"errors":[{{"message":"Serialization error: {}","severity":"error","code":"E000"}}]}}"#,
            e
        )
    })
}

fn validate_with_options_internal(
    sql: &str,
    dialect: &str,
    options_json: &str,
) -> CoreValidationResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return CoreValidationResult::with_errors(vec![polyglot_sql::ValidationError::error(
                format!("Invalid dialect: {}", e),
                "E000",
            )]);
        }
    };

    let options = if options_json.trim().is_empty() {
        CoreValidationOptions::default()
    } else {
        match serde_json::from_str::<CoreValidationOptions>(options_json) {
            Ok(o) => o,
            Err(e) => {
                return CoreValidationResult::with_errors(vec![
                    polyglot_sql::ValidationError::error(
                        format!("Invalid validation options JSON: {}", e),
                        "E000",
                    ),
                ]);
            }
        }
    };

    polyglot_sql::validate_with_options(sql, dialect_type, &options)
}

/// Validate SQL syntax + schema-aware checks (+ optional semantic warnings).
///
/// # Arguments
/// * `sql` - The SQL string to validate
/// * `schema_json` - Schema JSON matching ValidationSchema
/// * `dialect` - SQL dialect name
/// * `options_json` - Options JSON matching SchemaValidationOptions
///
/// # Returns
/// A JSON string containing ValidationResult
#[wasm_bindgen]
pub fn validate_with_schema(
    sql: &str,
    schema_json: &str,
    dialect: &str,
    options_json: &str,
) -> String {
    set_panic_hook();

    let result = validate_with_schema_internal(sql, schema_json, dialect, options_json);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"valid":false,"errors":[{{"message":"Serialization error: {}","severity":"error","code":"E000"}}]}}"#,
            e
        )
    })
}

fn validate_with_schema_internal(
    sql: &str,
    schema_json: &str,
    dialect: &str,
    options_json: &str,
) -> CoreValidationResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return CoreValidationResult::with_errors(vec![polyglot_sql::ValidationError::error(
                format!("Invalid dialect: {}", e),
                "E000",
            )]);
        }
    };

    let schema = match serde_json::from_str::<CoreValidationSchema>(schema_json) {
        Ok(s) => s,
        Err(e) => {
            return CoreValidationResult::with_errors(vec![polyglot_sql::ValidationError::error(
                format!("Invalid schema JSON: {}", e),
                "E000",
            )]);
        }
    };

    let options = if options_json.trim().is_empty() {
        CoreSchemaValidationOptions::default()
    } else {
        match serde_json::from_str::<CoreSchemaValidationOptions>(options_json) {
            Ok(o) => o,
            Err(e) => {
                return CoreValidationResult::with_errors(vec![
                    polyglot_sql::ValidationError::error(
                        format!("Invalid schema validation options JSON: {}", e),
                        "E000",
                    ),
                ]);
            }
        }
    };

    core_validate_with_schema(sql, dialect_type, &schema, &options)
}

/// Get the version of the library.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

// ============================================================================
// Lineage, Diff & Planner - Result types
// ============================================================================

#[derive(Serialize, Deserialize)]
pub struct LineageResult {
    pub success: bool,
    pub lineage: Option<LineageNode>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceTablesResult {
    pub success: bool,
    pub tables: Option<Vec<String>>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct DiffResult {
    pub success: bool,
    pub edits: Option<Vec<Edit>>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct PlanResult {
    pub success: bool,
    pub plan: Option<PlanInfo>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct PlanInfo {
    pub root: Step,
    pub dag: HashMap<usize, Vec<usize>>,
    pub leaves: Vec<Step>,
}

// ============================================================================
// Lineage, Diff & Planner - Helper
// ============================================================================

/// Parse SQL with a given dialect string, returning the first expression.
fn parse_first(sql: &str, dialect: &str) -> Result<(Expression, DialectType), String> {
    let dialect_type = dialect
        .parse::<DialectType>()
        .map_err(|e| format!("Invalid dialect: {}", e))?;

    let d = Dialect::get(dialect_type);
    let exprs = d.parse(sql).map_err(|e| e.to_string())?;
    let first = exprs
        .into_iter()
        .next()
        .ok_or_else(|| "No SQL statements found".to_string())?;
    Ok((first, dialect_type))
}

// ============================================================================
// Lineage WASM functions
// ============================================================================

/// Trace column lineage through a SQL query.
///
/// # Arguments
/// * `sql` - SQL string to analyze
/// * `column` - Column name to trace (e.g. "id", "users.name")
/// * `dialect` - Dialect for parsing (e.g. "generic", "postgres")
/// * `trim_selects` - Trim SELECT to only target column
///
/// # Returns
/// JSON string containing LineageResult
#[wasm_bindgen]
pub fn lineage_sql(sql: &str, column: &str, dialect: &str, trim_selects: bool) -> String {
    set_panic_hook();

    let result = lineage_internal(sql, column, dialect, trim_selects);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn lineage_internal(sql: &str, column: &str, dialect: &str, trim_selects: bool) -> LineageResult {
    let (expr, dialect_type) = match parse_first(sql, dialect) {
        Ok(v) => v,
        Err(e) => {
            return LineageResult {
                success: false,
                lineage: None,
                error: Some(e),
            };
        }
    };

    let dialect_opt = if dialect_type == DialectType::Generic {
        None
    } else {
        Some(dialect_type)
    };

    match lineage::lineage(column, &expr, dialect_opt, trim_selects) {
        Ok(node) => LineageResult {
            success: true,
            lineage: Some(node),
            error: None,
        },
        Err(e) => LineageResult {
            success: false,
            lineage: None,
            error: Some(e.to_string()),
        },
    }
}

/// Get all source tables that feed into a column.
///
/// # Arguments
/// * `sql` - SQL string to analyze
/// * `column` - Column name to trace
/// * `dialect` - Dialect for parsing
///
/// # Returns
/// JSON string containing SourceTablesResult
#[wasm_bindgen]
pub fn source_tables(sql: &str, column: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = source_tables_internal(sql, column, dialect);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn source_tables_internal(sql: &str, column: &str, dialect: &str) -> SourceTablesResult {
    let (expr, dialect_type) = match parse_first(sql, dialect) {
        Ok(v) => v,
        Err(e) => {
            return SourceTablesResult {
                success: false,
                tables: None,
                error: Some(e),
            };
        }
    };

    let dialect_opt = if dialect_type == DialectType::Generic {
        None
    } else {
        Some(dialect_type)
    };

    match lineage::lineage(column, &expr, dialect_opt, false) {
        Ok(node) => {
            let tables = lineage::get_source_tables(&node);
            let mut sorted: Vec<String> = tables.into_iter().collect();
            sorted.sort();
            SourceTablesResult {
                success: true,
                tables: Some(sorted),
                error: None,
            }
        }
        Err(e) => SourceTablesResult {
            success: false,
            tables: None,
            error: Some(e.to_string()),
        },
    }
}

// ============================================================================
// Diff WASM functions
// ============================================================================

/// Diff two SQL statements and return edit operations.
///
/// # Arguments
/// * `source_sql` - Source SQL string
/// * `target_sql` - Target SQL string
/// * `dialect` - Dialect for parsing
/// * `delta_only` - Exclude 'keep' edits from result
/// * `f` - Dice coefficient threshold for internal nodes (default 0.6)
/// * `t` - Leaf similarity threshold (default 0.6)
///
/// # Returns
/// JSON string containing DiffResult
#[wasm_bindgen]
pub fn diff_sql(
    source_sql: &str,
    target_sql: &str,
    dialect: &str,
    delta_only: bool,
    f: f64,
    t: f64,
) -> String {
    set_panic_hook();

    let result = diff_internal(source_sql, target_sql, dialect, delta_only, f, t);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn diff_internal(
    source_sql: &str,
    target_sql: &str,
    dialect: &str,
    delta_only: bool,
    f: f64,
    t: f64,
) -> DiffResult {
    let (src_expr, dialect_type) = match parse_first(source_sql, dialect) {
        Ok(v) => v,
        Err(e) => {
            return DiffResult {
                success: false,
                edits: None,
                error: Some(format!("Source SQL error: {}", e)),
            };
        }
    };

    let (tgt_expr, _) = match parse_first(target_sql, dialect) {
        Ok(v) => v,
        Err(e) => {
            return DiffResult {
                success: false,
                edits: None,
                error: Some(format!("Target SQL error: {}", e)),
            };
        }
    };

    let dialect_opt = if dialect_type == DialectType::Generic {
        None
    } else {
        Some(dialect_type)
    };

    let config = DiffConfig {
        f,
        t,
        dialect: dialect_opt,
    };

    let edits = diff_with_config(&src_expr, &tgt_expr, delta_only, &config);
    DiffResult {
        success: true,
        edits: Some(edits),
        error: None,
    }
}

// ============================================================================
// Planner WASM functions
// ============================================================================

/// Build an execution plan from a SQL query.
///
/// # Arguments
/// * `sql` - SQL string to plan
/// * `dialect` - Dialect for parsing
///
/// # Returns
/// JSON string containing PlanResult
#[wasm_bindgen]
pub fn plan(sql: &str, dialect: &str) -> String {
    set_panic_hook();

    let result = plan_internal(sql, dialect);
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn plan_internal(sql: &str, dialect: &str) -> PlanResult {
    let (expr, _) = match parse_first(sql, dialect) {
        Ok(v) => v,
        Err(e) => {
            return PlanResult {
                success: false,
                plan: None,
                error: Some(e),
            };
        }
    };

    match Plan::from_expression(&expr) {
        Some(mut p) => {
            // Convert dag HashSet<usize> → Vec<usize> for JSON
            let dag: HashMap<usize, Vec<usize>> = p
                .dag()
                .iter()
                .map(|(&k, v)| {
                    let mut sorted: Vec<usize> = v.iter().cloned().collect();
                    sorted.sort();
                    (k, sorted)
                })
                .collect();

            let leaves: Vec<Step> = p.leaves().into_iter().cloned().collect();

            PlanResult {
                success: true,
                plan: Some(PlanInfo {
                    root: p.root.clone(),
                    dag,
                    leaves,
                }),
                error: None,
            }
        }
        None => PlanResult {
            success: false,
            plan: None,
            error: Some("Could not build execution plan from the given SQL".to_string()),
        },
    }
}

// ============================================================================
// AST Getters & Transforms - Result types
// ============================================================================

#[derive(Serialize, Deserialize)]
pub struct StringArrayResult {
    pub success: bool,
    pub result: Option<Vec<String>>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct AstResult {
    pub success: bool,
    pub ast: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ScalarResult {
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

fn serialize_error_json(e: impl std::fmt::Display) -> String {
    format!(
        r#"{{"success":false,"error":"Serialization error: {}"}}"#,
        e
    )
}

// ============================================================================
// AST Getter Functions
// ============================================================================

/// Get all column names from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_column_names(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let names = ast_transforms::get_column_names(&expr);
            StringArrayResult {
                success: true,
                result: Some(names),
                error: None,
            }
        }
        Err(e) => StringArrayResult {
            success: false,
            result: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all table names from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_table_names(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let names = ast_transforms::get_table_names(&expr);
            StringArrayResult {
                success: true,
                result: Some(names),
                error: None,
            }
        }
        Err(e) => StringArrayResult {
            success: false,
            result: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all aggregate functions from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_aggregate_functions(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let exprs = ast_transforms::get_aggregate_functions(&expr);
            let cloned: Vec<Expression> = exprs.into_iter().cloned().collect();
            match serde_json::to_string(&cloned) {
                Ok(json) => AstResult {
                    success: true,
                    ast: Some(json),
                    error: None,
                },
                Err(e) => AstResult {
                    success: false,
                    ast: None,
                    error: Some(e.to_string()),
                },
            }
        }
        Err(e) => AstResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all window functions from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_window_functions(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let exprs = ast_transforms::get_window_functions(&expr);
            let cloned: Vec<Expression> = exprs.into_iter().cloned().collect();
            match serde_json::to_string(&cloned) {
                Ok(json) => AstResult {
                    success: true,
                    ast: Some(json),
                    error: None,
                },
                Err(e) => AstResult {
                    success: false,
                    ast: None,
                    error: Some(e.to_string()),
                },
            }
        }
        Err(e) => AstResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all function calls from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_functions(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let exprs = ast_transforms::get_functions(&expr);
            let cloned: Vec<Expression> = exprs.into_iter().cloned().collect();
            match serde_json::to_string(&cloned) {
                Ok(json) => AstResult {
                    success: true,
                    ast: Some(json),
                    error: None,
                },
                Err(e) => AstResult {
                    success: false,
                    ast: None,
                    error: Some(e.to_string()),
                },
            }
        }
        Err(e) => AstResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all subqueries from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_subqueries(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let exprs = ast_transforms::get_subqueries(&expr);
            let cloned: Vec<Expression> = exprs.into_iter().cloned().collect();
            match serde_json::to_string(&cloned) {
                Ok(json) => AstResult {
                    success: true,
                    ast: Some(json),
                    error: None,
                },
                Err(e) => AstResult {
                    success: false,
                    ast: None,
                    error: Some(e.to_string()),
                },
            }
        }
        Err(e) => AstResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Get all literals from an AST (as JSON string).
#[wasm_bindgen]
pub fn ast_get_literals(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let exprs = ast_transforms::get_literals(&expr);
            let cloned: Vec<Expression> = exprs.into_iter().cloned().collect();
            match serde_json::to_string(&cloned) {
                Ok(json) => AstResult {
                    success: true,
                    ast: Some(json),
                    error: None,
                },
                Err(e) => AstResult {
                    success: false,
                    ast: None,
                    error: Some(e.to_string()),
                },
            }
        }
        Err(e) => AstResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Count the total number of nodes in an AST.
#[wasm_bindgen]
pub fn ast_node_count(ast_json: &str) -> String {
    set_panic_hook();
    let result = match serde_json::from_str::<Expression>(ast_json) {
        Ok(expr) => {
            let count = ast_transforms::node_count(&expr);
            ScalarResult {
                success: true,
                result: Some(serde_json::Value::Number(serde_json::Number::from(count))),
                error: None,
            }
        }
        Err(e) => ScalarResult {
            success: false,
            result: None,
            error: Some(e.to_string()),
        },
    };
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

// ============================================================================
// AST Transform Functions
// ============================================================================

/// Rename columns in an AST.
#[wasm_bindgen]
pub fn ast_rename_columns(ast_json: &str, mapping_json: &str) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let mapping: HashMap<String, String> =
            serde_json::from_str(mapping_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::rename_columns(expr, &mapping);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Rename tables in an AST.
#[wasm_bindgen]
pub fn ast_rename_tables(ast_json: &str, mapping_json: &str) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let mapping: HashMap<String, String> =
            serde_json::from_str(mapping_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::rename_tables(expr, &mapping);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Qualify unqualified column references with a table name.
#[wasm_bindgen]
pub fn ast_qualify_columns(ast_json: &str, table_name: &str) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::qualify_columns(expr, table_name);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Add a WHERE condition to a SELECT AST.
#[wasm_bindgen]
pub fn ast_add_where(ast_json: &str, condition_json: &str, use_or: bool) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let condition: Expression =
            serde_json::from_str(condition_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::add_where(expr, condition, use_or);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Remove the WHERE clause from a SELECT AST.
#[wasm_bindgen]
pub fn ast_remove_where(ast_json: &str) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::remove_where(expr);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Set the LIMIT on a SELECT AST.
#[wasm_bindgen]
pub fn ast_set_limit(ast_json: &str, limit: u32) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::set_limit(expr, limit as usize);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

/// Set DISTINCT on a SELECT AST.
#[wasm_bindgen]
pub fn ast_set_distinct(ast_json: &str, distinct: bool) -> String {
    set_panic_hook();
    let result = (|| -> Result<AstResult, String> {
        let expr: Expression = serde_json::from_str(ast_json).map_err(|e| e.to_string())?;
        let transformed = ast_transforms::set_distinct(expr, distinct);
        let json = serde_json::to_string(&transformed).map_err(|e| e.to_string())?;
        Ok(AstResult {
            success: true,
            ast: Some(json),
            error: None,
        })
    })()
    .unwrap_or_else(|e| AstResult {
        success: false,
        ast: None,
        error: Some(e),
    });
    serde_json::to_string(&result).unwrap_or_else(|e| serialize_error_json(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Basic Success Tests
    // ============================================================================

    #[test]
    fn test_transpile() {
        let result = transpile("SELECT 1", "generic", "postgres");
        assert!(result.contains("success"));
        assert!(result.contains("true"));
    }

    #[test]
    fn test_parse() {
        let result = parse("SELECT 1", "generic");
        assert!(result.contains("success"));
        assert!(result.contains("true"));
    }

    #[test]
    #[cfg(feature = "all-dialects")]
    fn test_get_dialects() {
        let result = get_dialects();
        assert!(result.contains("postgresql"));
        assert!(result.contains("mysql"));
        assert!(result.contains("athena"));
        assert!(result.contains("exasol"));
    }

    #[test]
    fn test_validate_valid_sql() {
        let result = validate("SELECT 1", "generic");
        assert!(result.contains("\"valid\":true"));
    }

    #[test]
    fn test_validate_invalid_sql() {
        let result = validate("SELECT FROM", "generic");
        assert!(result.contains("\"valid\":false"));
        assert!(result.contains("\"severity\":\"error\""));
    }

    #[test]
    fn test_validate_with_options_strict_syntax_trailing_comma() {
        let options = r#"{"strictSyntax":true}"#;
        let result = validate_with_options("SELECT name, FROM employees", "generic", options);
        assert!(result.contains("\"valid\":false"), "Result: {}", result);
        assert!(result.contains("\"code\":\"E005\""), "Result: {}", result);
    }

    #[test]
    fn test_validate_with_schema_valid_sql() {
        let schema = r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "varchar"}
                    ]
                }
            ],
            "strict": true
        }"#;
        let options = r#"{"semantic":false}"#;
        let result = validate_with_schema("SELECT id FROM users", schema, "generic", options);
        assert!(result.contains("\"valid\":true"), "Result: {}", result);
    }

    #[test]
    fn test_validate_with_schema_unknown_table() {
        let schema = r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "integer"}
                    ]
                }
            ],
            "strict": true
        }"#;
        let options = r#"{}"#;
        let result = validate_with_schema("SELECT id FROM orders", schema, "generic", options);
        assert!(result.contains("\"valid\":false"), "Result: {}", result);
        assert!(result.contains("\"code\":\"E200\""), "Result: {}", result);
    }

    #[test]
    #[cfg(any(
        feature = "function-catalog-clickhouse",
        feature = "function-catalog-all-dialects"
    ))]
    fn test_validate_with_schema_embedded_function_catalog_unknown_function() {
        let schema = r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "integer"}
                    ]
                }
            ],
            "strict": true
        }"#;
        let options = r#"{"check_types":true}"#;
        let result = validate_with_schema(
            "SELECT made_up_fn(id) FROM users",
            schema,
            "clickhouse",
            options,
        );
        assert!(result.contains("\"valid\":false"), "Result: {}", result);
        assert!(result.contains("\"code\":\"E202\""), "Result: {}", result);
    }

    #[test]
    fn test_validate_with_schema_semantic_warning() {
        let schema = r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "integer"}
                    ]
                }
            ],
            "strict": true
        }"#;
        let options = r#"{"semantic":true}"#;
        let result =
            validate_with_schema("SELECT * FROM users LIMIT 10", schema, "generic", options);
        assert!(result.contains("\"valid\":true"), "Result: {}", result);
        assert!(result.contains("\"code\":\"W001\""), "Result: {}", result);
        assert!(result.contains("\"code\":\"W004\""), "Result: {}", result);
    }

    #[test]
    fn test_validate_with_schema_reference_check_error() {
        let schema = r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "integer", "primaryKey": true}
                    ],
                    "primaryKey": ["id"]
                },
                {
                    "name": "orders",
                    "columns": [
                        {
                            "name": "user_id",
                            "type": "integer",
                            "references": {"table": "missing_users", "column": "id"}
                        }
                    ]
                }
            ],
            "strict": true
        }"#;
        let options = r#"{"check_references":true}"#;
        let result = validate_with_schema("SELECT 1", schema, "generic", options);
        assert!(result.contains("\"valid\":false"), "Result: {}", result);
        assert!(result.contains("\"code\":\"E220\""), "Result: {}", result);
    }

    // ============================================================================
    // Invalid Dialect Tests
    // ============================================================================

    #[test]
    fn test_transpile_invalid_read_dialect() {
        let result = transpile("SELECT 1", "invalid_dialect", "postgres");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid read dialect"),
            "Expected invalid dialect error: {}",
            result
        );
    }

    #[test]
    fn test_transpile_invalid_write_dialect() {
        let result = transpile("SELECT 1", "generic", "invalid_dialect");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid write dialect"),
            "Expected invalid dialect error: {}",
            result
        );
    }

    #[test]
    fn test_parse_invalid_dialect() {
        let result = parse("SELECT 1", "invalid_dialect");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid dialect"),
            "Expected invalid dialect error: {}",
            result
        );
    }

    #[test]
    fn test_generate_invalid_dialect() {
        let result = generate("[{\"literal\":{\"string\":\"1\"}}]", "invalid_dialect");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid dialect"),
            "Expected invalid dialect error: {}",
            result
        );
    }

    #[test]
    fn test_validate_invalid_dialect() {
        let result = validate("SELECT 1", "invalid_dialect");
        assert!(result.contains("\"valid\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid dialect"),
            "Expected invalid dialect error: {}",
            result
        );
    }

    #[test]
    fn test_format_invalid_dialect() {
        let result = format_sql("SELECT 1", "invalid_dialect");
        assert!(result.contains("error"), "Result: {}", result);
    }

    // ============================================================================
    // Invalid SQL Tests
    // ============================================================================

    #[test]
    #[ignore] // TODO: Parser panics on incomplete input instead of returning error
    fn test_transpile_invalid_sql() {
        let result = transpile("SELECT (", "generic", "postgres");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("error"),
            "Expected error message: {}",
            result
        );
    }

    #[test]
    #[ignore] // TODO: Parser panics on incomplete input instead of returning error
    fn test_parse_invalid_sql() {
        let result = parse("SELECT (", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("error"),
            "Expected error message: {}",
            result
        );
    }

    #[test]
    #[ignore] // TODO: Parser panics on incomplete input instead of returning error
    fn test_format_invalid_sql() {
        let result = format_sql("SELECT (", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("error"),
            "Expected error message: {}",
            result
        );
    }

    // Test with different invalid SQL that doesn't cause panic
    #[test]
    fn test_transpile_missing_from() {
        let result = transpile("SELECT * users", "generic", "postgres");
        // This may succeed or fail depending on parser behavior
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(
            parsed.get("success").is_some(),
            "Should return success field: {}",
            result
        );
    }

    #[test]
    fn test_parse_syntax_error() {
        // Test with unbalanced parenthesis that parser handles
        let result = parse("SELECT 1 + 2)", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    // ============================================================================
    // Invalid AST JSON Tests
    // ============================================================================

    #[test]
    fn test_generate_invalid_json() {
        let result = generate("not valid json", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid AST JSON"),
            "Expected JSON error: {}",
            result
        );
    }

    #[test]
    fn test_generate_empty_json() {
        let result = generate("", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(
            result.contains("Invalid AST JSON"),
            "Expected JSON error: {}",
            result
        );
    }

    #[test]
    fn test_generate_null_json() {
        let result = generate("null", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    #[test]
    fn test_generate_malformed_ast() {
        // Valid JSON but not a valid AST structure
        let result = generate("{\"foo\": \"bar\"}", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    // ============================================================================
    // Empty Input Tests
    // ============================================================================

    #[test]
    fn test_transpile_empty_sql() {
        let result = transpile("", "generic", "postgres");
        // Empty input might succeed with empty output or error - both are acceptable
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(
            parsed.get("success").is_some(),
            "Should return success field: {}",
            result
        );
    }

    #[test]
    fn test_parse_empty_sql() {
        let result = parse("", "generic");
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(
            parsed.get("success").is_some(),
            "Should return success field: {}",
            result
        );
    }

    #[test]
    fn test_validate_empty_sql() {
        let result = validate("", "generic");
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(
            parsed.get("valid").is_some(),
            "Should return valid field: {}",
            result
        );
    }

    // ============================================================================
    // Whitespace-only Input Tests
    // ============================================================================

    #[test]
    fn test_transpile_whitespace_only() {
        let result = transpile("   \n\t  ", "generic", "postgres");
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(
            parsed.get("success").is_some(),
            "Should return success field: {}",
            result
        );
    }

    // ============================================================================
    // Dialect Case Sensitivity Tests
    // ============================================================================

    #[test]
    fn test_dialect_case_insensitive() {
        // All these should work
        let result1 = transpile("SELECT 1", "GENERIC", "POSTGRES");
        let result2 = transpile("SELECT 1", "Generic", "PostgreSQL");
        let result3 = transpile("SELECT 1", "generic", "postgresql");

        assert!(
            result1.contains("\"success\":true"),
            "Uppercase failed: {}",
            result1
        );
        assert!(
            result2.contains("\"success\":true"),
            "Mixed case failed: {}",
            result2
        );
        assert!(
            result3.contains("\"success\":true"),
            "Lowercase failed: {}",
            result3
        );
    }

    #[test]
    fn test_dialect_alternate_names() {
        // Test dialect aliases
        let tsql1 = transpile("SELECT 1", "generic", "tsql");
        let tsql2 = transpile("SELECT 1", "generic", "mssql");
        let tsql3 = transpile("SELECT 1", "generic", "sqlserver");

        assert!(tsql1.contains("\"success\":true"), "tsql failed: {}", tsql1);
        assert!(
            tsql2.contains("\"success\":true"),
            "mssql failed: {}",
            tsql2
        );
        assert!(
            tsql3.contains("\"success\":true"),
            "sqlserver failed: {}",
            tsql3
        );
    }

    // ============================================================================
    // Unicode Tests
    // ============================================================================

    #[test]
    fn test_transpile_unicode() {
        let result = transpile("SELECT '日本語', '你好世界'", "generic", "postgres");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        // Unicode should be preserved
        assert!(
            result.contains("日本語") || result.contains(r"\u"),
            "Unicode not preserved: {}",
            result
        );
    }

    #[test]
    fn test_parse_unicode() {
        let result = parse("SELECT '日本語'", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
    }

    // ============================================================================
    // Format SQL Tests
    // ============================================================================

    #[test]
    fn test_format_sql_success() {
        let result = format_sql("SELECT a,b,c FROM t WHERE x=1", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(result.contains("sql"), "Result: {}", result);
    }

    // ============================================================================
    // Generate SQL Tests
    // ============================================================================

    #[test]
    fn test_generate_valid_ast() {
        // First parse to get a valid AST
        let parse_result = parse("SELECT 1", "generic");
        let parsed: serde_json::Value = serde_json::from_str(&parse_result).unwrap();

        if let Some(ast) = parsed.get("ast") {
            let ast_str = ast.as_str().unwrap();
            let result = generate(ast_str, "postgres");
            assert!(result.contains("\"success\":true"), "Result: {}", result);
        }
    }

    // ============================================================================
    // Version Test
    // ============================================================================

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty(), "Version should not be empty");
        // Should be a semver-like string
        assert!(ver.contains("."), "Version should contain dots: {}", ver);
    }

    // ============================================================================
    // All Dialects Parse Test
    // ============================================================================

    #[test]
    fn test_all_dialects_can_parse() {
        let dialects = [
            "generic",
            "postgresql",
            "mysql",
            "bigquery",
            "snowflake",
            "duckdb",
            "sqlite",
            "hive",
            "spark",
            "trino",
            "presto",
            "redshift",
            "tsql",
            "oracle",
            "clickhouse",
            "databricks",
            "athena",
            "teradata",
            "doris",
            "starrocks",
            "materialize",
            "risingwave",
            "singlestore",
            "cockroachdb",
            "tidb",
            "druid",
            "solr",
            "tableau",
            "dune",
            "fabric",
            "drill",
            "dremio",
            "exasol",
            "datafusion",
        ];

        for dialect in dialects {
            let result = parse("SELECT 1", dialect);
            assert!(
                result.contains("\"success\":true"),
                "Dialect {} should parse: {}",
                dialect,
                result
            );
        }
    }

    // ============================================================================
    // All Dialect Pairs Transpile Test
    // ============================================================================

    #[test]
    #[cfg(all(feature = "all-dialects", feature = "transpile"))]
    fn test_all_priority_dialect_pairs() {
        let dialects = [
            "generic",
            "postgresql",
            "mysql",
            "bigquery",
            "snowflake",
            "duckdb",
            "tsql",
        ];

        for from in &dialects {
            for to in &dialects {
                let result = transpile("SELECT 1", from, to);
                assert!(
                    result.contains("\"success\":true"),
                    "Transpile from {} to {} should work: {}",
                    from,
                    to,
                    result
                );
            }
        }
    }

    // ============================================================================
    // Lineage Tests
    // ============================================================================

    #[test]
    fn test_lineage_simple() {
        let result = lineage_sql("SELECT a FROM t", "a", "generic", false);
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(result.contains("\"name\":\"a\""), "Result: {}", result);
    }

    #[test]
    fn test_lineage_invalid_column() {
        let result = lineage_sql("SELECT a FROM t", "nonexistent", "generic", false);
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    #[test]
    fn test_lineage_invalid_dialect() {
        let result = lineage_sql("SELECT a FROM t", "a", "invalid_dialect", false);
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    #[test]
    fn test_source_tables() {
        let result = source_tables("SELECT t.a FROM t JOIN s ON t.id = s.id", "a", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"t\""),
            "Should contain table t: {}",
            result
        );
    }

    #[test]
    fn test_source_tables_invalid_dialect() {
        let result = source_tables("SELECT a FROM t", "a", "invalid_dialect");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    // ============================================================================
    // Diff Tests
    // ============================================================================

    #[test]
    fn test_diff_identical() {
        let result = diff_sql(
            "SELECT a FROM t",
            "SELECT a FROM t",
            "generic",
            false,
            0.6,
            0.6,
        );
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"type\":\"keep\""),
            "Should have keep edits: {}",
            result
        );
    }

    #[test]
    fn test_diff_changes() {
        let result = diff_sql(
            "SELECT col_a FROM t",
            "SELECT col_b FROM t",
            "generic",
            true,
            0.6,
            0.6,
        );
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"type\":\"update\""),
            "Should have update edits: {}",
            result
        );
    }

    #[test]
    fn test_diff_delta_only() {
        let result = diff_sql(
            "SELECT a FROM t",
            "SELECT a FROM t",
            "generic",
            true,
            0.6,
            0.6,
        );
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        // delta_only=true → no keep edits
        assert!(
            !result.contains("\"type\":\"keep\""),
            "Should not have keep edits: {}",
            result
        );
    }

    #[test]
    fn test_diff_invalid_dialect() {
        let result = diff_sql("SELECT 1", "SELECT 2", "invalid_dialect", false, 0.6, 0.6);
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    // ============================================================================
    // Plan Tests
    // ============================================================================

    #[test]
    fn test_plan_simple() {
        let result = plan("SELECT a, b FROM t", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"kind\":\"scan\""),
            "Should have scan step: {}",
            result
        );
    }

    #[test]
    fn test_plan_join() {
        let result = plan("SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"join\""),
            "Should have join step: {}",
            result
        );
    }

    #[test]
    fn test_plan_aggregate() {
        let result = plan("SELECT x, SUM(y) FROM t GROUP BY x", "generic");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        assert!(
            result.contains("\"kind\":\"aggregate\""),
            "Should have aggregate: {}",
            result
        );
    }

    #[test]
    fn test_plan_invalid_dialect() {
        let result = plan("SELECT 1", "invalid_dialect");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
    }

    // ============================================================================
    // AST Getter Tests
    // ============================================================================

    fn parse_first_ast(sql: &str) -> String {
        let d = Dialect::get(DialectType::Generic);
        let exprs = d.parse(sql).unwrap();
        serde_json::to_string(&exprs[0]).unwrap()
    }

    #[test]
    fn test_ast_get_column_names() {
        let ast = parse_first_ast("SELECT a, b, c FROM t");
        let result = ast_get_column_names(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        let names = parsed["result"].as_array().unwrap();
        assert!(names.iter().any(|n| n.as_str() == Some("a")));
        assert!(names.iter().any(|n| n.as_str() == Some("b")));
        assert!(names.iter().any(|n| n.as_str() == Some("c")));
    }

    #[test]
    fn test_ast_get_table_names() {
        let ast = parse_first_ast("SELECT a FROM users");
        let result = ast_get_table_names(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_get_aggregate_functions() {
        let ast = parse_first_ast("SELECT COUNT(*), SUM(x) FROM t");
        let result = ast_get_aggregate_functions(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        let exprs: Vec<serde_json::Value> =
            serde_json::from_str(parsed["ast"].as_str().unwrap()).unwrap();
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn test_ast_get_window_functions() {
        let ast = parse_first_ast("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t");
        let result = ast_get_window_functions(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        let exprs: Vec<serde_json::Value> =
            serde_json::from_str(parsed["ast"].as_str().unwrap()).unwrap();
        assert_eq!(exprs.len(), 1);
    }

    #[test]
    fn test_ast_get_functions() {
        let ast = parse_first_ast("SELECT UPPER(name), LENGTH(name) FROM t");
        let result = ast_get_functions(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_get_subqueries() {
        let ast = parse_first_ast("SELECT * FROM (SELECT 1 AS x) AS sub");
        let result = ast_get_subqueries(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_get_literals() {
        let ast = parse_first_ast("SELECT 1, 'hello', 3.14");
        let result = ast_get_literals(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        let exprs: Vec<serde_json::Value> =
            serde_json::from_str(parsed["ast"].as_str().unwrap()).unwrap();
        assert_eq!(exprs.len(), 3);
    }

    #[test]
    fn test_ast_node_count() {
        let ast = parse_first_ast("SELECT a FROM t");
        let result = ast_node_count(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        assert!(parsed["result"].as_u64().unwrap() > 0);
    }

    #[test]
    fn test_ast_get_column_names_invalid_json() {
        let result = ast_get_column_names("not valid json");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(!parsed["success"].as_bool().unwrap());
        assert!(parsed["error"].as_str().is_some());
    }

    // ============================================================================
    // AST Transform Tests
    // ============================================================================

    #[test]
    fn test_ast_rename_columns() {
        let ast = parse_first_ast("SELECT a, b FROM t");
        let mapping = r#"{"a":"x","b":"y"}"#;
        let result = ast_rename_columns(&ast, mapping);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
        // Verify the renamed AST contains the new names
        let new_ast = parsed["ast"].as_str().unwrap();
        assert!(new_ast.contains("\"x\"") || new_ast.contains("x"));
    }

    #[test]
    fn test_ast_rename_tables() {
        let ast = parse_first_ast("SELECT a FROM old_table");
        let mapping = r#"{"old_table":"new_table"}"#;
        let result = ast_rename_tables(&ast, mapping);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_qualify_columns() {
        let ast = parse_first_ast("SELECT a, b FROM t");
        let result = ast_qualify_columns(&ast, "t");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_add_where() {
        let ast = parse_first_ast("SELECT a FROM t");
        // Parse a condition expression: parse "SELECT 1" and extract the expression,
        // or build a simple condition via the builder
        let cond_expr = polyglot_sql::builder::col("x").eq(polyglot_sql::builder::lit(1));
        let cond_json = serde_json::to_string(&cond_expr.0).unwrap();
        let result = ast_add_where(&ast, &cond_json, false);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_remove_where() {
        let ast = parse_first_ast("SELECT a FROM t WHERE x = 1");
        let result = ast_remove_where(&ast);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_set_limit() {
        let ast = parse_first_ast("SELECT a FROM t");
        let result = ast_set_limit(&ast, 10);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_set_distinct() {
        let ast = parse_first_ast("SELECT a FROM t");
        let result = ast_set_distinct(&ast, true);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed["success"].as_bool().unwrap(), "Result: {}", result);
    }

    #[test]
    fn test_ast_transform_invalid_json() {
        let result = ast_rename_columns("bad json", "{}");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(!parsed["success"].as_bool().unwrap());
    }

    #[test]
    fn test_ast_transform_invalid_mapping() {
        let ast = parse_first_ast("SELECT a FROM t");
        let result = ast_rename_columns(&ast, "not a map");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(!parsed["success"].as_bool().unwrap());
    }

    // ============================================================================
    // Per-Dialect Build Tests
    //
    // These tests verify that single-dialect WASM builds work correctly.
    // They exercise feature-gated code paths that the full-build tests don't cover.
    // ============================================================================

    /// When all-dialects is disabled, get_dialects() must always include "generic"
    /// and must NOT include all 34 dialects.
    #[test]
    #[cfg(not(feature = "all-dialects"))]
    fn test_per_dialect_get_dialects_subset() {
        let result = get_dialects();
        let dialects: Vec<String> = serde_json::from_str(&result).unwrap();
        assert!(
            dialects.contains(&"generic".to_string()),
            "generic must always be present: {:?}",
            dialects
        );
        assert!(
            dialects.len() < 34,
            "Per-dialect build should have fewer than 34 dialects, got {}",
            dialects.len()
        );
    }

    /// Same-dialect transpile (identity) must work even without the transpile feature.
    #[test]
    #[cfg(not(feature = "transpile"))]
    fn test_per_dialect_same_dialect_transpile() {
        // Generic always works
        let result = transpile("SELECT 1", "generic", "generic");
        assert!(
            result.contains("\"success\":true"),
            "Generic identity transpile should work: {}",
            result
        );
    }

    /// To/from generic must work without the transpile feature.
    #[test]
    #[cfg(not(feature = "transpile"))]
    fn test_per_dialect_to_from_generic_transpile() {
        let result = transpile("SELECT 1", "generic", "generic");
        assert!(
            result.contains("\"success\":true"),
            "Generic→Generic should work: {}",
            result
        );
    }

    /// Cross-dialect transpile must return an error without the transpile feature.
    #[test]
    #[cfg(not(feature = "transpile"))]
    fn test_per_dialect_cross_dialect_transpile_error() {
        // Try a cross-dialect transpile that should fail
        // We use two non-generic dialects; at least one may not be compiled in,
        // but the transpile error should fire before the "unknown dialect" error
        // for any pair of known non-generic dialects.
        let result = transpile("SELECT 1", "clickhouse", "postgresql");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        // Should either fail with "Cross-dialect transpilation not available"
        // or "Invalid read/write dialect" (if the dialect isn't compiled in).
        assert!(
            !parsed["success"].as_bool().unwrap_or(true),
            "Cross-dialect transpile should fail without transpile feature: {}",
            result
        );
    }

    // ---- Per-dialect: ClickHouse ------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-clickhouse")]
    fn test_clickhouse_parse() {
        let result = parse("SELECT toDate('2024-01-01')", "clickhouse");
        assert!(
            result.contains("\"success\":true"),
            "ClickHouse parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-clickhouse")]
    fn test_clickhouse_format() {
        let result = format_sql("SELECT   a,b FROM  t  WHERE x=1", "clickhouse");
        assert!(
            result.contains("\"success\":true"),
            "ClickHouse format failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-clickhouse")]
    fn test_clickhouse_validate() {
        let result = validate("SELECT 1", "clickhouse");
        assert!(
            result.contains("\"valid\":true"),
            "ClickHouse validate failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-clickhouse")]
    fn test_clickhouse_identity_transpile() {
        let result = transpile("SELECT 1", "clickhouse", "clickhouse");
        assert!(
            result.contains("\"success\":true"),
            "ClickHouse identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-clickhouse")]
    fn test_clickhouse_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("clickhouse"),
            "ClickHouse should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: PostgreSQL -------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-postgresql")]
    fn test_postgresql_parse() {
        let result = parse("SELECT now()::date", "postgresql");
        assert!(
            result.contains("\"success\":true"),
            "PostgreSQL parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-postgresql")]
    fn test_postgresql_format() {
        let result = format_sql("SELECT  a,b FROM  t  WHERE x=1", "postgresql");
        assert!(
            result.contains("\"success\":true"),
            "PostgreSQL format failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-postgresql")]
    fn test_postgresql_validate() {
        let result = validate("SELECT 1", "postgresql");
        assert!(
            result.contains("\"valid\":true"),
            "PostgreSQL validate failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-postgresql")]
    fn test_postgresql_identity_transpile() {
        let result = transpile("SELECT 1", "postgresql", "postgresql");
        assert!(
            result.contains("\"success\":true"),
            "PostgreSQL identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-postgresql")]
    fn test_postgresql_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("postgresql"),
            "PostgreSQL should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: MySQL -----------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-mysql")]
    fn test_mysql_parse() {
        let result = parse("SELECT `col` FROM `table`", "mysql");
        assert!(
            result.contains("\"success\":true"),
            "MySQL parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-mysql")]
    fn test_mysql_identity_transpile() {
        let result = transpile("SELECT 1", "mysql", "mysql");
        assert!(
            result.contains("\"success\":true"),
            "MySQL identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-mysql")]
    fn test_mysql_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("mysql"),
            "MySQL should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: BigQuery ---------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-bigquery")]
    fn test_bigquery_parse() {
        let result = parse(
            "SELECT `project.dataset.table`.col FROM `project.dataset.table`",
            "bigquery",
        );
        assert!(
            result.contains("\"success\":true"),
            "BigQuery parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-bigquery")]
    fn test_bigquery_identity_transpile() {
        let result = transpile("SELECT 1", "bigquery", "bigquery");
        assert!(
            result.contains("\"success\":true"),
            "BigQuery identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-bigquery")]
    fn test_bigquery_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("bigquery"),
            "BigQuery should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: Snowflake -------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-snowflake")]
    fn test_snowflake_parse() {
        let result = parse("SELECT $1 FROM @my_stage", "snowflake");
        assert!(
            result.contains("\"success\":true"),
            "Snowflake parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-snowflake")]
    fn test_snowflake_identity_transpile() {
        let result = transpile("SELECT 1", "snowflake", "snowflake");
        assert!(
            result.contains("\"success\":true"),
            "Snowflake identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-snowflake")]
    fn test_snowflake_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("snowflake"),
            "Snowflake should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: DuckDB ----------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-duckdb")]
    fn test_duckdb_parse() {
        let result = parse("SELECT * EXCLUDE (col) FROM t", "duckdb");
        assert!(
            result.contains("\"success\":true"),
            "DuckDB parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-duckdb")]
    fn test_duckdb_identity_transpile() {
        let result = transpile("SELECT 1", "duckdb", "duckdb");
        assert!(
            result.contains("\"success\":true"),
            "DuckDB identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-duckdb")]
    fn test_duckdb_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("duckdb"),
            "DuckDB should be in dialects list: {}",
            result
        );
    }

    // ---- Per-dialect: TSQL ------------------------------------------------------

    #[test]
    #[cfg(feature = "dialect-tsql")]
    fn test_tsql_parse() {
        let result = parse("SELECT TOP 10 * FROM [dbo].[table]", "tsql");
        assert!(
            result.contains("\"success\":true"),
            "TSQL parse failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-tsql")]
    fn test_tsql_identity_transpile() {
        let result = transpile("SELECT 1", "tsql", "tsql");
        assert!(
            result.contains("\"success\":true"),
            "TSQL identity transpile failed: {}",
            result
        );
    }

    #[test]
    #[cfg(feature = "dialect-tsql")]
    fn test_tsql_in_dialects_list() {
        let result = get_dialects();
        assert!(
            result.contains("tsql"),
            "TSQL should be in dialects list: {}",
            result
        );
    }

    // ============================================================================
    // Error Position Tests
    // ============================================================================

    #[test]
    fn test_parse_error_has_position() {
        let result = parse("SELECT 1 + 2)", "generic");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(
            !parsed["success"].as_bool().unwrap(),
            "Should fail: {}",
            result
        );
        assert!(
            parsed["errorLine"].is_number(),
            "Should have errorLine: {}",
            result
        );
        assert!(
            parsed["errorColumn"].is_number(),
            "Should have errorColumn: {}",
            result
        );
        assert_eq!(parsed["errorLine"].as_u64().unwrap(), 1);
    }

    #[test]
    fn test_transpile_error_has_position() {
        let result = transpile("SELECT 1 + 2)", "generic", "postgres");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(
            !parsed["success"].as_bool().unwrap(),
            "Should fail: {}",
            result
        );
        assert!(
            parsed["errorLine"].is_number(),
            "Should have errorLine: {}",
            result
        );
        assert!(
            parsed["errorColumn"].is_number(),
            "Should have errorColumn: {}",
            result
        );
    }

    #[test]
    fn test_success_result_has_no_position() {
        let result = parse("SELECT 1", "generic");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(
            parsed["success"].as_bool().unwrap(),
            "Should succeed: {}",
            result
        );
        assert!(
            parsed["errorLine"].is_null(),
            "Should not have errorLine on success: {}",
            result
        );
        assert!(
            parsed["errorColumn"].is_null(),
            "Should not have errorColumn on success: {}",
            result
        );
    }
}
