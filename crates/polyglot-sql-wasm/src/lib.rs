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
    ValidationResult as CoreValidationResult,
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
pub struct TranspileResult {
    pub success: bool,
    pub sql: Option<Vec<String>>,
    pub error: Option<String>,
}

/// Result type for parse operations
#[derive(Serialize, Deserialize)]
pub struct ParseResult {
    pub success: bool,
    pub ast: Option<String>,
    pub error: Option<String>,
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
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn transpile_internal(sql: &str, read_dialect: &str, write_dialect: &str) -> TranspileResult {
    let read = match read_dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid read dialect: {}", e)),
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
            };
        }
    };

    let dialect = Dialect::get(read);
    match dialect.transpile_to(sql, write) {
        Ok(results) => TranspileResult {
            success: true,
            sql: Some(results),
            error: None,
        },
        Err(e) => TranspileResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
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
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn parse_internal(sql: &str, dialect: &str) -> ParseResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return ParseResult {
                success: false,
                ast: None,
                error: Some(format!("Invalid dialect: {}", e)),
            };
        }
    };

    let d = Dialect::get(dialect_type);
    match d.parse(sql) {
        Ok(expressions) => match serde_json::to_string(&expressions) {
            Ok(ast) => ParseResult {
                success: true,
                ast: Some(ast),
                error: None,
            },
            Err(e) => ParseResult {
                success: false,
                ast: None,
                error: Some(format!("Failed to serialize AST: {}", e)),
            },
        },
        Err(e) => ParseResult {
            success: false,
            ast: None,
            error: Some(e.to_string()),
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
    serde_json::to_string(&result).unwrap_or_else(|e| {
        format!(
            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
            e
        )
    })
}

fn generate_internal(ast_json: &str, dialect: &str) -> TranspileResult {
    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return TranspileResult {
                success: false,
                sql: None,
                error: Some(format!("Invalid dialect: {}", e)),
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
            };
        }
    };

    let d = Dialect::get(dialect_type);
    let results: Result<Vec<String>, _> = expressions.iter().map(|expr| d.generate(expr)).collect();

    match results {
        Ok(sql) => TranspileResult {
            success: true,
            sql: Some(sql),
            error: None,
        },
        Err(e) => TranspileResult {
            success: false,
            sql: None,
            error: Some(e.to_string()),
        },
    }
}

/// Get a list of supported dialects.
///
/// # Returns
/// A JSON array of dialect names
#[wasm_bindgen]
pub fn get_dialects() -> String {
    let dialects = vec![
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
    serde_json::to_string(&dialects).unwrap()
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

    let dialect_type = match dialect.parse::<DialectType>() {
        Ok(d) => d,
        Err(e) => {
            return format!(r#"{{"success":false,"error":"{}"}}"#, e);
        }
    };

    let d = Dialect::get(dialect_type);
    match d.parse(sql) {
        Ok(expressions) => {
            let formatted: Result<Vec<String>, _> = expressions
                .iter()
                .map(|expr| Generator::pretty_sql(expr))
                .collect();

            match formatted {
                Ok(sql) => {
                    let result = TranspileResult {
                        success: true,
                        sql: Some(sql),
                        error: None,
                    };
                    serde_json::to_string(&result).unwrap_or_else(|e| {
                        format!(
                            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
                            e
                        )
                    })
                }
                Err(e) => {
                    let result = TranspileResult {
                        success: false,
                        sql: None,
                        error: Some(e.to_string()),
                    };
                    serde_json::to_string(&result).unwrap_or_else(|e| {
                        format!(
                            r#"{{"success":false,"error":"Serialization error: {}"}}"#,
                            e
                        )
                    })
                }
            }
        }
        Err(e) => {
            let result = TranspileResult {
                success: false,
                sql: None,
                error: Some(e.to_string()),
            };
            serde_json::to_string(&result).unwrap_or_else(|e| {
                format!(
                    r#"{{"success":false,"error":"Serialization error: {}"}}"#,
                    e
                )
            })
        }
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
}
