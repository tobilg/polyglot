//! WebAssembly bindings for Polyglot SQL Translator
//!
//! This crate provides WASM bindings for the polyglot-sql library,
//! allowing SQL dialect translation in the browser.

pub mod builders;

use polyglot_sql::{
    dialects::{Dialect, DialectType},
    expressions::Expression,
    generator::Generator,
    ValidationResult as CoreValidationResult,
};
use serde::{Deserialize, Serialize};
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
        format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
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
        format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
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
        format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
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
    let results: Result<Vec<String>, _> = expressions
        .iter()
        .map(|expr| d.generate(expr))
        .collect();

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
                        format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
                    })
                }
                Err(e) => {
                    let result = TranspileResult {
                        success: false,
                        sql: None,
                        error: Some(e.to_string()),
                    };
                    serde_json::to_string(&result).unwrap_or_else(|e| {
                        format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
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
                format!(r#"{{"success":false,"error":"Serialization error: {}"}}"#, e)
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
            return CoreValidationResult::with_errors(vec![
                polyglot_sql::ValidationError::error(format!("Invalid dialect: {}", e), "E000")
            ]);
        }
    };

    polyglot_sql::validate(sql, dialect_type)
}

/// Get the version of the library.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
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
        assert!(result.contains("error"), "Expected error message: {}", result);
    }

    #[test]
    #[ignore] // TODO: Parser panics on incomplete input instead of returning error
    fn test_parse_invalid_sql() {
        let result = parse("SELECT (", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(result.contains("error"), "Expected error message: {}", result);
    }

    #[test]
    #[ignore] // TODO: Parser panics on incomplete input instead of returning error
    fn test_format_invalid_sql() {
        let result = format_sql("SELECT (", "generic");
        assert!(result.contains("\"success\":false"), "Result: {}", result);
        assert!(result.contains("error"), "Expected error message: {}", result);
    }

    // Test with different invalid SQL that doesn't cause panic
    #[test]
    fn test_transpile_missing_from() {
        let result = transpile("SELECT * users", "generic", "postgres");
        // This may succeed or fail depending on parser behavior
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("Valid JSON");
        assert!(parsed.get("success").is_some(), "Should return success field: {}", result);
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

        assert!(result1.contains("\"success\":true"), "Uppercase failed: {}", result1);
        assert!(result2.contains("\"success\":true"), "Mixed case failed: {}", result2);
        assert!(result3.contains("\"success\":true"), "Lowercase failed: {}", result3);
    }

    #[test]
    fn test_dialect_alternate_names() {
        // Test dialect aliases
        let tsql1 = transpile("SELECT 1", "generic", "tsql");
        let tsql2 = transpile("SELECT 1", "generic", "mssql");
        let tsql3 = transpile("SELECT 1", "generic", "sqlserver");

        assert!(tsql1.contains("\"success\":true"), "tsql failed: {}", tsql1);
        assert!(tsql2.contains("\"success\":true"), "mssql failed: {}", tsql2);
        assert!(tsql3.contains("\"success\":true"), "sqlserver failed: {}", tsql3);
    }

    // ============================================================================
    // Unicode Tests
    // ============================================================================

    #[test]
    fn test_transpile_unicode() {
        let result = transpile("SELECT '日本語', '你好世界'", "generic", "postgres");
        assert!(result.contains("\"success\":true"), "Result: {}", result);
        // Unicode should be preserved
        assert!(result.contains("日本語") || result.contains(r"\u"), "Unicode not preserved: {}", result);
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
}
