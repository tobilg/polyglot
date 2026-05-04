//! Dialect Matrix Transpilation Tests
//!
//! Tests transpilation between all priority dialect pairs (7 dialects = 42 pairs).
//! Priority dialects: Generic, PostgreSQL, MySQL, BigQuery, Snowflake, DuckDB, TSQL
//!
//! Each test ensures SQL can be transpiled from one dialect to another
//! with expected function and syntax transformations.

use polyglot_sql::dialects::{Dialect, DialectType};

/// Helper function to test transpilation between dialects
fn transpile(sql: &str, from: DialectType, to: DialectType) -> String {
    let source_dialect = Dialect::get(from);
    let result = source_dialect.transpile(sql, to).expect(&format!(
        "Failed to transpile: {} from {:?} to {:?}",
        sql, from, to
    ));
    result[0].clone()
}

/// Helper to verify transpilation produces valid SQL (doesn't crash)
fn transpile_succeeds(sql: &str, from: DialectType, to: DialectType) -> bool {
    let source_dialect = Dialect::get(from);
    source_dialect.transpile(sql, to).is_ok()
}

mod fabric_regressions {
    use super::*;

    #[test]
    fn test_postgres_to_fabric_tpch_syntax() {
        assert_eq!(
            transpile(
                "SELECT DATE '1998-12-01'",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT CAST('1998-12-01' AS DATE)"
        );
        assert_eq!(
            transpile(
                "SELECT SUBSTRING(c_phone FROM 1 FOR 2)",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT SUBSTRING(c_phone, 1, 2)"
        );
        assert_eq!(
            transpile(
                "SELECT * FROM lineitem LIMIT 10",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT TOP 10 * FROM lineitem"
        );
        assert_eq!(
            transpile(
                "SELECT * FROM lineitem ORDER BY shipdate NULLS FIRST",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT * FROM lineitem ORDER BY shipdate"
        );
    }

    #[test]
    fn test_postgres_to_fabric_interval_arithmetic() {
        assert_eq!(
            transpile(
                "SELECT DATE '1998-12-01' + INTERVAL '90' DAY",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT DATEADD(DAY, 90, CAST('1998-12-01' AS DATE))"
        );
        assert_eq!(
            transpile(
                "SELECT shipdate - INTERVAL '3' DAY FROM lineitem",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT DATEADD(DAY, -3, shipdate) FROM lineitem"
        );
        assert_eq!(
            transpile(
                "SELECT shipdate - INTERVAL '-3' DAY FROM lineitem",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT DATEADD(DAY, 3, shipdate) FROM lineitem"
        );
        assert_eq!(
            transpile(
                "SELECT shipdate + INTERVAL '1 day' FROM lineitem",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT DATEADD(DAY, 1, shipdate) FROM lineitem"
        );
        assert_eq!(
            transpile(
                "SELECT shipdate + INTERVAL n DAY FROM lineitem",
                DialectType::PostgreSQL,
                DialectType::Fabric
            ),
            "SELECT DATEADD(DAY, n, shipdate) FROM lineitem"
        );
    }
}

// ============================================================================
// Basic SELECT Transpilation Tests
// ============================================================================

mod basic_select {
    use super::*;

    #[test]
    fn test_generic_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::DuckDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_postgres_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::DuckDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::PostgreSQL,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_mysql_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::DuckDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::MySQL,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_bigquery_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::DuckDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::BigQuery,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_snowflake_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::DuckDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Snowflake,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_duckdb_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::DuckDB,
            DialectType::TSQL
        ));
    }

    #[test]
    fn test_tsql_to_all() {
        let sql = "SELECT a, b FROM users WHERE id = 1";

        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::Generic
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::PostgreSQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::MySQL
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::BigQuery
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::Snowflake
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::TSQL,
            DialectType::DuckDB
        ));
    }
}

// ============================================================================
// NULL Handling Transpilation Tests (NVL, IFNULL, COALESCE)
// ============================================================================

mod null_handling {
    use super::*;

    // COALESCE should be preserved or converted appropriately
    #[test]
    fn test_coalesce_generic_to_postgres() {
        let result = transpile(
            "SELECT COALESCE(a, b)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.contains("COALESCE"),
            "PostgreSQL should use COALESCE: got {}",
            result
        );
    }

    #[test]
    fn test_coalesce_generic_to_mysql() {
        let result = transpile(
            "SELECT COALESCE(a, b)",
            DialectType::Generic,
            DialectType::MySQL,
        );
        // MySQL supports both COALESCE and IFNULL
        assert!(
            result.contains("COALESCE") || result.contains("IFNULL"),
            "MySQL should use COALESCE or IFNULL: got {}",
            result
        );
    }

    #[test]
    fn test_coalesce_generic_to_tsql() {
        let result = transpile(
            "SELECT COALESCE(a, b)",
            DialectType::Generic,
            DialectType::TSQL,
        );
        // SQL Server should convert 2-arg COALESCE to ISNULL
        assert!(
            result.contains("ISNULL") || result.contains("COALESCE"),
            "TSQL should use ISNULL or COALESCE: got {}",
            result
        );
    }

    // NVL transformations
    #[test]
    fn test_nvl_to_postgres() {
        let result = transpile(
            "SELECT NVL(a, b)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.contains("COALESCE"),
            "PostgreSQL should convert NVL to COALESCE: got {}",
            result
        );
    }

    #[test]
    fn test_nvl_to_mysql() {
        let result = transpile("SELECT NVL(a, b)", DialectType::Generic, DialectType::MySQL);
        assert!(
            result.contains("IFNULL") || result.contains("COALESCE"),
            "MySQL should convert NVL to IFNULL or COALESCE: got {}",
            result
        );
    }

    #[test]
    fn test_nvl_to_tsql() {
        let result = transpile("SELECT NVL(a, b)", DialectType::Generic, DialectType::TSQL);
        assert!(
            result.contains("ISNULL"),
            "TSQL should convert NVL to ISNULL: got {}",
            result
        );
    }

    // IFNULL transformations
    #[test]
    fn test_ifnull_to_postgres() {
        let result = transpile(
            "SELECT IFNULL(a, b)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.contains("COALESCE"),
            "PostgreSQL should convert IFNULL to COALESCE: got {}",
            result
        );
    }

    #[test]
    fn test_ifnull_to_snowflake() {
        let result = transpile(
            "SELECT IFNULL(a, b)",
            DialectType::Generic,
            DialectType::Snowflake,
        );
        // Snowflake supports both
        assert!(
            result.contains("IFNULL") || result.contains("COALESCE"),
            "Snowflake should accept IFNULL or COALESCE: got {}",
            result
        );
    }
}

// ============================================================================
// String Functions Transpilation Tests
// ============================================================================

mod string_functions {
    use super::*;

    // LENGTH vs LEN
    #[test]
    fn test_length_generic_to_postgres() {
        let result = transpile(
            "SELECT LENGTH(name)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("LENGTH"),
            "PostgreSQL uses LENGTH: got {}",
            result
        );
    }

    #[test]
    fn test_length_generic_to_tsql() {
        let result = transpile(
            "SELECT LENGTH(name)",
            DialectType::Generic,
            DialectType::TSQL,
        );
        assert!(
            result.to_uppercase().contains("LEN"),
            "TSQL should convert LENGTH to LEN: got {}",
            result
        );
    }

    // SUBSTR vs SUBSTRING
    #[test]
    fn test_substr_to_postgres() {
        let result = transpile(
            "SELECT SUBSTR(name, 1, 5)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("SUBSTRING") || result.to_uppercase().contains("SUBSTR"),
            "PostgreSQL uses SUBSTRING: got {}",
            result
        );
    }

    // CONCAT transformations
    #[test]
    fn test_concat_generic_to_postgres() {
        // Generic should support CONCAT function
        let result = transpile(
            "SELECT CONCAT(a, b)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("CONCAT") || result.contains("||"),
            "PostgreSQL should use CONCAT or ||: got {}",
            result
        );
    }

    #[test]
    fn test_postgres_dpipe_to_mysql_concat_issue_43() {
        let result = transpile(
            "SELECT 'A' || 'B'",
            DialectType::PostgreSQL,
            DialectType::MySQL,
        );
        assert_eq!(
            result, "SELECT CONCAT('A', 'B')",
            "PostgreSQL || should transpile to MySQL CONCAT: got {}",
            result
        );
    }

    #[test]
    fn test_mysql_dpipe_identity_is_or_issue_43() {
        let result = transpile("SELECT 'A' || 'B'", DialectType::MySQL, DialectType::MySQL);
        assert_eq!(
            result, "SELECT 'A' OR 'B'",
            "MySQL identity should treat || as OR: got {}",
            result
        );
    }

    #[test]
    fn test_generate_mysql_from_postgres_concat_ast_issue_43() {
        let ast = polyglot_sql::parse("SELECT 'A' || 'B'", DialectType::PostgreSQL).expect("parse");
        let mysql = Dialect::get(DialectType::MySQL);
        let sql = mysql.generate(&ast[0]).expect("generate");

        assert_eq!(
            sql, "SELECT CONCAT('A', 'B')",
            "MySQL generate should render semantic concat as CONCAT: got {}",
            sql
        );
    }

    // UPPER/LOWER should be universal
    #[test]
    fn test_upper_lower_preserved() {
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let upper_result = transpile("SELECT UPPER(name)", DialectType::Generic, dialect);
            let lower_result = transpile("SELECT LOWER(name)", DialectType::Generic, dialect);

            assert!(
                upper_result.to_uppercase().contains("UPPER"),
                "{:?} should preserve UPPER: got {}",
                dialect,
                upper_result
            );
            assert!(
                lower_result.to_uppercase().contains("LOWER"),
                "{:?} should preserve LOWER: got {}",
                dialect,
                lower_result
            );
        }
    }
}

// ============================================================================
// Date/Time Functions Transpilation Tests
// ============================================================================

mod date_functions {
    use super::*;

    // NOW() transformations
    #[test]
    fn test_now_to_postgres() {
        let result = transpile(
            "SELECT NOW()",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("NOW")
                || result.to_uppercase().contains("CURRENT_TIMESTAMP"),
            "PostgreSQL should use NOW or CURRENT_TIMESTAMP: got {}",
            result
        );
    }

    #[test]
    fn test_now_to_tsql() {
        let result = transpile("SELECT NOW()", DialectType::Generic, DialectType::TSQL);
        assert!(
            result.to_uppercase().contains("GETDATE")
                || result.to_uppercase().contains("CURRENT_TIMESTAMP"),
            "TSQL should convert NOW to GETDATE: got {}",
            result
        );
    }

    // CURRENT_DATE should be supported or converted
    #[test]
    fn test_current_date_to_all() {
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::Snowflake,
            DialectType::DuckDB,
        ];

        for dialect in dialects {
            let result = transpile("SELECT CURRENT_DATE", DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("CURRENT_DATE")
                    || result.to_uppercase().contains("GETDATE"),
                "{:?} should handle CURRENT_DATE: got {}",
                dialect,
                result
            );
        }
    }
}

// ============================================================================
// JSON Functions Transpilation Tests
// ============================================================================

mod json_functions {
    use super::*;

    #[test]
    fn test_json_search_mysql_to_duckdb_issue_42() {
        let sql = "SELECT JSON_SEARCH(meta, 'one', 'admin', NULL, '$.tags') IS NOT NULL FROM users";
        let result = transpile(sql, DialectType::MySQL, DialectType::DuckDB);
        let upper = result.to_uppercase();

        assert!(
            !upper.contains("JSON_SEARCH("),
            "DuckDB transpilation should rewrite JSON_SEARCH: got {}",
            result
        );
        assert!(
            upper.contains("JSON_TREE("),
            "DuckDB transpilation should use JSON_TREE lookup: got {}",
            result
        );
    }

    #[test]
    fn test_json_search_mysql_identity_preserved() {
        let sql = "SELECT JSON_SEARCH(meta, 'one', 'admin', NULL, '$.tags') FROM users";
        let result = transpile(sql, DialectType::MySQL, DialectType::MySQL);

        assert!(
            result.to_uppercase().contains("JSON_SEARCH("),
            "MySQL identity transpilation should preserve JSON_SEARCH: got {}",
            result
        );
    }
}

// ============================================================================
// Aggregate Functions Transpilation Tests
// ============================================================================

mod aggregate_functions {
    use super::*;

    // Basic aggregates should be universal
    #[test]
    fn test_count_preserved() {
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile("SELECT COUNT(*) FROM t", DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("COUNT"),
                "{:?} should preserve COUNT: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_sum_avg_min_max() {
        let functions = ["SUM", "AVG", "MIN", "MAX"];
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
        ];

        for func in functions {
            for dialect in dialects {
                let sql = format!("SELECT {}(x) FROM t", func);
                let result = transpile(&sql, DialectType::Generic, dialect);
                assert!(
                    result.to_uppercase().contains(func),
                    "{:?} should preserve {}: got {}",
                    dialect,
                    func,
                    result
                );
            }
        }
    }

    // GROUP_CONCAT / STRING_AGG / LISTAGG
    #[test]
    fn test_group_concat_to_postgres() {
        let result = transpile(
            "SELECT GROUP_CONCAT(name)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("STRING_AGG"),
            "PostgreSQL should convert GROUP_CONCAT to STRING_AGG: got {}",
            result
        );
    }

    #[test]
    fn test_group_concat_to_snowflake() {
        let result = transpile(
            "SELECT GROUP_CONCAT(name)",
            DialectType::Generic,
            DialectType::Snowflake,
        );
        assert!(
            result.to_uppercase().contains("LISTAGG"),
            "Snowflake should convert GROUP_CONCAT to LISTAGG: got {}",
            result
        );
    }

    #[test]
    fn test_group_concat_to_tsql() {
        let result = transpile(
            "SELECT GROUP_CONCAT(name)",
            DialectType::Generic,
            DialectType::TSQL,
        );
        assert!(
            result.to_uppercase().contains("STRING_AGG"),
            "TSQL should convert GROUP_CONCAT to STRING_AGG: got {}",
            result
        );
    }
}

// ============================================================================
// Statistical Functions Transpilation Tests
// ============================================================================

mod statistical_functions {
    use super::*;

    #[test]
    fn test_stddev_to_postgres() {
        let result = transpile(
            "SELECT STDDEV(x)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("STDDEV"),
            "PostgreSQL should preserve STDDEV: got {}",
            result
        );
    }

    #[test]
    fn test_stddev_to_tsql() {
        let result = transpile("SELECT STDDEV(x)", DialectType::Generic, DialectType::TSQL);
        assert!(
            result.to_uppercase().contains("STDEV"),
            "TSQL should convert STDDEV to STDEV: got {}",
            result
        );
    }

    #[test]
    fn test_variance_preserved() {
        let result = transpile(
            "SELECT VARIANCE(x)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("VARIANCE") || result.to_uppercase().contains("VAR"),
            "PostgreSQL should preserve VARIANCE: got {}",
            result
        );
    }
}

// ============================================================================
// Math Functions Transpilation Tests
// ============================================================================

mod math_functions {
    use super::*;

    #[test]
    fn test_random_to_postgres() {
        let result = transpile(
            "SELECT RANDOM()",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("RANDOM"),
            "PostgreSQL should use RANDOM: got {}",
            result
        );
    }

    #[test]
    fn test_rand_to_postgres() {
        let result = transpile(
            "SELECT RAND()",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("RANDOM") || result.to_uppercase().contains("RAND"),
            "PostgreSQL should convert RAND to RANDOM: got {}",
            result
        );
    }

    #[test]
    fn test_random_to_mysql() {
        let result = transpile("SELECT RANDOM()", DialectType::Generic, DialectType::MySQL);
        assert!(
            result.to_uppercase().contains("RAND"),
            "MySQL should convert RANDOM to RAND: got {}",
            result
        );
    }

    #[test]
    fn test_ln_to_postgres() {
        let result = transpile(
            "SELECT LN(x)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        assert!(
            result.to_uppercase().contains("LN"),
            "PostgreSQL should preserve LN: got {}",
            result
        );
    }

    #[test]
    fn test_ln_to_tsql() {
        let result = transpile("SELECT LN(x)", DialectType::Generic, DialectType::TSQL);
        assert!(
            result.to_uppercase().contains("LOG"),
            "TSQL should convert LN to LOG: got {}",
            result
        );
    }

    // CEIL/CEILING
    #[test]
    fn test_ceil_ceiling() {
        let result_pg = transpile(
            "SELECT CEIL(x)",
            DialectType::Generic,
            DialectType::PostgreSQL,
        );
        let result_tsql = transpile("SELECT CEIL(x)", DialectType::Generic, DialectType::TSQL);

        assert!(
            result_pg.to_uppercase().contains("CEIL"),
            "PostgreSQL should use CEIL: got {}",
            result_pg
        );
        assert!(
            result_tsql.to_uppercase().contains("CEILING")
                || result_tsql.to_uppercase().contains("CEIL"),
            "TSQL should use CEILING: got {}",
            result_tsql
        );
    }
}

// ============================================================================
// Cast Transpilation Tests
// ============================================================================

mod cast_functions {
    use super::*;

    #[test]
    fn test_cast_preserved() {
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile("SELECT CAST(x AS INT)", DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("CAST"),
                "{:?} should preserve CAST: got {}",
                dialect,
                result
            );
        }
    }
}

// ============================================================================
// Complex Query Transpilation Tests
// ============================================================================

mod complex_queries {
    use super::*;

    #[test]
    fn test_join_query() {
        let sql = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            assert!(
                transpile_succeeds(sql, DialectType::Generic, dialect),
                "{:?} should handle JOIN query",
                dialect
            );
        }
    }

    #[test]
    fn test_in_subquery() {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            assert!(
                transpile_succeeds(sql, DialectType::Generic, dialect),
                "{:?} should handle IN subquery",
                dialect
            );
        }
    }

    #[test]
    fn test_from_subquery() {
        let sql = "SELECT * FROM (SELECT a, b FROM t) sub";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            assert!(
                transpile_succeeds(sql, DialectType::Generic, dialect),
                "{:?} should handle FROM subquery",
                dialect
            );
        }
    }

    #[test]
    fn test_group_by_having() {
        let sql =
            "SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING COUNT(*) > 5";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            assert!(
                transpile_succeeds(sql, DialectType::Generic, dialect),
                "{:?} should handle GROUP BY HAVING",
                dialect
            );
        }
    }

    #[test]
    fn test_order_by_limit() {
        let sql = "SELECT * FROM users ORDER BY created_at DESC LIMIT 10";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("ORDER BY"),
                "{:?} should preserve ORDER BY: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_union_query() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("UNION"),
                "{:?} should preserve UNION: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_case_expression() {
        let sql =
            "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("CASE") && result.to_uppercase().contains("WHEN"),
                "{:?} should preserve CASE WHEN: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_window_function() {
        let sql =
            "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("ROW_NUMBER")
                    && result.to_uppercase().contains("OVER"),
                "{:?} should preserve window function: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_cte_query() {
        let sql = "WITH cte AS (SELECT id FROM users) SELECT * FROM cte";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("WITH"),
                "{:?} should preserve CTE: got {}",
                dialect,
                result
            );
        }
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

mod edge_cases {
    use super::*;

    #[test]
    fn test_same_dialect_noop() {
        let sql = "SELECT a FROM users";

        let dialects = [
            DialectType::Generic,
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, dialect.clone(), dialect.clone());
            assert!(
                result.to_uppercase().contains("SELECT"),
                "{:?} to {:?} should preserve SELECT: got {}",
                dialect,
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_empty_query_list() {
        // Comment-only input should be handled gracefully
        let sql = "-- just a comment";
        let dialects = [DialectType::PostgreSQL, DialectType::MySQL];

        for dialect in dialects {
            let source = Dialect::get(DialectType::Generic);
            let result = source.transpile(sql, dialect);
            // Should either succeed with empty result or error gracefully
            match result {
                Ok(statements) => {
                    // Empty is acceptable
                    assert!(statements.is_empty() || !statements[0].is_empty());
                }
                Err(_) => {
                    // Error is also acceptable for comment-only input
                }
            }
        }
    }

    #[test]
    fn test_unicode_preservation() {
        let sql = "SELECT '日本語', '你好'";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.contains("日本語") && result.contains("你好"),
                "{:?} should preserve Unicode: got {}",
                dialect,
                result
            );
        }
    }

    #[test]
    fn test_nested_functions() {
        let sql = "SELECT UPPER(LOWER(TRIM(name)))";

        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
            DialectType::TSQL,
        ];

        for dialect in dialects {
            let result = transpile(sql, DialectType::Generic, dialect);
            assert!(
                result.to_uppercase().contains("UPPER")
                    && result.to_uppercase().contains("LOWER")
                    && result.to_uppercase().contains("TRIM"),
                "{:?} should preserve nested functions: got {}",
                dialect,
                result
            );
        }
    }
}

// ============================================================================
// Secondary Dialects Matrix Tests
// ============================================================================

mod secondary_dialects {
    use super::*;

    #[test]
    fn test_oracle_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Oracle
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Oracle,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_sqlite_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::SQLite
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::SQLite,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_hive_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Hive
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Hive,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_spark_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Spark
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Spark,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_trino_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Trino
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Trino,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_redshift_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Redshift
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Redshift,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_clickhouse_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::ClickHouse
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::ClickHouse,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_databricks_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Databricks
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Databricks,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_presto_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::Presto
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::Presto,
            DialectType::Generic
        ));
    }

    #[test]
    fn test_cockroachdb_transpile() {
        let sql = "SELECT a, b FROM users WHERE id = 1";
        assert!(transpile_succeeds(
            sql,
            DialectType::Generic,
            DialectType::CockroachDB
        ));
        assert!(transpile_succeeds(
            sql,
            DialectType::CockroachDB,
            DialectType::Generic
        ));
    }
}

/// Tests for WITH ORDINALITY transpilation across dialect tiers.
///
/// Tier classification:
///   Tier 1 (native): PostgreSQL, Presto, Trino, DuckDB, BigQuery
///   Tier 2 (semantic equiv, not yet implemented): Snowflake, MySQL, TSQL, etc.
///   Tier 3 (emulated via ROW_NUMBER): DataFusion, StarRocks, Doris, Redshift
///   Tier 4 (no support): everything else
///
/// Test matrix covers:
///   Tier 1 → Tier 1: identity / ±1 base adjustment
///   Tier 1 → Tier 3: ROW_NUMBER() OVER () rewrite (C1: replace, not append)
///   Tier 1 → Tier 2: fail hard with guidance
///   Correlated UNNEST → Tier 3: fail hard (C2)
///   Multi-source UNNEST → Tier 3: fail hard (C3)
mod unnest_ordinality {
    use super::*;

    /// Helper for tests that expect transpilation to fail.
    fn transpile_err(sql: &str, from: DialectType, to: DialectType) -> String {
        let source_dialect = Dialect::get(from);
        match source_dialect.transpile(sql, to) {
            Err(e) => format!("{}", e),
            Ok(r) => panic!(
                "Expected error transpiling {:?}->{:?}, but got: {}",
                from, to, r[0]
            ),
        }
    }

    // ── Tier 1 → Tier 3: ROW_NUMBER() rewrite (C1: replace, not append) ──

    #[test]
    fn test_pg_to_datafusion_unnest_with_ordinality() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::PostgreSQL,
                DialectType::DataFusion
            ),
            "SELECT elem, ROW_NUMBER() OVER () AS ord FROM UNNEST(ARRAY[1, 2, 3]) AS t(elem)"
        );
    }

    #[test]
    fn test_duckdb_to_datafusion_unnest_with_ordinality() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST([1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::DuckDB,
                DialectType::DataFusion
            ),
            "SELECT elem, ROW_NUMBER() OVER () AS ord FROM UNNEST([1, 2, 3]) AS t(elem)"
        );
    }

    #[test]
    fn test_presto_to_datafusion_unnest_with_ordinality() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::Presto,
                DialectType::DataFusion
            ),
            "SELECT elem, ROW_NUMBER() OVER () AS ord FROM UNNEST(ARRAY[1, 2, 3]) AS t(elem)"
        );
    }

    #[test]
    fn test_trino_to_datafusion_unnest_with_ordinality() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::Trino,
                DialectType::DataFusion
            ),
            "SELECT elem, ROW_NUMBER() OVER () AS ord FROM UNNEST(ARRAY[1, 2, 3]) AS t(elem)"
        );
    }

    #[test]
    fn test_pg_to_datafusion_no_alias() {
        // SELECT * — no explicit ordinality ref to replace, so append
        assert_eq!(
            transpile(
                "SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY",
                DialectType::PostgreSQL,
                DialectType::DataFusion
            ),
            "SELECT *, ROW_NUMBER() OVER () AS ordinality FROM UNNEST(ARRAY[1, 2, 3])"
        );
    }

    #[test]
    fn test_pg_to_datafusion_custom_alias() {
        // C1: 'position' in SELECT list is REPLACED, not duplicated
        assert_eq!(
            transpile(
                "SELECT element, position FROM UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS t(element, position)",
                DialectType::PostgreSQL,
                DialectType::DataFusion
            ),
            "SELECT element, ROW_NUMBER() OVER () AS position FROM UNNEST(ARRAY[1, 2]) AS t(element)"
        );
    }

    // ── Tier 1 → Tier 2: fail hard with targeted guidance ───────────────

    #[test]
    fn test_pg_to_mysql_fails_with_guidance() {
        let err = transpile_err(
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
            DialectType::PostgreSQL,
            DialectType::MySQL,
        );
        assert!(err.contains("MySQL"), "Error should mention MySQL: {}", err);
        assert!(
            err.contains("JSON_TABLE"),
            "Error should mention JSON_TABLE: {}",
            err
        );
    }

    #[test]
    fn test_pg_to_tsql_fails_with_guidance() {
        let err = transpile_err(
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
            DialectType::PostgreSQL,
            DialectType::TSQL,
        );
        assert!(err.contains("TSQL"), "Error should mention TSQL: {}", err);
        assert!(
            err.contains("OPENJSON"),
            "Error should mention OPENJSON: {}",
            err
        );
    }

    #[test]
    fn test_pg_to_snowflake_fails_with_guidance() {
        let err = transpile_err(
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
            DialectType::PostgreSQL,
            DialectType::Snowflake,
        );
        assert!(
            err.contains("FLATTEN"),
            "Error should mention FLATTEN: {}",
            err
        );
    }

    // ── C2: Correlated UNNEST → fail hard ───────────────────────────────

    #[test]
    fn test_correlated_unnest_fails() {
        let err = transpile_err(
            "SELECT t.id, x, pos FROM tbl AS t, UNNEST(t.arr) WITH ORDINALITY AS u(x, pos)",
            DialectType::PostgreSQL,
            DialectType::DataFusion,
        );
        assert!(
            err.contains("Correlated"),
            "Error should mention correlated UNNEST: {}",
            err
        );
        assert!(
            err.contains("restart per input row"),
            "Error should explain the scoping issue: {}",
            err
        );
    }

    // ── Tier 1 → Tier 1 (Cat A → Cat A): identity preserved ────────────

    #[test]
    fn test_pg_to_pg_identity_with_ordinality() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::PostgreSQL,
                DialectType::PostgreSQL
            ),
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)"
        );
    }

    #[test]
    fn test_pg_to_duckdb_with_ordinality_preserved() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::PostgreSQL,
                DialectType::DuckDB
            ),
            "SELECT elem, ord FROM UNNEST([1, 2, 3]) WITH ORDINALITY AS t(elem, ord)"
        );
    }

    #[test]
    fn test_presto_to_pg_identity() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::Presto,
                DialectType::PostgreSQL
            ),
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)"
        );
    }

    #[test]
    fn test_duckdb_to_pg_identity() {
        assert_eq!(
            transpile(
                "SELECT elem, ord FROM UNNEST([1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
                DialectType::DuckDB,
                DialectType::PostgreSQL
            ),
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)"
        );
    }

    // ── Tier 1: Cat A → Cat B (1-based → 0-based, ±1 adjustment) ───────

    #[test]
    fn test_pg_to_bigquery_unnest_with_ordinality() {
        let result = transpile(
            "SELECT elem, ord FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(elem, ord)",
            DialectType::PostgreSQL,
            DialectType::BigQuery,
        );
        assert!(
            result.contains("WITH OFFSET"),
            "Expected WITH OFFSET for BigQuery target, got: {}",
            result
        );
    }

    // ── Tier 1: Cat B → Cat A (0-based → 1-based) ──────────────────────

    #[test]
    fn test_bigquery_to_pg_unnest_with_offset() {
        let result = transpile(
            "SELECT elem, off FROM UNNEST([1, 2, 3]) AS elem WITH OFFSET AS off",
            DialectType::BigQuery,
            DialectType::PostgreSQL,
        );
        assert!(
            result.contains("WITH ORDINALITY"),
            "Expected WITH ORDINALITY for DuckDB target, got: {}",
            result
        );
    }

    // ── Tier 1: Cat B → Cat B: identity ─────────────────────────────────

    #[test]
    fn test_bigquery_to_bigquery_identity() {
        let result = transpile(
            "SELECT elem, off FROM UNNEST([1, 2, 3]) AS elem WITH OFFSET AS off",
            DialectType::BigQuery,
            DialectType::BigQuery,
        );
        assert!(
            result.contains("WITH OFFSET"),
            "Expected WITH OFFSET preserved in BQ round-trip, got: {}",
            result
        );
    }
}
