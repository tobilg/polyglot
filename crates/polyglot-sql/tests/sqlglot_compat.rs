//! SQLGlot Compatibility Tests
//!
//! These tests are ported from sqlglot's test suite to ensure compatibility.
//! They cover parsing, generation, and transpilation between dialects.

use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::generator::Generator;
use polyglot_sql::parser::Parser;

/// Helper function to test roundtrip: parse SQL and regenerate it
fn roundtrip(sql: &str) -> String {
    let ast = Parser::parse_sql(sql).expect(&format!("Failed to parse: {}", sql));
    Generator::sql(&ast[0]).expect("Failed to generate SQL")
}

/// Helper function to test transpilation between dialects
fn transpile(sql: &str, from: DialectType, to: DialectType) -> String {
    let source_dialect = Dialect::get(from);
    let result = source_dialect.transpile_to(sql, to).expect(&format!(
        "Failed to transpile: {} from {:?} to {:?}",
        sql, from, to
    ));
    result[0].clone()
}

// ============================================================================
// Identity Tests - SQL should roundtrip identically
// Ported from sqlglot/tests/fixtures/identity.sql
// ============================================================================

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn test_literals() {
        assert_eq!(roundtrip("SELECT 1"), "SELECT 1");
        assert_eq!(roundtrip("SELECT 1.0"), "SELECT 1.0");
        assert_eq!(roundtrip("SELECT 1E2"), "SELECT 1E2");
        assert_eq!(roundtrip("SELECT 'x'"), "SELECT 'x'");
        assert_eq!(roundtrip("SELECT ''"), "SELECT ''");
    }

    #[test]
    fn test_arithmetic() {
        assert_eq!(roundtrip("SELECT 1 + 2"), "SELECT 1 + 2");
        assert_eq!(roundtrip("SELECT 1 - 2"), "SELECT 1 - 2");
        assert_eq!(roundtrip("SELECT 1 * 2"), "SELECT 1 * 2");
        assert_eq!(roundtrip("SELECT 1 / 2"), "SELECT 1 / 2");
        assert_eq!(roundtrip("SELECT 1 % 2"), "SELECT 1 % 2");
        assert_eq!(
            roundtrip("SELECT (1 * 2) / (3 - 5)"),
            "SELECT (1 * 2) / (3 - 5)"
        );
    }

    #[test]
    fn test_comparison() {
        assert_eq!(roundtrip("SELECT x < 1"), "SELECT x < 1");
        assert_eq!(roundtrip("SELECT x <= 1"), "SELECT x <= 1");
        assert_eq!(roundtrip("SELECT x > 1"), "SELECT x > 1");
        assert_eq!(roundtrip("SELECT x >= 1"), "SELECT x >= 1");
        assert_eq!(roundtrip("SELECT x = 1"), "SELECT x = 1");
        assert_eq!(roundtrip("SELECT x <> 1"), "SELECT x <> 1");
    }

    #[test]
    fn test_boolean_logic() {
        assert_eq!(roundtrip("SELECT x = y OR x > 1"), "SELECT x = y OR x > 1");
        assert_eq!(
            roundtrip("SELECT x = 1 AND y = 2"),
            "SELECT x = 1 AND y = 2"
        );
        assert_eq!(roundtrip("SELECT NOT x"), "SELECT NOT x");
    }

    // TODO: Bitwise operators not yet fully implemented in parser
    // #[test]
    // fn test_bitwise() {
    //     assert_eq!(roundtrip("SELECT x & 1"), "SELECT x & 1");
    //     assert_eq!(roundtrip("SELECT x | 1"), "SELECT x | 1");
    //     assert_eq!(roundtrip("SELECT x ^ 1"), "SELECT x ^ 1");
    //     assert_eq!(roundtrip("SELECT ~x"), "SELECT ~x");
    // }

    #[test]
    fn test_column_access() {
        assert_eq!(roundtrip("SELECT a.b"), "SELECT a.b");
        // TODO: Multi-level column access (a.b.c) needs parser enhancement
        // assert_eq!(roundtrip("SELECT a.b.c"), "SELECT a.b.c");
        // assert_eq!(roundtrip("SELECT a.b.c.d"), "SELECT a.b.c.d");
    }

    #[test]
    fn test_subscript() {
        assert_eq!(roundtrip("SELECT a[0]"), "SELECT a[0]");
        // TODO: Subscript followed by dot access needs parser enhancement
        // assert_eq!(roundtrip("SELECT a[0].b"), "SELECT a[0].b");
    }

    #[test]
    fn test_in_expression() {
        assert_eq!(roundtrip("SELECT x IN (1, 2, 3)"), "SELECT x IN (1, 2, 3)");
        assert_eq!(roundtrip("SELECT x IN (-1, 1)"), "SELECT x IN (-1, 1)");
    }

    #[test]
    fn test_between() {
        assert_eq!(
            roundtrip("SELECT x BETWEEN 1 AND 10"),
            "SELECT x BETWEEN 1 AND 10"
        );
        assert_eq!(
            roundtrip("SELECT x BETWEEN -1 AND 1"),
            "SELECT x BETWEEN -1 AND 1"
        );
    }

    #[test]
    fn test_is_null() {
        assert_eq!(roundtrip("SELECT x IS NULL"), "SELECT x IS NULL");
        assert_eq!(roundtrip("SELECT x IS NOT NULL"), "SELECT x IS NOT NULL");
    }

    #[test]
    fn test_like() {
        assert_eq!(
            roundtrip("SELECT x LIKE '%test%'"),
            "SELECT x LIKE '%test%'"
        );
    }

    #[test]
    fn test_case() {
        assert_eq!(
            roundtrip("SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END"),
            "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END"
        );
        assert_eq!(
            roundtrip("SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END"),
            "SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END"
        );
    }

    #[test]
    fn test_functions() {
        assert_eq!(roundtrip("SELECT COUNT(*)"), "SELECT COUNT(*)");
        assert_eq!(roundtrip("SELECT SUM(x)"), "SELECT SUM(x)");
        assert_eq!(roundtrip("SELECT AVG(x)"), "SELECT AVG(x)");
        assert_eq!(roundtrip("SELECT MIN(x)"), "SELECT MIN(x)");
        assert_eq!(roundtrip("SELECT MAX(x)"), "SELECT MAX(x)");
        assert_eq!(
            roundtrip("SELECT COALESCE(a, b, c)"),
            "SELECT COALESCE(a, b, c)"
        );
        assert_eq!(roundtrip("SELECT GREATEST(x)"), "SELECT GREATEST(x)");
        assert_eq!(roundtrip("SELECT LEAST(y)"), "SELECT LEAST(y)");
    }

    #[test]
    fn test_window_functions() {
        assert_eq!(
            roundtrip("SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)"),
            "SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)"
        );
        assert_eq!(
            roundtrip(
                "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            ),
            "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn test_aggregate_with_filter() {
        assert_eq!(
            roundtrip("SELECT COUNT(*) FILTER (WHERE x > 0)"),
            "SELECT COUNT(*) FILTER(WHERE x > 0)"
        );
    }

    #[test]
    fn test_subquery() {
        assert_eq!(
            roundtrip("SELECT * FROM (SELECT 1) AS t"),
            "SELECT * FROM (SELECT 1) AS t"
        );
        // TODO: Subquery in IN expression needs parser enhancement
        // assert_eq!(
        //     roundtrip("SELECT * FROM t WHERE x IN (SELECT y FROM s)"),
        //     "SELECT * FROM t WHERE x IN (SELECT y FROM s)"
        // );
    }

    #[test]
    fn test_union() {
        assert_eq!(
            roundtrip("SELECT 1 UNION SELECT 2"),
            "SELECT 1 UNION SELECT 2"
        );
        assert_eq!(
            roundtrip("SELECT 1 UNION ALL SELECT 2"),
            "SELECT 1 UNION ALL SELECT 2"
        );
    }

    #[test]
    fn test_join() {
        assert_eq!(
            roundtrip("SELECT * FROM a JOIN b ON a.id = b.id"),
            "SELECT * FROM a JOIN b ON a.id = b.id"
        );
        assert_eq!(
            roundtrip("SELECT * FROM a LEFT JOIN b ON a.id = b.id"),
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id"
        );
        assert_eq!(
            roundtrip("SELECT * FROM a RIGHT JOIN b ON a.id = b.id"),
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.id"
        );
        assert_eq!(
            roundtrip("SELECT * FROM a FULL JOIN b ON a.id = b.id"),
            "SELECT * FROM a FULL JOIN b ON a.id = b.id"
        );
        assert_eq!(
            roundtrip("SELECT * FROM a CROSS JOIN b"),
            "SELECT * FROM a CROSS JOIN b"
        );
    }

    #[test]
    fn test_cte() {
        assert_eq!(
            roundtrip("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        );
    }
}

// ============================================================================
// DDL Identity Tests
// Ported from sqlglot/tests/dialects/test_mysql.py and others
// ============================================================================

#[cfg(test)]
mod ddl_tests {
    use super::*;

    #[test]
    fn test_create_table_basic() {
        assert_eq!(
            roundtrip("CREATE TABLE t (id INT)"),
            "CREATE TABLE t (id INT)"
        );
        assert_eq!(
            roundtrip("CREATE TABLE t (id INT, name VARCHAR(100))"),
            "CREATE TABLE t (id INT, name VARCHAR(100))"
        );
    }

    #[test]
    fn test_create_table_constraints() {
        assert_eq!(
            roundtrip("CREATE TABLE t (id INT PRIMARY KEY)"),
            "CREATE TABLE t (id INT PRIMARY KEY)"
        );
        assert_eq!(
            roundtrip("CREATE TABLE t (id INT NOT NULL)"),
            "CREATE TABLE t (id INT NOT NULL)"
        );
        assert_eq!(
            roundtrip("CREATE TABLE t (id INT UNIQUE)"),
            "CREATE TABLE t (id INT UNIQUE)"
        );
    }

    #[test]
    fn test_create_table_if_not_exists() {
        assert_eq!(
            roundtrip("CREATE TABLE IF NOT EXISTS t (id INT)"),
            "CREATE TABLE IF NOT EXISTS t (id INT)"
        );
    }

    #[test]
    fn test_create_temporary_table() {
        assert_eq!(
            roundtrip("CREATE TEMPORARY TABLE t (id INT)"),
            "CREATE TEMPORARY TABLE t (id INT)"
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(roundtrip("DROP TABLE t"), "DROP TABLE t");
        assert_eq!(
            roundtrip("DROP TABLE IF EXISTS t"),
            "DROP TABLE IF EXISTS t"
        );
        assert_eq!(
            roundtrip("DROP TABLE IF EXISTS t CASCADE"),
            "DROP TABLE IF EXISTS t CASCADE"
        );
    }

    #[test]
    fn test_alter_table() {
        assert_eq!(
            roundtrip("ALTER TABLE t ADD COLUMN x INT"),
            "ALTER TABLE t ADD COLUMN x INT"
        );
        assert_eq!(
            roundtrip("ALTER TABLE t DROP COLUMN x"),
            "ALTER TABLE t DROP COLUMN x"
        );
    }

    #[test]
    fn test_create_index() {
        assert_eq!(
            roundtrip("CREATE INDEX idx ON t (col)"),
            "CREATE INDEX idx ON t (col)"
        );
        assert_eq!(
            roundtrip("CREATE UNIQUE INDEX idx ON t (col)"),
            "CREATE UNIQUE INDEX idx ON t (col)"
        );
    }

    #[test]
    fn test_drop_index() {
        assert_eq!(roundtrip("DROP INDEX idx"), "DROP INDEX idx");
        assert_eq!(
            roundtrip("DROP INDEX IF EXISTS idx"),
            "DROP INDEX IF EXISTS idx"
        );
    }

    #[test]
    fn test_create_view() {
        assert_eq!(
            roundtrip("CREATE VIEW v AS SELECT * FROM t"),
            "CREATE VIEW v AS SELECT * FROM t"
        );
        assert_eq!(
            roundtrip("CREATE OR REPLACE VIEW v AS SELECT * FROM t"),
            "CREATE OR REPLACE VIEW v AS SELECT * FROM t"
        );
    }

    #[test]
    fn test_drop_view() {
        assert_eq!(roundtrip("DROP VIEW v"), "DROP VIEW v");
        assert_eq!(roundtrip("DROP VIEW IF EXISTS v"), "DROP VIEW IF EXISTS v");
    }

    #[test]
    fn test_truncate() {
        assert_eq!(roundtrip("TRUNCATE TABLE t"), "TRUNCATE TABLE t");
        assert_eq!(
            roundtrip("TRUNCATE TABLE t CASCADE"),
            "TRUNCATE TABLE t CASCADE"
        );
    }

    // Phase 4: Additional DDL tests

    #[test]
    fn test_create_schema() {
        assert_eq!(
            roundtrip("CREATE SCHEMA my_schema"),
            "CREATE SCHEMA my_schema"
        );
        assert_eq!(
            roundtrip("CREATE SCHEMA IF NOT EXISTS my_schema"),
            "CREATE SCHEMA IF NOT EXISTS my_schema"
        );
        assert_eq!(
            roundtrip("CREATE SCHEMA my_schema AUTHORIZATION admin"),
            "CREATE SCHEMA my_schema AUTHORIZATION admin"
        );
    }

    #[test]
    fn test_drop_schema() {
        assert_eq!(roundtrip("DROP SCHEMA my_schema"), "DROP SCHEMA my_schema");
        assert_eq!(
            roundtrip("DROP SCHEMA IF EXISTS my_schema"),
            "DROP SCHEMA IF EXISTS my_schema"
        );
        assert_eq!(
            roundtrip("DROP SCHEMA IF EXISTS my_schema CASCADE"),
            "DROP SCHEMA IF EXISTS my_schema CASCADE"
        );
    }

    #[test]
    fn test_create_database() {
        assert_eq!(roundtrip("CREATE DATABASE mydb"), "CREATE DATABASE mydb");
        assert_eq!(
            roundtrip("CREATE DATABASE IF NOT EXISTS mydb"),
            "CREATE DATABASE IF NOT EXISTS mydb"
        );
    }

    #[test]
    fn test_drop_database() {
        assert_eq!(roundtrip("DROP DATABASE mydb"), "DROP DATABASE mydb");
        assert_eq!(
            roundtrip("DROP DATABASE IF EXISTS mydb"),
            "DROP DATABASE IF EXISTS mydb"
        );
    }

    #[test]
    fn test_create_sequence() {
        assert_eq!(
            roundtrip("CREATE SEQUENCE my_seq"),
            "CREATE SEQUENCE my_seq"
        );
        assert_eq!(
            roundtrip("CREATE SEQUENCE IF NOT EXISTS my_seq"),
            "CREATE SEQUENCE IF NOT EXISTS my_seq"
        );
        assert_eq!(
            roundtrip("CREATE SEQUENCE my_seq INCREMENT BY 1"),
            "CREATE SEQUENCE my_seq INCREMENT BY 1"
        );
        assert_eq!(
            roundtrip("CREATE SEQUENCE my_seq START WITH 100"),
            "CREATE SEQUENCE my_seq START WITH 100"
        );
        assert_eq!(
            roundtrip("CREATE SEQUENCE my_seq MINVALUE 1 MAXVALUE 1000"),
            "CREATE SEQUENCE my_seq MINVALUE 1 MAXVALUE 1000"
        );
        assert_eq!(
            roundtrip("CREATE SEQUENCE my_seq CYCLE"),
            "CREATE SEQUENCE my_seq CYCLE"
        );
    }

    #[test]
    fn test_drop_sequence() {
        assert_eq!(roundtrip("DROP SEQUENCE my_seq"), "DROP SEQUENCE my_seq");
        assert_eq!(
            roundtrip("DROP SEQUENCE IF EXISTS my_seq CASCADE"),
            "DROP SEQUENCE IF EXISTS my_seq CASCADE"
        );
    }

    #[test]
    fn test_alter_sequence() {
        assert_eq!(
            roundtrip("ALTER SEQUENCE my_seq INCREMENT BY 5"),
            "ALTER SEQUENCE my_seq INCREMENT BY 5"
        );
        assert_eq!(
            roundtrip("ALTER SEQUENCE my_seq RESTART"),
            "ALTER SEQUENCE my_seq RESTART"
        );
    }

    #[test]
    fn test_create_type_enum() {
        assert_eq!(
            roundtrip("CREATE TYPE status AS ENUM ('active', 'inactive')"),
            "CREATE TYPE status AS ENUM ('active', 'inactive')"
        );
    }

    #[test]
    fn test_create_type_composite() {
        assert_eq!(
            roundtrip("CREATE TYPE address AS (street VARCHAR, city VARCHAR)"),
            "CREATE TYPE address AS (street VARCHAR, city VARCHAR)"
        );
    }

    #[test]
    fn test_drop_type() {
        assert_eq!(roundtrip("DROP TYPE my_type"), "DROP TYPE my_type");
        assert_eq!(
            roundtrip("DROP TYPE IF EXISTS my_type CASCADE"),
            "DROP TYPE IF EXISTS my_type CASCADE"
        );
    }

    #[test]
    fn test_alter_view() {
        assert_eq!(
            roundtrip("ALTER VIEW my_view RENAME TO new_view"),
            "ALTER VIEW my_view RENAME TO new_view"
        );
    }

    #[test]
    fn test_alter_index() {
        assert_eq!(
            roundtrip("ALTER INDEX my_idx RENAME TO new_idx"),
            "ALTER INDEX my_idx RENAME TO new_idx"
        );
    }
}

// ============================================================================
// Transpilation Tests
// Ported from sqlglot's dialect-specific tests
// ============================================================================

#[cfg(test)]
mod transpile_tests {
    use super::*;

    // NVL/IFNULL/COALESCE conversions
    #[test]
    fn test_nvl_to_ifnull_mysql() {
        // NVL -> IFNULL in MySQL
        assert_eq!(
            transpile("SELECT NVL(a, b)", DialectType::Generic, DialectType::MySQL),
            "SELECT IFNULL(a, b)"
        );
    }

    #[test]
    fn test_nvl_to_coalesce_postgres() {
        // NVL -> COALESCE in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b)",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT COALESCE(a, b)"
        );
    }

    #[test]
    fn test_ifnull_to_coalesce_postgres() {
        // IFNULL -> COALESCE in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b)",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT COALESCE(a, b)"
        );
    }

    // TODO: COALESCE optimization not implemented yet
    // #[test]
    // fn test_coalesce_2args_to_ifnull_mysql() {
    //     // COALESCE with 2 args -> IFNULL in MySQL (optimization)
    //     assert_eq!(
    //         transpile("SELECT COALESCE(a, b)", DialectType::Generic, DialectType::MySQL),
    //         "SELECT IFNULL(a, b)"
    //     );
    // }

    // TODO: String aggregation function transformations need implementation
    // These require parsing the functions first, then transforming
    // #[test]
    // fn test_group_concat_to_string_agg_postgres() {
    //     // GROUP_CONCAT -> STRING_AGG in PostgreSQL
    //     assert_eq!(
    //         transpile("SELECT GROUP_CONCAT(name)", DialectType::MySQL, DialectType::PostgreSQL),
    //         "SELECT STRING_AGG(name)"
    //     );
    // }

    // #[test]
    // fn test_array_agg_to_group_concat_mysql() {
    //     // ARRAY_AGG -> GROUP_CONCAT in MySQL
    //     assert_eq!(
    //         transpile("SELECT ARRAY_AGG(name)", DialectType::Generic, DialectType::MySQL),
    //         "SELECT GROUP_CONCAT(name)"
    //     );
    // }

    // Substring function conversions
    #[test]
    fn test_substr_to_substring_postgres() {
        // SUBSTR -> SUBSTRING in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT SUBSTR(name, 1, 5)",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT SUBSTRING(name, 1, 5)"
        );
    }

    // Basic transpilation - should preserve SQL structure
    #[test]
    fn test_basic_select_transpile() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT a, b FROM t"
        );
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::MySQL
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_select_with_where_transpile() {
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE x = 1",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT * FROM t WHERE x = 1"
        );
    }

    #[test]
    fn test_join_transpile() {
        assert_eq!(
            transpile(
                "SELECT * FROM a JOIN b ON a.id = b.id",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT * FROM a JOIN b ON a.id = b.id"
        );
    }
}

// ============================================================================
// DML Tests
// ============================================================================

#[cfg(test)]
mod dml_tests {
    use super::*;

    #[test]
    fn test_insert() {
        assert_eq!(
            roundtrip("INSERT INTO t (a, b) VALUES (1, 2)"),
            "INSERT INTO t (a, b) VALUES (1, 2)"
        );
        assert_eq!(
            roundtrip("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)"),
            "INSERT INTO t (a, b) VALUES (1, 2), (3, 4)"
        );
    }

    #[test]
    fn test_update() {
        assert_eq!(roundtrip("UPDATE t SET a = 1"), "UPDATE t SET a = 1");
        assert_eq!(
            roundtrip("UPDATE t SET a = 1 WHERE b = 2"),
            "UPDATE t SET a = 1 WHERE b = 2"
        );
        assert_eq!(
            roundtrip("UPDATE t SET a = 1, b = 2 WHERE c = 3"),
            "UPDATE t SET a = 1, b = 2 WHERE c = 3"
        );
    }

    #[test]
    fn test_delete() {
        assert_eq!(roundtrip("DELETE FROM t"), "DELETE FROM t");
        assert_eq!(
            roundtrip("DELETE FROM t WHERE a = 1"),
            "DELETE FROM t WHERE a = 1"
        );
    }
}

// ============================================================================
// Complex Query Tests
// ============================================================================

#[cfg(test)]
mod complex_tests {
    use super::*;

    #[test]
    fn test_nested_subqueries() {
        assert_eq!(
            roundtrip("SELECT * FROM (SELECT * FROM (SELECT 1) AS a) AS b"),
            "SELECT * FROM (SELECT * FROM (SELECT 1) AS a) AS b"
        );
    }

    // TODO: Scalar subqueries in expressions need parser enhancement
    // #[test]
    // fn test_correlated_subquery() {
    //     assert_eq!(
    //         roundtrip("SELECT * FROM t WHERE x = (SELECT MAX(y) FROM s WHERE s.id = t.id)"),
    //         "SELECT * FROM t WHERE x = (SELECT MAX(y) FROM s WHERE s.id = t.id)"
    //     );
    // }

    #[test]
    fn test_multiple_joins() {
        assert_eq!(
            roundtrip("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"),
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
        );
    }

    #[test]
    fn test_group_by_having() {
        assert_eq!(
            roundtrip("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1"),
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1"
        );
    }

    #[test]
    fn test_order_by_limit() {
        assert_eq!(
            roundtrip("SELECT * FROM t ORDER BY a DESC LIMIT 10"),
            "SELECT * FROM t ORDER BY a DESC LIMIT 10"
        );
        // ASC is the default direction and may be omitted in output
        assert_eq!(
            roundtrip("SELECT * FROM t ORDER BY a, b DESC"),
            "SELECT * FROM t ORDER BY a, b DESC"
        );
    }

    #[test]
    fn test_distinct() {
        assert_eq!(
            roundtrip("SELECT DISTINCT a FROM t"),
            "SELECT DISTINCT a FROM t"
        );
    }

    #[test]
    fn test_exists() {
        assert_eq!(
            roundtrip("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)"),
            "SELECT * FROM t WHERE EXISTS(SELECT 1 FROM s WHERE s.id = t.id)"
        );
    }
}

// ============================================================================
// Phase 3 Tests - Parser Enhancements
// ============================================================================

#[cfg(test)]
mod phase3_tests {
    use super::*;

    #[test]
    fn test_top_clause() {
        assert_eq!(
            roundtrip("SELECT TOP (10) * FROM t"),
            "SELECT TOP (10) * FROM t"
        );
        assert_eq!(
            roundtrip("SELECT TOP (10) PERCENT * FROM t"),
            "SELECT TOP (10) PERCENT * FROM t"
        );
        assert_eq!(
            roundtrip("SELECT TOP (10) WITH TIES * FROM t"),
            "SELECT TOP (10) WITH TIES * FROM t"
        );
    }

    #[test]
    fn test_distinct_on() {
        assert_eq!(
            roundtrip("SELECT DISTINCT ON (a) * FROM t"),
            "SELECT DISTINCT ON (a) * FROM t"
        );
        assert_eq!(
            roundtrip("SELECT DISTINCT ON (a, b) * FROM t"),
            "SELECT DISTINCT ON (a, b) * FROM t"
        );
    }

    #[test]
    fn test_qualify_clause() {
        assert_eq!(
            roundtrip("SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1"),
            "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1"
        );
    }

    #[test]
    fn test_materialized_cte() {
        assert_eq!(
            roundtrip("WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte"),
            "WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte"
        );
        assert_eq!(
            roundtrip("WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte"),
            "WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte"
        );
    }

    #[test]
    fn test_pivot() {
        assert_eq!(
            roundtrip("SELECT * FROM t PIVOT (SUM(amount) FOR product IN ('A', 'B', 'C'))"),
            "SELECT * FROM t PIVOT(SUM(amount) FOR product IN ('A', 'B', 'C'))"
        );
    }

    #[test]
    fn test_unpivot() {
        assert_eq!(
            roundtrip("SELECT * FROM t UNPIVOT (value FOR name IN (a, b, c))"),
            "SELECT * FROM t UNPIVOT(value FOR name IN (a, b, c))"
        );
    }

    #[test]
    fn test_any_all_subquery() {
        assert_eq!(
            roundtrip("SELECT * FROM t WHERE x > ANY (SELECT y FROM s)"),
            "SELECT * FROM t WHERE x > ANY (SELECT y FROM s)"
        );
        assert_eq!(
            roundtrip("SELECT * FROM t WHERE x = ALL (SELECT y FROM s)"),
            "SELECT * FROM t WHERE x = ALL (SELECT y FROM s)"
        );
    }

    #[test]
    fn test_cross_apply() {
        assert_eq!(
            roundtrip("SELECT * FROM t CROSS APPLY s"),
            "SELECT * FROM t CROSS APPLY s"
        );
        assert_eq!(
            roundtrip("SELECT * FROM t OUTER APPLY s"),
            "SELECT * FROM t OUTER APPLY s"
        );
    }

    #[test]
    fn test_lateral_join() {
        assert_eq!(
            roundtrip("SELECT * FROM t LATERAL JOIN s ON t.id = s.id"),
            "SELECT * FROM t LATERAL JOIN s ON t.id = s.id"
        );
        assert_eq!(
            roundtrip("SELECT * FROM t LEFT LATERAL JOIN s ON t.id = s.id"),
            "SELECT * FROM t LEFT LATERAL JOIN s ON t.id = s.id"
        );
    }

    #[test]
    fn test_asof_join() {
        assert_eq!(
            roundtrip("SELECT * FROM t ASOF JOIN s ON t.id = s.id"),
            "SELECT * FROM t ASOF JOIN s ON t.id = s.id"
        );
    }

    #[test]
    fn test_lateral_view() {
        // Basic LATERAL VIEW EXPLODE
        assert_eq!(
            roundtrip("SELECT * FROM t LATERAL VIEW EXPLODE(arr) AS x"),
            "SELECT * FROM t LATERAL VIEW EXPLODE(arr) AS x"
        );

        // LATERAL VIEW with table alias
        assert_eq!(
            roundtrip("SELECT * FROM t LATERAL VIEW EXPLODE(arr) tmp AS x"),
            "SELECT * FROM t LATERAL VIEW EXPLODE(arr) tmp AS x"
        );

        // LATERAL VIEW OUTER (preserves nulls)
        assert_eq!(
            roundtrip("SELECT * FROM t LATERAL VIEW OUTER EXPLODE(arr) tmp AS x"),
            "SELECT * FROM t LATERAL VIEW OUTER EXPLODE(arr) tmp AS x"
        );

        // Multiple column aliases (for map explode)
        assert_eq!(
            roundtrip("SELECT * FROM t LATERAL VIEW EXPLODE(map_col) tmp AS k, v"),
            "SELECT * FROM t LATERAL VIEW EXPLODE(map_col) tmp AS k, v"
        );
    }
}

// ============================================================================
// Dialect-Specific Type and Syntax Tests (Phase 5)
// ============================================================================

#[cfg(test)]
mod dialect_type_tests {
    use super::*;
    use polyglot_sql::dialects::DialectType;
    use polyglot_sql::expressions::{BooleanLiteral, Expression};
    use polyglot_sql::generator::{Generator, GeneratorConfig};

    fn generate_with_dialect(expr: &Expression, dialect: DialectType) -> String {
        let config = GeneratorConfig {
            dialect: Some(dialect),
            ..Default::default()
        };
        let mut gen = Generator::with_config(config);
        gen.generate(expr).unwrap()
    }

    // Boolean literal format tests
    #[test]
    fn test_boolean_literal_tsql() {
        let true_expr = Expression::Boolean(BooleanLiteral { value: true });
        let false_expr = Expression::Boolean(BooleanLiteral { value: false });

        // SQL Server uses 1/0 for boolean literals
        assert_eq!(generate_with_dialect(&true_expr, DialectType::TSQL), "1");
        assert_eq!(generate_with_dialect(&false_expr, DialectType::TSQL), "0");
    }

    #[test]
    fn test_boolean_literal_oracle() {
        let true_expr = Expression::Boolean(BooleanLiteral { value: true });
        let false_expr = Expression::Boolean(BooleanLiteral { value: false });

        // Oracle uses 1/0 for boolean literals
        assert_eq!(generate_with_dialect(&true_expr, DialectType::Oracle), "1");
        assert_eq!(generate_with_dialect(&false_expr, DialectType::Oracle), "0");
    }

    #[test]
    fn test_boolean_literal_postgres() {
        let true_expr = Expression::Boolean(BooleanLiteral { value: true });
        let false_expr = Expression::Boolean(BooleanLiteral { value: false });

        // PostgreSQL uses TRUE/FALSE
        assert_eq!(
            generate_with_dialect(&true_expr, DialectType::PostgreSQL),
            "TRUE"
        );
        assert_eq!(
            generate_with_dialect(&false_expr, DialectType::PostgreSQL),
            "FALSE"
        );
    }

    #[test]
    fn test_boolean_literal_mysql() {
        let true_expr = Expression::Boolean(BooleanLiteral { value: true });
        let false_expr = Expression::Boolean(BooleanLiteral { value: false });

        // MySQL uses TRUE/FALSE
        assert_eq!(
            generate_with_dialect(&true_expr, DialectType::MySQL),
            "TRUE"
        );
        assert_eq!(
            generate_with_dialect(&false_expr, DialectType::MySQL),
            "FALSE"
        );
    }

    // MINUS vs EXCEPT tests
    #[test]
    fn test_except_to_minus_oracle() {
        // EXCEPT should become MINUS in Oracle
        assert_eq!(
            transpile(
                "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
                DialectType::Generic,
                DialectType::Oracle
            ),
            "SELECT a FROM t1 MINUS SELECT a FROM t2"
        );
    }

    #[test]
    fn test_except_postgres() {
        // EXCEPT should stay EXCEPT in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT a FROM t1 EXCEPT SELECT a FROM t2"
        );
    }

    #[test]
    fn test_except_all() {
        // EXCEPT ALL in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2"
        );
    }

    // TOP vs LIMIT tests
    #[test]
    fn test_limit_to_top_tsql() {
        // LIMIT should become TOP in SQL Server
        assert_eq!(
            transpile(
                "SELECT a FROM t LIMIT 10",
                DialectType::Generic,
                DialectType::TSQL
            ),
            "SELECT TOP (10) a FROM t"
        );
    }

    #[test]
    fn test_limit_postgres() {
        // LIMIT should stay LIMIT in PostgreSQL
        assert_eq!(
            transpile(
                "SELECT a FROM t LIMIT 10",
                DialectType::Generic,
                DialectType::PostgreSQL
            ),
            "SELECT a FROM t LIMIT 10"
        );
    }

    #[test]
    fn test_top_tsql_roundtrip() {
        // TOP should roundtrip correctly
        assert_eq!(
            roundtrip("SELECT TOP (10) a FROM t"),
            "SELECT TOP (10) a FROM t"
        );
    }

    #[test]
    fn test_limit_with_offset_tsql() {
        // LIMIT with OFFSET in SQL Server uses OFFSET ... FETCH syntax
        assert_eq!(
            transpile(
                "SELECT a FROM t ORDER BY a LIMIT 10 OFFSET 5",
                DialectType::Generic,
                DialectType::TSQL
            ),
            "SELECT a FROM t ORDER BY a OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
        );
    }
}

// ============================================================================
// New Dialect Tests - SQLite, Presto, Trino, Redshift, ClickHouse, Databricks
// ============================================================================

#[cfg(test)]
mod new_dialect_tests {
    use super::*;

    // SQLite tests
    #[test]
    fn test_sqlite_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::SQLite
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_sqlite_ifnull_to_coalesce() {
        // SQLite supports both IFNULL and COALESCE, but we normalize to COALESCE
        assert_eq!(
            transpile(
                "SELECT COALESCE(a, b) FROM t",
                DialectType::Generic,
                DialectType::SQLite
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Presto tests
    #[test]
    fn test_presto_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Presto
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_presto_nvl_to_coalesce() {
        // Presto uses COALESCE instead of NVL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Presto
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Trino tests
    #[test]
    fn test_trino_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Trino
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_trino_ifnull_to_coalesce() {
        // Trino uses COALESCE instead of IFNULL
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Trino
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Redshift tests
    #[test]
    fn test_redshift_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Redshift
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_redshift_nvl_to_coalesce() {
        // Redshift supports NVL but we normalize to COALESCE
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Redshift
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // ClickHouse tests
    #[test]
    fn test_clickhouse_basic_select() {
        // ClickHouse uses uppercase keywords (matching Python sqlglot behavior)
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::ClickHouse
            ),
            "SELECT a, b FROM t"
        );
    }

    // Databricks tests
    #[test]
    fn test_databricks_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Databricks
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_databricks_nvl_to_coalesce() {
        // Databricks uses COALESCE for null handling
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Databricks
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Cross-dialect transpilation tests
    #[test]
    fn test_sqlite_to_postgres() {
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE a = 1",
                DialectType::SQLite,
                DialectType::PostgreSQL
            ),
            "SELECT * FROM t WHERE a = 1"
        );
    }

    #[test]
    fn test_presto_to_trino() {
        // Presto and Trino are highly compatible
        assert_eq!(
            transpile(
                "SELECT COUNT(*) FROM t GROUP BY a",
                DialectType::Presto,
                DialectType::Trino
            ),
            "SELECT COUNT(*) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_redshift_to_postgres() {
        // Redshift is PostgreSQL-based
        assert_eq!(
            transpile(
                "SELECT a, SUM(b) FROM t GROUP BY a",
                DialectType::Redshift,
                DialectType::PostgreSQL
            ),
            "SELECT a, SUM(b) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_databricks_to_spark() {
        // Databricks extends Spark
        assert_eq!(
            transpile(
                "SELECT * FROM t",
                DialectType::Databricks,
                DialectType::Spark
            ),
            "SELECT * FROM t"
        );
    }

    // Athena tests (Phase 6.3)
    #[test]
    fn test_athena_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Athena
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_athena_ifnull_to_coalesce() {
        // Athena (Trino-based) uses COALESCE instead of IFNULL
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Athena
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    #[test]
    fn test_athena_nvl_to_coalesce() {
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Athena
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Teradata tests (Phase 6.3)
    #[test]
    fn test_teradata_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Teradata
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_teradata_ifnull_to_coalesce() {
        // Teradata uses COALESCE
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Teradata
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // Doris tests (Phase 6.3)
    #[test]
    fn test_doris_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Doris
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_doris_nvl_to_ifnull() {
        // Doris (MySQL-compatible) uses IFNULL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::Generic,
                DialectType::Doris
            ),
            "SELECT IFNULL(a, b) FROM t"
        );
    }

    // StarRocks tests (Phase 6.3)
    #[test]
    fn test_starrocks_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::StarRocks
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_starrocks_nvl_to_ifnull() {
        // StarRocks (MySQL-compatible) uses IFNULL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::Generic,
                DialectType::StarRocks
            ),
            "SELECT IFNULL(a, b) FROM t"
        );
    }

    // Materialize tests (Phase 6.3)
    #[test]
    fn test_materialize_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::Materialize
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_materialize_ifnull_to_coalesce() {
        // Materialize (PostgreSQL-compatible) uses COALESCE
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::Materialize
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // RisingWave tests (Phase 6.3)
    #[test]
    fn test_risingwave_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::RisingWave
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_risingwave_ifnull_to_coalesce() {
        // RisingWave (PostgreSQL-compatible) uses COALESCE
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::RisingWave
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // SingleStore tests (Phase 6.3)
    #[test]
    fn test_singlestore_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::SingleStore
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_singlestore_nvl_to_ifnull() {
        // SingleStore (MySQL-compatible) uses IFNULL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::Generic,
                DialectType::SingleStore
            ),
            "SELECT IFNULL(a, b) FROM t"
        );
    }

    // CockroachDB tests (Phase 6.3)
    #[test]
    fn test_cockroachdb_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::CockroachDB
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_cockroachdb_ifnull_to_coalesce() {
        // CockroachDB (PostgreSQL-compatible) uses COALESCE
        assert_eq!(
            transpile(
                "SELECT IFNULL(a, b) FROM t",
                DialectType::MySQL,
                DialectType::CockroachDB
            ),
            "SELECT COALESCE(a, b) FROM t"
        );
    }

    // TiDB tests (Phase 6.3)
    #[test]
    fn test_tidb_basic_select() {
        assert_eq!(
            transpile(
                "SELECT a, b FROM t",
                DialectType::Generic,
                DialectType::TiDB
            ),
            "SELECT a, b FROM t"
        );
    }

    #[test]
    fn test_tidb_nvl_to_ifnull() {
        // TiDB (MySQL-compatible) uses IFNULL
        assert_eq!(
            transpile(
                "SELECT NVL(a, b) FROM t",
                DialectType::Generic,
                DialectType::TiDB
            ),
            "SELECT IFNULL(a, b) FROM t"
        );
    }

    // Cross-dialect tests for Phase 6.3 dialects
    #[test]
    fn test_athena_to_trino() {
        // Athena is based on Trino
        assert_eq!(
            transpile(
                "SELECT COUNT(*) FROM t GROUP BY a",
                DialectType::Athena,
                DialectType::Trino
            ),
            "SELECT COUNT(*) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_materialize_to_postgres() {
        // Materialize is PostgreSQL-compatible
        assert_eq!(
            transpile(
                "SELECT a, SUM(b) FROM t GROUP BY a",
                DialectType::Materialize,
                DialectType::PostgreSQL
            ),
            "SELECT a, SUM(b) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_risingwave_to_postgres() {
        // RisingWave is PostgreSQL-compatible
        assert_eq!(
            transpile(
                "SELECT a, SUM(b) FROM t GROUP BY a",
                DialectType::RisingWave,
                DialectType::PostgreSQL
            ),
            "SELECT a, SUM(b) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_cockroachdb_to_postgres() {
        // CockroachDB is PostgreSQL-compatible
        assert_eq!(
            transpile(
                "SELECT a, SUM(b) FROM t GROUP BY a",
                DialectType::CockroachDB,
                DialectType::PostgreSQL
            ),
            "SELECT a, SUM(b) FROM t GROUP BY a"
        );
    }

    #[test]
    fn test_tidb_to_mysql() {
        // TiDB is MySQL-compatible
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE a = 1",
                DialectType::TiDB,
                DialectType::MySQL
            ),
            "SELECT * FROM t WHERE a = 1"
        );
    }

    #[test]
    fn test_singlestore_to_mysql() {
        // SingleStore is MySQL-compatible
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE a = 1",
                DialectType::SingleStore,
                DialectType::MySQL
            ),
            "SELECT * FROM t WHERE a = 1"
        );
    }

    #[test]
    fn test_doris_to_mysql() {
        // Doris is MySQL-compatible
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE a = 1",
                DialectType::Doris,
                DialectType::MySQL
            ),
            "SELECT * FROM t WHERE a = 1"
        );
    }

    #[test]
    fn test_starrocks_to_mysql() {
        // StarRocks is MySQL-compatible
        assert_eq!(
            transpile(
                "SELECT * FROM t WHERE a = 1",
                DialectType::StarRocks,
                DialectType::MySQL
            ),
            "SELECT * FROM t WHERE a = 1"
        );
    }
}

// ============================================================================
// CONNECT BY Tests - Oracle Hierarchical Queries (Phase C.1)
// ============================================================================

#[cfg(test)]
mod connect_by_tests {
    use super::*;

    #[test]
    fn test_connect_by_basic() {
        // Basic CONNECT BY with PRIOR
        let result = roundtrip("SELECT * FROM employees CONNECT BY PRIOR employee_id = manager_id");
        assert!(result.contains("CONNECT BY"));
        assert!(result.contains("PRIOR"));
    }

    #[test]
    fn test_start_with_before_connect_by() {
        // START WITH before CONNECT BY
        let result = roundtrip("SELECT * FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id");
        assert!(result.contains("START WITH"));
        assert!(result.contains("CONNECT BY"));
        assert!(result.contains("PRIOR"));
    }

    #[test]
    fn test_start_with_after_connect_by() {
        // START WITH after CONNECT BY (should also be valid)
        let result = roundtrip("SELECT * FROM employees CONNECT BY PRIOR employee_id = manager_id START WITH manager_id IS NULL");
        assert!(result.contains("START WITH"));
        assert!(result.contains("CONNECT BY"));
    }

    #[test]
    fn test_connect_by_nocycle() {
        // CONNECT BY with NOCYCLE
        let result =
            roundtrip("SELECT * FROM employees CONNECT BY NOCYCLE PRIOR employee_id = manager_id");
        assert!(result.contains("CONNECT BY"));
        assert!(result.contains("NOCYCLE"));
        assert!(result.contains("PRIOR"));
    }

    #[test]
    fn test_connect_by_with_level() {
        // CONNECT BY with LEVEL pseudocolumn
        let result = roundtrip(
            "SELECT employee_id, LEVEL FROM employees CONNECT BY PRIOR employee_id = manager_id",
        );
        assert!(result.contains("LEVEL"));
        assert!(result.contains("CONNECT BY"));
    }

    #[test]
    fn test_connect_by_root() {
        // CONNECT_BY_ROOT function
        let result = roundtrip("SELECT CONNECT_BY_ROOT(employee_id) FROM employees CONNECT BY PRIOR employee_id = manager_id");
        assert!(result.contains("CONNECT_BY_ROOT"));
    }

    #[test]
    fn test_connect_by_with_where() {
        // CONNECT BY after WHERE clause
        let result = roundtrip("SELECT * FROM employees WHERE department_id = 10 CONNECT BY PRIOR employee_id = manager_id");
        assert!(result.contains("WHERE"));
        assert!(result.contains("CONNECT BY"));
    }

    #[test]
    fn test_connect_by_complex() {
        // More complex CONNECT BY with AND in condition
        let result = roundtrip("SELECT * FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 5");
        assert!(result.contains("START WITH"));
        assert!(result.contains("CONNECT BY"));
        assert!(result.contains("AND"));
    }
}

// ============================================================================
// MATCH_RECOGNIZE Tests - Oracle/Snowflake Pattern Matching (Phase C.2)
// ============================================================================

#[cfg(test)]
mod match_recognize_tests {
    use super::*;
    use polyglot_sql::generator::GeneratorConfig;

    fn roundtrip_oracle(sql: &str) -> String {
        let ast = Parser::parse_sql(sql).expect(&format!("Failed to parse: {}", sql));
        let config = GeneratorConfig {
            dialect: Some(DialectType::Oracle),
            ..Default::default()
        };
        let mut gen = Generator::with_config(config);
        gen.generate(&ast[0]).expect("Failed to generate SQL")
    }

    #[test]
    fn test_match_recognize_basic() {
        // Basic MATCH_RECOGNIZE with PATTERN and DEFINE
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (PATTERN (A B) DEFINE A AS A.price > 10) AS mr",
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("PATTERN"));
        assert!(result.contains("DEFINE"));
    }

    #[test]
    fn test_match_recognize_partition_by() {
        // MATCH_RECOGNIZE with PARTITION BY
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("PARTITION BY"));
        assert!(result.contains("symbol"));
    }

    #[test]
    fn test_match_recognize_order_by() {
        // MATCH_RECOGNIZE with ORDER BY
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (ORDER BY trade_date PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("ORDER BY"));
        assert!(result.contains("trade_date"));
    }

    #[test]
    fn test_match_recognize_measures() {
        // MATCH_RECOGNIZE with MEASURES
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (MEASURES A.price AS start_price, B.price AS end_price PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("MEASURES"));
        assert!(result.contains("start_price"));
        assert!(result.contains("end_price"));
    }

    #[test]
    fn test_match_recognize_one_row_per_match() {
        // MATCH_RECOGNIZE with ONE ROW PER MATCH
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (ONE ROW PER MATCH PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("ONE ROW PER MATCH"));
    }

    #[test]
    fn test_match_recognize_all_rows_per_match() {
        // MATCH_RECOGNIZE with ALL ROWS PER MATCH
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (ALL ROWS PER MATCH PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("ALL ROWS PER MATCH"));
    }

    #[test]
    fn test_match_recognize_after_match_skip() {
        // MATCH_RECOGNIZE with AFTER MATCH SKIP
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (AFTER MATCH SKIP PAST LAST ROW PATTERN (A B) DEFINE A AS A.price > 10)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("AFTER MATCH SKIP PAST LAST ROW"));
    }

    #[test]
    fn test_match_recognize_complex() {
        // Full MATCH_RECOGNIZE with all clauses
        let result = roundtrip_oracle(
            "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY trade_date MEASURES A.price AS start_price ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B+ C) DEFINE A AS A.price > 10, B AS B.price > A.price)"
        );
        assert!(result.contains("MATCH_RECOGNIZE"));
        assert!(result.contains("PARTITION BY"));
        assert!(result.contains("ORDER BY"));
        assert!(result.contains("MEASURES"));
        assert!(result.contains("ONE ROW PER MATCH"));
        assert!(result.contains("AFTER MATCH SKIP"));
        assert!(result.contains("PATTERN"));
        assert!(result.contains("DEFINE"));
    }

    #[test]
    fn test_match_recognize_unsupported_dialect() {
        // MATCH_RECOGNIZE should generate comment for unsupported dialects
        let ast = Parser::parse_sql(
            "SELECT * FROM ticker MATCH_RECOGNIZE (PATTERN (A B) DEFINE A AS A.price > 10)",
        )
        .expect("Failed to parse");
        let config = GeneratorConfig {
            dialect: Some(DialectType::PostgreSQL),
            ..Default::default()
        };
        let mut gen = Generator::with_config(config);
        let result = gen.generate(&ast[0]).expect("Failed to generate SQL");
        assert!(result.contains("MATCH_RECOGNIZE not supported"));
    }
}
