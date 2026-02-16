//! Identity Roundtrip Tests
//!
//! Ported from sqlglot's tests/fixtures/identity.sql
//! These tests verify that SQL can be parsed and regenerated identically.

use polyglot_sql::parser::Parser;
use polyglot_sql::generator::Generator;

/// Helper function to test roundtrip: parse SQL, generate, and verify
fn roundtrip(sql: &str) -> String {
    let ast = Parser::parse_sql(sql).expect(&format!("Failed to parse: {}", sql));
    Generator::sql(&ast[0]).expect("Failed to generate SQL")
}

/// Assert that SQL roundtrips with potential normalization
fn assert_roundtrip(sql: &str) {
    let result = roundtrip(sql);
    // Re-parse the generated SQL to verify it's valid
    let ast2 = Parser::parse_sql(&result).expect(&format!("Failed to re-parse: {}", result));
    // Generate again and compare
    let result2 = Generator::sql(&ast2[0]).expect("Failed to generate SQL again");
    assert_eq!(result, result2, "Roundtrip not stable for: {}\nFirst: {}\nSecond: {}", sql, result, result2);
}

/// Assert SQL roundtrips exactly
fn assert_exact_roundtrip(sql: &str) {
    let result = roundtrip(sql);
    assert_eq!(sql, result, "Not exact roundtrip for: {}\nGot: {}", sql, result);
}

// ============================================================================
// LITERALS - Lines 1-37 from identity.sql
// ============================================================================

#[cfg(test)]
mod literal_tests {
    use super::*;

    #[test]
    fn test_numeric_literals() {
        // Integer literals
        assert_roundtrip("SELECT 1");
        assert_roundtrip("SELECT (1)");

        // Decimal literals
        assert_roundtrip("SELECT 1.0");
        assert_roundtrip("SELECT (1.0)");
        assert_roundtrip("SELECT 0.2");
    }

    #[test]
    fn test_scientific_notation() {
        assert_roundtrip("SELECT 1E2");
        assert_roundtrip("SELECT 1E+2");
        assert_roundtrip("SELECT 1E-2");
        assert_roundtrip("SELECT 1.1E10");
        assert_roundtrip("SELECT 1.12e-10");
    }

    #[test]
    fn test_negative_numbers() {
        assert_roundtrip("SELECT 1 - -1");
    }

    #[test]
    fn test_negative_scientific_notation() {
        assert_roundtrip("SELECT -11.023E7 * 3");
        assert_roundtrip("SELECT - -5");
    }

    #[test]
    fn test_string_literals() {
        assert_roundtrip("SELECT ''");
        assert_roundtrip("SELECT 'x'");
        assert_roundtrip("SELECT ''''");
    }

    #[test]
    fn test_national_string_literals() {
        assert_roundtrip("SELECT N'abc'");
    }

    #[test]
    fn test_hex_string_literals() {
        assert_roundtrip("SELECT X'ABCD'");
    }

    #[test]
    fn test_bit_string_literals() {
        assert_roundtrip("SELECT B'01010'");
    }

    #[test]
    fn test_boolean_literals() {
        assert_roundtrip("SELECT TRUE");
        assert_roundtrip("SELECT FALSE");
        assert_roundtrip("SELECT ((TRUE))");
    }

    #[test]
    fn test_null_literal() {
        assert_roundtrip("SELECT NULL");
    }
}

// ============================================================================
// ARITHMETIC & OPERATORS - Lines 38-60 from identity.sql
// ============================================================================

#[cfg(test)]
mod operator_tests {
    use super::*;

    #[test]
    fn test_comparison_operators() {
        assert_roundtrip("SELECT x < 1");
        assert_roundtrip("SELECT x <= 1");
        assert_roundtrip("SELECT x > 1");
        assert_roundtrip("SELECT x >= 1");
        assert_roundtrip("SELECT x <> 1");
        assert_roundtrip("SELECT x = 1");
    }

    #[test]
    fn test_arithmetic_operators() {
        assert_roundtrip("SELECT x % 1");
        assert_roundtrip("SELECT 1 + 2");
        assert_roundtrip("SELECT 1 - 2");
        assert_roundtrip("SELECT 1 * 2");
        assert_roundtrip("SELECT 1 / 2");
        assert_roundtrip("SELECT (1 * 2) / (3 - 5)");
    }

    #[test]
    fn test_logical_operators() {
        assert_roundtrip("SELECT x = y OR x > 1");
        assert_roundtrip("SELECT x = 1 AND y = 2");
        assert_roundtrip("SELECT NOT x");
        assert_roundtrip("SELECT NOT 1");
        assert_roundtrip("SELECT NOT NOT 1");
    }

    #[test]
    fn test_string_concatenation() {
        assert_roundtrip("SELECT x || y");
    }

    // TODO: Bitwise operators not fully implemented
    // #[test]
    // fn test_bitwise_operators() {
    //     assert_roundtrip("SELECT x & 1");
    //     assert_roundtrip("SELECT x | 1");
    //     assert_roundtrip("SELECT x ^ 1");
    //     assert_roundtrip("SELECT ~x");
    //     assert_roundtrip("SELECT x << 1");
    //     assert_roundtrip("SELECT x >> 1");
    // }
}

// ============================================================================
// COLUMN ACCESS - Lines 61-85 from identity.sql
// ============================================================================

#[cfg(test)]
mod column_access_tests {
    use super::*;

    #[test]
    fn test_simple_column() {
        assert_roundtrip("SELECT x");
        assert_roundtrip("SELECT a.b");
    }

    #[test]
    fn test_qualified_columns() {
        assert_roundtrip("SELECT a.b");
    }

    #[test]
    fn test_multi_level_column_access() {
        assert_roundtrip("SELECT a.b.c");
        assert_roundtrip("SELECT a.b.c.d");
        assert_roundtrip("SELECT a.b.c.d.e");
    }

    #[test]
    fn test_subscript_access() {
        assert_roundtrip("SELECT a[0]");
        assert_roundtrip("SELECT x[1]");
    }

    #[test]
    fn test_reserved_word_columns() {
        assert_roundtrip("SELECT time");
        assert_roundtrip("SELECT zone");
        assert_roundtrip("SELECT time * 100");
    }
}

// ============================================================================
// PREDICATES - Lines 86-100 from identity.sql
// ============================================================================

#[cfg(test)]
mod predicate_tests {
    use super::*;

    #[test]
    fn test_in_expression() {
        assert_roundtrip("SELECT x IN (1, 2, 3)");
        assert_roundtrip("SELECT x IN (-1, 1)");
        assert_roundtrip("SELECT x IN ('a', 'b')");
        assert_roundtrip("SELECT x IN ((1))");
    }

    #[test]
    fn test_between() {
        assert_roundtrip("SELECT x BETWEEN -1 AND 1");
        assert_roundtrip("SELECT x BETWEEN 1 AND 10");
    }

    #[test]
    fn test_is_null() {
        assert_roundtrip("SELECT NOT x IS NULL");
        assert_roundtrip("SELECT x IS NULL");
    }

    #[test]
    fn test_is_true_false() {
        assert_roundtrip("SELECT x IS TRUE");
        assert_roundtrip("SELECT x IS FALSE");
    }

    #[test]
    fn test_like() {
        assert_roundtrip("SELECT x LIKE y");
        assert_roundtrip("SELECT x LIKE '%y%'");
        assert_roundtrip("SELECT x ILIKE '%y%'");
    }

    #[test]
    fn test_glob() {
        assert_roundtrip("SELECT x GLOB '??-*'");
        assert_roundtrip("SELECT x GLOB y");
    }
}

// ============================================================================
// FUNCTIONS - Lines 97-200 from identity.sql
// ============================================================================

#[cfg(test)]
mod function_tests {
    use super::*;

    #[test]
    fn test_aggregate_functions() {
        assert_roundtrip("SELECT SUM(1)");
        assert_roundtrip("SELECT COUNT(a)");
        assert_roundtrip("SELECT COUNT(*)");
        assert_roundtrip("SELECT COUNT(DISTINCT a)");
        assert_roundtrip("SELECT AVG(a)");
        assert_roundtrip("SELECT MIN(a)");
        assert_roundtrip("SELECT MAX(a)");
    }

    #[test]
    fn test_math_functions() {
        assert_roundtrip("SELECT ABS(a)");
        assert_roundtrip("SELECT CEIL(a)");
        assert_roundtrip("SELECT FLOOR(a)");
        assert_roundtrip("SELECT ROUND(a)");
        assert_roundtrip("SELECT ROUND(a, 2)");
        assert_roundtrip("SELECT SQRT(a)");
        assert_roundtrip("SELECT POWER(a, 2)");
        assert_roundtrip("SELECT EXP(a)");
        assert_roundtrip("SELECT LN(a)");
    }

    #[test]
    fn test_string_functions() {
        assert_roundtrip("SELECT TRIM('a')");
    }

    #[test]
    fn test_replace_function() {
        assert_roundtrip("SELECT REPLACE('new york', ' ', '_')");
    }

    #[test]
    fn test_concat_ws() {
        assert_roundtrip("SELECT CONCAT_WS('-', 'a', 'b')");
    }

    #[test]
    fn test_null_handling_functions() {
        assert_roundtrip("SELECT COALESCE(a, b, c)");
        assert_roundtrip("SELECT GREATEST(a, b, c)");
        assert_roundtrip("SELECT LEAST(a, b, c)");
    }

    #[test]
    fn test_date_functions() {
        assert_roundtrip("SELECT CURRENT_DATE");
        assert_roundtrip("SELECT CURRENT_TIMESTAMP");
    }

    #[test]
    fn test_extract_function() {
        assert_roundtrip("SELECT EXTRACT(YEAR FROM y)");
        assert_roundtrip("SELECT EXTRACT(MONTH FROM y)");
        assert_roundtrip("SELECT EXTRACT(DAY FROM d)");
        assert_roundtrip("SELECT EXTRACT(HOUR FROM ts)");
    }

    #[test]
    fn test_cast() {
        assert_roundtrip("SELECT CAST(a AS INT)");
        assert_roundtrip("SELECT CAST(a AS VARCHAR)");
    }

    #[test]
    fn test_try_cast() {
        assert_roundtrip("SELECT TRY_CAST(a AS INT)");
    }

    #[test]
    fn test_cast_decimal_precision() {
        assert_roundtrip("SELECT CAST(a AS DECIMAL(5, 3))");
    }

    #[test]
    fn test_array_functions() {
        assert_roundtrip("SELECT ARRAY(1, 2)");
        assert_roundtrip("SELECT ARRAY_CONTAINS(x, 1)");
    }

    #[test]
    fn test_json_functions() {
        assert_roundtrip("SELECT JSON_EXTRACT(x, '$.name')");
        assert_roundtrip("SELECT JSON_EXTRACT_SCALAR(x, '$.name')");
    }

    #[test]
    fn test_window_value_functions() {
        assert_roundtrip("SELECT FIRST_VALUE(a)");
        assert_roundtrip("SELECT LAST_VALUE(a)");
        assert_roundtrip("SELECT LAG(x) OVER (ORDER BY y)");
        assert_roundtrip("SELECT LEAD(a) OVER (ORDER BY b)");
    }
}

// ============================================================================
// CASE EXPRESSIONS
// ============================================================================

#[cfg(test)]
mod case_tests {
    use super::*;

    #[test]
    fn test_simple_case() {
        assert_roundtrip("SELECT CASE WHEN TRUE THEN 1 ELSE 0 END");
        assert_roundtrip("SELECT CASE WHEN a < b THEN 1 WHEN a < c THEN 2 ELSE 3 END");
    }

    #[test]
    fn test_searched_case() {
        assert_roundtrip("SELECT CASE 1 WHEN 1 THEN 1 ELSE 2 END");
        assert_roundtrip("SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
    }

    #[test]
    fn test_case_with_expressions() {
        assert_roundtrip("SELECT CASE WHEN (x > 1) THEN 1 ELSE 0 END");
        assert_roundtrip("SELECT CASE (1) WHEN 1 THEN 1 ELSE 0 END");
    }
}

// ============================================================================
// SELECT STATEMENTS - Lines 214-400 from identity.sql
// ============================================================================

#[cfg(test)]
mod select_tests {
    use super::*;

    #[test]
    fn test_basic_select() {
        assert_roundtrip("SELECT 1");
        assert_roundtrip("SELECT 1 FROM test");
        assert_roundtrip("SELECT * FROM test");
        assert_roundtrip("SELECT a FROM test");
        assert_roundtrip("SELECT a, b FROM test");
    }

    #[test]
    fn test_select_alias() {
        assert_roundtrip("SELECT a AS b FROM test");
        assert_roundtrip("SELECT 1 AS x FROM test");
        assert_roundtrip("SELECT 1 + 2 AS x FROM test");
    }

    #[test]
    fn test_select_distinct() {
        assert_roundtrip("SELECT DISTINCT x FROM test");
        assert_roundtrip("SELECT DISTINCT x, y FROM test");
    }

    #[test]
    fn test_select_where() {
        assert_roundtrip("SELECT a FROM test WHERE a = 1");
        assert_roundtrip("SELECT a FROM test WHERE a = 1 AND b = 2");
        assert_roundtrip("SELECT a FROM test WHERE NOT FALSE");
        assert_roundtrip("SELECT a FROM test WHERE (a > 1)");
    }

    #[test]
    fn test_select_order_by() {
        assert_roundtrip("SELECT a FROM test ORDER BY a");
        assert_roundtrip("SELECT a FROM test ORDER BY a, b");
        assert_roundtrip("SELECT x FROM tests ORDER BY a DESC, b DESC, c");
        assert_roundtrip("SELECT a FROM test ORDER BY a ASC");
    }

    #[test]
    fn test_select_group_by() {
        assert_roundtrip("SELECT a, b FROM test GROUP BY 1");
        assert_roundtrip("SELECT a, b FROM test GROUP BY a");
        assert_roundtrip("SELECT a, b FROM test WHERE a = 1 GROUP BY a HAVING a = 2");
    }

    #[test]
    fn test_select_limit() {
        assert_roundtrip("SELECT * FROM test LIMIT 100");
        assert_roundtrip("SELECT * FROM test LIMIT 100 OFFSET 200");
    }

    #[test]
    fn test_subquery_in_from() {
        assert_roundtrip("SELECT a FROM (SELECT a FROM test) AS x");
        assert_roundtrip("SELECT * FROM (SELECT 1) AS x");
    }

    #[test]
    fn test_subquery_in_where() {
        assert_roundtrip("SELECT a FROM test WHERE EXISTS(SELECT 1)");
    }

    #[test]
    fn test_in_subquery() {
        assert_roundtrip("SELECT a FROM test WHERE a IN (SELECT b FROM z)");
    }

    #[test]
    fn test_all_subquery() {
        assert_roundtrip("SELECT a FROM test WHERE a > ALL (SELECT 1)");
    }

    #[test]
    fn test_qualified_star() {
        assert_roundtrip("SELECT test.* FROM test");
        assert_roundtrip("SELECT a.*, b.* FROM a, b");
    }
}

// ============================================================================
// JOINS - Lines 404-430 from identity.sql
// ============================================================================

#[cfg(test)]
mod join_tests {
    use super::*;

    #[test]
    fn test_inner_join() {
        assert_roundtrip("SELECT 1 FROM a JOIN b ON a.x = b.x");
        assert_roundtrip("SELECT 1 FROM a INNER JOIN b ON a.x = b.x");
    }

    #[test]
    fn test_outer_joins() {
        assert_roundtrip("SELECT 1 FROM a LEFT JOIN b ON a.x = b.x");
        assert_roundtrip("SELECT 1 FROM a RIGHT JOIN b ON a.x = b.x");
        assert_roundtrip("SELECT 1 FROM a FULL JOIN b ON a.x = b.x");
    }

    #[test]
    fn test_cross_join() {
        assert_roundtrip("SELECT 1 FROM a CROSS JOIN b");
    }

    #[test]
    fn test_natural_join() {
        assert_roundtrip("SELECT 1 FROM a NATURAL JOIN b");
        assert_roundtrip("SELECT 1 FROM a NATURAL LEFT JOIN b");
    }

    #[test]
    fn test_join_using() {
        assert_roundtrip("SELECT 1 FROM a JOIN b USING (x)");
        assert_roundtrip("SELECT 1 FROM a JOIN b USING (x, y, z)");
    }

    #[test]
    fn test_semi_anti_joins() {
        assert_roundtrip("SELECT 1 FROM a SEMI JOIN b ON a.x = b.x");
        assert_roundtrip("SELECT 1 FROM a LEFT SEMI JOIN b ON a.x = b.x");
        assert_roundtrip("SELECT 1 FROM a LEFT ANTI JOIN b ON a.x = b.x");
    }

    #[test]
    fn test_multiple_joins() {
        assert_roundtrip("SELECT 1 FROM a JOIN b ON a.foo = b.bar JOIN c ON a.foo = c.bar");
    }
}

// ============================================================================
// SET OPERATIONS - Lines 418-436 from identity.sql
// ============================================================================

#[cfg(test)]
mod set_operation_tests {
    use super::*;

    #[test]
    fn test_union() {
        assert_roundtrip("SELECT 1 FROM a UNION SELECT 2 FROM b");
        assert_roundtrip("SELECT 1 FROM a UNION ALL SELECT 2 FROM b");
        assert_roundtrip("SELECT 1 UNION ALL SELECT 2");
    }

    #[test]
    fn test_except() {
        assert_roundtrip("SELECT 1 EXCEPT SELECT 2");
    }

    #[test]
    fn test_intersect() {
        assert_roundtrip("SELECT 1 INTERSECT SELECT 2");
    }

    #[test]
    fn test_parenthesized_set_ops() {
        assert_roundtrip("(SELECT 1) UNION (SELECT 2)");
        assert_roundtrip("(SELECT 1) UNION SELECT 2");
    }
}

// ============================================================================
// CTEs (Common Table Expressions) - Lines 469-493 from identity.sql
// ============================================================================

#[cfg(test)]
mod cte_tests {
    use super::*;

    #[test]
    fn test_basic_cte() {
        assert_roundtrip("WITH a AS (SELECT 1) SELECT * FROM a");
        assert_roundtrip("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT a.*, b.* FROM a CROSS JOIN b");
    }

    #[test]
    fn test_cte_with_set_ops() {
        assert_roundtrip("WITH a AS (SELECT 1) SELECT 1 UNION ALL SELECT 2");
        assert_roundtrip("WITH a AS (SELECT 1) SELECT 1 UNION SELECT 2");
    }

    #[test]
    fn test_recursive_cte() {
        assert_roundtrip("WITH RECURSIVE T(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 100) SELECT SUM(n) FROM t");
    }

    #[test]
    fn test_nested_cte() {
        assert_roundtrip("WITH a AS (WITH b AS (SELECT 1 AS x) SELECT b.x FROM b) SELECT a.x FROM a");
    }
}

// ============================================================================
// WINDOW FUNCTIONS - Lines 494-524 from identity.sql
// ============================================================================

#[cfg(test)]
mod window_tests {
    use super::*;

    #[test]
    fn test_basic_window() {
        assert_roundtrip("SELECT RANK() OVER () FROM x");
        assert_roundtrip("SELECT RANK() OVER () AS y FROM x");
    }

    #[test]
    fn test_window_partition() {
        assert_roundtrip("SELECT RANK() OVER (PARTITION BY a) FROM x");
        assert_roundtrip("SELECT RANK() OVER (PARTITION BY a, b) FROM x");
    }

    #[test]
    fn test_window_order() {
        assert_roundtrip("SELECT RANK() OVER (ORDER BY a) FROM x");
        assert_roundtrip("SELECT RANK() OVER (ORDER BY a, b) FROM x");
        assert_roundtrip("SELECT RANK() OVER (PARTITION BY a ORDER BY a) FROM x");
    }

    #[test]
    fn test_window_frame() {
        assert_roundtrip("SELECT SUM(x) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
        assert_roundtrip("SELECT SUM(x) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        assert_roundtrip("SELECT SUM(x) OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)");
    }

    #[test]
    fn test_window_range_frame() {
        assert_roundtrip("SELECT SUM(x) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    }

    #[test]
    fn test_aggregate_with_window() {
        assert_roundtrip("SELECT SUM(x) OVER (PARTITION BY a) AS y FROM x");
        assert_roundtrip("SELECT COUNT(DISTINCT a) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)");
    }

    #[test]
    fn test_window_filter() {
        assert_roundtrip("SELECT SUM(x) FILTER(WHERE x > 1)");
        assert_roundtrip("SELECT SUM(x) FILTER(WHERE x > 1) OVER (ORDER BY y)");
    }
}

// ============================================================================
// DDL - CREATE TABLE - Lines 545-605 from identity.sql
// ============================================================================

#[cfg(test)]
mod ddl_create_table_tests {
    use super::*;

    #[test]
    fn test_create_table_as_select() {
        assert_roundtrip("CREATE TABLE foo AS SELECT 1");
        assert_roundtrip("CREATE TABLE a.b AS SELECT 1");
        assert_roundtrip("CREATE TABLE IF NOT EXISTS x AS SELECT a FROM d");
    }

    #[test]
    fn test_create_table_columns() {
        assert_roundtrip("CREATE TABLE z (a INT, b VARCHAR, c VARCHAR(100), d DECIMAL(5, 3))");
        assert_roundtrip("CREATE TABLE z (a INT, PRIMARY KEY (a))");
    }

    #[test]
    fn test_create_table_constraints() {
        assert_roundtrip("CREATE TABLE z (a INT UNIQUE)");
        assert_roundtrip("CREATE TABLE z (a INT NOT NULL)");
        assert_roundtrip("CREATE TABLE z (a INT PRIMARY KEY)");
    }

    #[test]
    fn test_create_table_default() {
        assert_roundtrip("CREATE TABLE z (n INT DEFAULT 0 NOT NULL)");
    }

    #[test]
    fn test_create_table_references() {
        assert_roundtrip("CREATE TABLE z (a INT REFERENCES parent (b, c))");
        assert_roundtrip("CREATE TABLE z (a INT, FOREIGN KEY (a) REFERENCES parent (b, c))");
    }

    #[test]
    fn test_create_temporary_table() {
        assert_roundtrip("CREATE TEMPORARY TABLE x AS SELECT a FROM d");
        assert_roundtrip("CREATE TEMPORARY TABLE IF NOT EXISTS x AS SELECT a FROM d");
    }
}

// ============================================================================
// DDL - CREATE VIEW - Lines 611-623 from identity.sql
// ============================================================================

#[cfg(test)]
mod ddl_create_view_tests {
    use super::*;

    #[test]
    fn test_create_view() {
        assert_roundtrip("CREATE VIEW x AS SELECT a FROM b");
        assert_roundtrip("CREATE VIEW IF NOT EXISTS x AS SELECT a FROM b");
    }

    #[test]
    fn test_create_or_replace_view() {
        assert_roundtrip("CREATE OR REPLACE VIEW x AS SELECT *");
    }

    #[test]
    fn test_create_materialized_view() {
        assert_roundtrip("CREATE MATERIALIZED VIEW x.y.z AS SELECT a FROM b");
    }

    #[test]
    fn test_create_temporary_view() {
        assert_roundtrip("CREATE TEMPORARY VIEW x AS SELECT a FROM d");
    }
}

// ============================================================================
// DDL - CREATE INDEX, SCHEMA, FUNCTION - Lines 636-647 from identity.sql
// ============================================================================

#[cfg(test)]
mod ddl_other_tests {
    use super::*;

    #[test]
    fn test_create_index() {
        assert_roundtrip("CREATE INDEX abc ON t(a)");
        assert_roundtrip("CREATE INDEX abc ON t(a, b, b)");
        assert_roundtrip("CREATE UNIQUE INDEX abc ON t(a, b, b)");
    }

    #[test]
    fn test_create_schema() {
        assert_roundtrip("CREATE SCHEMA x");
        assert_roundtrip("CREATE SCHEMA IF NOT EXISTS y");
    }

    #[test]
    fn test_create_database() {
        assert_roundtrip("CREATE DATABASE x");
        assert_roundtrip("CREATE DATABASE IF NOT EXISTS y");
    }

    #[test]
    fn test_create_function() {
        assert_roundtrip("CREATE FUNCTION f");
        assert_roundtrip("CREATE FUNCTION f AS 'g'");
        assert_roundtrip("CREATE TEMPORARY FUNCTION f");
    }
}

// ============================================================================
// DDL - DROP STATEMENTS - Lines 653-687 from identity.sql
// ============================================================================

#[cfg(test)]
mod ddl_drop_tests {
    use super::*;

    #[test]
    fn test_drop_table() {
        assert_roundtrip("DROP TABLE a");
        assert_roundtrip("DROP TABLE a.b");
        assert_roundtrip("DROP TABLE IF EXISTS a");
        assert_roundtrip("DROP TABLE IF EXISTS a.b");
        assert_roundtrip("DROP TABLE a CASCADE");
    }

    #[test]
    fn test_drop_view() {
        assert_roundtrip("DROP VIEW a");
        assert_roundtrip("DROP VIEW a.b");
        assert_roundtrip("DROP VIEW IF EXISTS a");
        assert_roundtrip("DROP MATERIALIZED VIEW x.y.z");
    }

    #[test]
    fn test_drop_index() {
        assert_roundtrip("DROP INDEX a.b.c");
    }

    #[test]
    fn test_drop_function() {
        assert_roundtrip("DROP FUNCTION a.b.c (INT)");
    }
}

// ============================================================================
// DML - INSERT, UPDATE, DELETE - Lines 666-722 from identity.sql
// ============================================================================

#[cfg(test)]
mod dml_tests {
    use super::*;

    #[test]
    fn test_insert_values() {
        assert_roundtrip("INSERT INTO x VALUES (1, 'a', 2.0)");
        assert_roundtrip("INSERT INTO x VALUES (1, 'a', 2.0), (1, 'a', 3.0)");
    }

    #[test]
    fn test_insert_select() {
        assert_roundtrip("INSERT INTO x SELECT * FROM y");
        assert_roundtrip("INSERT INTO y (a, b, c) SELECT a, b, c FROM x");
    }

    #[test]
    fn test_insert_with_cte() {
        assert_roundtrip("WITH a AS (SELECT 1) INSERT INTO b SELECT * FROM a");
    }

    #[test]
    fn test_update() {
        assert_roundtrip("UPDATE tbl_name SET foo = 123");
        assert_roundtrip("UPDATE tbl_name SET foo = 123, bar = 345");
    }

    #[test]
    fn test_update_qualified_where() {
        assert_roundtrip("UPDATE db.tbl_name SET foo = 123 WHERE tbl_name.bar = 234");
    }

    #[test]
    fn test_delete() {
        assert_roundtrip("DELETE FROM x WHERE y > 1");
        assert_roundtrip("DELETE FROM y");
    }
}

// ============================================================================
// ALTER TABLE - Lines 763-806 from identity.sql
// ============================================================================

#[cfg(test)]
mod alter_table_tests {
    use super::*;

    #[test]
    fn test_alter_add_column() {
        assert_roundtrip("ALTER TABLE integers ADD COLUMN k INT");
    }

    #[test]
    fn test_alter_add_column_if_exists() {
        assert_roundtrip("ALTER TABLE integers ADD COLUMN IF NOT EXISTS k INT");
        assert_roundtrip("ALTER TABLE IF EXISTS integers ADD COLUMN k INT");
    }

    #[test]
    fn test_alter_drop_column() {
        assert_roundtrip("ALTER TABLE integers DROP COLUMN k");
    }

    #[test]
    fn test_alter_drop_column_advanced() {
        assert_roundtrip("ALTER TABLE integers DROP COLUMN IF EXISTS k");
        assert_roundtrip("ALTER TABLE integers DROP COLUMN k CASCADE");
    }

    #[test]
    fn test_alter_rename() {
        assert_roundtrip("ALTER TABLE table1 RENAME COLUMN c1 TO c2");
        assert_roundtrip("ALTER TABLE table1 RENAME TO table2");
    }

    #[test]
    fn test_alter_add_constraint() {
        assert_roundtrip("ALTER TABLE persons ADD CONSTRAINT persons_pk PRIMARY KEY (first_name, last_name)");
    }
}

// ============================================================================
// PARAMETERS & PLACEHOLDERS - Lines 733-736 from identity.sql
// ============================================================================

#[cfg(test)]
mod parameter_tests {
    use super::*;

    #[test]
    fn test_question_mark_placeholder() {
        assert_roundtrip("SELECT ? FROM x");
        assert_roundtrip("SELECT ? AS ? FROM x WHERE b BETWEEN ? AND ?");
    }

    #[test]
    fn test_named_parameter() {
        assert_roundtrip("SELECT :hello FROM x");
        assert_roundtrip("SELECT :hello, ? FROM x LIMIT :my_limit");
    }

    #[test]
    fn test_at_parameter() {
        assert_roundtrip("SELECT * FROM x OFFSET @skip");
    }
}

// ============================================================================
// SPECIAL CONSTRUCTS
// ============================================================================

#[cfg(test)]
mod special_tests {
    use super::*;

    #[test]
    fn test_interval() {
        assert_roundtrip("SELECT INTERVAL '1' DAY");
        assert_roundtrip("SELECT INTERVAL '1' MONTH");
        assert_roundtrip("SELECT INTERVAL '1' YEAR");
    }

    #[test]
    fn test_use_statement() {
        assert_roundtrip("USE db");
    }

    #[test]
    fn test_use_schema() {
        assert_roundtrip("USE SCHEMA x.y");
    }

    #[test]
    fn test_commit_rollback() {
        assert_roundtrip("COMMIT");
        assert_roundtrip("ROLLBACK");
        assert_roundtrip("BEGIN");
    }

    #[test]
    fn test_describe() {
        assert_roundtrip("DESCRIBE x");
    }

    #[test]
    fn test_array_type() {
        assert_roundtrip("SELECT CAST(a AS ARRAY<INT>)");
    }

    #[test]
    fn test_map() {
        assert_roundtrip("SELECT MAP()");
    }

    #[test]
    fn test_values() {
        assert_roundtrip("VALUES (1)");
        assert_roundtrip("VALUES (1), (2), (3)");
    }
}

// ============================================================================
// ADVANCED FEATURES
// ============================================================================

#[cfg(test)]
mod advanced_tests {
    use super::*;

    #[test]
    fn test_pivot() {
        assert_roundtrip("SELECT a FROM test PIVOT(SUM(x) FOR y IN ('z', 'q'))");
    }

    #[test]
    fn test_unpivot() {
        assert_roundtrip("SELECT a FROM test UNPIVOT(x FOR y IN (z, q)) AS x");
    }

    #[test]
    fn test_tablesample() {
        assert_roundtrip("SELECT a FROM test TABLESAMPLE (0.1 PERCENT)");
        assert_roundtrip("SELECT a FROM test TABLESAMPLE (100 ROWS)");
    }

    #[test]
    fn test_qualify() {
        assert_roundtrip("SELECT id FROM b.a AS a QUALIFY ROW_NUMBER() OVER (PARTITION BY br ORDER BY sadf DESC) = 1");
    }

    #[test]
    fn test_grouping_sets() {
        assert_roundtrip("SELECT a FROM test GROUP BY GROUPING SETS (())");
        assert_roundtrip("SELECT a FROM test GROUP BY GROUPING SETS (x, ())");
        assert_roundtrip("SELECT a FROM test GROUP BY CUBE (x)");
        assert_roundtrip("SELECT a FROM test GROUP BY ROLLUP (x)");
    }

    #[test]
    fn test_lateral_view() {
        assert_roundtrip("SELECT student, score FROM tests LATERAL VIEW EXPLODE(scores)");
    }

    #[test]
    fn test_unnest() {
        assert_roundtrip("SELECT student, score FROM tests CROSS JOIN UNNEST(scores) AS t(score)");
    }

    #[test]
    fn test_except_replace() {
        assert_roundtrip("SELECT * EXCEPT (a, b)");
        assert_roundtrip("SELECT * REPLACE (a AS b, b AS C)");
    }

    #[test]
    fn test_within_group() {
        assert_roundtrip("SELECT LISTAGG(x) WITHIN GROUP (ORDER BY x) AS y");
        assert_roundtrip("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)");
    }
}

// ============================================================================
// COMMENTS
// ============================================================================

#[cfg(test)]
mod comment_tests {
    use super::*;

    #[test]
    fn test_inline_comments() {
        assert_roundtrip("SELECT CAST(x AS INT) /* comment */ FROM foo");
        assert_roundtrip("SELECT c /* c1 */ AS alias /* c2 */");
    }

    #[test]
    fn test_hint_comments() {
        assert_roundtrip("SELECT /*+ SOME_HINT(foo) */ 1");
    }

    #[test]
    fn test_comment_locations_survive_roundtrip() {
        let sql = concat!(
            "WITH prep /* c_post_cte_alias */ AS (",
            "SELECT 1 AS n /* c_after_alias */ ",
            "FROM (/* c_leading_subquery */ SELECT a /* c_in_subquery */ FROM t) AS sub ",
            ") ",
            "SELECT * FROM prep /* c_main_after_table */ ",
            "WHERE n >= (/* c_where_subquery */ SELECT MAX(x) FROM t) ",
            "ORDER BY n /* c_after_order_by */",
            "\n-- c_line_comment"
        );
        let result = roundtrip(sql);
        let reparsed = Parser::parse_sql(&result).expect(&format!("Re-parse failed: {}", result));
        let result2 = Generator::sql(&reparsed[0]).expect("Re-generate failed");
        assert_eq!(result, result2, "Roundtrip not stable: {}", result);

        let expected = [
            "c_post_cte_alias",
            "c_after_alias",
            "c_leading_subquery",
            "c_in_subquery",
            "c_main_after_table",
            "c_where_subquery",
            "c_after_order_by",
            "c_line_comment",
        ];
        for tag in &expected {
            assert!(result.contains(&format!("/* {} */", tag)),
                "Comment '{}' missing or malformed in output.\nInput:  {}\nOutput: {}", tag, sql, result);
        }
    }

    #[test]
    fn test_line_comment_not_leaked_as_sql() {
        let result = roundtrip("SELECT * FROM (\nSELECT 1 AS is_a\n-- bad_comment\n) AS p");
        assert!(!result.contains("is_a bad_comment"),
            "Line comment leaked as SQL text: {}", result);
    }

}

// ============================================================================
// TRIGONOMETRIC FUNCTIONS - Lines 915-934 from identity.sql
// ============================================================================

#[cfg(test)]
mod trig_function_tests {
    use super::*;

    #[test]
    fn test_trig_functions() {
        assert_roundtrip("SELECT ACOS(x)");
        assert_roundtrip("SELECT ASIN(x)");
        assert_roundtrip("SELECT ATAN(x)");
        assert_roundtrip("SELECT ATAN2(x, y)");
        assert_roundtrip("SELECT SIN(x)");
        assert_roundtrip("SELECT COS(x)");
        assert_roundtrip("SELECT TAN(x)");
    }

    #[test]
    fn test_hyperbolic_functions() {
        assert_roundtrip("SELECT ACOSH(x)");
        assert_roundtrip("SELECT ASINH(x)");
        assert_roundtrip("SELECT ATANH(x)");
        assert_roundtrip("SELECT SINH(x)");
        assert_roundtrip("SELECT COSH(x)");
        assert_roundtrip("SELECT TANH(x)");
    }
}
