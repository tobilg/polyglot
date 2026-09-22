//! Dialect Matrix Transpilation Tests
//!
//! Tests transpilation between all priority dialect pairs (7 dialects = 42 pairs).
//! Priority dialects: Generic, PostgreSQL, MySQL, BigQuery, Snowflake, DuckDB, TSQL
//!
//! Each test ensures SQL can be transpiled from one dialect to another
//! with expected function and syntax transformations.

use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::{TranspileOptions, UnsupportedLevel};

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

mod strict_unsupported_regressions {
    use super::*;

    fn transpile_with_level(
        sql: &str,
        read: DialectType,
        write: DialectType,
        level: UnsupportedLevel,
    ) -> polyglot_sql::Result<Vec<String>> {
        Dialect::get(read).transpile_with(
            sql,
            write,
            TranspileOptions::default().with_unsupported_level(level),
        )
    }

    #[test]
    fn default_transpile_still_allows_known_unsupported_leftovers() {
        let cases = [
            (
                "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
                DialectType::PostgreSQL,
                DialectType::Fabric,
            ),
            (
                "SELECT ARRAY_AGG(x) FROM t",
                DialectType::PostgreSQL,
                DialectType::Fabric,
            ),
            (
                "SELECT ROW_TO_JSON(t) FROM t",
                DialectType::PostgreSQL,
                DialectType::Fabric,
            ),
            (
                "SELECT lpad(s, 5, 'x') FROM t",
                DialectType::PostgreSQL,
                DialectType::Fabric,
            ),
            (
                "SELECT * FROM t, LATERAL (SELECT 1 AS x) AS s",
                DialectType::PostgreSQL,
                DialectType::Fabric,
            ),
        ];

        for (sql, read, write) in cases {
            let result = Dialect::get(read).transpile(sql, write);
            assert!(
                result.is_ok(),
                "default transpile should not reject {read:?} -> {write:?}: {result:?}"
            );
        }
    }

    #[test]
    fn strict_transpile_rejects_fabric_recursive_ctes() {
        let err = transpile_with_level(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
            DialectType::PostgreSQL,
            DialectType::Fabric,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Fabric transpile should reject recursive CTEs");

        assert!(err.to_string().contains("recursive CTEs"));
    }

    #[test]
    fn strict_transpile_rejects_hive_recursive_ctes() {
        let err = transpile_with_level(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
            DialectType::PostgreSQL,
            DialectType::Hive,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Hive transpile should reject recursive CTEs");

        assert!(err.to_string().contains("recursive CTEs"));
    }

    #[test]
    fn strict_transpile_rejects_remaining_lateral_for_tsql_targets() {
        let err = transpile_with_level(
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS id) a USING (id))",
            DialectType::PostgreSQL,
            DialectType::Fabric,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Fabric transpile should reject remaining LATERAL");

        assert!(err.to_string().contains("LATERAL"));
    }

    #[test]
    fn strict_transpile_rejects_except_intersect_all_for_tsql_targets() {
        let cases = [
            (
                "SELECT a FROM t EXCEPT ALL SELECT b FROM t",
                "EXCEPT ALL is not supported",
            ),
            (
                "SELECT a FROM t INTERSECT ALL SELECT b FROM t",
                "INTERSECT ALL is not supported",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    target,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict transpile should reject unsupported set operation");

                assert!(
                    err.to_string().contains(expected),
                    "expected {expected:?} in error for {target:?}, got {err}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_allows_distinct_except_intersect_for_tsql_targets() {
        let cases = [
            "SELECT a FROM t EXCEPT SELECT b FROM t",
            "SELECT a FROM t INTERSECT SELECT b FROM t",
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for sql in cases {
                let result = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    target,
                    UnsupportedLevel::Raise,
                )
                .unwrap_or_else(|err| {
                    panic!("strict {target:?} transpile should allow distinct set operation: {err}")
                });

                assert_eq!(result.len(), 1);
                assert!(
                    !result[0].contains(" ALL "),
                    "distinct set operation should not contain ALL for {target:?}: {}",
                    result[0]
                );
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_remaining_unnest_for_unsupported_targets() {
        let err = transpile_with_level(
            "SELECT UNNEST(arr) FROM t",
            DialectType::PostgreSQL,
            DialectType::Redshift,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Redshift transpile should reject remaining UNNEST");

        assert!(err.to_string().contains("UNNEST"));
    }

    #[test]
    fn strict_transpile_allows_rewritten_unnest_for_supported_targets() {
        let result = transpile_with_level(
            "SELECT * FROM t CROSS JOIN UNNEST(arr) AS x",
            DialectType::BigQuery,
            DialectType::DuckDB,
            UnsupportedLevel::Raise,
        )
        .expect("strict DuckDB transpile should allow supported/re-written UNNEST");

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn strict_transpile_rejects_remaining_explode_for_unsupported_targets() {
        let err = transpile_with_level(
            "SELECT EXPLODE(arr) FROM t",
            DialectType::Generic,
            DialectType::SQLite,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict SQLite transpile should reject remaining EXPLODE");

        assert!(err.to_string().contains("EXPLODE"));
    }

    #[test]
    fn strict_transpile_rejects_array_agg_for_lossy_targets() {
        let err = transpile_with_level(
            "SELECT ARRAY_AGG(x) FROM t",
            DialectType::PostgreSQL,
            DialectType::Fabric,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Fabric transpile should reject ARRAY_AGG");

        assert!(err.to_string().contains("ARRAY_AGG"));
    }

    #[test]
    fn strict_transpile_rejects_nth_value_for_tsql_targets() {
        let nth_value = "SELECT NTH_VALUE(salary, 2) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM empsalary";

        for write in [DialectType::Fabric, DialectType::TSQL] {
            let permissive = Dialect::get(DialectType::PostgreSQL)
                .transpile(nth_value, write)
                .expect("default transpile should preserve unsupported NTH_VALUE");
            assert!(permissive[0].contains("NTH_VALUE"));

            let err = transpile_with_level(
                nth_value,
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict TSQL/Fabric transpile should reject NTH_VALUE");
            assert!(
                err.to_string().contains("NTH_VALUE"),
                "unexpected error for PostgreSQL -> {write:?}: {err}"
            );

            transpile_with_level(
                "SELECT FIRST_VALUE(salary) OVER (PARTITION BY depname ORDER BY empno) FROM empsalary",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should allow FIRST_VALUE");
        }
    }

    #[test]
    fn strict_transpile_validates_tsql_window_frame_capabilities() {
        let unsupported = [
            (
                "SELECT SUM(x) OVER (ORDER BY x RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t",
                "value-offset RANGE window frames",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
                "GROUPS window frames",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
                "window frame EXCLUDE clauses",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) FROM t",
                "window frame EXCLUDE clauses",
            ),
            (
                "SELECT SUM(x) OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
                "window frames without ORDER BY",
            ),
            (
                "SELECT FIRST_VALUE(x) OVER (PARTITION BY p) FROM t",
                "FIRST_VALUE without ORDER BY",
            ),
            (
                "SELECT LAST_VALUE(x) OVER (PARTITION BY p) FROM t",
                "LAST_VALUE without ORDER BY",
            ),
            (
                "SELECT SUM(x) OVER w FROM t WINDOW w AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                "window frames without ORDER BY",
            ),
            (
                "SELECT FIRST_VALUE(x) OVER w FROM t WINDOW w AS (PARTITION BY p)",
                "FIRST_VALUE without ORDER BY",
            ),
        ];
        let supported = [
            "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t",
            "SELECT SUM(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
            "SELECT SUM(x) OVER (PARTITION BY p) FROM t",
            "SELECT FIRST_VALUE(x) OVER (PARTITION BY p ORDER BY x) FROM t",
            "SELECT LAST_VALUE(x) OVER w FROM t WINDOW w AS (PARTITION BY p ORDER BY x)",
            "SELECT FIRST_VALUE(x) OVER w2 FROM t WINDOW w1 AS (PARTITION BY p ORDER BY x), w2 AS (w1)",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for (sql, expected) in unsupported {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject unsupported window frame");
                assert!(
                    err.to_string().contains(expected),
                    "expected {expected:?} for PostgreSQL -> {write:?}, got {err}"
                );
            }

            for sql in supported {
                transpile_with_level(sql, DialectType::PostgreSQL, write, UnsupportedLevel::Raise)
                    .expect("strict TSQL/Fabric transpile should allow supported window frame");
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_regex_predicates_for_tsql_targets() {
        let sqls = [
            "SELECT 1 FROM t WHERE p_brand SIMILAR TO 'Brand#[1-3][0-9]'",
            "SELECT 1 FROM t WHERE c_phone ~ '^1[0-9]'",
            "SELECT 1 FROM t WHERE c_phone !~ '^1[0-9]'",
            "SELECT 1 FROM t WHERE c_phone ~* '^1[0-9]'",
            "SELECT 1 FROM t WHERE c_phone !~* '^1[0-9]'",
            "SELECT 1 FROM t WHERE REGEXP_LIKE(c_phone, '^1[0-9]')",
            "SELECT count(*) FILTER (WHERE c_phone ~ '123') FROM t",
            "SELECT count(*) FILTER (WHERE c_phone !~ '[0-9]') FROM t",
            "SELECT sum(amount) FILTER (WHERE c_phone ~* 'abc') FROM t",
            "SELECT sum(amount) FILTER (WHERE c_phone !~* 'abc') FROM t",
        ];

        for sql in sqls {
            for write in [DialectType::Fabric, DialectType::TSQL] {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject regex predicates");

                assert!(
                    err.to_string().contains("regular expression predicates"),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }
        }

        for write in [DialectType::Fabric, DialectType::TSQL] {
            transpile_with_level(
                "SELECT count(*) FILTER (WHERE c_phone LIKE '123%') FROM t",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should preserve LIKE inside FILTER");
        }
    }

    #[test]
    fn strict_transpile_rejects_join_using_and_natural_for_tsql_targets() {
        let unsupported = [
            ("SELECT * FROM a JOIN b USING (i)", "JOIN USING clauses"),
            (
                "SELECT * FROM a LEFT JOIN b USING (i, j)",
                "JOIN USING clauses",
            ),
            (
                "SELECT * FROM (a RIGHT JOIN b USING (i)) AS ab",
                "JOIN USING clauses",
            ),
            ("SELECT * FROM a NATURAL JOIN b", "NATURAL JOIN"),
            ("SELECT * FROM a NATURAL LEFT JOIN b", "NATURAL JOIN"),
            ("SELECT * FROM a NATURAL RIGHT JOIN b", "NATURAL JOIN"),
            ("SELECT * FROM a NATURAL FULL JOIN b", "NATURAL JOIN"),
        ];
        let supported = [
            "SELECT * FROM a JOIN b ON a.i = b.i",
            "SELECT * FROM a LEFT JOIN b ON a.i = b.i AND a.j = b.j",
            "SELECT * FROM a CROSS JOIN b",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for (sql, expected) in unsupported {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject implicit join keys");
                assert!(
                    err.to_string().contains(expected),
                    "expected {expected:?} for PostgreSQL -> {write:?}, got {err}"
                );
            }

            for sql in supported {
                transpile_with_level(sql, DialectType::PostgreSQL, write, UnsupportedLevel::Raise)
                    .expect("strict TSQL/Fabric transpile should allow explicit join forms");
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_relation_column_alias_lists_for_tsql_targets() {
        let unsupported = [
            "SELECT * FROM t AS x (a, b, c)",
            "SELECT * FROM t x (a, b, c)",
            "SELECT * FROM t AS x (a, b), u AS y (c, d)",
            "SELECT a, c FROM (t CROSS JOIN u) AS x (a, b, c, d)",
            "SELECT a FROM (t JOIN u ON t.id = u.id) x (a, b, c)",
        ];
        let supported = [
            "SELECT * FROM t AS x",
            "SELECT a, b FROM (SELECT x, y FROM t) AS s(a, b)",
            "WITH s(a, b) AS (SELECT x, y FROM t) SELECT a, b FROM s",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for sql in unsupported {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject relation column aliases");
                assert!(
                    err.to_string()
                        .contains("column alias lists on base or joined table references"),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }

            for sql in supported {
                transpile_with_level(sql, DialectType::PostgreSQL, write, UnsupportedLevel::Raise)
                    .expect("strict TSQL/Fabric transpile should allow supported aliases");
            }
        }

        let postgres = transpile_with_level(
            "SELECT a, c FROM (t CROSS JOIN u) AS x (a, b, c, d)",
            DialectType::PostgreSQL,
            DialectType::PostgreSQL,
            UnsupportedLevel::Raise,
        )
        .expect("PostgreSQL should preserve joined-table column aliases");
        assert_eq!(
            postgres,
            ["SELECT a, c FROM (t CROSS JOIN u) AS x(a, b, c, d)"]
        );
    }

    #[test]
    fn strict_transpile_rejects_whole_row_aggregate_arguments_for_tsql_targets() {
        let unsupported = [
            "SELECT count(t2.*) FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            "SELECT sum(t.*) FROM t",
            "SELECT avg(t.*) OVER (PARTITION BY t.group_id) FROM t",
        ];
        let supported = [
            "SELECT count(*) FROM t",
            "SELECT count(t.id) FROM t",
            "SELECT sum(t.amount) FROM t",
            "SELECT t.* FROM t",
            "SELECT count(x) FILTER (WHERE EXISTS (SELECT t.* FROM t)) FROM u",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for sql in unsupported {
                let permissive = Dialect::get(DialectType::PostgreSQL)
                    .transpile(sql, write)
                    .expect("default transpile should preserve its best-effort output");
                assert!(
                    permissive[0].contains("t.*") || permissive[0].contains("t2.*"),
                    "unexpected permissive output for PostgreSQL -> {write:?}: {permissive:?}"
                );

                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err(
                    "strict TSQL/Fabric transpile should reject whole-row aggregate arguments",
                );
                assert!(
                    err.to_string()
                        .contains("qualified whole-row aggregate arguments"),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }

            for sql in supported {
                transpile_with_level(sql, DialectType::PostgreSQL, write, UnsupportedLevel::Raise)
                    .expect("strict TSQL/Fabric transpile should allow scalar aggregate arguments");
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_postgres_composite_values_for_tsql_targets() {
        let unsupported = [
            "SELECT ROW(a, b) FROM t",
            "SELECT (a, b) FROM t",
            "SELECT (a, b), SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b))",
            "SELECT SUM(c) FROM t GROUP BY GROUPING SETS ((ROW(a, b)))",
            "SELECT (ROW(a, b)).f1 FROM t",
            "SELECT ROW(a, b) IS NOT NULL FROM t",
            "SELECT ROW(a, ROW(b, c)) FROM t",
            "SELECT rt.*::rt_rowtype FROM rt",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for sql in unsupported {
                Dialect::get(DialectType::PostgreSQL)
                    .transpile(sql, write)
                    .expect("default transpile should preserve its best-effort output");

                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject composite values");
                assert!(
                    err.to_string().contains("PostgreSQL")
                        && (err.to_string().contains("row/composite")
                            || err.to_string().contains("qualified whole-row casts")),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }

            transpile_with_level(
                "SELECT * FROM (VALUES (1, 2), (3, 4)) AS v(a, b)",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should allow table value constructors");

            let json = transpile_with_level(
                "SELECT json_agg(x), jsonb_agg(x) FROM t",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("current TSQL/Fabric targets should support JSON_ARRAYAGG");
            assert_eq!(
                json,
                ["SELECT JSON_ARRAYAGG(x NULL ON NULL), JSON_ARRAYAGG(x NULL ON NULL) FROM t"]
            );
        }
    }

    #[test]
    fn strict_transpile_rejects_unrepresentable_postgres_json_aggregates() {
        let unsupported = [
            ("SELECT json_agg(DISTINCT x) FROM t", "DISTINCT"),
            ("SELECT jsonb_agg(x) FILTER (WHERE active) FROM t", "FILTER"),
            ("SELECT json_agg(x, y) FROM t", "invalid argument count"),
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for (sql, expected_diagnostic) in unsupported {
                Dialect::get(DialectType::PostgreSQL)
                    .transpile(sql, write)
                    .expect("default transpile should preserve best-effort JSON aggregation");

                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict transpile should reject unsafe JSON aggregate modifiers");
                let message = err.to_string();
                assert!(
                    message.contains("PostgreSQL")
                        && (message.contains("JSON_AGG") || message.contains("JSONB_AGG"))
                        && message.contains(expected_diagnostic),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }

            let ordered = transpile_with_level(
                "SELECT json_agg(x ORDER BY y) FROM t",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("ordered PostgreSQL JSON aggregation should be representable");
            assert!(
                ordered[0].contains("JSON_ARRAYAGG(")
                    && ordered[0].contains("ORDER BY")
                    && ordered[0].contains("NULL ON NULL"),
                "unexpected ordered JSON aggregate for {write:?}: {ordered:?}"
            );

            let native = transpile_with_level(
                "SELECT JSON_ARRAYAGG(x) FROM t",
                write,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("native JSON_ARRAYAGG should remain supported");
            assert_eq!(native, ["SELECT JSON_ARRAYAGG(x) FROM t"]);
        }
    }

    #[test]
    fn postgres_clickhouse_json_text_extraction_preserves_nullability() {
        let cases = [
            (
                "SELECT payload ->> 'status' FROM events",
                "SELECT JSONExtract(payload, 'status', 'Nullable(String)') FROM events",
            ),
            (
                "SELECT payload ->> '0' FROM events",
                "SELECT JSONExtract(payload, '0', 'Nullable(String)') FROM events",
            ),
            (
                "SELECT payload ->> 0 FROM events",
                "SELECT JSONExtract(payload, 1, 'Nullable(String)') FROM events",
            ),
            (
                "SELECT payload #>> '{user,status}' FROM events",
                "SELECT JSONExtract(payload, 'user', 'status', 'Nullable(String)') FROM events",
            ),
            (
                "SELECT payload #>> '{users,0,status}' FROM events",
                "SELECT JSONExtract(payload, 'users', 1, 'status', 'Nullable(String)') FROM events",
            ),
            (
                "SELECT JSON_EXTRACT_PATH_TEXT(payload, k1, k2) FROM events",
                "SELECT JSONExtract(payload, k1, k2, 'Nullable(String)') FROM events",
            ),
        ];

        for (sql, expected) in cases {
            let actual = transpile_with_level(
                sql,
                DialectType::PostgreSQL,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .unwrap_or_else(|error| panic!("strict PostgreSQL -> ClickHouse failed: {error}"));

            assert_eq!(actual, [expected], "unexpected transpilation for {sql}");
        }
    }

    #[test]
    fn postgres_unknown_null_casts_lower_safely_for_tsql_targets() {
        for write in [DialectType::Fabric, DialectType::TSQL] {
            let direct = transpile_with_level(
                "SELECT NULL::unknown",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should erase NULL::unknown");
            assert_eq!(direct, ["SELECT NULL"]);

            let predicate = transpile_with_level(
                "SELECT (NULL::unknown IS NOT NULL) AS no FROM t",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should lower an UNKNOWN NULL predicate");
            assert_eq!(
                predicate,
                ["SELECT CAST(CASE WHEN (NULL IS NOT NULL) THEN 1 ELSE 0 END AS BIT) AS no FROM t"]
            );

            let standard_cast = transpile_with_level(
                "SELECT CAST(NULL AS unknown)",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("standard UNKNOWN NULL casts should use the same safe rewrite");
            assert_eq!(standard_cast, ["SELECT NULL"]);

            for sql in ["SELECT 'x'::unknown", "SELECT value::unknown FROM t"] {
                Dialect::get(DialectType::PostgreSQL)
                    .transpile(sql, write)
                    .expect("default transpile should retain best-effort UNKNOWN casts");

                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject unresolved UNKNOWN casts");
                assert!(
                    err.to_string()
                        .contains("PostgreSQL unresolved UNKNOWN casts"),
                    "unexpected error for PostgreSQL -> {write:?}: {err}"
                );
            }
        }

        let postgres = transpile_with_level(
            "SELECT NULL::unknown",
            DialectType::PostgreSQL,
            DialectType::PostgreSQL,
            UnsupportedLevel::Raise,
        )
        .expect("PostgreSQL identity should retain its UNKNOWN cast");
        assert_eq!(postgres, ["SELECT CAST(NULL AS UNKNOWN)"]);
    }

    #[test]
    fn transpile_legalizes_nested_order_by_for_tsql_targets() {
        let nested = [
            "SELECT count(*) FROM (SELECT * FROM t ORDER BY a) AS x",
            "WITH x AS (SELECT * FROM t ORDER BY a) SELECT count(*) FROM x",
            "SELECT x.a FROM u LEFT JOIN (SELECT a FROM t ORDER BY a) AS x ON x.a = u.a",
            "CREATE VIEW ordered_t AS SELECT * FROM t ORDER BY a",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for sql in nested {
                let output = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect("strict TSQL/Fabric transpile should legalize nested ORDER BY")
                .join("; ");
                assert_eq!(
                    output.matches("OFFSET 0 ROWS").count(),
                    1,
                    "expected one nested OFFSET for PostgreSQL -> {write:?}: {output}"
                );
            }

            let top_level = transpile_with_level(
                "SELECT * FROM t ORDER BY a",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("top-level ORDER BY should remain valid")
            .join("; ");
            assert!(
                !top_level.contains("OFFSET"),
                "unexpected offset: {top_level}"
            );

            let bounded = transpile_with_level(
                "SELECT count(*) FROM (SELECT * FROM t ORDER BY a LIMIT 5) AS x",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("LIMIT should become a legal nested TOP")
            .join("; ");
            assert!(bounded.contains("TOP 5"), "expected TOP: {bounded}");
            assert!(!bounded.contains("OFFSET"), "unexpected offset: {bounded}");

            let existing_offset = transpile_with_level(
                "SELECT count(*) FROM (SELECT * FROM t ORDER BY a OFFSET 2) AS x",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("existing nested OFFSET should remain valid")
            .join("; ");
            assert!(
                existing_offset.contains("OFFSET 2 ROWS"),
                "expected original offset: {existing_offset}"
            );
            assert!(!existing_offset.contains("OFFSET 0 ROWS"));

            let window_only = transpile_with_level(
                "SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("window ordering should not create a query offset")
            .join("; ");
            assert!(
                !window_only.contains("OFFSET"),
                "unexpected offset: {window_only}"
            );
        }
    }

    #[test]
    fn transpile_drops_unbounded_nested_set_order_by_for_tsql_targets() {
        let unbounded = [
            "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1)))",
            "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl UNION SELECT q1 FROM int8_tbl ORDER BY 1)))",
            "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl INTERSECT SELECT q1 FROM int8_tbl ORDER BY 1)))",
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for level in [UnsupportedLevel::Warn, UnsupportedLevel::Raise] {
                for sql in unbounded {
                    let output = transpile_with_level(sql, DialectType::PostgreSQL, write, level)
                        .unwrap_or_else(|err| {
                            panic!("nested set operation should transpile for {write:?}: {err}")
                        })
                        .join("; ");
                    assert!(
                        !output.contains("ORDER BY"),
                        "unbounded nested set ordering should be dropped for {write:?}: {output}"
                    );
                    assert!(
                        !output.contains("AS _l_0"),
                        "dropping the ordering should avoid a generated wrapper for {write:?}: {output}"
                    );
                }
            }

            let top_level = transpile_with_level(
                "SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("top-level set ordering should remain valid")
            .join("; ");
            assert!(
                top_level.contains("ORDER BY"),
                "top-level set ordering should be preserved for {write:?}: {top_level}"
            );

            let parenthesized_top_level = transpile_with_level(
                "((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1))",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("parenthesized top-level set ordering should remain valid")
            .join("; ");
            assert!(
                parenthesized_top_level.contains("ORDER BY"),
                "parenthesized top-level ordering should be preserved for {write:?}: {parenthesized_top_level}"
            );

            let bounded = [
                (
                    "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1 LIMIT 5)))",
                    "TOP 5",
                ),
                (
                    "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1 OFFSET 2)))",
                    "OFFSET 2 ROWS",
                ),
                (
                    "SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1 LIMIT 5 OFFSET 2)))",
                    "FETCH NEXT 5 ROWS ONLY",
                ),
            ];
            for (sql, expected_bound) in bounded {
                let output = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect("row-bounded nested set ordering should remain valid")
                .join("; ");
                assert!(
                    output.contains("ORDER BY") && output.contains(expected_bound),
                    "row-bounded ordering should be preserved for {write:?}: {output}"
                );
            }
        }
    }

    #[test]
    fn transpile_legalizes_unordered_offsets_for_tsql_targets() {
        let inert = [
            "SELECT * FROM t OFFSET 0",
            "SELECT * FROM t OFFSET NULL",
            "SELECT * FROM (SELECT x FROM t OFFSET 0) AS s",
            "WITH s AS (SELECT x FROM t OFFSET 0) SELECT * FROM s",
            "SELECT * FROM t LIMIT 5 OFFSET 0",
            "(SELECT id FROM a UNION ALL SELECT id FROM b) OFFSET 0",
            "SELECT x.q1, ss.y FROM int8_tbl x CROSS JOIN LATERAL (SELECT x.q1 AS y OFFSET 0) ss",
        ];
        let unordered = [
            ("SELECT * FROM t OFFSET 5", "OFFSET 5 ROWS"),
            (
                "SELECT * FROM (SELECT x FROM t OFFSET 2) AS s",
                "OFFSET 2 ROWS",
            ),
            (
                "SELECT * FROM t OFFSET 0 ROWS FETCH FIRST 5 ROWS ONLY",
                "OFFSET 0 ROWS FETCH FIRST 5 ROWS ONLY",
            ),
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for sql in inert {
                let output = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect("strict TSQL/Fabric transpile should erase inert unordered offsets")
                .join("; ");
                assert!(
                    !output.contains("OFFSET"),
                    "unexpected offset for PostgreSQL -> {write:?}: {output}"
                );
            }

            for (sql, expected_offset) in unordered {
                let output = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect("strict TSQL/Fabric transpile should legalize unordered offsets")
                .join("; ");
                assert!(
                    output.contains("ORDER BY (SELECT NULL)"),
                    "missing neutral order for PostgreSQL -> {write:?}: {output}"
                );
                assert!(
                    output.contains(expected_offset),
                    "missing {expected_offset:?} for PostgreSQL -> {write:?}: {output}"
                );
            }

            let ordered = transpile_with_level(
                "SELECT * FROM t ORDER BY id OFFSET 5",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should preserve ordered offsets")
            .join("; ");
            assert!(
                ordered.contains("OFFSET 5 ROWS"),
                "unexpected SQL: {ordered}"
            );
            assert!(
                !ordered.contains("ORDER BY (SELECT NULL)"),
                "explicit ordering was replaced: {ordered}"
            );
        }
    }

    #[test]
    fn transpile_exposes_preceding_comma_sources_to_tsql_apply() {
        let correlated = [
            (
                "SELECT * FROM int8_tbl a, int8_tbl x LEFT JOIN LATERAL (SELECT a.q1 FROM int4_tbl y) ss(z) ON x.q2 = ss.z",
                "OUTER APPLY",
                "a.q1",
            ),
            (
                "SELECT * FROM int8_tbl a, int8_tbl x JOIN LATERAL (SELECT y.q1 FROM int4_tbl y) ss(z) ON a.q1 = ss.z",
                "CROSS APPLY",
                "a.q1",
            ),
            (
                "SELECT * FROM int8_tbl a, int8_tbl x JOIN LATERAL (VALUES (a.q1)) ss(z) ON TRUE",
                "CROSS APPLY",
                "VALUES (a.q1)",
            ),
        ];

        for write in [DialectType::Fabric, DialectType::TSQL] {
            for (sql, expected_apply, expected_reference) in correlated {
                let output = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect("strict TSQL/Fabric transpile should expose all preceding LATERAL inputs")
                .join("; ");
                let cross_join = output
                    .find("CROSS JOIN")
                    .unwrap_or_else(|| panic!("missing explicit preceding product: {output}"));
                let apply = output
                    .find(expected_apply)
                    .unwrap_or_else(|| panic!("missing {expected_apply}: {output}"));
                assert!(
                    cross_join < apply,
                    "preceding product must be the APPLY left operand: {output}"
                );
                assert!(
                    output.contains(expected_reference),
                    "lost sibling reference {expected_reference:?}: {output}"
                );
            }

            let multiple = transpile_with_level(
                "SELECT * FROM int8_tbl a, int8_tbl b, int8_tbl x CROSS JOIN LATERAL (SELECT a.q1 + b.q1 AS z) ss",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("strict TSQL/Fabric transpile should expose multiple preceding siblings")
            .join("; ");
            assert_eq!(
                multiple.matches("CROSS JOIN").count(),
                2,
                "unexpected preceding product: {multiple}"
            );
            assert!(
                multiple.contains("CROSS APPLY"),
                "unexpected SQL: {multiple}"
            );

            let immediate_left = transpile_with_level(
                "SELECT * FROM int8_tbl a LEFT JOIN LATERAL (SELECT a.q1 AS z) ss ON TRUE",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("immediate-left LATERAL should remain supported")
            .join("; ");
            assert!(
                !immediate_left.contains("CROSS JOIN"),
                "single-source APPLY changed shape: {immediate_left}"
            );
            assert!(immediate_left.contains("OUTER APPLY"));

            let plain_comma = transpile_with_level(
                "SELECT * FROM int8_tbl a, int8_tbl b",
                DialectType::PostgreSQL,
                write,
                UnsupportedLevel::Raise,
            )
            .expect("ordinary comma joins should remain supported")
            .join("; ");
            assert!(
                !plain_comma.contains("CROSS JOIN"),
                "ordinary comma join changed shape: {plain_comma}"
            );
        }
    }

    #[test]
    fn strict_transpile_rejects_residual_fetch_ties_overlaps_and_date_bin_for_tsql_targets() {
        let cases = [
            (
                "SELECT store_id FROM orders ORDER BY promised_at OFFSET 2 ROWS FETCH FIRST 5 ROWS WITH TIES",
                "FETCH WITH TIES without TOP",
            ),
            ("SELECT 1 FROM t WHERE a OVERLAPS b", "OVERLAPS"),
            (
                "SELECT date_bin('1 month', completed_at, TIMESTAMP '2001-01-01') FROM order_fulfillment_history",
                "DATE_BIN",
            ),
        ];

        for target in [DialectType::Fabric, DialectType::TSQL] {
            for (sql, expected) in cases {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    target,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject residual unsupported node");

                assert!(
                    err.to_string().contains(expected),
                    "expected {expected:?} for PostgreSQL -> {target:?}, got {err}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_postgres_only_functions() {
        let err = transpile_with_level(
            "SELECT ROW_TO_JSON(docs), TO_TSVECTOR(body) FROM docs",
            DialectType::PostgreSQL,
            DialectType::Fabric,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict Fabric transpile should reject PostgreSQL-only functions");

        let message = err.to_string();
        assert!(message.contains("ROW_TO_JSON"));
        assert!(message.contains("TO_TSVECTOR"));
    }

    #[test]
    fn strict_transpile_rejects_postgres_only_scalar_functions_for_tsql_targets() {
        let cases = [
            ("SELECT lpad(s, 5, 'x') FROM t", "LPAD"),
            ("SELECT rpad(s, 5, 'x') FROM t", "RPAD"),
            ("SELECT split_part(s, ',', 1) FROM t", "SPLIT_PART"),
            ("SELECT initcap(s) FROM t", "INITCAP"),
            ("SELECT to_jsonb(s) FROM t", "TO_JSONB"),
            ("SELECT scale(val) FROM num_tbl", "SCALE"),
            ("SELECT trim_scale(val) FROM num_tbl", "TRIM_SCALE"),
            ("SELECT min_scale(val) FROM num_tbl", "MIN_SCALE"),
            ("SELECT factorial(5) FROM num_tbl", "FACTORIAL"),
            ("SELECT pg_lsn((23783416)::numeric) FROM num_tbl", "PG_LSN"),
        ];

        for (sql, function_name) in cases {
            for write in [DialectType::Fabric, DialectType::TSQL] {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject PostgreSQL-only functions");

                assert!(
                    err.to_string().contains(function_name),
                    "unexpected error for {function_name} to {write:?}: {err}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_postgres_json_functions_for_tsql_targets() {
        let cases = [
            ("SELECT to_json(s) FROM t", "TO_JSON"),
            ("SELECT row_to_json(t) FROM t", "ROW_TO_JSON"),
            ("SELECT jsonb_object_agg(k, s) FROM t", "JSONB_OBJECT_AGG"),
        ];

        for (sql, function_name) in cases {
            for write in [DialectType::Fabric, DialectType::TSQL] {
                let err = transpile_with_level(
                    sql,
                    DialectType::PostgreSQL,
                    write,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict TSQL/Fabric transpile should reject PostgreSQL JSON functions");

                assert!(
                    err.to_string().contains(function_name),
                    "unexpected error for {function_name} to {write:?}: {err}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_honors_max_unsupported() {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(
                "SELECT ARRAY_AGG(x), ROW_TO_JSON(docs), TO_TSVECTOR(body) FROM docs",
                DialectType::Fabric,
                TranspileOptions::strict().with_max_unsupported(2),
            )
            .expect_err("strict Fabric transpile should report unsupported diagnostics");

        let message = err.to_string();
        assert!(message.contains("ARRAY_AGG"));
        assert!(message.contains("ROW_TO_JSON"));
        assert!(message.contains("... and 1 more"));
    }

    #[test]
    fn bigquery_clickhouse_timestamp_preserves_utc_and_microseconds() {
        let without_zone = transpile_with_level(
            "SELECT TIMESTAMP('2025-01-01 00:00:00')",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("strict ClickHouse transpile should preserve BigQuery's default UTC zone");
        assert_eq!(
            without_zone,
            vec!["SELECT toDateTime64('2025-01-01 00:00:00', 6, 'UTC')"]
        );

        let with_zone = transpile_with_level(
            "SELECT TIMESTAMP('2025-01-01 00:00:00', 'America/Los_Angeles')",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("strict ClickHouse transpile should preserve an explicit BigQuery time zone");
        assert_eq!(
            with_zone,
            vec!["SELECT toTimeZone(toDateTime64('2025-01-01 00:00:00', 6, 'America/Los_Angeles'), 'UTC')"]
        );
    }

    #[test]
    fn tsql_and_fabric_float_widths_map_to_clickhouse_semantics() {
        let cases = [
            (
                "SELECT CONVERT(FLOAT, 1)",
                "SELECT CAST(1 AS Nullable(Float64))",
            ),
            (
                "SELECT CONVERT(FLOAT(1), 1)",
                "SELECT CAST(1 AS Nullable(Float32))",
            ),
            (
                "SELECT CONVERT(FLOAT(24), 1)",
                "SELECT CAST(1 AS Nullable(Float32))",
            ),
            (
                "SELECT CONVERT(FLOAT(25), 1)",
                "SELECT CAST(1 AS Nullable(Float64))",
            ),
            (
                "SELECT CONVERT(FLOAT(53), 1)",
                "SELECT CAST(1 AS Nullable(Float64))",
            ),
            (
                "SELECT CONVERT(REAL, 1)",
                "SELECT CAST(1 AS Nullable(Float32))",
            ),
        ];

        for source in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let output = transpile_with_level(
                    sql,
                    source,
                    DialectType::ClickHouse,
                    UnsupportedLevel::Raise,
                )
                .unwrap_or_else(|error| {
                    panic!("strict {source:?} -> ClickHouse transpilation failed: {error}")
                });
                assert_eq!(output, vec![expected], "{source:?}: {sql}");
            }
        }
    }

    #[test]
    fn tsql_float_width_boundary_is_source_defined_for_spark_and_hive() {
        for target in [DialectType::Spark, DialectType::Hive] {
            for (sql, expected_type) in [
                ("SELECT CONVERT(FLOAT, 1)", "DOUBLE"),
                ("SELECT CONVERT(FLOAT(24), 1)", "FLOAT"),
                ("SELECT CONVERT(FLOAT(25), 1)", "DOUBLE"),
                ("SELECT CONVERT(FLOAT(32), 1)", "DOUBLE"),
                ("SELECT CONVERT(FLOAT(53), 1)", "DOUBLE"),
            ] {
                let output =
                    transpile_with_level(sql, DialectType::TSQL, target, UnsupportedLevel::Raise)
                        .unwrap_or_else(|error| {
                            panic!("strict TSQL -> {target:?} transpilation failed: {error}")
                        });
                assert_eq!(
                    output,
                    vec![format!("SELECT CAST(1 AS {expected_type})")],
                    "{target:?}: {sql}"
                );
            }
        }
    }

    #[test]
    fn tsql_and_fabric_float_identity_keeps_declared_precision() {
        for source in [DialectType::TSQL, DialectType::Fabric] {
            for target in [DialectType::TSQL, DialectType::Fabric] {
                let output = transpile_with_level(
                    "SELECT CAST(1 AS FLOAT), CAST(1 AS FLOAT(25))",
                    source,
                    target,
                    UnsupportedLevel::Raise,
                )
                .unwrap_or_else(|error| {
                    panic!("strict {source:?} -> {target:?} transpilation failed: {error}")
                });
                assert_eq!(
                    output,
                    vec!["SELECT CAST(1 AS FLOAT), CAST(1 AS FLOAT(25))"],
                    "{source:?} -> {target:?}"
                );
            }
        }
    }

    #[test]
    fn strict_snowflake_clickhouse_lowers_supported_conversions() {
        let temporal = transpile_with_level(
            "SELECT TO_CHAR(DATE '2025-01-15', 'YYYY-MM')",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("temporal TO_CHAR should lower to ClickHouse");
        assert_eq!(
            temporal,
            vec!["SELECT formatDateTime(CAST('2025-01-15' AS DATE), '%Y-%m')"]
        );

        let safe_double = transpile_with_level(
            "SELECT TRY_TO_DOUBLE('abc')",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("one-argument TRY_TO_DOUBLE should lower to ClickHouse");
        assert_eq!(safe_double, vec!["SELECT toFloat64OrNull('abc')"]);
    }

    #[test]
    fn snowflake_clickhouse_current_time_preserves_precision_in_permissive_mode() {
        for level in [UnsupportedLevel::Warn, UnsupportedLevel::Ignore] {
            let output = transpile_with_level(
                "SELECT CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIME(), CURRENT_TIMESTAMP(0), CURRENT_TIME(3), CURRENT_TIMESTAMP(9)",
                DialectType::Snowflake,
                DialectType::ClickHouse,
                level,
            )
            .expect("permissive ClickHouse transpile should retain session-relative expressions");

            assert_eq!(
                output,
                vec!["SELECT CURRENT_DATE, now64(9), toTime64(now64(9), 9), now64(0), toTime64(now64(3), 3), now64(9)"]
            );
        }
    }

    #[test]
    fn strict_snowflake_clickhouse_rejects_session_dependent_temporal_semantics() {
        let cases = [
            ("SELECT CURRENT_DATE", "TIMEZONE"),
            ("SELECT CURRENT_DATE()", "TIMEZONE"),
            ("SELECT CURRENT_TIME(3)", "TIMEZONE"),
            ("SELECT CURRENT_TIMESTAMP(3)", "TIMEZONE"),
            ("SELECT LOCALTIME(3)", "TIMEZONE"),
            ("SELECT LOCALTIMESTAMP(3)", "TIMEZONE"),
            ("SELECT TO_CHAR(CURRENT_DATE(), 'YYYY-MM')", "TIMEZONE"),
            (
                "SELECT DATE_TRUNC('WEEK', TO_DATE('2025-01-05'))",
                "WEEK_START",
            ),
            (
                "SELECT DATE_TRUNC('WK', TO_DATE('2025-01-05'))",
                "WEEK_START",
            ),
        ];

        for (sql, expected) in cases {
            let err = transpile_with_level(
                sql,
                DialectType::Snowflake,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpile should reject unknown session semantics");
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?} in error for {sql:?}, got {err}"
            );
        }
    }

    #[test]
    fn strict_snowflake_clickhouse_session_diagnostics_are_deduplicated() {
        let err = transpile_with_level(
            "SELECT CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIME(), DATE_TRUNC('WEEK', TO_DATE('2025-01-05'))",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpile should report unknown session semantics");
        let message = err.to_string();

        assert_eq!(message.matches("TIMEZONE").count(), 1, "{message}");
        assert_eq!(message.matches("WEEK_START").count(), 1, "{message}");

        let immediate = transpile_with_level(
            "SELECT CURRENT_DATE(), DATE_TRUNC('WEEK', TO_DATE('2025-01-05'))",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Immediate,
        )
        .expect_err("immediate mode should stop at the first session diagnostic");
        let immediate_message = immediate.to_string();
        assert!(
            immediate_message.contains("TIMEZONE"),
            "{immediate_message}"
        );
        assert!(
            !immediate_message.contains("WEEK_START"),
            "{immediate_message}"
        );
    }

    #[test]
    fn strict_snowflake_clickhouse_allows_session_independent_temporal_semantics() {
        let output = transpile_with_level(
            "SELECT DATE_TRUNC('MONTH', TO_DATE('2025-01-05'))",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("non-week truncation should remain supported in strict mode");

        assert_eq!(
            output,
            vec!["SELECT dateTrunc('MONTH', CAST('2025-01-05' AS DATE))"]
        );
    }

    #[test]
    fn permissive_clickhouse_transpile_does_not_inject_null_semantics_settings() {
        let cases = [
            (
                "SELECT b.value FROM a LEFT JOIN b ON a.id = b.id",
                DialectType::Databricks,
                "SELECT b.value FROM a LEFT JOIN b ON a.id = b.id",
            ),
            (
                "SELECT SUM(value) FILTER (WHERE paid) FROM events",
                DialectType::PostgreSQL,
                "SELECT SUM(value) FILTER(WHERE paid) FROM events",
            ),
        ];

        for (sql, source, expected) in cases {
            let default_output = Dialect::get(source)
                .transpile(sql, DialectType::ClickHouse)
                .expect("default ClickHouse transpile should remain permissive");
            assert_eq!(default_output, vec![expected]);

            for level in [UnsupportedLevel::Warn, UnsupportedLevel::Ignore] {
                let output = transpile_with_level(sql, source, DialectType::ClickHouse, level)
                    .expect("permissive ClickHouse transpile should preserve the SQL");
                assert_eq!(output, vec![expected]);
                assert!(!output[0].contains("SETTINGS"), "{}", output[0]);
            }
        }
    }

    #[test]
    fn strict_clickhouse_rejects_null_extending_joins_without_known_target_setting() {
        let cases = [
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.id",
            "SELECT * FROM a FULL JOIN b ON a.id = b.id",
            "SELECT * FROM a NATURAL LEFT JOIN b",
            "SELECT * FROM a NATURAL RIGHT JOIN b",
            "SELECT * FROM a NATURAL FULL JOIN b",
            "SELECT * FROM (a LEFT JOIN b ON a.id = b.id) AS ab",
        ];

        for sql in cases {
            let err = transpile_with_level(
                sql,
                DialectType::Generic,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpile should reject unknown outer-join semantics");
            assert!(
                err.to_string().contains("join_use_nulls = 1"),
                "expected join_use_nulls diagnostic for {sql:?}, got {err}"
            );
        }
    }

    #[test]
    fn strict_clickhouse_rejects_empty_input_sensitive_aggregates() {
        let cases = [
            "SELECT SUM(value) FROM events",
            "SELECT SUM(value) FILTER (WHERE paid) FROM events",
            "SELECT AVG(value) FROM events",
            "SELECT SUM(value) OVER () FROM events",
        ];

        for sql in cases {
            let err = transpile_with_level(
                sql,
                DialectType::PostgreSQL,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpile should reject unknown empty-input semantics");
            assert!(
                err.to_string()
                    .contains("aggregate_functions_null_for_empty = 1"),
                "expected aggregate_functions_null_for_empty diagnostic for {sql:?}, got {err}"
            );
        }
    }

    #[test]
    fn strict_clickhouse_allows_count_like_aggregates_and_inner_joins() {
        let cases = [
            "SELECT COUNT(*) FROM events",
            "SELECT COUNT(*) FILTER (WHERE paid) FROM events",
            "SELECT COUNT_IF(paid) FROM events",
            "SELECT APPROX_COUNT_DISTINCT(value) FROM events",
            "SELECT * FROM a INNER JOIN b ON a.id = b.id",
        ];

        for sql in cases {
            transpile_with_level(
                sql,
                DialectType::Databricks,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .unwrap_or_else(|err| panic!("strict ClickHouse transpile rejected {sql:?}: {err}"));
        }
    }

    #[test]
    fn strict_clickhouse_null_semantics_diagnostics_are_deduplicated() {
        let sql = "SELECT SUM(b.value), AVG(b.value) FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id";
        let err = transpile_with_level(
            sql,
            DialectType::Databricks,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpile should report target setting dependencies");
        let message = err.to_string();

        assert_eq!(
            message.matches("join_use_nulls = 1").count(),
            1,
            "{message}"
        );
        assert_eq!(
            message
                .matches("aggregate_functions_null_for_empty = 1")
                .count(),
            1,
            "{message}"
        );

        let immediate = transpile_with_level(
            sql,
            DialectType::Databricks,
            DialectType::ClickHouse,
            UnsupportedLevel::Immediate,
        )
        .expect_err("immediate mode should stop at the first target setting dependency")
        .to_string();
        let diagnostic_count = immediate.matches("join_use_nulls = 1").count()
            + immediate
                .matches("aggregate_functions_null_for_empty = 1")
                .count();
        assert_eq!(diagnostic_count, 1, "{immediate}");
    }

    #[test]
    fn strict_clickhouse_identity_preserves_native_null_semantics() {
        let output = transpile_with_level(
            "SELECT SUM(b.value) FROM a LEFT JOIN b ON a.id = b.id",
            DialectType::ClickHouse,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("ClickHouse identity transpilation should preserve native semantics");

        assert_eq!(
            output,
            vec!["SELECT SUM(b.value) FROM a LEFT JOIN b ON a.id = b.id"]
        );
    }

    #[test]
    fn strict_snowflake_clickhouse_rejects_lossy_conversions_and_flatten() {
        let cases = [
            (
                "SELECT TO_CHAR(123, '999')",
                "Snowflake TO_CHAR overload or dynamic format",
            ),
            (
                "SELECT TRY_TO_DOUBLE('1,23', 'TM9')",
                "Snowflake TRY_TO_DOUBLE with a format model",
            ),
            (
                "SELECT TRY_TO_NUMBER('12.34', 9, 2)",
                "Snowflake TRY_TO_NUMBER decimal conversion semantics",
            ),
            (
                "SELECT f.VALUE FROM events, LATERAL FLATTEN(INPUT => payload) AS f",
                "Snowflake FLATTEN table-function semantics",
            ),
        ];

        for (sql, expected) in cases {
            let err = transpile_with_level(
                sql,
                DialectType::Snowflake,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpile should reject lossy Snowflake semantics");
            assert!(
                err.to_string().contains(expected),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn strict_bigquery_clickhouse_safe_divide_guards_float_overflow() {
        let output = transpile_with_level(
            "SELECT SAFE_DIVIDE(CAST(1e308 AS FLOAT64), CAST(1e-308 AS FLOAT64))",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("strict ClickHouse transpile should preserve Float64 SAFE_DIVIDE overflow");

        assert_eq!(output.len(), 1);
        assert!(
            output[0].contains("isNaN"),
            "unexpected output: {}",
            output[0]
        );
        assert!(
            output[0].contains("isFinite"),
            "unexpected output: {}",
            output[0]
        );
        assert!(
            output[0].contains("NULL"),
            "unexpected output: {}",
            output[0]
        );
    }

    #[test]
    fn clickhouse_preserves_postgres_redshift_and_tsql_integer_division() {
        for source in [
            DialectType::PostgreSQL,
            DialectType::Redshift,
            DialectType::TSQL,
        ] {
            let output = transpile_with_level(
                "SELECT 5 / 2, (-5) / 2, 1 / 0",
                source,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect("statically typed integer division should lower exactly to ClickHouse");

            assert_eq!(output.len(), 1);
            assert_eq!(
                output[0].matches("intDiv(").count(),
                3,
                "{source:?}: {}",
                output[0]
            );
            assert!(!output[0].contains(" / "), "{source:?}: {}", output[0]);
            assert!(output[0].contains("Int32"), "{source:?}: {}", output[0]);
        }

        let mixed_width = transpile_with_level(
            "SELECT CAST(a AS SMALLINT) / CAST(b AS BIGINT) FROM measurements",
            DialectType::Redshift,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("explicit mixed-width integer division should use the wider source type");
        assert!(mixed_width[0].contains("intDiv("), "{}", mixed_width[0]);
        assert!(
            mixed_width[0].contains("Nullable(Int16)"),
            "{}",
            mixed_width[0]
        );
        assert!(
            mixed_width[0].contains("Nullable(Int64)"),
            "{}",
            mixed_width[0]
        );
    }

    #[test]
    fn clickhouse_int_div_expression_uses_native_function_name() {
        let output = transpile_with_level(
            "SELECT DIV(5, 2)",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("BigQuery DIV should lower to ClickHouse intDiv");

        assert_eq!(output, vec!["SELECT intDiv(5, 2)"]);
    }

    #[test]
    fn strict_clickhouse_rejects_throwing_source_float_division() {
        for source in [
            DialectType::PostgreSQL,
            DialectType::Redshift,
            DialectType::TSQL,
        ] {
            for sql in [
                "SELECT CAST(1 AS DOUBLE PRECISION) / 0",
                "SELECT CAST(1e308 AS DOUBLE PRECISION) / CAST(1e-308 AS DOUBLE PRECISION)",
                "SELECT CAST(numerator AS DOUBLE PRECISION) / CAST(denominator AS DOUBLE PRECISION) FROM measurements",
            ] {
                let err = transpile_with_level(
                    sql,
                    source,
                    DialectType::ClickHouse,
                    UnsupportedLevel::Raise,
                )
                .expect_err("strict ClickHouse transpilation should reject non-finite results");

                assert!(
                    err.to_string().contains("floating-point division"),
                    "{source:?}: {err}"
                );
            }
        }
    }

    #[test]
    fn strict_clickhouse_allows_statically_safe_float_division() {
        let output = transpile_with_level(
            "SELECT CAST(5 AS DOUBLE PRECISION) / CAST(2 AS DOUBLE PRECISION)",
            DialectType::PostgreSQL,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("finite nonzero literal division is safe in ClickHouse");

        assert_eq!(
            output,
            vec!["SELECT CAST(5 AS Nullable(Float64)) / CAST(2 AS Nullable(Float64))"]
        );
    }

    #[test]
    fn strict_clickhouse_rejects_decimal_and_unresolved_source_division() {
        let decimal_err = transpile_with_level(
            "SELECT CAST(1 AS DECIMAL(18, 4)) / CAST(3 AS DECIMAL(18, 4))",
            DialectType::Redshift,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpilation should reject Redshift decimal semantics");
        assert!(decimal_err.to_string().contains("decimal division"));

        for source in [
            DialectType::PostgreSQL,
            DialectType::Redshift,
            DialectType::TSQL,
        ] {
            let unresolved_err = transpile_with_level(
                "SELECT numerator / denominator FROM measurements",
                source,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpilation should reject unresolved operand types");
            assert!(
                unresolved_err
                    .to_string()
                    .contains("unresolved operand types"),
                "{source:?}: {unresolved_err}"
            );
        }
    }

    #[test]
    fn permissive_clickhouse_keeps_unresolved_and_decimal_division_best_effort() {
        let unresolved = transpile_with_level(
            "SELECT numerator / denominator FROM measurements",
            DialectType::PostgreSQL,
            DialectType::ClickHouse,
            UnsupportedLevel::Warn,
        )
        .expect("permissive transpilation should keep unresolved division");
        assert_eq!(
            unresolved,
            vec!["SELECT numerator / denominator FROM measurements"]
        );

        let decimal = transpile_with_level(
            "SELECT CAST(1 AS DECIMAL(18, 4)) / CAST(3 AS DECIMAL(18, 4))",
            DialectType::Redshift,
            DialectType::ClickHouse,
            UnsupportedLevel::Warn,
        )
        .expect("permissive transpilation should keep decimal division best effort");
        assert_eq!(
            decimal,
            vec!["SELECT CAST(1 AS Decimal(18, 4)) / CAST(3 AS Decimal(18, 4))"]
        );
    }

    #[test]
    fn strict_bigquery_clickhouse_rejects_unresolved_safe_divide() {
        let err = transpile_with_level(
            "SELECT SAFE_DIVIDE(x, y) FROM t",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpile should reject unresolved SAFE_DIVIDE types");

        assert!(err.to_string().contains("SAFE_DIVIDE"));
        assert!(err.to_string().contains("unresolved"));
    }

    #[test]
    fn strict_bigquery_clickhouse_rejects_float_rounding() {
        for value in ["2.5", "-2.5"] {
            let sql = format!("SELECT ROUND(CAST({value} AS FLOAT64))");
            let err = transpile_with_level(
                &sql,
                DialectType::BigQuery,
                DialectType::ClickHouse,
                UnsupportedLevel::Raise,
            )
            .expect_err("strict ClickHouse transpile should reject BigQuery Float64 ROUND");

            assert!(err.to_string().contains("ROUND over FLOAT64"));
            assert!(err.to_string().contains("half-away-from-zero"));
        }
    }

    #[test]
    fn strict_bigquery_clickhouse_allows_matching_decimal_rounding() {
        let output = transpile_with_level(
            "SELECT ROUND(CAST(2.5 AS NUMERIC))",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("ClickHouse Decimal ROUND uses BigQuery's half-away-from-zero rule");

        assert_eq!(output.len(), 1);
        assert!(output[0].contains("ROUND"));
        assert!(output[0].contains("Decimal"));
    }

    #[test]
    fn strict_snowflake_clickhouse_preserves_full_string_regexp_like() {
        let output = transpile_with_level(
            "SELECT REGEXP_LIKE('xprefixy', 'prefix')",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("Snowflake REGEXP_LIKE can be anchored for ClickHouse");

        assert_eq!(output, vec!["SELECT match('xprefixy', '(?-s)^(prefix)$')"]);

        let case_insensitive = transpile_with_level(
            "SELECT REGEXP_LIKE('Prefix', 'prefix', 'i')",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("literal Snowflake regex flags can be mapped to RE2 modifiers");
        assert_eq!(
            case_insensitive,
            vec!["SELECT match('Prefix', '(?i-s)^(prefix)$')"]
        );
    }

    #[test]
    fn strict_snowflake_clickhouse_rejects_dynamic_regexp_parameters() {
        let err = transpile_with_level(
            "SELECT REGEXP_LIKE(value, pattern, parameters) FROM events",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("dynamic Snowflake regex parameters cannot be mapped safely");

        assert!(err.to_string().contains("REGEXP_LIKE"));
        assert!(err.to_string().contains("dynamic parameters"));
    }

    #[test]
    fn snowflake_clickhouse_uses_exact_interpolated_median() {
        let output = transpile(
            "SELECT MEDIAN(value) FROM events",
            DialectType::Snowflake,
            DialectType::ClickHouse,
        );
        assert_eq!(
            output,
            "SELECT medianExactWeightedInterpolatedOrNull(value, 1) FROM events"
        );

        let decimal = transpile_with_level(
            "SELECT MEDIAN(CAST(value AS NUMBER(10, 2))) FROM events",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("fixed-point Snowflake MEDIAN has an exact ClickHouse lowering");
        assert_eq!(
            decimal,
            vec!["SELECT medianExactWeightedInterpolatedOrNull(CAST(value AS Decimal(10, 2)), 1) FROM events"]
        );

        let integer = transpile_with_level(
            "SELECT MEDIAN(CAST(value AS NUMBER(10, 0))) FROM events",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("integer medians can retain an interpolated half value through Decimal scale");
        assert_eq!(
            integer,
            vec!["SELECT medianExactWeightedInterpolatedOrNull(CAST(CAST(value AS Decimal(10, 0)) AS Decimal(11, 1)), 1) FROM events"]
        );
    }

    #[test]
    fn strict_snowflake_clickhouse_rejects_unresolved_median_type() {
        let err = transpile_with_level(
            "SELECT MEDIAN(value) FROM events",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict mode must not guess the Snowflake MEDIAN input type");

        assert!(err.to_string().contains("MEDIAN"));
        assert!(err.to_string().contains("unresolved input type"));
    }

    #[test]
    fn strict_snowflake_clickhouse_preserves_greatest_least_nulls() {
        let output = transpile_with_level(
            "SELECT GREATEST(1, NULL), LEAST(1, NULL)",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("Snowflake NULL propagation can be expressed with CASE");

        assert_eq!(
            output,
            vec!["SELECT CASE WHEN 1 IS NULL OR NULL IS NULL THEN NULL ELSE GREATEST(1, NULL) END, CASE WHEN 1 IS NULL OR NULL IS NULL THEN NULL ELSE LEAST(1, NULL) END"]
        );
    }

    #[test]
    fn strict_snowflake_clickhouse_handles_rounding_semantics() {
        let err = transpile_with_level(
            "SELECT ROUND(CAST(2.5 AS DOUBLE))",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("ClickHouse Float ROUND uses banker's rounding");
        assert!(err.to_string().contains("Snowflake ROUND"));
        assert!(err.to_string().contains("half-away-from-zero"));

        let decimal = transpile_with_level(
            "SELECT ROUND(CAST(2.5 AS NUMBER(10, 1)))",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("ClickHouse Decimal ROUND matches Snowflake's default mode");
        assert_eq!(decimal, vec!["SELECT ROUND(CAST(2.5 AS Decimal(10, 1)))"]);

        let half_even = transpile_with_level(
            "SELECT ROUND(CAST(2.5 AS NUMBER(10, 1)), 0, 'HALF_TO_EVEN')",
            DialectType::Snowflake,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect("Snowflake HALF_TO_EVEN can use ClickHouse roundBankers");
        assert_eq!(
            half_even,
            vec!["SELECT roundBankers(CAST(2.5 AS Decimal(10, 1)), 0)"]
        );
    }

    #[test]
    fn strict_bigquery_clickhouse_rejects_unrepresentable_timestamp_cases() {
        let out_of_range = transpile_with_level(
            "SELECT TIMESTAMP('1800-01-01 00:00:00')",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpile should reject an out-of-range timestamp");
        assert!(out_of_range.to_string().contains("1900-2299"));

        let dynamic_zone = transpile_with_level(
            "SELECT TIMESTAMP(event_time, timezone_name) FROM events",
            DialectType::BigQuery,
            DialectType::ClickHouse,
            UnsupportedLevel::Raise,
        )
        .expect_err("strict ClickHouse transpile should reject a dynamic time zone");
        assert!(dynamic_zone.to_string().contains("non-constant time zone"));
    }

    #[test]
    fn transpile_options_deserialize_unsupported_level_from_json() {
        let opts: TranspileOptions =
            serde_json::from_str(r#"{"unsupportedLevel":"raise","maxUnsupported":2}"#)
                .expect("options JSON should deserialize");

        assert_eq!(opts.unsupported_level, UnsupportedLevel::Raise);
        assert_eq!(opts.max_unsupported, 2);
        assert!(!opts.pretty);
    }

    #[test]
    fn transpile_options_deserialize_complexity_guard_from_json() {
        let opts: TranspileOptions =
            serde_json::from_str(r#"{"complexityGuard":{"maxFunctionCallDepth":128}}"#)
                .expect("options JSON should deserialize");

        assert_eq!(opts.complexity_guard.max_function_call_depth, Some(128));
        assert_eq!(
            opts.complexity_guard.max_parenthesis_depth,
            polyglot_sql::ComplexityGuardOptions::default().max_parenthesis_depth
        );
    }
}

mod tsql_fabric_regressions {
    use super::*;
    use polyglot_sql::builder::{cast, col};
    use polyglot_sql::expressions::{Expression, Grouping};

    fn pg_to_target(sql: &str, target: DialectType) -> String {
        Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, target, TranspileOptions::strict())
            .unwrap_or_else(|err| panic!("transpile failed for {sql:?} to {target:?}: {err}"))
            .into_iter()
            .next()
            .expect("expected one generated statement")
    }

    fn pg_to_target_with_options(
        sql: &str,
        target: DialectType,
        options: TranspileOptions,
    ) -> polyglot_sql::Result<String> {
        Ok(Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, target, options)?
            .into_iter()
            .next()
            .expect("expected one generated statement"))
    }

    #[test]
    fn postgres_multi_argument_grouping_uses_grouping_id_for_tsql_targets() {
        let input = "SELECT a, b, GROUPING(a, b), GROUPING(a), GROUPING_ID(a, b) FROM gs_tbl GROUP BY ROLLUP(a, b)";
        let expected = "SELECT a, b, GROUPING_ID(a, b), GROUPING(a), GROUPING_ID(a, b) FROM gs_tbl GROUP BY ROLLUP (a, b)";
        let typed_grouping = Expression::Grouping(Box::new(Grouping {
            expressions: vec![col("a").0, col("b").0],
        }));

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(pg_to_target(input, target), expected);
            assert_eq!(
                Dialect::get(target).generate(&typed_grouping).unwrap(),
                "GROUPING_ID(a, b)"
            );
        }
    }

    #[test]
    fn postgres_distinct_advanced_grouping_expands_for_tsql_targets() {
        let cases = [
            (
                "SELECT a, b, c FROM t GROUP BY DISTINCT ROLLUP(a, b), ROLLUP(a, c)",
                "SELECT a, b, c FROM t GROUP BY GROUPING SETS ((a, b, c), (a, b), (a, c), (a), ())",
            ),
            (
                "SELECT a, b FROM t GROUP BY DISTINCT CUBE(a, b)",
                "SELECT a, b FROM t GROUP BY GROUPING SETS ((a, b), (a), (b), ())",
            ),
            (
                "SELECT a FROM t GROUP BY DISTINCT GROUPING SETS ((a), (a), ())",
                "SELECT a FROM t GROUP BY GROUPING SETS ((a), ())",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_nested_grouping_sets_normalize_for_tsql_targets() {
        let cases = [
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS ((), GROUPING SETS ((), GROUPING SETS (()))) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS ((), (), ())",
            ),
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS ((), GROUPING SETS ((), GROUPING SETS (((a, b))))) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS ((), (), (a, b))",
            ),
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS (a, GROUPING SETS (a, CUBE (b))) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS (a, a, CUBE (b))",
            ),
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS ((a, (a, b)), GROUPING SETS ((a, (a, b)), a)) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS ((a, b), (a, b), a)",
            ),
            (
                "SELECT a, b, SUM(v), COUNT(*) FROM imported_groupingsets.gstest_empty GROUP BY GROUPING SETS ((a, b), a)",
                "GROUP BY GROUPING SETS ((a, b), a)",
            ),
            (
                "SELECT a, b, c, d FROM imported_groupingsets.gstest2 GROUP BY ROLLUP (a, b), GROUPING SETS (c, d)",
                "GROUP BY GROUPING SETS (c, d), ROLLUP (a, b)",
            ),
            (
                "SELECT a, b, SUM(c), COUNT(*) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS (ROLLUP (a, b), a)",
                "GROUP BY GROUPING SETS (ROLLUP (a, b), a)",
            ),
            (
                "SELECT ten, GROUPING(ten) FROM imported_groupingsets.onek GROUP BY GROUPING SETS (ten, four) HAVING GROUPING(ten) > 0 ORDER BY 2, 1",
                "GROUP BY GROUPING SETS (ten, four)",
            ),
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS (GROUPING SETS (a, GROUPING SETS (a), a)) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS (a, a, a)",
            ),
            (
                "SELECT SUM(c) FROM imported_groupingsets.gstest2 GROUP BY GROUPING SETS (GROUPING SETS (a, GROUPING SETS (a, GROUPING SETS (a), ((a)), a, GROUPING SETS (a), (a)), a)) ORDER BY 1 DESC",
                "GROUP BY GROUPING SETS (a, a, a, (a), a, a, (a), a)",
            ),
            (
                "SELECT four, x FROM (SELECT four, ten, 'foo'::text AS x FROM imported_groupingsets.tenk1) AS t GROUP BY GROUPING SETS (four, x) HAVING x = 'foo'",
                "GROUP BY GROUPING SETS (four, x)",
            ),
            (
                "SELECT four, x || 'x' FROM (SELECT four, ten, 'foo'::text AS x FROM imported_groupingsets.tenk1) AS t GROUP BY GROUPING SETS (four, x) ORDER BY four",
                "GROUP BY GROUPING SETS (four, x)",
            ),
            (
                "SELECT a, b, COUNT(*), MAX(a), MAX(b) FROM imported_groupingsets.gstest3 GROUP BY GROUPING SETS (a, b, ()) ORDER BY a, b",
                "GROUP BY GROUPING SETS (a, b, ())",
            ),
            (
                "SELECT (x + y) * 1, SUM(z) FROM imported_groupingsets.gs_xyz GROUP BY GROUPING SETS (x + y, x)",
                "GROUP BY GROUPING SETS (x + y, x)",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected_group_by) in cases {
                let strict = pg_to_target(sql, target);
                let default = Dialect::get(DialectType::PostgreSQL)
                    .transpile(sql, target)
                    .expect("default transpile should normalize grouping sets")
                    .into_iter()
                    .next()
                    .expect("expected one generated statement");

                assert_eq!(
                    strict, default,
                    "strict/default mismatch for {target:?}: {sql}"
                );
                assert!(
                    strict.contains(expected_group_by),
                    "expected {expected_group_by:?} for {target:?}, got {strict:?}"
                );
                assert_eq!(
                    strict.matches("GROUPING SETS").count(),
                    1,
                    "nested GROUPING SETS remained for {target:?}: {strict}"
                );
            }
        }
    }

    #[test]
    fn simple_distinct_and_all_grouping_modifiers_remain_unchanged_for_tsql_targets() {
        let cases = [
            (
                "SELECT a, b FROM t GROUP BY DISTINCT a, b",
                "SELECT a, b FROM t GROUP BY DISTINCT a, b",
            ),
            (
                "SELECT a, b, c FROM t GROUP BY ALL ROLLUP(a, b), ROLLUP(a, c)",
                "SELECT a, b, c FROM t GROUP BY ALL ROLLUP (a, b), ROLLUP (a, c)",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_cte_materialization_hints_are_stripped_for_tsql_and_fabric() {
        let cases = [
            (
                "WITH x AS MATERIALIZED (SELECT f1 FROM t) SELECT * FROM x WHERE f1 = 1",
                "WITH x AS (SELECT f1 FROM t) SELECT * FROM x WHERE f1 = 1",
            ),
            (
                "WITH x AS NOT MATERIALIZED (SELECT f1 FROM t) SELECT * FROM x WHERE f1 = 1",
                "WITH x AS (SELECT f1 FROM t) SELECT * FROM x WHERE f1 = 1",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target(sql, target);
                assert_eq!(result, expected, "failed for target {target:?}");
            }
        }
    }

    #[test]
    fn nested_ctes_in_from_subqueries_are_hoisted_for_tsql_and_fabric() {
        let sql = "WITH x AS (SELECT * FROM t) SELECT * FROM (WITH y AS (SELECT * FROM x) SELECT * FROM y) ss";
        let expected =
            "WITH x AS (SELECT * FROM t), y AS (SELECT * FROM x) SELECT * FROM (SELECT * FROM y) AS ss";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target(sql, target);
            assert_eq!(result, expected, "failed for target {target:?}");
        }
    }

    #[test]
    fn tsql_and_fabric_nvarchar_generation_differ_by_target() {
        let expr = cast(col("x"), "NVARCHAR(MAX)").into_inner();

        let tsql = Dialect::get(DialectType::TSQL).generate(&expr).unwrap();
        assert_eq!(tsql, "CAST(x AS NVARCHAR(MAX))");

        let fabric = Dialect::get(DialectType::Fabric).generate(&expr).unwrap();
        assert_eq!(fabric, "CAST(x AS VARCHAR(MAX))");
    }

    #[test]
    fn tsql_preserves_nvarchar_in_cast_parsing() {
        let result = Dialect::get(DialectType::TSQL)
            .transpile("SELECT CAST(x AS NVARCHAR(100))", DialectType::TSQL)
            .unwrap();

        assert_eq!(result[0], "SELECT CAST(x AS NVARCHAR(100))");
    }

    #[test]
    fn fabric_maps_nvarchar_to_supported_varchar() {
        let result = Dialect::get(DialectType::Fabric)
            .transpile("SELECT CAST(x AS NVARCHAR(MAX))", DialectType::Fabric)
            .unwrap();

        assert_eq!(result[0], "SELECT CAST(x AS VARCHAR(MAX))");
    }

    #[test]
    fn distinct_order_by_asc_nulls_last_uses_valid_wrapper_for_tsql_and_fabric() {
        let expected = "SELECT v FROM (SELECT DISTINCT v, CASE WHEN v IS NULL THEN 1 ELSE 0 END AS _polyglot_order_null_0, v AS _polyglot_order_key_0 FROM t) AS _polyglot_distinct_order ORDER BY _polyglot_order_null_0, _polyglot_order_key_0";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT DISTINCT v FROM t ORDER BY v", target),
                expected,
                "failed for target {target:?}"
            );
        }
    }

    #[test]
    fn distinct_order_by_desc_nulls_first_uses_valid_wrapper_for_tsql_and_fabric() {
        let expected = "SELECT v FROM (SELECT DISTINCT v, CASE WHEN v IS NULL THEN 1 ELSE 0 END AS _polyglot_order_null_0, v AS _polyglot_order_key_0 FROM t) AS _polyglot_distinct_order ORDER BY _polyglot_order_null_0 DESC, _polyglot_order_key_0 DESC";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT DISTINCT v FROM t ORDER BY v DESC", target),
                expected,
                "failed for target {target:?}"
            );
        }
    }

    #[test]
    fn distinct_order_by_alias_resolves_to_selected_expression() {
        let expected = "SELECT x FROM (SELECT DISTINCT v AS x, CASE WHEN v IS NULL THEN 1 ELSE 0 END AS _polyglot_order_null_0, v AS _polyglot_order_key_0 FROM t) AS _polyglot_distinct_order ORDER BY _polyglot_order_null_0, _polyglot_order_key_0";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT DISTINCT v AS x FROM t ORDER BY x", target),
                expected,
                "failed for target {target:?}"
            );
        }
    }

    #[test]
    fn distinct_order_by_target_default_null_ordering_does_not_wrap() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT DISTINCT v FROM t ORDER BY v NULLS FIRST", target),
                "SELECT DISTINCT v FROM t ORDER BY v",
                "failed for target {target:?}"
            );
        }
    }

    #[test]
    fn non_distinct_order_by_keeps_existing_tsql_null_ordering_emulation() {
        let expected = "SELECT v FROM t ORDER BY CASE WHEN v IS NULL THEN 1 ELSE 0 END, v";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT v FROM t ORDER BY v", target),
                expected,
                "failed for target {target:?}"
            );
        }
    }

    #[test]
    fn strict_mode_rejects_distinct_order_by_unselected_expression() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let err = Dialect::get(DialectType::PostgreSQL)
                .transpile_with(
                    "SELECT DISTINCT v FROM t ORDER BY LOWER(v)",
                    target,
                    TranspileOptions::strict(),
                )
                .expect_err("strict mode should reject unselected emulated ORDER BY expression");

            assert!(
                err.to_string()
                    .contains("SELECT DISTINCT with emulated NULL ordering"),
                "unexpected error for {target:?}: {err}"
            );
        }
    }

    #[test]
    fn nested_distinct_on_rewrites_for_tsql_and_fabric() {
        let sql = "SELECT * FROM foo WHERE id IN (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM bar) AS s)";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target(sql, target);
            assert!(
                !result.contains("DISTINCT ON"),
                "nested DISTINCT ON should be eliminated for {target:?}: {result}"
            );
            assert!(
                result.contains("ROW_NUMBER() OVER (PARTITION BY id2 ORDER BY id2)"),
                "nested DISTINCT ON should use ROW_NUMBER for {target:?}: {result}"
            );
        }
    }

    #[test]
    fn cte_distinct_on_rewrites_for_tsql_and_fabric() {
        let sql = "WITH s AS (SELECT DISTINCT ON (id2) id1, id2 FROM bar) SELECT id2 FROM s";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target(sql, target);
            assert!(
                !result.contains("DISTINCT ON"),
                "CTE DISTINCT ON should be eliminated for {target:?}: {result}"
            );
            assert!(
                result.contains("ROW_NUMBER() OVER (PARTITION BY id2 ORDER BY id2)"),
                "CTE DISTINCT ON should use ROW_NUMBER for {target:?}: {result}"
            );
        }
    }

    #[test]
    fn default_transpile_resolves_positional_order_by_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT f1 FROM t ORDER BY 1",
                "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1",
            ),
            (
                "SELECT f1 FROM t ORDER BY 1 DESC",
                "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END DESC, f1 DESC",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target_with_options(sql, target, TranspileOptions::default())
                    .unwrap_or_else(|err| panic!("default {target:?} transpile failed: {err}"));
                assert_eq!(result, expected, "failed for {target:?}: {sql}");
            }
        }
    }

    #[test]
    fn default_transpile_resolves_positional_order_by_on_set_operations_for_tsql_and_fabric() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target_with_options(
                "SELECT f1 FROM a UNION SELECT f1 FROM b ORDER BY 1",
                target,
                TranspileOptions::default(),
            )
            .unwrap_or_else(|err| panic!("default {target:?} transpile failed: {err}"));

            assert!(
                result.ends_with("ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1"),
                "set operation should resolve positional ORDER BY for {target:?}: {result}"
            );
            assert!(
                !result.contains("CASE WHEN 1 IS NULL"),
                "set operation should not emit a constant NULL-ordering CASE for {target:?}: {result}"
            );
        }
    }

    #[test]
    fn strict_transpile_resolves_positional_order_by_null_ordering_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT f1 FROM t ORDER BY 1",
                "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1",
            ),
            (
                "SELECT f1 FROM t ORDER BY 1 DESC",
                "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END DESC, f1 DESC",
            ),
            (
                "SELECT f1, f2 FROM t ORDER BY 2",
                "SELECT f1, f2 FROM t ORDER BY CASE WHEN f2 IS NULL THEN 1 ELSE 0 END, f2",
            ),
            (
                "SELECT f1 AS x FROM t ORDER BY 1",
                "SELECT f1 AS x FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1",
            ),
            (
                "SELECT f1 + 1 AS x FROM t ORDER BY 1",
                "SELECT f1 + 1 AS x FROM t ORDER BY CASE WHEN f1 + 1 IS NULL THEN 1 ELSE 0 END, f1 + 1",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target_with_options(sql, target, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict {target:?} transpile failed: {err}"));
                assert_eq!(result, expected, "failed for {target:?}: {sql}");
            }
        }
    }

    #[test]
    fn strict_transpile_disambiguates_duplicate_order_by_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT a, a FROM t ORDER BY a",
                "SELECT a, a FROM t ORDER BY CASE WHEN t.a IS NULL THEN 1 ELSE 0 END, t.a",
            ),
            (
                "SELECT a, a FROM t ORDER BY 1",
                "SELECT a, a FROM t ORDER BY CASE WHEN t.a IS NULL THEN 1 ELSE 0 END, t.a",
            ),
            (
                "SELECT a, a FROM t AS src ORDER BY a",
                "SELECT a, a FROM t AS src ORDER BY CASE WHEN src.a IS NULL THEN 1 ELSE 0 END, src.a",
            ),
            (
                "SELECT t.a, t.a FROM t ORDER BY a",
                "SELECT t.a, t.a FROM t ORDER BY CASE WHEN t.a IS NULL THEN 1 ELSE 0 END, t.a",
            ),
            (
                "SELECT a AS a1, a AS a2 FROM t ORDER BY a",
                "SELECT a AS a1, a AS a2 FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END, a",
            ),
            (
                "SELECT a, a FROM t ORDER BY t.a",
                "SELECT a, a FROM t ORDER BY CASE WHEN t.a IS NULL THEN 1 ELSE 0 END, t.a",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target_with_options(sql, target, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict {target:?} transpile failed: {err}"));
                assert_eq!(result, expected, "failed for {target:?}: {sql}");
            }
        }
    }

    #[test]
    fn strict_transpile_drops_inert_aggregate_ordering_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT SUM(two ORDER BY two), MAX(four ORDER BY four), MIN(ten ORDER BY ten) FROM tenk1",
                "SELECT SUM(two), MAX(four), MIN(ten) FROM tenk1",
            ),
            (
                "SELECT AVG(x ORDER BY y), ANY_VALUE(x ORDER BY y), APPROX_COUNT_DISTINCT(x ORDER BY y) FROM t",
                "SELECT AVG(x), ANY_VALUE(x), APPROX_COUNT_DISTINCT(x) FROM t",
            ),
            (
                "SELECT SUM(DISTINCT x ORDER BY x) FILTER (WHERE keep) FROM t",
                "SELECT SUM(DISTINCT CASE WHEN keep <> 0 THEN x END) FROM t",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY y) FROM t",
                "SELECT SUM(x) OVER (ORDER BY CASE WHEN y IS NULL THEN 1 ELSE 0 END, y) FROM t",
            ),
            (
                "SELECT STRING_AGG(x, ',' ORDER BY y) FROM t",
                "SELECT STRING_AGG(x, ',') WITHIN GROUP (ORDER BY CASE WHEN y IS NULL THEN 1 ELSE 0 END, y) FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target_with_options(sql, target, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict {target:?} transpile failed: {err}"));
                let expected = if target == DialectType::TSQL && expected.contains("ANY_VALUE") {
                    expected.replace("ANY_VALUE", "MAX")
                } else {
                    expected.to_string()
                };
                assert_eq!(result, expected, "failed for {target:?}: {sql}");
            }
        }
    }

    #[test]
    fn strict_transpile_rejects_hypothetical_sets_for_tsql_and_fabric() {
        let unsupported = ["RANK", "DENSE_RANK", "CUME_DIST", "PERCENT_RANK"];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for function in unsupported {
                let sql = format!("SELECT {function}(3) WITHIN GROUP (ORDER BY x) FROM t");
                let err = pg_to_target_with_options(&sql, target, TranspileOptions::strict())
                    .expect_err("strict transpile should reject hypothetical-set aggregates");
                assert!(
                    err.to_string().contains("hypothetical-set aggregates"),
                    "unexpected error for {target:?}: {sql}: {err}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_keeps_window_rank_functions_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT RANK() OVER (ORDER BY x) FROM t",
                "SELECT RANK() OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x) FROM t",
            ),
            (
                "SELECT DENSE_RANK() OVER (ORDER BY x) FROM t",
                "SELECT DENSE_RANK() OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x) FROM t",
            ),
            (
                "SELECT CUME_DIST() OVER (ORDER BY x) FROM t",
                "SELECT CUME_DIST() OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x) FROM t",
            ),
            (
                "SELECT PERCENT_RANK() OVER (ORDER BY x) FROM t",
                "SELECT PERCENT_RANK() OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x) FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                let result = pg_to_target_with_options(sql, target, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict {target:?} transpile failed: {err}"));
                assert_eq!(result, expected, "failed for {target:?}: {sql}");
            }
        }
    }

    #[test]
    fn fabric_drops_constant_grouping_order_terms_for_single_level_groups() {
        let cases = [
            (
                "SELECT ten, GROUPING(ten) FROM t GROUP BY GROUPING SETS (ten) ORDER BY 2, 1",
                "SELECT ten, GROUPING(ten) FROM t GROUP BY GROUPING SETS (ten) ORDER BY CASE WHEN ten IS NULL THEN 1 ELSE 0 END, ten",
            ),
            (
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ten ORDER BY 2, 1",
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ten ORDER BY CASE WHEN ten IS NULL THEN 1 ELSE 0 END, ten",
            ),
            (
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ten ORDER BY GROUPING(ten)",
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ten",
            ),
        ];

        for (sql, expected) in cases {
            assert_eq!(
                pg_to_target_with_options(sql, DialectType::Fabric, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict Fabric transpile failed: {err}")),
                expected,
                "failed for {sql}"
            );
        }
    }

    #[test]
    fn fabric_keeps_grouping_order_terms_for_multi_level_groups() {
        let cases = [
            (
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ROLLUP (ten) ORDER BY 2, 1",
                "SELECT ten, GROUPING(ten) FROM t GROUP BY ROLLUP (ten) ORDER BY CASE WHEN GROUPING(ten) IS NULL THEN 1 ELSE 0 END, GROUPING(ten), CASE WHEN ten IS NULL THEN 1 ELSE 0 END, ten",
            ),
            (
                "SELECT ten, GROUPING(ten) FROM t GROUP BY GROUPING SETS (ten, ()) ORDER BY 2, 1",
                "SELECT ten, GROUPING(ten) FROM t GROUP BY GROUPING SETS (ten, ()) ORDER BY CASE WHEN GROUPING(ten) IS NULL THEN 1 ELSE 0 END, GROUPING(ten), CASE WHEN ten IS NULL THEN 1 ELSE 0 END, ten",
            ),
        ];

        for (sql, expected) in cases {
            assert_eq!(
                pg_to_target_with_options(sql, DialectType::Fabric, TranspileOptions::strict())
                    .unwrap_or_else(|err| panic!("strict Fabric transpile failed: {err}")),
                expected,
                "failed for {sql}"
            );
        }
    }

    #[test]
    fn strict_transpile_resolves_positional_order_by_on_set_operations_for_tsql_and_fabric() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target_with_options(
                "SELECT f1 FROM a UNION SELECT f1 FROM b ORDER BY 1",
                target,
                TranspileOptions::strict(),
            )
            .unwrap_or_else(|err| panic!("strict {target:?} transpile failed: {err}"));

            assert!(
                result.ends_with("ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1"),
                "set operation should resolve positional ORDER BY for {target:?}: {result}"
            );
            assert!(
                !result.contains("CASE WHEN 1 IS NULL"),
                "set operation should not emit a constant NULL-ordering CASE for {target:?}: {result}"
            );
        }
    }

    #[test]
    fn strict_transpile_rejects_unresolved_positional_order_by_null_ordering_for_tsql_and_fabric() {
        let cases = [
            "SELECT * FROM t ORDER BY 1",
            "SELECT 1 FROM t ORDER BY 1",
            "SELECT f1 + 1 FROM a UNION SELECT f1 + 1 FROM b ORDER BY 1",
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for sql in cases {
                let err = pg_to_target_with_options(sql, target, TranspileOptions::strict())
                    .expect_err("strict transpile should reject unresolved positional ordering");
                let message = err.to_string();
                assert!(
                    message.contains("positional ordering"),
                    "unexpected error for {target:?} {sql:?}: {message}"
                );
            }
        }
    }

    #[test]
    fn strict_transpile_keeps_named_order_by_null_ordering_rewrite_for_tsql_and_fabric() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let result = pg_to_target_with_options(
                "SELECT f1 FROM t ORDER BY f1",
                target,
                TranspileOptions::strict(),
            )
            .unwrap_or_else(|err| panic!("strict {target:?} named ORDER BY should work: {err}"));

            assert_eq!(
                result, "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1",
                "failed for {target:?}"
            );
        }
    }

    #[test]
    fn negated_boolean_tests_preserve_null_rows_in_predicate_context() {
        let cases = [
            (
                "SELECT d FROM t WHERE b IS NOT FALSE",
                "SELECT d FROM t WHERE b = 1 OR b IS NULL",
            ),
            (
                "SELECT d FROM t WHERE b IS NOT TRUE",
                "SELECT d FROM t WHERE b = 0 OR b IS NULL",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn non_negated_boolean_tests_remain_simple_predicates() {
        let cases = [
            (
                "SELECT d FROM t WHERE b IS TRUE",
                "SELECT d FROM t WHERE b = 1",
            ),
            (
                "SELECT d FROM t WHERE b IS FALSE",
                "SELECT d FROM t WHERE b = 0",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn scalar_boolean_tests_are_definite_bit_values() {
        let cases = [
            (
                "SELECT b IS TRUE AS istrue FROM t",
                "SELECT CAST(CASE WHEN b = 1 THEN 1 ELSE 0 END AS BIT) AS istrue FROM t",
            ),
            (
                "SELECT b IS FALSE AS isfalse FROM t",
                "SELECT CAST(CASE WHEN b = 0 THEN 1 ELSE 0 END AS BIT) AS isfalse FROM t",
            ),
            (
                "SELECT b IS NOT TRUE AS isnt FROM t",
                "SELECT CAST(CASE WHEN b = 0 OR b IS NULL THEN 1 ELSE 0 END AS BIT) AS isnt FROM t",
            ),
            (
                "SELECT b IS NOT FALSE AS isnf FROM t",
                "SELECT CAST(CASE WHEN b = 1 OR b IS NULL THEN 1 ELSE 0 END AS BIT) AS isnf FROM t",
            ),
            (
                "SELECT b IS UNKNOWN AS isu FROM t",
                "SELECT CAST(CASE WHEN b IS NULL THEN 1 ELSE 0 END AS BIT) AS isu FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_boolean_literals_and_logical_values_are_materialized_for_tsql_targets() {
        let cases = [
            (
                "SELECT true AS t FROM one",
                "SELECT CAST(1 AS BIT) AS t FROM one",
            ),
            (
                "SELECT (true OR false) AS r FROM one",
                "SELECT CAST(CASE WHEN ((1 = 1) OR (1 = 0)) THEN 1 ELSE 0 END AS BIT) AS r FROM one",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn simple_case_when_values_remain_scalar_for_tsql_targets() {
        let cases = [
            (
                "SELECT CASE 1 WHEN 0 THEN 1 ELSE 2 END",
                "SELECT CASE 1 WHEN 0 THEN 1 ELSE 2 END",
            ),
            (
                "SELECT CASE b WHEN true THEN 1 ELSE 2 END FROM t",
                "SELECT CASE b WHEN CAST(1 AS BIT) THEN 1 ELSE 2 END FROM t",
            ),
            (
                "SELECT CASE WHEN 1 = 0 THEN 1 ELSE 2 END",
                "SELECT CASE WHEN 1 = 0 THEN 1 ELSE 2 END",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_float_to_integer_casts_round_for_tsql_targets() {
        let cases = [
            (
                "SELECT '32767.6'::real::smallint AS c",
                "SELECT CAST(ROUND(CAST('32767.6' AS REAL), 0) AS SMALLINT) AS c",
                "SELECT CAST(ROUND(CAST('32767.6' AS REAL), 0) AS SMALLINT) AS c",
            ),
            (
                "SELECT '-1.6'::double precision::bigint AS c",
                "SELECT CAST(ROUND(CAST('-1.6' AS FLOAT), 0) AS BIGINT) AS c",
                "SELECT CAST(ROUND(CAST('-1.6' AS FLOAT), 0) AS BIGINT) AS c",
            ),
            (
                "SELECT (('32767.6'::real))::smallint AS c",
                "SELECT CAST(ROUND(((CAST('32767.6' AS REAL))), 0) AS SMALLINT) AS c",
                "SELECT CAST(ROUND(((CAST('32767.6' AS REAL))), 0) AS SMALLINT) AS c",
            ),
            (
                "SELECT (((('-32768.6'::double precision))))::bigint AS c",
                "SELECT CAST(ROUND(((((CAST('-32768.6' AS FLOAT))))), 0) AS BIGINT) AS c",
                "SELECT CAST(ROUND(((((CAST('-32768.6' AS FLOAT))))), 0) AS BIGINT) AS c",
            ),
            (
                "SELECT smallint(real('32767.6')) AS c",
                "SELECT CAST(ROUND(CAST('32767.6' AS REAL), 0) AS SMALLINT) AS c",
                "SELECT CAST(ROUND(CAST('32767.6' AS REAL), 0) AS SMALLINT) AS c",
            ),
            (
                "SELECT '1.6'::numeric::integer AS c",
                "SELECT CAST(CAST('1.6' AS NUMERIC) AS INTEGER) AS c",
                "SELECT CAST(CAST('1.6' AS DECIMAL(38, 10)) AS INT) AS c",
            ),
            (
                "SELECT (('1.6'::numeric))::integer AS c",
                "SELECT CAST(((CAST('1.6' AS NUMERIC))) AS INTEGER) AS c",
                "SELECT CAST(((CAST('1.6' AS DECIMAL(38, 10)))) AS INT) AS c",
            ),
            (
                "SELECT value::integer AS c FROM t",
                "SELECT CAST(value AS INTEGER) AS c FROM t",
                "SELECT CAST(value AS INT) AS c FROM t",
            ),
        ];

        for (target, expected_index) in [(DialectType::TSQL, 0), (DialectType::Fabric, 1)] {
            for (sql, tsql_expected, fabric_expected) in cases {
                let expected = [tsql_expected, fabric_expected][expected_index];
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_subsecond_date_parts_preserve_values_and_result_types_for_tsql_targets() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let time = match target {
                DialectType::Fabric => "CAST('13:30:25.575401' AS TIME(6))",
                _ => "CAST('13:30:25.575401' AS TIME)",
            };
            let timestamp = match target {
                DialectType::Fabric => "CAST('2001-02-16 20:38:40.575401' AS DATETIME2(6))",
                _ => "CAST('2001-02-16 20:38:40.575401' AS DATETIME2)",
            };
            let timestamp_column = match target {
                DialectType::Fabric => "CAST(ts AS DATETIME2(6))",
                _ => "CAST(ts AS DATETIME2)",
            };
            let timestamp_with_time_zone = match target {
                DialectType::Fabric => "CAST('2001-02-16 20:38:40.575401+00' AS DATETIME2(6))",
                _ => "CAST('2001-02-16 20:38:40.575401+00' AS DATETIME2)",
            };
            let cases = [
                (
                    "SELECT EXTRACT(second FROM '13:30:25.575401'::time) AS value",
                    format!(
                        "SELECT DATEPART(SECOND, {time}) + DATEPART(MICROSECOND, {time}) / 1000000.0 AS value"
                    ),
                ),
                (
                    "SELECT date_part('second', TIME '13:30:25.575401') AS value",
                    format!(
                        "SELECT CAST(DATEPART(SECOND, {time}) + DATEPART(MICROSECOND, {time}) / 1000000.0 AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT date_part('milliseconds', '13:30:25.575401'::time) AS value",
                    format!(
                        "SELECT CAST(DATEPART(SECOND, {time}) * 1000 + DATEPART(MICROSECOND, {time}) / 1000.0 AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT EXTRACT(microsecond FROM '13:30:25.575401'::time) AS value",
                    format!(
                        "SELECT DATEPART(SECOND, {time}) * 1000000 + DATEPART(MICROSECOND, {time}) AS value"
                    ),
                ),
                (
                    "SELECT date_part('microseconds', TIME '13:30:25.575401') AS value",
                    format!(
                        "SELECT CAST(DATEPART(SECOND, {time}) * 1000000 + DATEPART(MICROSECOND, {time}) AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT date_part('epoch', '13:30:25.575401'::time) AS value",
                    format!(
                        "SELECT CAST(DATEDIFF(SECOND, CAST('00:00:00' AS TIME(6)), {time}) + DATEPART(MICROSECOND, {time}) / 1000000.0 AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT EXTRACT(epoch FROM TIME '13:30:25.575401') AS value",
                    format!(
                        "SELECT DATEDIFF(SECOND, CAST('00:00:00' AS TIME(6)), {time}) + DATEPART(MICROSECOND, {time}) / 1000000.0 AS value"
                    ),
                ),
                (
                    "SELECT date_part('second', ts) AS value FROM t",
                    "SELECT CAST(DATEPART(SECOND, ts) + DATEPART(MICROSECOND, ts) / 1000000.0 AS FLOAT) AS value FROM t".to_string(),
                ),
                (
                    "SELECT date_part('milliseconds', ts) AS value FROM t",
                    "SELECT CAST(DATEPART(SECOND, ts) * 1000 + DATEPART(MICROSECOND, ts) / 1000.0 AS FLOAT) AS value FROM t".to_string(),
                ),
                (
                    "SELECT date_part('microseconds', ts) AS value FROM t",
                    "SELECT CAST(DATEPART(SECOND, ts) * 1000000 + DATEPART(MICROSECOND, ts) AS FLOAT) AS value FROM t".to_string(),
                ),
                (
                    "SELECT date_part('second', TIMESTAMP '2001-02-16 20:38:40.575401') AS value",
                    format!(
                        "SELECT CAST(DATEPART(SECOND, {timestamp}) + DATEPART(MICROSECOND, {timestamp}) / 1000000.0 AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT EXTRACT(milliseconds FROM ts::timestamp) AS value FROM t",
                    format!(
                        "SELECT DATEPART(SECOND, {timestamp_column}) * 1000 + DATEPART(MICROSECOND, {timestamp_column}) / 1000.0 AS value FROM t"
                    ),
                ),
                (
                    "SELECT date_part('microseconds', TIMESTAMPTZ '2001-02-16 20:38:40.575401+00') AS value",
                    format!(
                        "SELECT CAST(DATEPART(SECOND, {timestamp_with_time_zone}) * 1000000 + DATEPART(MICROSECOND, {timestamp_with_time_zone}) AS FLOAT) AS value"
                    ),
                ),
                (
                    "SELECT EXTRACT(hour FROM '13:30:25.575401'::time) AS value",
                    format!("SELECT DATEPART(HOUR, {time}) AS value"),
                ),
                (
                    "SELECT date_part('hour', TIME '13:30:25.575401') AS value",
                    format!("SELECT CAST(DATEPART(hour, {time}) AS FLOAT) AS value"),
                ),
            ];

            for (sql, expected) in &cases {
                for options in [TranspileOptions::default(), TranspileOptions::strict()] {
                    assert_eq!(
                        pg_to_target_with_options(sql, target, options)
                            .unwrap_or_else(|err| panic!("transpile failed for {sql:?}: {err}")),
                        *expected,
                        "failed for {target:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn postgres_date_part_float_cast_is_source_scoped_for_tsql_targets() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let native_sql = "SELECT DATEPART(HOUR, ts) AS value FROM t";
            assert_eq!(
                Dialect::get(target)
                    .transpile_with(native_sql, target, TranspileOptions::strict())
                    .expect("native DATEPART should transpile")
                    .remove(0),
                native_sql,
            );

            assert_eq!(
                Dialect::get(DialectType::Generic)
                    .transpile_with(
                        "SELECT DATE_PART('hour', ts) AS value FROM t",
                        target,
                        TranspileOptions::strict(),
                    )
                    .expect("non-PostgreSQL DATE_PART should transpile")
                    .remove(0),
                "SELECT DATEPART(hour, ts) AS value FROM t",
            );
        }
    }

    #[test]
    fn postgres_boolean_operator_functions_lower_to_tsql_predicates() {
        let cases = [
            (
                "SELECT b FROM tb WHERE booleq(false, b)",
                "SELECT b FROM tb WHERE 0 = b",
            ),
            (
                "SELECT b FROM tb WHERE boolne(false, b)",
                "SELECT b FROM tb WHERE 0 <> b",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn postgres_known_boolean_text_casts_preserve_text_and_null_semantics() {
        let cases = [
            ("SELECT true::text", "SELECT 'true'"),
            (
                "SELECT (b::boolean)::text FROM tb",
                "SELECT CASE WHEN (CAST(b AS BIT) <> 0) THEN 'true' WHEN NOT (CAST(b AS BIT) <> 0) THEN 'false' ELSE NULL END FROM tb",
            ),
            (
                "SELECT (a = 1)::text FROM t",
                "SELECT CASE WHEN (a = 1) THEN 'true' WHEN NOT (a = 1) THEN 'false' ELSE NULL END FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn nested_boolean_casts_materialize_for_tsql_targets() {
        let predicate_case = "CASE WHEN (l_discount > 0.05) THEN 1 WHEN NOT (l_discount > 0.05) THEN 0 ELSE NULL END";
        for target in [DialectType::TSQL, DialectType::Fabric] {
            let (integer, numeric) = match target {
                DialectType::Fabric => ("INT", "DECIMAL(38, 10)"),
                _ => ("INTEGER", "NUMERIC"),
            };
            let cases = [
                (
                    "SELECT (l_discount > 0.05)::int AS x FROM t",
                    format!(
                        "SELECT CAST(CAST({predicate_case} AS BIT) AS {integer}) AS x FROM t"
                    ),
                ),
                (
                    "SELECT SUM((l_discount > 0.05)::int) AS x FROM t",
                    format!(
                        "SELECT SUM(CAST(CAST({predicate_case} AS BIT) AS {integer})) AS x FROM t"
                    ),
                ),
                (
                    "SELECT abs((l_discount > 0.05)::int) AS x FROM t",
                    format!(
                        "SELECT ABS(CAST(CAST({predicate_case} AS BIT) AS {integer})) AS x FROM t"
                    ),
                ),
                (
                    "SELECT ((l_discount > 0.05)::int) + 0 AS x FROM t",
                    format!(
                        "SELECT (CAST(CAST({predicate_case} AS BIT) AS {integer})) + 0 AS x FROM t"
                    ),
                ),
                (
                    "SELECT ABS(((l_discount > 0.05)::int) + 1) AS x FROM t",
                    format!(
                        "SELECT ABS((CAST(CAST({predicate_case} AS BIT) AS {integer})) + 1) AS x FROM t"
                    ),
                ),
                (
                    "SELECT AVG((l_returnflag = 'R')::int::numeric) AS x FROM t",
                    format!(
                        "SELECT AVG(CAST(CAST(CAST(CASE WHEN (l_returnflag = 'R') THEN 1 WHEN NOT (l_returnflag = 'R') THEN 0 ELSE NULL END AS BIT) AS {integer}) AS {numeric})) AS x FROM t"
                    ),
                ),
            ];

            for (sql, expected) in &cases {
                assert_eq!(
                    pg_to_target(sql, target),
                    *expected,
                    "failed for {target:?}: {sql}"
                );
            }
        }
    }

    #[test]
    fn scalar_boolean_values_preserve_unknown_for_tsql_targets() {
        let expected = "SELECT CAST(CASE WHEN (nullable_value > 0) THEN 1 WHEN NOT (nullable_value > 0) THEN 0 ELSE NULL END AS BIT) AS flag FROM t";

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT (nullable_value > 0) AS flag FROM t", target),
                expected,
                "failed for {target:?}"
            );
        }
    }

    #[test]
    fn strict_unknown_column_text_cast_uses_best_effort_for_tsql_targets() {
        for source in [DialectType::PostgreSQL, DialectType::CockroachDB] {
            for target in [DialectType::TSQL, DialectType::Fabric] {
                let strict = Dialect::get(source)
                    .transpile_with(
                        "SELECT (b)::text FROM tb",
                        target,
                        TranspileOptions::strict(),
                    )
                    .expect("strict mode should use the best-effort cast for an untyped column");
                let default = Dialect::get(source)
                    .transpile_with(
                        "SELECT (b)::text FROM tb",
                        target,
                        TranspileOptions::default(),
                    )
                    .expect("default mode should retain best-effort behavior");

                assert_eq!(strict, default, "failed for {source:?} -> {target:?}");
                assert_eq!(
                    strict,
                    vec!["SELECT CAST((b) AS VARCHAR(MAX)) FROM tb".to_string()],
                    "failed for {source:?} -> {target:?}"
                );
            }
        }
    }

    #[test]
    fn boolean_tests_on_predicate_operands_do_not_compare_predicates_to_integers() {
        let cases = [
            (
                "SELECT d FROM t WHERE (a > 1) IS NOT FALSE",
                "SELECT d FROM t WHERE CASE WHEN NOT (a > 1) THEN 0 ELSE 1 END = 1",
            ),
            (
                "SELECT d FROM t WHERE (a > 1) IS NOT TRUE",
                "SELECT d FROM t WHERE CASE WHEN (a > 1) THEN 0 ELSE 1 END = 1",
            ),
            (
                "SELECT (a > 1) IS TRUE AS ok FROM t",
                "SELECT CAST(CASE WHEN (a > 1) THEN 1 ELSE 0 END AS BIT) AS ok FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn bare_boolean_predicates_are_coerced_for_tsql_and_fabric() {
        let cases = [
            ("SELECT o FROM t WHERE b", "SELECT o FROM t WHERE b <> 0"),
            (
                "SELECT o FROM t WHERE NOT b",
                "SELECT o FROM t WHERE NOT b <> 0",
            ),
            (
                "SELECT o FROM t WHERE b AND c",
                "SELECT o FROM t WHERE b <> 0 AND c <> 0",
            ),
            (
                "SELECT o FROM t WHERE b OR c",
                "SELECT o FROM t WHERE b <> 0 OR c <> 0",
            ),
            (
                "SELECT o FROM t GROUP BY o HAVING b",
                "SELECT o FROM t GROUP BY o HAVING b <> 0",
            ),
            (
                "SELECT CASE WHEN b THEN 1 ELSE 0 END FROM t",
                "SELECT CASE WHEN b <> 0 THEN 1 ELSE 0 END FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn projected_boolean_aliases_are_coerced_inside_scalar_predicates() {
        let cases = [
            (
                "SELECT NOT x FROM (SELECT q1 = 1 AS x FROM t) AS s",
                "SELECT CAST(CASE WHEN NOT x <> 0 THEN 1 WHEN NOT NOT x <> 0 THEN 0 ELSE NULL END AS BIT) FROM (SELECT CAST(CASE WHEN q1 = 1 THEN 1 WHEN NOT q1 = 1 THEN 0 ELSE NULL END AS BIT) AS x FROM t) AS s",
            ),
            (
                "SELECT x AND y FROM (SELECT q1 = 1 AS x, q2 = 2 AS y FROM t) AS s",
                "SELECT CAST(CASE WHEN x <> 0 AND y <> 0 THEN 1 WHEN NOT (x <> 0 AND y <> 0) THEN 0 ELSE NULL END AS BIT) FROM (SELECT CAST(CASE WHEN q1 = 1 THEN 1 WHEN NOT q1 = 1 THEN 0 ELSE NULL END AS BIT) AS x, CAST(CASE WHEN q2 = 2 THEN 1 WHEN NOT q2 = 2 THEN 0 ELSE NULL END AS BIT) AS y FROM t) AS s",
            ),
            (
                "SELECT NOT (q1 = 1) FROM t",
                "SELECT CAST(CASE WHEN NOT (q1 = 1) THEN 1 WHEN NOT NOT (q1 = 1) THEN 0 ELSE NULL END AS BIT) FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn bare_boolean_join_on_predicates_are_coerced_for_tsql_and_fabric() {
        let cases = [
            (
                "SELECT o FROM t JOIN u ON t.b",
                "SELECT o FROM t JOIN u ON t.b <> 0",
            ),
            (
                "SELECT o FROM t JOIN u ON NOT t.b",
                "SELECT o FROM t JOIN u ON NOT t.b <> 0",
            ),
            (
                "SELECT o FROM t JOIN u ON t.b AND u.c",
                "SELECT o FROM t JOIN u ON t.b <> 0 AND u.c <> 0",
            ),
            (
                "SELECT a.f1 FROM (int4_tbl AS a LEFT JOIN int4_tbl AS b ON true)",
                "SELECT a.f1 FROM (int4_tbl AS a LEFT JOIN int4_tbl AS b ON (1 = 1))",
            ),
            (
                "SELECT a.f1 FROM (int4_tbl AS a LEFT JOIN int4_tbl AS b ON false)",
                "SELECT a.f1 FROM (int4_tbl AS a LEFT JOIN int4_tbl AS b ON (1 = 0))",
            ),
            (
                "SELECT a.id FROM ((a JOIN b ON true) LEFT JOIN c ON false)",
                "SELECT a.id FROM ((a JOIN b ON (1 = 1)) LEFT JOIN c ON (1 = 0))",
            ),
            (
                "SELECT a.id FROM (a JOIN b ON a.id = b.id)",
                "SELECT a.id FROM (a JOIN b ON a.id = b.id)",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn nested_boolean_predicates_are_coerced_for_tsql_and_fabric() {
        assert_eq!(
            pg_to_target(
                "SELECT * FROM (SELECT o FROM t WHERE b) AS x",
                DialectType::TSQL,
            ),
            "SELECT * FROM (SELECT o AS o FROM t WHERE b <> 0) AS x"
        );
        assert_eq!(
            pg_to_target(
                "SELECT * FROM (SELECT o FROM t WHERE b) AS x",
                DialectType::Fabric,
            ),
            "SELECT * FROM (SELECT o FROM t WHERE b <> 0) AS x"
        );

        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target(
                    "WITH x AS (SELECT o FROM t WHERE b) SELECT o FROM x",
                    target,
                ),
                "WITH x AS (SELECT o FROM t WHERE b <> 0) SELECT o FROM x",
                "failed for {target:?}"
            );
        }
    }

    #[test]
    fn null_safe_comparisons_in_select_list_are_materialized_as_bit_values() {
        let cases = [
            (
                "SELECT f1 IS DISTINCT FROM 2 AS not2 FROM t",
                "SELECT CAST(CASE WHEN f1 IS DISTINCT FROM 2 THEN 1 ELSE 0 END AS BIT) AS not2 FROM t",
            ),
            (
                "SELECT f1 IS NOT DISTINCT FROM 2 AS is2 FROM t",
                "SELECT CAST(CASE WHEN f1 IS NOT DISTINCT FROM 2 THEN 1 ELSE 0 END AS BIT) AS is2 FROM t",
            ),
            (
                "SELECT (f1 IS DISTINCT FROM f2) AS diff FROM t",
                "SELECT CAST(CASE WHEN (f1 IS DISTINCT FROM f2) THEN 1 ELSE 0 END AS BIT) AS diff FROM t",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }

    #[test]
    fn null_safe_comparisons_in_scalar_sort_contexts_are_materialized() {
        for target in [DialectType::TSQL, DialectType::Fabric] {
            assert_eq!(
                pg_to_target("SELECT f1 FROM t ORDER BY f1 IS DISTINCT FROM 2", target),
                "SELECT f1 FROM t ORDER BY CASE WHEN CAST(CASE WHEN f1 IS DISTINCT FROM 2 THEN 1 ELSE 0 END AS BIT) IS NULL THEN 1 ELSE 0 END, CAST(CASE WHEN f1 IS DISTINCT FROM 2 THEN 1 ELSE 0 END AS BIT)",
                "failed for {target:?}"
            );
        }
    }

    #[test]
    fn null_safe_comparisons_in_predicate_contexts_remain_predicates() {
        let cases = [
            (
                "SELECT f1 FROM t WHERE f1 IS DISTINCT FROM 2",
                "SELECT f1 FROM t WHERE f1 IS DISTINCT FROM 2",
            ),
            (
                "SELECT f1 FROM t WHERE f1 IS NOT DISTINCT FROM 2",
                "SELECT f1 FROM t WHERE f1 IS NOT DISTINCT FROM 2",
            ),
            (
                "SELECT f1 FROM t JOIN u ON t.f1 IS DISTINCT FROM u.f1",
                "SELECT f1 FROM t JOIN u ON t.f1 IS DISTINCT FROM u.f1",
            ),
        ];

        for target in [DialectType::TSQL, DialectType::Fabric] {
            for (sql, expected) in cases {
                assert_eq!(pg_to_target(sql, target), expected, "failed for {target:?}");
            }
        }
    }
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

    #[test]
    fn test_unicode_date_time_issue_examples() {
        for (sql, from, to, expected) in [
            (
                "SELECT TO_TIMESTAMP(x, 'éyyyy')",
                DialectType::Snowflake,
                DialectType::Snowflake,
                "SELECT TO_TIMESTAMP(x, 'éyyyy')",
            ),
            (
                "SELECT TO_TIMESTAMP(x, 'éyyyy')",
                DialectType::DuckDB,
                DialectType::DuckDB,
                "SELECT STRPTIME(x, 'é%Y')",
            ),
            (
                "SELECT TO_CHAR(x, 'éYYYY')",
                DialectType::Oracle,
                DialectType::Presto,
                "SELECT DATE_FORMAT(x, 'éYYYY')",
            ),
            (
                "SELECT TO_TIMESTAMP(x, 'YYYY年MM月DD日')",
                DialectType::Snowflake,
                DialectType::DuckDB,
                "SELECT STRPTIME(x, '%Y年%m月%d日')",
            ),
            (
                "SELECT TO_TIMESTAMP(x, 'ßYYYY')",
                DialectType::Snowflake,
                DialectType::Snowflake,
                "SELECT TO_TIMESTAMP(x, 'ßyyyy')",
            ),
            (
                "SELECT TO_CHAR(x, 'éyyyy年mm')",
                DialectType::Oracle,
                DialectType::Presto,
                "SELECT DATE_FORMAT(x, 'é%Y年%m')",
            ),
        ] {
            assert_eq!(
                transpile(sql, from, to),
                expected,
                "{from:?} -> {to:?}: {sql}"
            );
        }
    }

    #[test]
    fn test_unicode_date_time_presto_conversion() {
        use polyglot_sql::dialects::PrestoDialect;
        for literal in ["é", "年", "🦀", "e\u{301}", "ß", "ſ"] {
            assert_eq!(PrestoDialect::oracle_to_presto_format(literal), literal);
            assert_eq!(
                PrestoDialect::oracle_to_presto_format(&format!(
                    "{literal}yyyy{literal}mm{literal}"
                )),
                format!("{literal}%Y{literal}%m{literal}")
            );
        }
        assert_eq!(PrestoDialect::oracle_to_presto_format(""), "");
        assert_eq!(
            PrestoDialect::oracle_to_presto_format("yyyy yy hh24 hh mi mm dd ss YYYY %q"),
            "%Y %y %H %H %i %m %d %s YYYY %q"
        );
    }

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
    fn duckdb_quantile_aggregates_preserve_existing_target_lowerings() {
        let cases = [
            (
                "SELECT QUANTILE(a, 0.5) FROM t",
                DialectType::Spark,
                "SELECT PERCENTILE(a, 0.5) FROM t",
            ),
            (
                "SELECT QUANTILE_CONT(a, 0.5) FROM t",
                DialectType::PostgreSQL,
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a) FROM t",
            ),
            (
                "SELECT QUANTILE_DISC(a, 0.5) FROM t",
                DialectType::PostgreSQL,
                "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY a) FROM t",
            ),
            (
                "SELECT APPROX_QUANTILE(a, 0.5) FROM t",
                DialectType::Snowflake,
                "SELECT APPROX_PERCENTILE(a, 0.5) FROM t",
            ),
        ];

        for (sql, target, expected) in cases {
            assert_eq!(transpile(sql, DialectType::DuckDB, target), expected);
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

#[cfg(feature = "dialect-hana")]
mod hana_regressions {
    use super::*;
    use polyglot_sql::expressions::Expression;
    use polyglot_sql::ExpressionWalk;

    #[test]
    fn shared_function_nodes_participate_in_generic_analysis() {
        use polyglot_sql::optimizer::{normalize_identifiers, TypeAnnotator};
        let hana = Dialect::get(DialectType::HANA);
        let ast = hana
            .parse("SELECT COALESCE(mixed, 1), mixed FROM tab")
            .unwrap()
            .remove(0);
        assert!(ast.dfs().any(|e| matches!(e, Expression::Function(_))));
        let normalized = normalize_identifiers(ast, Some(DialectType::HANA));
        assert_eq!(
            hana.generate(&normalized).unwrap(),
            "SELECT COALESCE(MIXED, 1), MIXED FROM TAB"
        );
        let ast = hana
            .parse("SELECT COALESCE(1, 2) FROM DUMMY")
            .unwrap()
            .remove(0);
        let call = ast
            .dfs()
            .find(|e| matches!(e, Expression::Function(_)))
            .unwrap();
        assert_eq!(call.source_dialect(), Some(DialectType::HANA));
        assert!(matches!(
            TypeAnnotator::new(None, Some(DialectType::HANA)).annotate(call),
            Some(polyglot_sql::expressions::DataType::Int { .. })
        ));
        for dialect in [DialectType::HANA, DialectType::PostgreSQL] {
            let ast = Dialect::get(dialect)
                .parse(r#"SELECT "f"(x) FROM t"#)
                .unwrap()
                .remove(0);
            let call = ast
                .dfs()
                .find(|e| matches!(e, Expression::Function(_)))
                .unwrap();
            assert_eq!(
                call.source_dialect(),
                None,
                "quoted UDFs are not native built-ins"
            );
        }
    }

    #[test]
    fn source_metadata_survives_optimizers_and_standalone_generation() {
        use polyglot_sql::optimizer::{canonicalize, normalize_identifiers};
        let hana = Dialect::get(DialectType::HANA);
        let trino = Dialect::get(DialectType::Trino);
        let ast = hana
            .parse("SELECT LOCATE('abcabc', 'bc', 1, 2) FROM DUMMY")
            .unwrap()
            .remove(0);
        let ast = canonicalize(
            normalize_identifiers(ast, Some(DialectType::HANA)),
            Some(DialectType::HANA),
        );
        let call = ast
            .dfs()
            .find(|e| matches!(e, Expression::Function(_)))
            .unwrap();
        let json = serde_json::to_string(call).unwrap();
        assert!(json.contains("\"source_dialect\":\"hana\""));
        assert!(!json.contains("hana_function"));
        let call: Expression = serde_json::from_str(&json).unwrap();
        assert_eq!(trino.generate(&call).unwrap(), "STRPOS('abcabc', 'bc', 2)");
        assert_eq!(
            hana.generate(&call).unwrap(),
            "LOCATE('abcabc', 'bc', 1, 2)"
        );
        for sql in [
            "SELECT SUBSTR_REGEXPR('a' IN value OCCURRENCE 2) FROM t",
            "SELECT JSON_VALUE(payload, '$.n' DEFAULT 0 ON EMPTY) FROM t",
            "SELECT STRING_AGG(value, ',' ORDER BY id) FROM t",
        ] {
            let ast = hana.parse(sql).unwrap().remove(0);
            let json = serde_json::to_string(&ast).unwrap();
            let decoded: Expression = serde_json::from_str(&json).unwrap();
            assert_eq!(hana.generate(&decoded).unwrap(), sql);
            assert!(trino.generate(&decoded).is_err(), "{sql}");
            assert!(!decoded.dfs().any(|e| e.variant_name().starts_with("hana_")));
        }
    }

    #[test]
    fn create_table_traversal_is_independent_of_source_metadata() {
        use polyglot_sql::ast_transforms::rename_columns;
        let hana = Dialect::get(DialectType::HANA);
        let ast = hana
            .parse("CREATE TABLE t (value INT CHECK (value > 0))")
            .unwrap()
            .remove(0);
        let mut outputs = Vec::new();
        for source in [None, Some(DialectType::HANA)] {
            let mut ast = ast.clone();
            if let Expression::CreateTable(table) = &mut ast {
                table.source_dialect = source;
            }
            let renamed =
                rename_columns(ast, &[("value".to_owned(), "new_value".to_owned())].into());
            outputs.push(hana.generate(&renamed).unwrap());
        }
        assert_eq!(outputs[0], outputs[1]);
        assert!(outputs[0].contains("CHECK (new_value > 0)"));
    }

    #[test]
    fn native_syntax_uses_shared_expression_categories() {
        let hana = Dialect::get(DialectType::HANA);
        for (sql, expected) in [
            ("SELECT JSON_VALUE(j, '$.a') FROM t", "j_s_o_n_value"),
            ("SELECT JSON_QUERY(j, '$.a') FROM t", "json_query"),
            (
                "SELECT * FROM JSON_TABLE(j, '$' COLUMNS (v INT PATH '$.v'))",
                "j_s_o_n_table",
            ),
            (
                "SELECT * FROM t WHERE value LIKE_REGEXPR 'a'",
                "regexp_like",
            ),
            (
                "SELECT SUBSTR_REGEXPR('a' IN value) FROM t",
                "regexp_extract",
            ),
            (
                "SELECT a FROM t GROUP BY ROLLUP STRUCTURED RESULT (a)",
                "rollup",
            ),
            ("SELECT * FROM t WITH HINT (NO_INLINE)", "hint"),
            (
                "CREATE TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 4",
                "partition_by_property",
            ),
            ("CALL demo.proc(1)", "call"),
            ("UPSERT t VALUES (1) WITH PRIMARY KEY", "upsert"),
        ] {
            let ast = hana.parse(sql).unwrap().remove(0);
            assert!(
                ast.dfs().any(|e| e.variant_name() == expected),
                "{sql}: expected {expected}"
            );
            assert_eq!(hana.generate(&ast).unwrap(), sql);
        }
    }

    #[test]
    fn native_semantics_survive_transpile_and_json() {
        let hana = Dialect::get(DialectType::HANA);
        for sql in [
            "SELECT LOCATE('abcabc', 'bc', 1, 2) FROM DUMMY",
            "SELECT TO_DECIMAL(7654321.888888, 10, 3) FROM DUMMY",
            "SELECT CURRENT_UTCTIMESTAMP(7), CURRENT_UTCDATE, CURRENT_UTCTIME FROM DUMMY",
            "SELECT ADD_DAYS(d, 7), BITAND(a, b), TO_VARCHAR(d, 'YYYY-MM-DD') FROM t",
            "SELECT * FROM t FOR JSON",
            "SELECT * FROM t FOR XML",
            "SELECT * FROM t FOR JSON ('arraywrap' = 'NO')",
            "SELECT * FROM t WITH HINT (NO_INLINE)",
            "ALTER TABLE t ADD (x INT)",
            "CREATE TABLE t (xs INT ARRAY)",
            "CREATE COLUMN TABLE t (id INT)",
            "CREATE ROW TABLE t (id INT)",
            "CREATE LOCAL TEMPORARY COLUMN TABLE t (id INT)",
            "SELECT * FROM t FOR UPDATE IGNORE LOCKED",
            "SELECT * FROM t WHERE CONTAINS(name, 'abc', FUZZY(0.8))",
            "SELECT * FROM t WHERE name LIKE_REGEXPR '^a' FLAG 'i'",
            "SELECT SUBSTR_REGEXPR('a' IN s OCCURRENCE 2) FROM t",
            "UPSERT t VALUES (1, 8) WITH PRIMARY KEY",
            "UPSERT t VALUES (2, 2) WHERE id = 2",
            "UPSERT t SELECT id, val FROM source",
            "CREATE COLUMN TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 4",
            "CREATE COLUMN TABLE t (id INT) PARTITION BY HASH (id) NO PRIMARY KEY CHECK PARTITIONS 4",
            "CREATE COLUMN TABLE t (id INT) PARTITION BY RANGE (id) PRIMARY KEY CHECK (PARTITION OTHERS)",
            "SELECT * FROM HIERARCHY(SOURCE (SELECT id AS node_id, parent AS parent_id FROM t))",
            "SELECT * FROM HIERARCHY(SOURCE t START WHERE id = 1 SIBLING ORDER BY id ORPHAN ROOT)",
            r#"SELECT * FROM "_SYS_BIC"."pkg/view"(PLACEHOLDER."$$P$$" => 'X')"#,
            "SELECT * FROM t FOR JSON ('arraywrap' = 'NO') RETURNS NVARCHAR(5000)",
            "SELECT * FROM t FOR XML RETURNS NCLOB",
            "SELECT * FROM t FOR SHARE LOCK WAIT 3",
            "SELECT * FROM t FOR SHARE LOCK IGNORE LOCKED",
            "SELECT * FROM t ORDER BY a WITH COLLATION ENGLISH WITH HINT (NO_INLINE CASCADE)",
            "CALL demo.proc(1, ?) ASYNC WITH HINT (NO_INLINE)",
            "CALL demo.library:member(1)",
            "SELECT JSON_VALUE(j, '$.a' RETURNING INT DEFAULT 0 ON EMPTY ERROR ON ERROR) FROM t",
            "SELECT JSON_QUERY(j, '$.a' WITH CONDITIONAL ARRAY WRAPPER EMPTY ARRAY ON EMPTY NULL ON ERROR) FROM t",
            "SELECT * FROM JSON_TABLE(j, '$' COLUMNS (rn FOR ORDINALITY, v NVARCHAR(20) PATH '$.v' DEFAULT 'missing' ON EMPTY ERROR ON ERROR))",
            "SELECT * FROM JSON_TABLE(j, '$' COLUMNS (NESTED PATH '$.a[*]' COLUMNS (v INT PATH '$.v')) ERROR ON ERROR)",
            "SELECT CAST(x AS ST_GEOMETRY(4326)), CAST(y AS REAL_VECTOR(3)) FROM t",
            "CREATE COLUMN TABLE t (id INT) NO AUTO MERGE UNLOAD PRIORITY 5",
            "CREATE TABLE t (id INT) RECORD COMMIT TIMESTAMP",
            "CREATE TABLE t (id INT, next_id INT GENERATED ALWAYS AS id + 1)",

            "CREATE COLUMN TABLE t (id INT) PARTITION BY RANGE (id) (PARTITION 0 <= VALUES < 100, PARTITION OTHERS)",
            "CREATE COLUMN TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 4 SUBPARTITION BY RANGE (id) (PARTITION VALUES < 100, PARTITION OTHERS DYNAMIC THRESHOLD 1000)",
            "CREATE COLUMN TABLE t (id INT) PARTITION BY RANGE (id) (PARTITION VALUES = 1 PAGE LOADABLE, PARTITION OTHERS DYNAMIC INCREASING INTERVAL 10)",
            "SELECT CURRENT_CONNECTION, CURRENT_SCHEMA, CURRENT_USER, SESSION_USER, SYSUUID FROM DUMMY",
            "SELECT NTH_VALUE(x, 2 ORDER BY y), STRING_AGG(s, ',' ORDER BY id) FROM t",
            "SELECT * FROM t WHERE s NOT LIKE_REGEXPR '^a' FLAG 'i'",
            "SELECT REPLACE_REGEXPR('a' IN s WITH 'b' OCCURRENCE ALL) FROM t",
            "SELECT LOCATE_REGEXPR(AFTER 'a' IN s FROM 2 OCCURRENCE 1 GROUP 0) FROM t",
            "SELECT a, b, SUM(x) FROM t GROUP BY GROUPING SETS BEST -1 LIMIT 2 OFFSET 1 WITH SUBTOTAL WITH BALANCE WITH TOTAL ((a), (b))",
            "SELECT a, SUM(x) FROM t GROUP BY ROLLUP STRUCTURED RESULT WITH OVERVIEW PREFIX '#groups' (a)",
            "SELECT a, SUM(x) FROM t GROUP BY CUBE MULTIPLE RESULTSETS (a)",
            "SELECT * FROM t LIMIT 1 OFFSET 2 TOTAL ROWCOUNT",
            "SELECT LOCALTOUTC(ts, 'EST', 'sap' NULL ON ERROR) FROM t",
            "SELECT UTCTOLOCAL(ts, 'INVALID', 'sap' DEFAULT '2025-01-01 00:00:00' ON ERROR) FROM t",
        ] {
            let ast = hana.parse(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
            assert!(!ast[0].dfs().any(|node| matches!(node, Expression::Raw(_) | Expression::Command(_))), "{sql}");
            let json = serde_json::to_string(&ast).unwrap();
            let decoded: Vec<Expression> = serde_json::from_str(&json).unwrap();
            let output = hana.generate(&decoded[0]).unwrap();
            assert_eq!(output, sql, "AST round trip: {sql}");
            assert_eq!(hana.transpile(sql, DialectType::HANA).unwrap(), vec![sql], "transpile: {sql}");
        }
    }

    #[test]
    fn preserves_quoted_and_qualified_names() {
        let sql = "SELECT \"CURRENT_UTCDATE\", t.CURRENT_UTCTIME, \"ADD_DAYS\"(d, 1) FROM t";
        assert_eq!(transpile(sql, DialectType::HANA, DialectType::HANA), sql);
    }

    #[test]
    fn native_children_participate_in_ast_mutations() {
        use polyglot_sql::ast_transforms::{rename_columns, rename_tables};
        let hana = Dialect::get(DialectType::HANA);
        for sql in [
            "SELECT LOCATE(value, 'a'), JSON_VALUE(payload, '$.n' DEFAULT value ON EMPTY) FROM records",
            "SELECT SUBSTR_REGEXPR('a' IN value), LOCALTOUTC(value, 'UTC') FROM records ORDER BY value",
            "SELECT value, SUM(n) FROM records GROUP BY ROLLUP BEST 1 (value)",
            "SELECT * FROM HIERARCHY(SOURCE (SELECT value AS node_id, parent AS parent_id FROM records))",
            "UPSERT target_table SELECT value FROM source_table",
            "CREATE TABLE records (value INT, next_value INT GENERATED ALWAYS AS value + 1) PARTITION BY HASH (value) PARTITIONS 4",
        ] {
            let ast = hana.parse(sql).unwrap().remove(0);
            assert!(ast.dfs().any(|e| matches!(e, Expression::Column(c) if c.name.name == "value")), "{sql}");
            let renamed = rename_columns(ast.clone(), &[("value".to_owned(), "renamed".to_owned())].into());
            assert!(!renamed.dfs().any(|e| matches!(e, Expression::Column(c) if c.name.name == "value")), "{sql}");
            assert!(renamed.dfs().any(|e| matches!(e, Expression::Column(c) if c.name.name == "renamed")), "{sql}");
            assert!(ast.dfs().any(|e| matches!(e, Expression::Column(c) if c.name.name == "value")), "{sql}");
        }
        let ast = hana
            .parse("UPSERT target_table SELECT value FROM source_table")
            .unwrap()
            .remove(0);
        let renamed = rename_tables(
            ast,
            &[
                ("target_table".to_owned(), "new_target".to_owned()),
                ("source_table".to_owned(), "new_source".to_owned()),
            ]
            .into(),
        );
        assert_eq!(
            hana.generate(&renamed).unwrap(),
            "UPSERT new_target SELECT value FROM new_source"
        );
    }

    #[test]
    fn quoted_reserved_names_and_qualified_calls_are_not_builtins() {
        for (sql, trino) in [
            (
                r#"SELECT "CURRENT_USER", t.CURRENT_SCHEMA, demo."ADD_DAYS"(d, 1) FROM t"#,
                r#"SELECT "CURRENT_USER", t."CURRENT_SCHEMA", demo."ADD_DAYS"(d, 1) FROM t"#,
            ),
            (r#"SELECT "COUNT"(x) FROM t"#, r#"SELECT "COUNT"(x) FROM t"#),
        ] {
            assert_eq!(transpile(sql, DialectType::HANA, DialectType::HANA), sql);
            assert_eq!(transpile(sql, DialectType::HANA, DialectType::Trino), trino);
        }
    }

    #[test]
    fn incoming_queries_generate_native_syntax_or_report_unsupported() {
        assert_eq!(
            transpile("SELECT 1", DialectType::Generic, DialectType::HANA),
            "SELECT 1 FROM DUMMY"
        );
        for (sql, source) in [
            ("SELECT SUBSTRING('abcdef', -2, 2)", DialectType::DuckDB),
            ("SELECT DATE_ADD('day', 1, d) FROM t", DialectType::Trino),
            ("SELECT x ILIKE 'a%' FROM t", DialectType::PostgreSQL),
        ] {
            assert!(
                Dialect::get(source)
                    .transpile(sql, DialectType::HANA)
                    .is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn source_casts_retain_conversion_semantics() {
        assert_eq!(
            transpile("SELECT UNKNOWN", DialectType::HANA, DialectType::HANA),
            "SELECT NULL FROM DUMMY"
        );
        assert_eq!(
            transpile("SELECT UNKNOWN", DialectType::HANA, DialectType::DuckDB),
            "SELECT NULL"
        );
        assert_eq!(
            transpile(
                r#"SELECT "UNKNOWN" FROM t"#,
                DialectType::HANA,
                DialectType::HANA
            ),
            r#"SELECT "UNKNOWN" FROM t"#
        );

        for (sql, expected) in [
            (
                "SELECT CAST(-12.349 AS DECIMAL(5, 2))",
                "SELECT CAST(TRUNC(-12.349, 2) AS DECIMAL(5, 2))",
            ),
            (
                "SELECT CAST(12.99 AS DECIMAL(5))",
                "SELECT CAST(TRUNC(12.99, 0) AS DECIMAL(5, 0))",
            ),
            (
                "SELECT CAST(-12.349 AS DEC(5, 2))",
                "SELECT CAST(TRUNC(-12.349, 2) AS DECIMAL(5, 2))",
            ),
        ] {
            assert_eq!(
                transpile(sql, DialectType::HANA, DialectType::DuckDB),
                expected
            );
        }
        assert_eq!(
            transpile(
                "SELECT CAST(255 AS TINYINT)",
                DialectType::HANA,
                DialectType::Trino
            ),
            "SELECT CAST(255 AS SMALLINT)"
        );
        assert!(Dialect::get(DialectType::HANA)
            .transpile(
                "SELECT CAST(x AS DECIMAL(10, 2)) FROM t",
                DialectType::DuckDB
            )
            .is_err());
    }

    #[test]
    fn rejects_unverified_string_domains_and_volatile_bounds() {
        for sql in [
            "SELECT SUBSTRING(s, -2, 2) FROM t",
            "SELECT SUBSTRING('abcdef', RAND(), 2) FROM DUMMY",
            "SELECT SUBSTRING('a😀b', 2, 1) FROM DUMMY",
            "SELECT SUBSTRING(x'ABCDEF', 1, 2) FROM DUMMY",
            "SELECT LOCATE('a😀b', 'b') FROM DUMMY",
            "SELECT LOCATE(s, 'b') FROM t",
            "SELECT LOCATE('abc', '', 2) FROM DUMMY",
            "SELECT LOCATE('abc', '', 1, 2) FROM DUMMY",
            "SELECT TO_DECIMAL(CAST(s AS NVARCHAR(30)), 10, 2) FROM t",
        ] {
            for target in [
                DialectType::Trino,
                DialectType::DuckDB,
                DialectType::PostgreSQL,
            ] {
                assert!(
                    Dialect::get(DialectType::HANA)
                        .transpile(sql, target)
                        .is_err(),
                    "{sql} -> {target}"
                );
            }
        }
    }

    #[test]
    fn validates_native_precision_and_function_arity() {
        let hana = Dialect::get(DialectType::HANA);
        for sql in [
            "SELECT LOCATE('a') FROM DUMMY",
            "SELECT SUBSTRING('a') FROM DUMMY",
            "SELECT CURRENT_UTCTIMESTAMP(8) FROM DUMMY",
            "SELECT CURRENT_UTCTIMESTAMP(x) FROM t",
            "SELECT CAST(x AS FLOAT(0)) FROM t",
            "SELECT CAST(x AS FLOAT(54)) FROM t",
            "SELECT CAST(x AS DECIMAL(10, 11)) FROM t",
            "SELECT CAST(x AS SMALLDECIMAL(10, 2)) FROM t",
            "SELECT CAST(x AS NVARCHAR(5001)) FROM t",
        ] {
            assert!(hana.parse(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn source_semantics_generate_safe_target_expressions() {
        for target in [
            DialectType::Presto,
            DialectType::Athena,
            DialectType::Dune,
            DialectType::DuckDB,
        ] {
            assert!(Dialect::get(DialectType::HANA)
                .transpile(
                    "SELECT TO_TIMESTAMP(s, 'YYYY-MM-DD HH24:MI:SS') FROM t",
                    target
                )
                .is_err());
        }
        for (sql, target, expected) in [
            (
                "SELECT LOCATE('abcabc', 'bc')",
                DialectType::Trino,
                "SELECT STRPOS('abcabc', 'bc')",
            ),
            (
                "SELECT LOCATE('length in char', '')",
                DialectType::Trino,
                "SELECT STRPOS('length in char', '')",
            ),
            (
                "SELECT TO_TIMESTAMP('2024-01-15 12:30:45', 'YYYY-MM-DD HH24:MI:SS')",
                DialectType::Trino,
                "SELECT CAST(DATE_PARSE('2024-01-15 12:30:45', '%Y-%m-%d %H:%i:%s') AS TIMESTAMP(7))",
            ),
            (
                "SELECT SUBSTRING('abcdef', -2, 2)",
                DialectType::DuckDB,
                "SELECT SUBSTRING('abcdef', 1, 2)",
            ),
            (
                "SELECT TO_DECIMAL(7654321.888888, 10, 3)",
                DialectType::DuckDB,
                "SELECT CAST(TRUNC(7654321.888888, 3) AS DECIMAL(10, 3))",
            ),
            (
                "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD')",
                DialectType::Trino,
                "SELECT CAST(DATE_PARSE('2024-01-15', '%Y-%m-%d') AS DATE)",
            ),
            (
                "SELECT CAST(x AS FLOAT(24)), CAST(y AS FLOAT(53)) FROM t",
                DialectType::Trino,
                "SELECT CAST(x AS REAL), CAST(y AS DOUBLE) FROM t",
            ),
            (
                "SELECT CAST(x AS LONGDATE), CAST(y AS TIME) FROM t",
                DialectType::Trino,
                "SELECT CAST(x AS TIMESTAMP(7)), CAST(y AS TIME(0)) FROM t",
            ),
        ] {
            assert_eq!(transpile(sql, DialectType::HANA, target), expected);
            let ast = Dialect::get(DialectType::HANA).parse(sql).unwrap();
            assert_eq!(Dialect::get(target).generate(&ast[0]).unwrap(), expected);
        }
    }

    #[test]
    fn incompatible_hana_features_fail_in_every_mode() {
        for sql in [
            "SELECT * FROM t FOR JSON",
            "SELECT * FROM t FOR XML",
            "SELECT TO_VARCHAR(1234.5, '9,999.00')",
            "SELECT TO_VARCHAR(d, fmt) FROM t",
            "SELECT TO_TIMESTAMP(s, 'YYYY-MM-DD HH24:MI:SS.FF7') FROM t",
            "SELECT CAST(x AS SMALLDECIMAL) FROM t",
            "SELECT ADD_MONTHS_LAST(d, 1) FROM t",
            "SELECT * FROM t WITH HINT (NO_INLINE)",
            "SELECT * FROM t FOR UPDATE IGNORE LOCKED",
            "SELECT * FROM HIERARCHY(SOURCE t)",
            "SELECT JSON_VALUE(j, '$.a' ERROR ON ERROR) FROM t",
            "SELECT SUBSTR_REGEXPR('a' IN s) FROM t",
            "UPSERT t VALUES (1) WITH PRIMARY KEY",
            "CREATE COLUMN TABLE t (a INT)",
            "CALL demo.proc(1)",
            "SELECT a, SUM(x) FROM t GROUP BY ROLLUP BEST 1 (a)",
            "SELECT * FROM t LIMIT 1 TOTAL ROWCOUNT",
            "SELECT LOCALTOUTC(ts, 'EST', 'sap' NULL ON ERROR) FROM t",
        ] {
            for target in [
                DialectType::Generic,
                DialectType::PostgreSQL,
                DialectType::MySQL,
                DialectType::BigQuery,
                DialectType::Snowflake,
                DialectType::DuckDB,
                DialectType::SQLite,
                DialectType::Hive,
                DialectType::Spark,
                DialectType::Trino,
                DialectType::Presto,
                DialectType::Redshift,
                DialectType::TSQL,
                DialectType::Oracle,
                DialectType::ClickHouse,
                DialectType::Databricks,
                DialectType::Athena,
                DialectType::Teradata,
                DialectType::Doris,
                DialectType::StarRocks,
                DialectType::Materialize,
                DialectType::RisingWave,
                DialectType::SingleStore,
                DialectType::CockroachDB,
                DialectType::TiDB,
                DialectType::Druid,
                DialectType::Solr,
                DialectType::Tableau,
                DialectType::Dune,
                DialectType::Fabric,
                DialectType::Drill,
                DialectType::Dremio,
                DialectType::Exasol,
                DialectType::DataFusion,
            ] {
                let ast = Dialect::get(DialectType::HANA).parse(sql).unwrap();
                assert!(
                    Dialect::get(target).generate(&ast[0]).is_err(),
                    "direct: {sql} -> {target}"
                );
                for level in [
                    UnsupportedLevel::Ignore,
                    UnsupportedLevel::Warn,
                    UnsupportedLevel::Raise,
                    UnsupportedLevel::Immediate,
                ] {
                    assert!(
                        Dialect::get(DialectType::HANA)
                            .transpile_with(
                                sql,
                                target,
                                TranspileOptions::default().with_unsupported_level(level)
                            )
                            .is_err(),
                        "{sql} -> {target}: {level:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn hana_storage_checks_do_not_apply_to_other_sources() {
        let sql = "CREATE GLOBAL TEMPORARY TABLE t (a INT)";
        let output = Dialect::get(DialectType::Teradata)
            .transpile(sql, DialectType::Teradata)
            .unwrap();
        assert!(output[0].contains("GLOBAL TEMPORARY TABLE"));
        assert!(Dialect::get(DialectType::HANA)
            .transpile(sql, DialectType::Teradata)
            .is_err());
    }

    #[test]
    fn unrelated_custom_types_are_unchanged() {
        for name in ["SMALLDECIMAL", "ALPHANUM", "SECONDDATE"] {
            let sql = format!("SELECT CAST(x AS {name}) FROM t");
            assert_eq!(
                transpile(&sql, DialectType::PostgreSQL, DialectType::PostgreSQL),
                sql
            );
        }
    }
}
