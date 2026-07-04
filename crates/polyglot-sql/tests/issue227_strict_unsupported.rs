use polyglot_sql::{Dialect, DialectType, TranspileOptions, UnsupportedLevel};

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
            "SELECT JSONB_BUILD_OBJECT('k', v) FROM t",
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
        "SELECT * FROM t, LATERAL (SELECT 1 AS x) AS s",
        DialectType::PostgreSQL,
        DialectType::Fabric,
        UnsupportedLevel::Raise,
    )
    .expect_err("strict Fabric transpile should reject remaining LATERAL");

    assert!(err.to_string().contains("LATERAL"));
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
fn strict_transpile_rejects_regex_predicates_for_tsql_targets() {
    let sqls = [
        "SELECT 1 FROM t WHERE p_brand SIMILAR TO 'Brand#[1-3][0-9]'",
        "SELECT 1 FROM t WHERE c_phone ~ '^1[0-9]'",
        "SELECT 1 FROM t WHERE c_phone !~ '^1[0-9]'",
        "SELECT 1 FROM t WHERE c_phone ~* '^1[0-9]'",
        "SELECT 1 FROM t WHERE c_phone !~* '^1[0-9]'",
        "SELECT 1 FROM t WHERE REGEXP_LIKE(c_phone, '^1[0-9]')",
    ];

    for sql in sqls {
        for write in [DialectType::Fabric, DialectType::TSQL] {
            let err =
                transpile_with_level(sql, DialectType::PostgreSQL, write, UnsupportedLevel::Raise)
                    .expect_err("strict TSQL/Fabric transpile should reject regex predicates");

            assert!(
                err.to_string().contains("regular expression predicates"),
                "unexpected error for PostgreSQL -> {write:?}: {err}"
            );
        }
    }
}

#[test]
fn strict_transpile_rejects_postgres_only_functions() {
    let err = transpile_with_level(
        "SELECT JSONB_BUILD_OBJECT('k', v), TO_TSVECTOR(body) FROM docs",
        DialectType::PostgreSQL,
        DialectType::Fabric,
        UnsupportedLevel::Raise,
    )
    .expect_err("strict Fabric transpile should reject PostgreSQL-only functions");

    let message = err.to_string();
    assert!(message.contains("JSONB_BUILD_OBJECT"));
    assert!(message.contains("TO_TSVECTOR"));
}

#[test]
fn strict_transpile_honors_max_unsupported() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT ARRAY_AGG(x), JSONB_BUILD_OBJECT('k', v), TO_TSVECTOR(body) FROM docs",
            DialectType::Fabric,
            TranspileOptions::strict().with_max_unsupported(2),
        )
        .expect_err("strict Fabric transpile should report unsupported diagnostics");

    let message = err.to_string();
    assert!(message.contains("ARRAY_AGG"));
    assert!(message.contains("JSONB_BUILD_OBJECT"));
    assert!(message.contains("... and 1 more"));
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
