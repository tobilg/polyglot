use polyglot_sql::dialects::Dialect;
use polyglot_sql::{parse, DialectType};

fn assert_clickhouse_parse(sql: &str) {
    let parsed = parse(sql, DialectType::ClickHouse).expect("ClickHouse SQL should parse");
    assert_eq!(parsed.len(), 1);
}

fn normalize_clickhouse(sql: &str) -> String {
    let dialect = Dialect::get(DialectType::ClickHouse);
    let parsed = dialect.parse(sql).expect("ClickHouse SQL should parse");
    let statement = parsed.first().expect("expected a statement");
    let transformed = dialect
        .transform(statement.clone())
        .expect("ClickHouse transform should succeed");
    dialect
        .generate_with_source(&transformed, DialectType::ClickHouse)
        .expect("ClickHouse generation should succeed")
}

fn assert_clickhouse_normalized_roundtrip(sql: &str) {
    let first = normalize_clickhouse(sql);
    let second = normalize_clickhouse(&first);

    assert_eq!(second, first);
}

#[test]
fn clickhouse_parses_array_literal_with_nested_subscript_and_cast() {
    assert_clickhouse_parse("SELECT [[1][1]]::Array(UInt32)");
}

#[test]
fn clickhouse_parses_array_literal_elements_with_type_casts() {
    assert_clickhouse_parse("SELECT [1::UInt32, 2::UInt32]::Array(UInt64)");
    assert_clickhouse_parse("SELECT [[1, 2]::Array(UInt32), [3]]::Array(Array(UInt64))");
}

#[test]
fn clickhouse_parses_nested_array_literal_subscript_chain() {
    assert_clickhouse_parse(
        "SELECT [[10, 2, 13, 15][toNullable(toLowCardinality(1))]][materialize(toLowCardinality(1))]",
    );
}

#[test]
fn clickhouse_parses_array_literal_followed_by_multiple_casts() {
    assert_clickhouse_parse("SELECT [[[1, 2, 3]::Array(UInt64)::Dynamic]]");
}

#[test]
fn clickhouse_recovers_terminal_unterminated_string_probe() {
    assert_eq!(
        normalize_clickhouse("select 'select json"),
        "SELECT 'select json'"
    );
}

#[test]
fn clickhouse_normalizes_ctas_without_repeated_parentheses() {
    let sql = "CREATE TABLE x ENGINE=Memory AS (SELECT 1)";
    let first = normalize_clickhouse(sql);
    let second = normalize_clickhouse(&first);

    assert_eq!(first, "CREATE TABLE x ENGINE=Memory AS (SELECT 1)");
    assert_eq!(second, first);
}

#[test]
fn clickhouse_preserves_table_function_ctas_source() {
    assert_eq!(
        normalize_clickhouse("CREATE TABLE t AS numbers(5)"),
        "CREATE TABLE t AS numbers(5)"
    );
    assert_eq!(
        normalize_clickhouse("CREATE TABLE t (n UInt64) AS numbers(1)"),
        "CREATE TABLE t (n UInt64) AS numbers(1)"
    );
}

#[test]
fn clickhouse_bare_with_totals_stays_parseable() {
    assert_eq!(
        normalize_clickhouse("SELECT count() WITH TOTALS"),
        "SELECT count() WITH TOTALS"
    );
}

#[test]
fn clickhouse_preserves_quoted_dotted_array_join_alias() {
    assert_eq!(
        normalize_clickhouse("SELECT x FROM t ARRAY JOIN s.a AS `s.a`"),
        "SELECT x FROM t ARRAY JOIN s.a AS \"s.a\""
    );
}

#[test]
fn clickhouse_preserves_quoted_database_names() {
    assert_eq!(
        normalize_clickhouse("CREATE DATABASE `this.is.a.valid.databasename`"),
        "CREATE DATABASE \"this.is.a.valid.databasename\""
    );
}

#[test]
fn clickhouse_preserves_incomplete_insert_probe_as_command() {
    assert_eq!(normalize_clickhouse("INSERT INTO t0"), "INSERT INTO t0");
}

#[test]
fn clickhouse_recovers_missing_terminal_rparen_for_extracted_subquery() {
    assert_eq!(
        normalize_clickhouse("SELECT count() FROM (SELECT 1"),
        "SELECT count() FROM (SELECT 1)"
    );
}

#[test]
fn clickhouse_preserves_sample_clause_keyword() {
    assert_eq!(
        normalize_clickhouse("SELECT count() FROM t SAMPLE 0.1"),
        "SELECT count() FROM t SAMPLE 0.1"
    );
}

#[test]
fn clickhouse_ttl_set_clause_stays_stable() {
    let sql = "CREATE TABLE t (key Int, date Date, value String) ENGINE = MergeTree() ORDER BY key TTL date + INTERVAL 2 MONTH GROUP BY key SET value = argMax(value, date)";

    assert_clickhouse_normalized_roundtrip(sql);
    assert_eq!(
        normalize_clickhouse(sql),
        "CREATE TABLE t (key Int32, date DATE, value String) ENGINE=MergeTree() ORDER BY key TTL date + INTERVAL '2' MONTH GROUP BY key SET value = argMax(value, date)"
    );
}

#[test]
fn clickhouse_preserves_enum8_custom_type_name() {
    assert_eq!(
        normalize_clickhouse("SELECT CAST(x AS Enum8('hello' = -123, 'world'))"),
        "SELECT CAST(x AS Enum8('hello' = -123, 'world'))"
    );
}

#[test]
fn clickhouse_enum8_custom_type_suffix_escapes_control_characters() {
    let sql = "SELECT CAST(x AS Enum8('a\tb' = 1, 'c\\\\d' = 2))";

    assert_clickhouse_normalized_roundtrip(sql);
}

#[test]
fn clickhouse_preserves_partial_with_probe_as_command() {
    assert_eq!(normalize_clickhouse("WITH build AS ("), "WITH build AS (");
}

#[test]
fn clickhouse_preserves_alter_update_mutation_as_command() {
    assert_eq!(
        normalize_clickhouse("ALTER TABLE tab UPDATE str = 'I am not inverted' WHERE 1"),
        "ALTER TABLE tab UPDATE str = 'I am not inverted' WHERE 1"
    );
}

#[test]
fn clickhouse_parses_with_trailing_comma_before_select() {
    assert_clickhouse_parse("WITH 1 AS a, SELECT a");
    assert_clickhouse_parse("WITH 1 AS a, 2 AS b, SELECT a + b");
    assert_clickhouse_parse("WITH (SELECT 1) AS a, SELECT a");
}

#[test]
fn clickhouse_parses_standard_overlay_syntax() {
    assert_clickhouse_parse("SELECT OVERLAY('Hello World' PLACING 'SQL' FROM 7 FOR 5)");
    assert_clickhouse_parse("SELECT OVERLAY('abcdef' PLACING 'XY' FROM 3)");
    assert_clickhouse_parse("SELECT overlay('hello', 'world', 2, 3, 'extra')");
    assert_clickhouse_parse("SELECT overlayUTF8('Spark SQL和CH' PLACING '_' FROM 6)");
    assert_clickhouse_parse("SELECT OVERLAY('abcdef' PLACING 'XY', 3)");
    assert_clickhouse_parse("SELECT overlay('abcdef', 'XY' FROM 3)");
    assert_clickhouse_normalized_roundtrip("SELECT OVERLAY('abcdef' PLACING 'XY', 3)");
    assert_eq!(
        normalize_clickhouse("SELECT OVERLAY('abcdef', 'XY', 3)"),
        "SELECT OVERLAY('abcdef', 'XY', 3)"
    );
}

#[test]
fn clickhouse_parses_updated_corpus_ddl_shapes() {
    assert_clickhouse_parse(
        "CREATE TABLE test_merge (a Int32, b String) AS merge(currentDatabase(), '^test_[ab]$')",
    );
    assert_clickhouse_parse(
        "CREATE TABLE t TO INNER UUID '00000000-0000-0000-0000-000000000001' (id UInt32) ORDER BY id",
    );
    assert_clickhouse_parse(
        "CREATE TABLE test_idx_settings_cov (id UInt64, PROJECTION region_proj INDEX region TYPE basic WITH SETTINGS (index_granularity = 2)) ENGINE = MergeTree ORDER BY id",
    );
    assert_clickhouse_normalized_roundtrip(
        "CREATE TABLE mt_commit_order_idx(a UInt64, b UInt64, PROJECTION commit_order INDEX b TYPE commit_order) ENGINE = MergeTree ORDER BY a",
    );
    assert_clickhouse_parse(
        "CREATE TABLE t_constraint_trans (a Int64, b Int64, c Int64, d Int32, CONSTRAINT c1 ASSUME (a = b) AND (c = d), CONSTRAINT c2 ASSUME b = c) ENGINE = TinyLog",
    );
}

#[test]
fn clickhouse_parses_newer_select_tolerance_shapes() {
    assert_clickhouse_parse(
        "SELECT toUInt32OrZero(extract(last_headers['strict-transport-security'], 'max-age=(\\d+)')) AS hsts_max_age FROM t",
    );
    assert_clickhouse_parse("SELECT * FROM t1 NATURAL CROSS JOIN t2");
    assert_clickhouse_parse(
        "SELECT l.s AS s FROM t_l AS l LEFT JOIN t_r AS r ON r.s = l.s ORDER BY l.s DESC COLLATE 'en' LIMIT 10",
    );
    assert_clickhouse_parse(
        "ALTER TABLE t0 DELETE IN PARTITION tuple() WHERE equals(c0, 1) SETTINGS mutations_sync = 2",
    );
    assert_clickhouse_parse("WITH cte AS (SELECT number FROM numbers(3)), SELECT * FROM cte");
    assert_clickhouse_parse("WITH 1 AS a,, SELECT a");
    assert_clickhouse_parse(
        "SELECT 1 FROM (SELECT 1 FROM (SELECT 1 PREWHERE (SELECT 1 FROM VALUES(NULL) AS t0d2) QUALIFY (SELECT 1 FROM VALUES(NULL) AS t0d2)))",
    );
    assert_clickhouse_parse(
        "SELECT count() FROM (SELECT c0 FROM ((SELECT 'a') EXCEPT ALL SELECT (1, 2))(c0)) AS t0 WHERE t0.c0 ILIKE t0.c0 = true",
    );
    assert_clickhouse_parse(
        "SELECT accurateCastOrNull((SELECT modulo(intDiv(1, 1), NULL), -1 LIMIT -1), 'Point') AS r",
    );
    assert_clickhouse_parse(
        "SELECT count() FROM t_constraint_corr WHERE exists((SELECT toUInt8(1) PREWHERE murmurHash3_64(xxHash32(a))))",
    );
    assert_clickhouse_parse(
        "SELECT * FROM (WITH t AS MATERIALIZED (SELECT a + number AS x FROM numbers(65536)) SELECT * FROM (SELECT NULL AS a, x FROM t))",
    );
    assert_clickhouse_normalized_roundtrip(
        "WITH interval AS (SELECT 1 AS val), t0_renamed AS (SELECT * FROM t0_renamed) SELECT TOP 10 *, t0_renamed.*, *, t0_renamed.* WHERE -t0_renamed.v1 GROUP BY t0_renamed.v1, t0_renamed.v2, t0_renamed.v3",
    );
    assert_clickhouse_normalized_roundtrip("SELECT 1 == SOME (SELECT number FROM numbers(10))");
}

#[test]
fn clickhouse_foreign_typed_nulls_have_consistent_nullable_scalar_types() {
    let sql = "SELECT CAST(NULL AS VARCHAR) AS s, CAST(NULL AS BIGINT) AS i, CAST(NULL AS DATE) AS d, CAST(NULL AS DOUBLE) AS f, CAST(NULL AS BOOLEAN) AS b";
    for source in [
        DialectType::Generic,
        DialectType::DuckDB,
        DialectType::PostgreSQL,
        DialectType::Trino,
    ] {
        let output = polyglot_sql::transpile(sql, source, DialectType::ClickHouse).unwrap();
        assert_eq!(output[0], "SELECT CAST(NULL AS Nullable(String)) AS s, CAST(NULL AS Nullable(Int64)) AS i, CAST(NULL AS Nullable(DATE)) AS d, CAST(NULL AS Nullable(Float64)) AS f, CAST(NULL AS Nullable(Bool)) AS b", "{source:?}");
    }
}

#[test]
fn clickhouse_nullable_scalars_include_parameters_and_nullable_expressions() {
    for (dtype, expected) in [
        ("VARCHAR(20)", "String"),
        ("CHAR(3)", "String"),
        ("DECIMAL(10, 2)", "Decimal(10, 2)"),
        ("UUID", "UUID"),
        ("DOUBLE PRECISION", "Float64"),
        ("DATE32", "Date32"),
    ] {
        let sql = format!("SELECT CAST(a AS {dtype}) FROM t");
        let output =
            polyglot_sql::transpile(&sql, DialectType::Generic, DialectType::ClickHouse).unwrap();
        assert_eq!(
            output[0],
            format!("SELECT CAST(a AS Nullable({expected})) FROM t")
        );
    }
}

#[test]
fn clickhouse_keeps_native_nullability_and_map_key_rules() {
    for sql in [
        "SELECT CAST(NULL AS Nullable(String))",
        "SELECT CAST(NULL AS LowCardinality(Nullable(String)))",
        "SELECT CAST(1 AS Int64)",
        "SELECT CAST('2025-01-01' AS Date)",
    ] {
        let output =
            polyglot_sql::transpile(sql, DialectType::ClickHouse, DialectType::ClickHouse).unwrap();
        assert!(!output[0].contains("Nullable(Nullable("), "{}", output[0]);
        if sql.contains("CAST(1") || sql.contains("2025") {
            assert!(!output[0].contains("Nullable"), "{}", output[0]);
        }
        assert_eq!(
            polyglot_sql::transpile(&output[0], DialectType::ClickHouse, DialectType::ClickHouse)
                .unwrap(),
            output
        );
    }
    let output = polyglot_sql::transpile(
        "SELECT CAST(x AS MAP(VARCHAR, BIGINT)) FROM t",
        DialectType::DuckDB,
        DialectType::ClickHouse,
    )
    .unwrap();
    assert!(
        output[0].contains("Map(String, Nullable(Int64))"),
        "{}",
        output[0]
    );
    for dtype in ["INT[]", "MAP(VARCHAR, BIGINT)"] {
        let error = Dialect::get(DialectType::DuckDB)
            .transpile_with(
                &format!("SELECT CAST(NULL AS {dtype})"),
                DialectType::ClickHouse,
                polyglot_sql::TranspileOptions::strict(),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("cannot represent a NULL"),
            "{error}"
        );
    }
}

#[test]
fn clickhouse_native_generator_keeps_explicit_nullability_without_source_metadata() {
    let dialect = Dialect::get(DialectType::ClickHouse);
    for sql in [
        "CAST(1 AS Bool)",
        "CAST(x AS DATETIME)",
        "CAST(x AS DECIMAL(38, 2))",
        "CAST(x AS UInt64)",
        "CAST(x AS Date)",
    ] {
        let expression = dialect.parse(sql).unwrap().remove(0);
        let expression = dialect.transform(expression).unwrap();
        let generated = dialect.generate(&expression).unwrap();
        assert!(!generated.contains("Nullable"), "{generated}");
    }
}

#[test]
fn clickhouse_foreign_wide_and_unsigned_typed_nulls_are_nullable() {
    for (source_type, target_type) in [
        ("HUGEINT", "Int128"),
        ("UTINYINT", "UInt8"),
        ("USMALLINT", "UInt16"),
        ("UINTEGER", "UInt32"),
        ("UBIGINT", "UInt64"),
        ("UHUGEINT", "UInt128"),
    ] {
        let output = polyglot_sql::transpile(
            &format!("SELECT CAST(NULL AS {source_type})"),
            DialectType::DuckDB,
            DialectType::ClickHouse,
        )
        .unwrap();
        assert_eq!(
            output[0],
            format!("SELECT CAST(NULL AS Nullable({target_type}))")
        );
    }
}

#[test]
fn clickhouse_native_string_alias_casts_keep_stable_nullability() {
    for sql in [
        "SELECT 'key'::Varchar(255)",
        "SELECT ['y']::Array(Char)",
        "SELECT ['y']::Array(Varchar(255))",
    ] {
        assert_clickhouse_normalized_roundtrip(sql);
        let output =
            polyglot_sql::transpile(sql, DialectType::ClickHouse, DialectType::ClickHouse).unwrap();
        assert!(!output[0].contains("Nullable"), "{}", output[0]);
    }
}
