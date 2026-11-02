use polyglot_sql::{dialects::Dialect, DialectType};

#[test]
fn trino_inline_routines_preserve_scopes_and_control_flow() {
    // https://trino.io/docs/current/udf/sql/examples.html
    let sql = "WITH FUNCTION f(n INTEGER) RETURNS INTEGER BEGIN DECLARE x INTEGER DEFAULT 0; top: WHILE x < n DO IF x = 2 THEN RETURN CASE WHEN n > 3 THEN x ELSE n END; ELSE SET x = x + 1; END IF; END WHILE; RETURN x; END WITH t AS (SELECT 4 AS n) SELECT F(n) FROM t";
    assert_eq!(normalize(sql, DialectType::Trino), sql);
    assert!(matches!(
        polyglot_sql::parse_one(sql, DialectType::Trino).unwrap(),
        polyglot_sql::Expression::WithFunction(_)
    ));
    assert!(Dialect::get(DialectType::Trino)
        .transpile(sql, DialectType::PostgreSQL)
        .is_err());
    for invalid in [
        "WITH FUNCTION f() RETURNS INTEGER BEGIN RETURN 1 END SELECT F()",
        "WITH FUNCTION f() RETURNS INTEGER BEGIN WHILE TRUE RETURN 1; END WHILE; END SELECT F()",
        "WITH FUNCTION f() RETURNS INTEGER BEGIN RETURN 1; SELECT F()",
    ] {
        assert!(
            polyglot_sql::parse_one(invalid, DialectType::Trino).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn trino_temporal_literals_keep_precision_and_zone() {
    for (input, expected) in [
        (
            "TIMESTAMP '2026-03-01 00:00:00.123456789'",
            "CAST('2026-03-01 00:00:00.123456789' AS TIMESTAMP(9))",
        ),
        (
            "TIME '01:02:03.456 -08:00'",
            "CAST('01:02:03.456 -08:00' AS TIME(3) WITH TIME ZONE)",
        ),
    ] {
        assert_eq!(normalize(input, DialectType::Trino), expected);
    }
}

#[test]
fn databricks_function_body_and_replacement_keys_survive_round_trip() {
    for sql in [
        "CREATE FUNCTION f(col STRING) RETURNS STRING DETERMINISTIC RETURN col",
        "CREATE FUNCTION f(col STRING) RETURNS STRING NOT DETERMINISTIC RETURN col",
        "INSERT INTO target REPLACE USING (a, b) SELECT a, b FROM source",
    ] {
        assert_eq!(normalize(sql, DialectType::Databricks), sql);
    }
    assert!(Dialect::get(DialectType::Databricks)
        .transpile(
            "INSERT INTO t REPLACE USING (a) SELECT a FROM s",
            DialectType::PostgreSQL
        )
        .is_err());
}

#[test]
fn mysql_ddl_keeps_drop_scope_and_constraint_options() {
    for sql in [
        "DROP TEMPORARY TABLE IF EXISTS db.t1, t2 CASCADE",
        "ALTER TABLE t DROP COLUMN b RESTRICT",
        "CREATE TABLE t (a INT, CONSTRAINT c FOREIGN KEY fk (a) REFERENCES p (a))",
        "CREATE TABLE t (a INT, PRIMARY KEY (a) USING BTREE KEY_BLOCK_SIZE = 8 COMMENT 'c')",
    ] {
        assert_eq!(normalize(sql, DialectType::MySQL), sql);
    }
    assert_eq!(
        normalize("INSERT INTO value (value) VALUE (1)", DialectType::MySQL),
        "INSERT INTO value (value) VALUES (1)"
    );
    assert_eq!(
        normalize(
            "CREATE TABLE t (a INT CONSTRAINT CHECK (a > 0))",
            DialectType::MySQL
        ),
        "CREATE TABLE t (a INT CHECK (a > 0))"
    );
    assert_eq!(
        normalize("SELECT TIMESTAMPADD(SECOND, n, d)", DialectType::MySQL),
        "SELECT DATE_ADD(d, INTERVAL n SECOND)"
    );
}

fn normalize(sql: &str, dialect: DialectType) -> String {
    Dialect::get(dialect)
        .transpile(sql, dialect)
        .unwrap()
        .join("; ")
}

#[test]
fn duckdb_nearest_joins_preserve_metric_and_join_type() {
    // https://duckdb.org/docs/2.0/sql/query_syntax/from#nearest-joins
    for sql in [
        "SELECT * FROM a LEFT JOIN b APPROX NEAREST 2 BY SIMILARITY ARRAY_COSINE_SIMILARITY(a.x, b.x)",
        "SELECT * FROM a JOIN BY (TYPE MARK) b ON a.id = b.id",
        "SELECT * FROM t USING SAMPLE RESERVOIR (50 ROWS) REPEATABLE (100) ORDER BY x LIMIT 2",
    ] { assert_eq!(normalize(sql, DialectType::DuckDB), sql); }
    assert!(Dialect::get(DialectType::DuckDB)
        .transpile(
            "SELECT * FROM a JOIN b NEAREST 2 BY DISTANCE abs(a.x - b.x)",
            DialectType::PostgreSQL
        )
        .is_err());
    assert_eq!(
        normalize(
            "SELECT * FROM a nearest JOIN b approx ON nearest.id = approx.id",
            DialectType::DuckDB
        ),
        "SELECT * FROM a AS nearest JOIN b AS approx ON nearest.id = approx.id"
    );
}

#[test]
fn clickhouse_pipe_stages_keep_scope_and_round_trip() {
    // https://clickhouse.com/blog/pipelined-sql-26.8
    for sql in [
        "FROM orders |> WHERE amount > 100 |> SELECT customer |> ORDER BY customer |> LIMIT 2 OFFSET 1",
        "WITH big AS (FROM orders |> WHERE amount >= 250) FROM big |> SELECT customer",
        "FROM orders |> EXTEND amount * 2 AS doubled |> SET amount = doubled |> DROP doubled |> DISTINCT",
        "FROM orders |> AGGREGATE sum(amount) AS total GROUP BY customer |> WHERE total > 200",
        "FROM orders |> AS o |> JOIN customers AS c ON o.customer = c.id |> SELECT c.name",
        "FROM orders |> SELECT customer |> UNION ALL (SELECT 'bob') |> DISTINCT",
        "CREATE VIEW v AS FROM orders |> WHERE amount > 100 |> SELECT customer",
        "SELECT count() FROM view(FROM orders |> LIMIT 2 SETTINGS max_threads = 1)",
    ] {
        let first = normalize(sql, DialectType::ClickHouse);
        assert!(first.contains(" |> "), "{sql}: {first}");
        assert_eq!(normalize(&first, DialectType::ClickHouse), first, "{sql}");
        assert!(Dialect::get(DialectType::ClickHouse).transpile(sql, DialectType::PostgreSQL).is_err());
    }
    assert!(polyglot_sql::parse_one("FROM t |> UNKNOWN x", DialectType::ClickHouse).is_err());
}

#[test]
fn clickhouse_stream_watermark_and_negative_argument_checks() {
    let sql = "SELECT ts FROM t STREAM BOUNDED WATERMARK FOR ts AS ts - toIntervalSecond(5) WHERE ts > now()";
    assert_eq!(normalize(sql, DialectType::ClickHouse), sql);
    for sql in [
        "SELECT nth_value(x) OVER ()",
        "SELECT nth_value(x, 1, 2) OVER ()",
        "CREATE FUNCTION bad AS (x) -> arrayMap(y.z -> y.z + x, [1])",
    ] {
        assert!(
            polyglot_sql::parse_one(sql, DialectType::ClickHouse).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn literal_types_and_single_store_collations_are_preserved() {
    // PostgreSQL's quoted internal char type must never become SQL CHAR.
    assert_eq!(
        normalize("65::\"char\"", DialectType::PostgreSQL),
        "CAST(65 AS \"char\")"
    );
    for sql in ["SELECT JSON '{}'", "SELECT PARSE_JSON('{}')"] {
        assert_eq!(normalize(sql, DialectType::BigQuery), sql);
    }
    let sql = "SELECT name :> LONGTEXT COLLATE 'utf8mb4_bin' FROM users";
    assert_eq!(normalize(sql, DialectType::SingleStore), sql);
    assert_eq!(
        normalize(sql, DialectType::SingleStore),
        normalize(
            &normalize(sql, DialectType::SingleStore),
            DialectType::SingleStore
        )
    );
}

#[test]
fn duckdb_2_regex_lambdas_and_fractional_intervals() {
    // https://duckdb.org/docs/2.0/operations_manual/migration
    assert_eq!(
        normalize("SELECT x ~ y", DialectType::DuckDB),
        "SELECT REGEXP_MATCHES(x, y)"
    );
    assert_eq!(
        normalize(
            "SELECT list_transform([1, 2], x -> x + 1)",
            DialectType::DuckDB
        ),
        "SELECT LIST_TRANSFORM([1, 2], LAMBDA x : x + 1)"
    );
    // https://duckdb.org/docs/2.0/sql/data_types/interval
    assert_eq!(
        normalize("SELECT INTERVAL '1.5 DAY'", DialectType::DuckDB),
        "SELECT INTERVAL '1.5 DAY'"
    );
    assert_eq!(
        normalize("SELECT ([1, 2, 3])[:-:-1]", DialectType::DuckDB),
        "SELECT ([1, 2, 3])[:-:-1]"
    );
    assert_eq!(
        normalize("SELECT a -> 'it''s' FROM t", DialectType::DuckDB),
        "SELECT a -> 'it''s' FROM t"
    );
}

#[test]
fn duckdb_explicit_sort_options_survive_session_defaults() {
    // default_order and default_null_order are configurable, so these arguments
    // cannot be removed even when they equal the factory defaults.
    for sql in [
        "SELECT LIST_SORT(x, 'ASC', 'NULLS LAST')",
        "SELECT ARRAY_SORT(x, 'ASC', 'bad')",
    ] {
        assert_eq!(normalize(sql, DialectType::DuckDB), sql);
    }
}

#[test]
fn doris_match_predicates_preserve_analyzers_and_boolean_structure() {
    // https://doris.apache.org/docs/4.x/table-design/index/inverted-index/search-operators/
    for sql in [
        "SELECT * FROM t WHERE c MATCH_PHRASE 'word1 word2 ~3'",
        "SELECT * FROM t WHERE NOT c MATCH_ANY 'x' AND d MATCH_ALL 'y' OR e = 1",
        "SELECT * FROM t WHERE c MATCH 'hello' USING ANALYZER std_analyzer",
        "SELECT * FROM t WHERE c MATCH_REGEXP '^key_word.*'",
    ] {
        assert_eq!(normalize(sql, DialectType::Doris), sql);
        assert!(Dialect::get(DialectType::Doris)
            .transpile(sql, DialectType::PostgreSQL)
            .is_err());
    }
    assert_eq!(
        normalize("SELECT c AS match_any FROM t", DialectType::Doris),
        "SELECT c AS `match_any` FROM t"
    );
    assert_eq!(
        normalize("DROP TABLE my_table FORCE", DialectType::Doris),
        "DROP TABLE my_table FORCE"
    );
}

#[test]
fn clickhouse_new_modifiers_preserve_meaning() {
    for sql in [
        "UPDATE t ON CLUSTER cluster_name SET x = 1 WHERE id = 2",
        "SELECT x FROM t ORDER BY x LIMIT 1 OFFSET 1 WITH TIES",
        "SELECT TRY_CAST('invalid' AS Int32)",
        "SELECT max(c) FROM t ORDER BY max(c) ASC COLLATE 'en'",
        "SELECT bitShiftRight(bitShiftLeft(x, 192) + bitShiftLeft(y, 128), 64)",
        "SELECT {db: Identifier}.{table: Identifier}.x FROM {db: Identifier}.{table: Identifier}",
    ] {
        let first = normalize(sql, DialectType::ClickHouse);
        assert_eq!(normalize(&first, DialectType::ClickHouse), first);
        if sql.contains("WITH TIES") {
            assert!(first.contains("WITH TIES"));
        }
        if sql.contains("TRY_CAST") {
            assert!(first.contains("TRY_CAST"));
        }
        if sql.contains("ON CLUSTER") {
            assert!(first.contains("ON CLUSTER cluster_name"));
        }
        assert!(!first.contains("AggFunc"));
    }
}

#[test]
fn sqlite_is_precedence_and_concat_grouping_survive_generation() {
    // https://sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
    assert_eq!(
        normalize("NULL IS NULL < FALSE", DialectType::SQLite),
        "NULL IS (NULL < FALSE)"
    );
    assert_eq!(
        normalize("SELECT 0xFF, 1 + (x || y), a REGEXP b", DialectType::SQLite),
        "SELECT 255, 1 + (x || y), a REGEXP b"
    );
    let sql = "INSERT INTO t (a) VALUES (1) ON CONFLICT(a) DO UPDATE SET a = 2 WHERE a > 0";
    assert_eq!(normalize(sql, DialectType::SQLite), sql);
}

#[test]
fn postgres_internal_char_json_keys_and_ddl_options_survive() {
    // https://www.postgresql.org/docs/current/functions-json.html
    for sql in [
        "SELECT CAST(65 AS \"char\"), doc -> idx, doc -> -1",
        "ALTER TABLE t ALTER COLUMN a SET STATISTICS 100",
        "ALTER TABLE t ALTER COLUMN a RESTART WITH 42",
        "CREATE VIEW v WITH (security_barrier=TRUE) AS SELECT * FROM t WITH LOCAL CHECK OPTION",
        "ANALYZE t(a), u (b)",
    ] {
        assert_eq!(normalize(sql, DialectType::PostgreSQL), sql);
    }
}

#[test]
fn oracle_alternative_quotes_and_spark_unicode_preserve_contents() {
    // https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Literals.html
    assert_eq!(
        normalize("SELECT q'[it's fine]' FROM dual", DialectType::Oracle),
        "SELECT 'it''s fine' FROM dual"
    );
    assert_eq!(
        normalize(r"SELECT q'!a\b!' FROM dual", DialectType::Oracle),
        r"SELECT 'a\b' FROM dual"
    );
    assert_eq!(
        normalize(r"SELECT '\u96EA\U0001F600'", DialectType::Spark),
        "SELECT '雪😀'"
    );
}

#[test]
fn mysql_character_sets_and_alter_options_are_retained() {
    for sql in [
        "ALTER TABLE t COMMENT='hi'",
        "ALTER TABLE t ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8",
        "ALTER TABLE t ADD COLUMN c INT SECONDARY_ENGINE_ATTRIBUTE='{}', ADD COLUMN z INT",
        "ALTER TABLE t DROP CHECK c",
        "ALTER TABLE t FORCE",
    ] {
        assert_eq!(normalize(sql, DialectType::MySQL), sql);
    }
    assert_eq!(
        normalize(
            "SELECT CONVERT(x, CHAR(5) CHARACTER SET utf8mb4)",
            DialectType::MySQL
        ),
        "SELECT CAST(x AS CHAR(5) CHARACTER SET utf8mb4)"
    );
}

#[test]
fn tsql_json_query_does_not_gain_a_scalar_fallback() {
    // JSON_QUERY returns objects/arrays; JSON_VALUE handles scalars.
    // https://learn.microsoft.com/en-us/sql/t-sql/functions/json-query-transact-sql
    let sql = "SELECT JSON_QUERY(doc, '$.x'), JSON_VALUE(doc, '$.y')";
    assert_eq!(normalize(sql, DialectType::TSQL), sql);
    let sql = "SELECT ISNULL(JSON_QUERY(doc, '$.x'), JSON_VALUE(alternate_doc, '$.y'))";
    assert_eq!(normalize(sql, DialectType::TSQL), sql);
    let translated = Dialect::get(DialectType::TSQL)
        .transpile(sql, DialectType::PostgreSQL)
        .unwrap()
        .join("; ");
    assert!(
        translated.contains("alternate_doc"),
        "fallback was lost: {translated}"
    );
}

#[test]
fn tsql_set_pagination_keeps_output_names_in_scope() {
    // https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql
    for sql in [
        "SELECT x.a FROM x UNION ALL SELECT y.a FROM y ORDER BY x.a OFFSET 1 ROWS",
        "SELECT a, a FROM x UNION ALL SELECT a, a FROM y ORDER BY 1 OFFSET 1 ROWS",
        "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
    ] {
        assert_eq!(normalize(sql, DialectType::TSQL), sql);
    }
    assert_eq!(
        normalize(
            "SELECT * FROM dbo.t AS s /* alias */ (NOLOCK /* hint */)",
            DialectType::TSQL
        ),
        "SELECT * FROM dbo.t AS s /* alias */ WITH (NOLOCK /* hint */)"
    );
}

#[test]
fn fabric_time_zone_conversion_keeps_the_input_offset_type() {
    let sql = "SELECT CAST('2026-01-01' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'UTC'";
    assert_eq!(
        normalize(sql, DialectType::Fabric),
        "SELECT CAST(CAST('2026-01-01' AS DATETIMEOFFSET(6)) AT TIME ZONE 'UTC' AS DATETIME2(6))"
    );
}

#[test]
fn hive_multiple_inserts_retain_shared_joins_laterals_and_branches() {
    // https://hive.apache.org/docs/latest/language/languagemanual-dml/
    for sql in [
        "FROM x INSERT OVERWRITE TABLE a SELECT k INSERT INTO b SELECT v WHERE k > 0",
        "FROM a JOIN b ON a.k = b.k INSERT OVERWRITE TABLE t SELECT a.k INSERT INTO u SELECT b.k",
        "FROM x LATERAL VIEW EXPLODE(arr) a AS c INSERT INTO t SELECT c INSERT INTO u SELECT c + 1",
        "WITH c AS (SELECT 1 AS k) FROM c INSERT OVERWRITE TABLE a SELECT k INSERT INTO b SELECT k",
    ] {
        assert_eq!(normalize(sql, DialectType::Hive), sql);
        assert!(Dialect::get(DialectType::Hive)
            .transpile(sql, DialectType::PostgreSQL)
            .is_err());
    }
    assert_eq!(
        normalize(
            "ALTER TABLE t REPLACE COLUMNS (a INT) CASCADE",
            DialectType::Hive
        ),
        "ALTER TABLE t REPLACE COLUMNS (a INT) CASCADE"
    );
}

#[test]
fn athena_quoting_switches_between_statements_and_ctas() {
    // https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html
    let dialect = Dialect::get(DialectType::Athena);
    let sql =
        r#"CREATE DATABASE foo COMMENT "a\"b"; SELECT '\'; CREATE TABLE "t" AS SELECT '\' AS x"#;
    assert_eq!(
        dialect.transpile(sql, DialectType::Athena).unwrap(),
        vec![
            "CREATE DATABASE foo COMMENT 'a\"b'",
            r"SELECT '\'",
            r#"CREATE TABLE "t" AS SELECT '\' AS x"#,
        ]
    );
}

#[test]
fn starrocks_native_intervals_refresh_and_column_aggregation() {
    for sql in [
        "SELECT TIME_SLICE(dt, INTERVAL 5 MINUTE, FLOOR)",
        "SELECT LAG(x IGNORE NULLS, 1, 0) OVER (ORDER BY t)",
        "SELECT MAP{1:'a', 2:NULL}",
        "REFRESH MATERIALIZED VIEW db.mv PARTITION START ('2020-02-01') END ('2020-03-01') FORCE WITH SYNC MODE",
        "CREATE TABLE t (k INT, v HLL HLL_UNION) AGGREGATE KEY (k) DISTRIBUTED BY HASH (k)",
        "ALTER TABLE t ADD COLUMN c INT TO r1, ADD COLUMN d INT",
    ] { assert_eq!(normalize(sql, DialectType::StarRocks), sql); }
}

#[test]
fn spark_nested_column_positions_and_values_modifiers_are_retained() {
    let sql = "ALTER TABLE t ADD COLUMNS (a.b INT COMMENT 'x' AFTER c, d.e.f STRING)";
    assert_eq!(normalize(sql, DialectType::Spark), sql);
    assert_eq!(
        normalize(
            "SELECT 1 AS a UNION ALL (VALUES (2), (3) AS v(a) ORDER BY 1 DESC LIMIT 1)",
            DialectType::Spark
        ),
        "SELECT 1 AS a UNION ALL (SELECT * FROM VALUES (2), (3) AS v(a) ORDER BY 1 DESC LIMIT 1)"
    );
}

#[test]
fn snowflake_merge_by_name_and_copy_tags_keep_their_meaning() {
    // https://docs.snowflake.com/en/sql-reference/sql/merge
    let merge = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE ALL BY NAME WHEN NOT MATCHED THEN INSERT ALL BY NAME";
    assert_eq!(normalize(merge, DialectType::Snowflake), merge);
    assert!(Dialect::get(DialectType::Snowflake)
        .transpile(merge, DialectType::PostgreSQL)
        .is_err());
    assert_eq!(
        normalize(
            "CREATE OR REPLACE TABLE t (a INT) COPY GRANTS COPY TAGS",
            DialectType::Snowflake
        ),
        "CREATE OR REPLACE TABLE t (a INT) COPY GRANTS COPY TAGS"
    );
    let outer_join = "SELECT t1.a FROM t1, t2 WHERE t1.a = t2.a (+)";
    assert_eq!(normalize(outer_join, DialectType::Snowflake), outer_join);
}

#[test]
fn oracle_physical_properties_partition_flashback_and_json_options() {
    for sql in [
        "CREATE TABLE t (id NUMBER) TABLESPACE \"Mixed Ts\"",
        "CREATE TABLE t (id NUMBER) TABLESPACE ts PCTFREE 10 STORAGE (INITIAL 64K)",
        "SELECT * FROM sales PARTITION (p1) AS OF SCN 123 s",
        "INSERT INTO t SUBPARTITION (p1) \"Alias\" VALUES (1)",
        "SELECT JSON_VALUE(doc, '$.x' RETURNING NUMBER DEFAULT 0 ON ERROR NULL ON EMPTY) FROM t",
    ] {
        let canonical = normalize(sql, DialectType::Oracle);
        assert_eq!(normalize(&canonical, DialectType::Oracle), canonical);
        assert!(!canonical.contains("ISNULL"));
    }
}

#[test]
fn clickhouse_refresh_and_set_operation_output_modifiers_survive() {
    for sql in [
        "CREATE MATERIALIZED VIEW v REFRESH EVERY 5 MINUTE OFFSET 1 MINUTE RANDOMIZE FOR 1 MINUTE DEPENDS ON u SETTINGS refresh_retries = 2 APPEND TO target AS SELECT 1",
        "CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON u AS SELECT 1",
        "SELECT 1 UNION ALL (SELECT 2 INTERSECT SELECT 2) FORMAT JSON",
        "SELECT 1 UNION ALL (SELECT 2 INTERSECT SELECT 2 LIMIT 1) SETTINGS limit = 1",
        "ALTER TABLE t ALTER COLUMN IF EXISTS c TYPE Int64 DEFAULT 7",
    ] {
        let canonical = normalize(sql, DialectType::ClickHouse);
        assert_eq!(normalize(&canonical, DialectType::ClickHouse), canonical);
        if sql.contains("REFRESH") { assert!(canonical.contains("REFRESH")); }
        if sql.contains("APPEND") { assert!(canonical.contains("APPEND")); }
    }
    assert!(polyglot_sql::parse_one(
        "CREATE MATERIALIZED VIEW v REFRESH AFTER 1 HOUR OFFSET 1 MINUTE AS SELECT 1",
        DialectType::ClickHouse
    )
    .is_err());
}

#[test]
fn structured_update_targets_and_rename_literal_escaping() {
    let sql = "UPDATE t SET (b, c) = (3, 4), xs[1] = 7";
    assert_eq!(normalize(sql, DialectType::PostgreSQL), sql);
    assert_eq!(
        normalize(
            "ALTER TABLE a RENAME TO [x'; SELECT 1; --]",
            DialectType::TSQL
        ),
        "EXEC sp_rename 'a', 'x''; SELECT 1; --'"
    );
    assert_eq!(
        normalize("DROP VIEW dbo.a, dbo.b", DialectType::TSQL),
        "DROP VIEW dbo.a, dbo.b"
    );
    assert!(polyglot_sql::parse_one("DROP VIEW other_db.dbo.a", DialectType::TSQL).is_err());
}

#[test]
fn generic_semantic_modifiers_do_not_reserve_aliases() {
    for sql in [
        "AVG(AGG(x) PER (c))",
        "LAG(x) OVER (PARTITION BY EXCLUDING c ORDER BY d)",
        "SELECT SUM(x) AS per FROM t",
    ] {
        assert_eq!(normalize(sql, DialectType::Generic), sql);
    }
    assert_eq!(
        normalize("SELECT SUM(x) per FROM t", DialectType::Generic),
        "SELECT SUM(x) AS per FROM t"
    );
    assert!(Dialect::get(DialectType::Generic)
        .transpile("AVG(AGG(x) PER (c))", DialectType::PostgreSQL)
        .is_err());
}

#[test]
fn hive_and_spark_control_characters_round_trip() {
    for dialect in [
        DialectType::Hive,
        DialectType::Spark,
        DialectType::Databricks,
    ] {
        let sql = "SELECT '\u{1}\u{2}\u{1f}'";
        let canonical = normalize(sql, dialect);
        assert_eq!(normalize(&canonical, dialect), canonical);
        assert_eq!(
            polyglot_sql::parse_one(sql, dialect).unwrap(),
            polyglot_sql::parse_one(&canonical, dialect).unwrap()
        );
    }
}

fn translate(sql: &str, source: DialectType, target: DialectType) -> String {
    Dialect::get(source)
        .transpile(sql, target)
        .unwrap()
        .join("; ")
}

#[test]
fn decimal_literals_keep_fractional_scale_and_significant_digits() {
    // HiveDecimal removes fractional trailing zeroes, not integer zeroes.
    assert_eq!(translate("SELECT 1.2500bd, 0.0010bd, 0bd, 1000bd", DialectType::Hive, DialectType::DuckDB),
        "SELECT TRY_CAST('1.2500' AS DECIMAL(3, 2)), TRY_CAST('0.0010' AS DECIMAL(3, 3)), TRY_CAST('0' AS DECIMAL(1, 0)), TRY_CAST('1000' AS DECIMAL(4, 0))");
}

#[test]
fn date_format_conversion_preserves_iso_fields_and_quoted_literals() {
    assert_eq!(
        translate(
            "TO_CHAR(x, 'IYYY-IW')",
            DialectType::PostgreSQL,
            DialectType::DuckDB
        ),
        "STRFTIME(x, '%G-%V')"
    );
    assert_eq!(
        translate(
            "TO_CHAR(x, 'YyYy-Mm-dD\"T\"Hh24:mI:sS.fF3')",
            DialectType::Snowflake,
            DialectType::DuckDB
        ),
        "STRFTIME(x, '%Y-%m-%dT%H:%M:%S.%g')"
    );
    assert_eq!(
        translate(
            "DATE_FORMAT(x, '%M %W %f %x-%v %r')",
            DialectType::MySQL,
            DialectType::DuckDB
        ),
        "STRFTIME(x, '%B %A %f %G-%V %I:%M:%S %p')"
    );
}

#[test]
fn count_filters_preserve_wide_counts_and_distinct_values() {
    assert_eq!(
        translate(
            "SELECT COUNT_IF(x > 0) FILTER(WHERE enabled) FROM t",
            DialectType::Generic,
            DialectType::TSQL
        ),
        "SELECT COUNT_BIG(IIF(enabled AND x > 0, 1, NULL)) FROM t"
    );
    assert_eq!(
        translate(
            "SELECT COUNT(DISTINCT x) FILTER(WHERE enabled) FROM t",
            DialectType::Generic,
            DialectType::BigQuery
        ),
        "SELECT COUNT(DISTINCT IF(enabled, x, NULL)) FROM t"
    );
}

#[test]
fn sqlite_compound_left_grouping_preserves_duplicate_column_names() {
    let sql = "((SELECT 1 AS x, 2 AS x UNION SELECT 3, 4)) INTERSECT SELECT 1, 2";
    assert_eq!(
        translate(sql, DialectType::Generic, DialectType::SQLite),
        "SELECT 1 AS x, 2 AS x UNION SELECT 3, 4 INTERSECT SELECT 1, 2"
    );
    let commented = translate(
        "(SELECT 1 UNION SELECT 2) /* keep */ INTERSECT SELECT 3",
        DialectType::Generic,
        DialectType::SQLite,
    );
    assert!(commented.contains("/* keep */"), "{commented}");
    // A right-hand group still needs a derived table to preserve evaluation order.
    assert!(translate(
        "SELECT 1 EXCEPT (SELECT 2 UNION SELECT 3)",
        DialectType::Generic,
        DialectType::SQLite
    )
    .contains("SELECT * FROM ("));
}

#[test]
fn list_boundaries_and_array_insertion_use_source_indexes() {
    assert_eq!(
        translate(
            "SELECT RANGE(5, 1, -1)",
            DialectType::DuckDB,
            DialectType::BigQuery
        ),
        "SELECT GENERATE_ARRAY(5, 1 + 1, -1)"
    );
    assert_eq!(
        translate(
            "SELECT ARRAY_SLICE(a, 2, 4)",
            DialectType::DuckDB,
            DialectType::Spark
        ),
        "SELECT SLICE(a, 2, 3)"
    );
    assert_eq!(
        translate(
            "ARRAY_INSERT(ARRAY('a', 'b', 'c'), -2, 'z')",
            DialectType::Spark,
            DialectType::Snowflake
        ),
        "ARRAY_INSERT(['a', 'b', 'c'], -1, 'z')"
    );
    assert_eq!(
        translate(
            "ARRAY_INSERT(make_array(), -1, 'z')",
            DialectType::Spark,
            DialectType::Snowflake
        ),
        "ARRAY_APPEND(MAKE_ARRAY(), 'z')"
    );
}

#[test]
fn sqlite_null_contracts_survive_translation() {
    assert_eq!(
        translate(
            "CONCAT('a', NULL, 'b')",
            DialectType::SQLite,
            DialectType::MySQL
        ),
        "CONCAT(COALESCE('a', ''), COALESCE(NULL, ''), COALESCE('b', ''))"
    );
    assert_eq!(
        translate(
            "CONCAT_WS(',', 'a', NULL, 'b')",
            DialectType::SQLite,
            DialectType::Snowflake
        ),
        "ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT('a', NULL, 'b'), ',')"
    );
    assert_eq!(
        translate(
            "MAKE_INTERVAL(days => NULL)",
            DialectType::PostgreSQL,
            DialectType::DuckDB
        ),
        "NULL"
    );
}

#[test]
fn unsupported_translations_report_semantic_loss() {
    // These expected failures are based on vendor contracts, not SQLGlot output.
    let cases = [
        (
            "SELECT ARRAY_AGG(x) FILTER(WHERE enabled) FROM t",
            DialectType::Generic,
            DialectType::BigQuery,
            "NULL array elements",
        ),
        (
            "CAST(1.5 AS DECIMAL(30, 0))",
            DialectType::Trino,
            DialectType::BigQuery,
            "precision and scale enforcement",
        ),
        (
            "CAST(1234567890123456789012345678901 AS DECIMAL(30, 0))",
            DialectType::Trino,
            DialectType::BigQuery,
            "precision and scale enforcement",
        ),
        (
            "SELECT COUNT_IF(enabled) FROM t",
            DialectType::Snowflake,
            DialectType::TSQL,
            "returns NULL",
        ),
        (
            "APPROX_PERCENTILE(x, 0.5, 0.01)",
            DialectType::Presto,
            DialectType::BigQuery,
            "accuracy",
        ),
        (
            "CREATE TABLE t (a INT UNIQUE)",
            DialectType::Spark,
            DialectType::Materialize,
            "UNIQUE",
        ),
        (
            "SELECT r.idx FROM t, LATERAL UNNEST(t.a) WITH ORDINALITY AS r(v, idx)",
            DialectType::PostgreSQL,
            DialectType::Spark,
            "one-based",
        ),
        (
            "SELECT x / y",
            DialectType::DuckDB,
            DialectType::PostgreSQL,
            "infinity/NaN",
        ),
        (
            "JSON_EXTRACT(x, '$.n')",
            DialectType::SQLite,
            DialectType::PostgreSQL,
            "dynamic SQL scalar types",
        ),
        (
            "JSON_VALUE(x, '$.a')",
            DialectType::TSQL,
            DialectType::DuckDB,
            "4000-character",
        ),
        (
            "LOCKING ROW FOR ACCESS SELECT * FROM t",
            DialectType::Teradata,
            DialectType::Snowflake,
            "isolation",
        ),
        (
            "ADD_MONTHS(TIMESTAMP '2024-02-29 12:34:56', 1)",
            DialectType::Oracle,
            DialectType::DuckDB,
            "time-of-day",
        ),
        (
            r"SELECT b'\xff'",
            DialectType::BigQuery,
            DialectType::PostgreSQL,
            "byte-literal decoding",
        ),
        (
            "ARRAY_INSERT(a, 5, 'z')",
            DialectType::Spark,
            DialectType::Snowflake,
            "NULL-preserving padding",
        ),
    ];
    for (sql, source, target, diagnostic) in cases {
        let error = Dialect::get(source)
            .transpile(sql, target)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(diagnostic),
            "{source:?} -> {target:?}: {sql}: {error}"
        );
    }
}

#[test]
fn clickhouse_json_paths_keep_numeric_keys_separate_from_indexes() {
    for (input, expected) in [
        (
            r#"JSON_EXTRACT(x, '$["1"]')"#,
            "nullIf(JSONExtractRaw(x, '1'), '')",
        ),
        (
            "JSON_EXTRACT(x, '$[1]')",
            "nullIf(JSONExtractRaw(x, 2), '')",
        ),
        (
            r#"JSON_EXTRACT(x, '$["a]b"]')"#,
            "nullIf(JSONExtractRaw(x, 'a]b'), '')",
        ),
        (
            r#"JSON_EXTRACT(x, '$["*"]')"#,
            "nullIf(JSONExtractRaw(x, '*'), '')",
        ),
    ] {
        assert_eq!(
            translate(input, DialectType::Generic, DialectType::ClickHouse),
            expected
        );
    }
    assert!(Dialect::get(DialectType::Generic)
        .transpile("JSON_EXTRACT(x, '$[*]')", DialectType::ClickHouse)
        .is_err());
}

#[test]
fn wide_integer_decimal_cast_is_exact_in_bigquery() {
    assert_eq!(
        translate(
            "CAST(123 AS DECIMAL(30, 0))",
            DialectType::Trino,
            DialectType::BigQuery
        ),
        "CAST(123 AS BIGNUMERIC)"
    );
}

#[test]
fn clickhouse_column_changes_preserve_order_names_and_typeless_aliases() {
    // https://clickhouse.com/docs/reference/statements/alter/column#modify-column
    for (sql, expected) in [
        (
            "ALTER TABLE t MODIFY COLUMN type UInt32 FIRST",
            "ALTER TABLE t ALTER COLUMN type TYPE UInt32 FIRST",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN key LowCardinality(Int8) AFTER timestamp",
            "ALTER TABLE t ALTER COLUMN key TYPE LowCardinality(Int8) AFTER timestamp",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN n.value ALIAS array",
            "ALTER TABLE t MODIFY COLUMN \"n.value\" ALIAS array",
        ),
        (
            "ALTER TABLE t ALTER COLUMN IF EXISTS n.value TYPE Int64 DEFAULT 7 AFTER key",
            "ALTER TABLE t ALTER COLUMN IF EXISTS \"n.value\" TYPE Int64 DEFAULT 7 AFTER key",
        ),
    ] {
        let canonical = normalize(sql, DialectType::ClickHouse);
        assert_eq!(canonical, expected);
        assert_eq!(normalize(&canonical, DialectType::ClickHouse), canonical);
    }
}
