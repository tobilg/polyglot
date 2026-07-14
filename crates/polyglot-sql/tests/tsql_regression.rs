//! Regression tests for T-SQL parsing/generation and PostgreSQL -> T-SQL transpilation.

use polyglot_sql::generator::{Generator, GeneratorConfig};
use polyglot_sql::{
    generate, generate_data_type, get_all_tables, parse, parse_data_type, transpile, validate,
    DataType, Dialect, DialectType, Expression, ExpressionWalk, Parser, TokenType,
    TranspileOptions,
};

fn pg_to_tsql(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::TSQL)
        .unwrap_or_else(|e| panic!("transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

fn pg_to_tsql_strict(sql: &str) -> String {
    Dialect::get(DialectType::PostgreSQL)
        .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
        .unwrap_or_else(|e| panic!("strict transpile failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("expected at least one statement")
}

fn generate_tsql(expr: &Expression) -> String {
    let config = GeneratorConfig {
        dialect: Some(DialectType::TSQL),
        ..Default::default()
    };
    let mut generator = Generator::with_config(config);
    generator
        .generate(expr)
        .expect("expression should generate as T-SQL")
}

#[test]
fn postgres_json_operators_map_to_valid_tsql_json_functions() {
    let cases = [
        (
            "SELECT a -> 'name' FROM t",
            "SELECT JSON_QUERY(a, '$.name') FROM t",
        ),
        (
            "SELECT a ->> 'name' FROM t",
            "SELECT JSON_VALUE(a, '$.name') FROM t",
        ),
        (
            "SELECT a #> '{x,y}' FROM t",
            "SELECT JSON_QUERY(a, '$.x.y') FROM t",
        ),
        (
            "SELECT a #>> '{x,y}' FROM t",
            "SELECT JSON_VALUE(a, '$.x.y') FROM t",
        ),
        (
            "SELECT data -> 'a' -> 'b' FROM t",
            "SELECT JSON_QUERY(data, '$.a.b') FROM t",
        ),
        (
            "SELECT data -> 'a' ->> 'b' FROM t",
            "SELECT JSON_VALUE(data, '$.a.b') FROM t",
        ),
        (
            "SELECT data -> 0 FROM t",
            "SELECT JSON_QUERY(data, '$[0]') FROM t",
        ),
        (
            "SELECT a #> '{x,0}' FROM t",
            "SELECT JSON_QUERY(a, '$.x[0]') FROM t",
        ),
        (
            "SELECT a -> 'first name' FROM t",
            "SELECT JSON_QUERY(a, '$.\"first name\"') FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_json_constructors_and_aggregates_map_to_tsql_json_functions() {
    let cases = [
        (
            "SELECT jsonb_build_object('a', 1)",
            "SELECT JSON_OBJECT('a': 1)",
        ),
        (
            "SELECT json_build_object('a', 1, 'b', value) FROM t",
            "SELECT JSON_OBJECT('a': 1, 'b': value) FROM t",
        ),
        (
            "SELECT json_agg(a) FROM t",
            "SELECT JSON_ARRAYAGG(a) FROM t",
        ),
        (
            "SELECT jsonb_agg(a) FROM t",
            "SELECT JSON_ARRAYAGG(a) FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_json_array_elements_projection_maps_to_tsql_openjson() {
    assert_eq!(
        pg_to_tsql_strict("SELECT jsonb_array_elements('[1,2]'::jsonb)"),
        "SELECT value FROM OPENJSON(CAST('[1,2]' AS NVARCHAR(MAX)))"
    );
}

#[test]
fn postgres_json_row_shapes_without_scalar_tsql_equivalent_fail_in_strict_mode() {
    let cases = [
        ("SELECT row_to_json(t) FROM t", "ROW_TO_JSON"),
        (
            "SELECT jsonb_array_elements(data) FROM t",
            "JSONB_ARRAY_ELEMENTS",
        ),
    ];

    for (sql, expected) in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL JSON row shapes");

        assert!(
            err.to_string().contains(expected),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_current_temporal_niladics_map_to_tsql_expressions() {
    let cases = [
        (
            "SELECT current_date AS c FROM t",
            "SELECT CAST(GETDATE() AS DATE) AS c FROM t",
        ),
        (
            "SELECT current_time AS c FROM t",
            "SELECT CAST(GETDATE() AS TIME) AS c FROM t",
        ),
        (
            "SELECT localtimestamp AS c FROM t",
            "SELECT GETDATE() AS c FROM t",
        ),
        ("SELECT now() AS c FROM t", "SELECT GETDATE() AS c FROM t"),
        (
            "SELECT current_timestamp AS c FROM t",
            "SELECT GETDATE() AS c FROM t",
        ),
        (
            "SELECT clock_timestamp() AS c FROM t",
            "SELECT SYSDATETIME() AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_math_and_string_functions_map_to_tsql_semantics() {
    let cases = [
        ("SELECT log(x) FROM t", "SELECT LOG10(x) FROM t"),
        ("SELECT ln(x) FROM t", "SELECT LOG(x) FROM t"),
        ("SELECT log(2, x) FROM t", "SELECT LOG(x, 2) FROM t"),
        ("SELECT log10(x) FROM t", "SELECT LOG10(x) FROM t"),
        (
            "SELECT div(7, 2)",
            "SELECT CAST(CAST(CAST(7 AS FLOAT) / 2 AS INTEGER) AS NUMERIC)",
        ),
        (
            "SELECT cbrt(27)",
            "SELECT POWER(CAST(27 AS FLOAT), 1.0 / 3.0)",
        ),
        (
            "SELECT repeat('ab', 3) FROM t",
            "SELECT REPLICATE('ab', 3) FROM t",
        ),
        ("SELECT chr(65) FROM t", "SELECT CHAR(65) FROM t"),
        (
            "SELECT overlay('hello' placing 'XX' from 2 for 2)",
            "SELECT STUFF('hello', 2, 2, 'XX')",
        ),
        ("SELECT btrim('  hi  ')", "SELECT TRIM('  hi  ')"),
        (
            "SELECT btrim('xxhixx', 'x')",
            "SELECT TRIM('x' FROM 'xxhixx')",
        ),
        (
            "SELECT trim(both 'x' from 'xxhelloxx')",
            "SELECT TRIM('x' FROM 'xxhelloxx')",
        ),
        (
            "SELECT trim(leading 'x' from 'xxhello')",
            "SELECT LTRIM('xxhello', 'x')",
        ),
        (
            "SELECT trim(trailing 'x' from 'helloxx')",
            "SELECT RTRIM('helloxx', 'x')",
        ),
        (
            "SELECT md5('abc')",
            "SELECT LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', 'abc'), 2))",
        ),
        ("SELECT octet_length('hello')", "SELECT DATALENGTH('hello')"),
        (
            "SELECT bit_length('hello')",
            "SELECT DATALENGTH('hello') * 8",
        ),
        (
            "SELECT starts_with('hello', 'he')",
            "SELECT CAST(CASE WHEN LEFT('hello', LEN('he')) = 'he' THEN 1 ELSE 0 END AS BIT)",
        ),
        (
            "SELECT to_hex(255)",
            "SELECT LOWER(CONVERT(VARCHAR(MAX), CAST(255 AS VARBINARY(MAX)), 2))",
        ),
        (
            "SELECT encode('abc', 'hex')",
            "SELECT LOWER(CONVERT(VARCHAR(MAX), CAST('abc' AS VARBINARY(MAX)), 2))",
        ),
        (
            "SELECT to_number('123.45', '999.99')",
            "SELECT TRY_CONVERT(DECIMAL(18, 2), '123.45')",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_math_functions_without_tsql_equivalent_fail_in_strict_mode() {
    let cases = [
        ("SELECT erf(x)", "ERF"),
        ("SELECT gcd(12, 8)", "GCD"),
        ("SELECT lcm(4, 6)", "LCM"),
        ("SELECT width_bucket(5, 0, 10, 5)", "WIDTH_BUCKET"),
    ];

    for (sql, expected) in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL math functions");

        assert!(
            err.to_string().contains(expected),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_string_functions_with_unsupported_tsql_shapes_fail_in_strict_mode() {
    let cases = [
        ("SELECT encode('abc', 'base64')", "ENCODE"),
        ("SELECT quote_literal(name) FROM t", "QUOTE_LITERAL"),
        ("SELECT to_number('123.45', format_mask)", "TO_NUMBER"),
    ];

    for (sql, expected) in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL string function shapes");

        assert!(
            err.to_string().contains(expected),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_system_functions_without_tsql_equivalent_fail_in_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT pg_typeof(a) FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject unsupported PostgreSQL system functions");

    assert!(
        err.to_string().contains("PG_TYPEOF"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_regex_operators_map_to_tsql_patindex_predicates_in_default_mode() {
    let cases = [
        (
            "SELECT name ~ '^a.*z$' FROM t",
            "SELECT CAST(CASE WHEN PATINDEX('^a.*z$', name) > 0 THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
        (
            "SELECT name ~* 'abc' FROM t",
            "SELECT CAST(CASE WHEN PATINDEX(LOWER('abc'), LOWER(name)) > 0 THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
        (
            "SELECT name !~ 'abc' FROM t",
            "SELECT CAST(CASE WHEN NOT PATINDEX('abc', name) > 0 THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
        (
            "SELECT name !~* 'abc' FROM t",
            "SELECT CAST(CASE WHEN NOT PATINDEX(LOWER('abc'), LOWER(name)) > 0 THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_similar_to_maps_to_tsql_like_when_pattern_is_compatible() {
    let cases = [
        (
            "SELECT name SIMILAR TO 'a%' FROM t",
            "SELECT CAST(CASE WHEN name LIKE 'a%' THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
        (
            "SELECT id FROM t WHERE name SIMILAR TO 'Brand#[1-3][0-9]'",
            "SELECT id FROM t WHERE name LIKE 'Brand#[1-3][0-9]'",
        ),
        (
            "SELECT name NOT SIMILAR TO 'z%' FROM t",
            "SELECT CAST(CASE WHEN NOT name LIKE 'z%' THEN 1 ELSE 0 END AS BIT) FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_complex_similar_to_patterns_still_fail_for_tsql_in_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT id FROM t WHERE name SIMILAR TO '%(b|d)%'",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject SQL-regex SIMILAR TO patterns");

    assert!(
        err.to_string().contains("regular expression predicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_array_semantics_without_tsql_equivalent_fail_in_strict_mode() {
    let cases = [
        ("SELECT ARRAY[1, 2, 3]", "array literals"),
        ("SELECT (ARRAY[1,2,3])[1]", "array subscripts"),
        ("SELECT array_length(ARRAY[1,2,3], 1)", "ARRAY_LENGTH"),
        ("SELECT unnest(ARRAY[1,2,3])", "UNNEST"),
        (
            "SELECT array_to_string(ARRAY['a','b'], ',')",
            "ARRAY_TO_STRING",
        ),
        ("SELECT cardinality(ARRAY[1,2,3])", "CARDINALITY"),
        ("SELECT string_to_array('a,b,c', ',')", "STRING_TO_ARRAY"),
    ];

    for (sql, expected) in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL array semantics");

        assert!(
            err.to_string().contains(expected),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn default_postgres_array_length_preserves_dimension_argument_for_tsql() {
    assert_eq!(
        pg_to_tsql("SELECT array_length(ARRAY[1,2,3], 1)"),
        "SELECT ARRAY_LENGTH(ARRAY[1, 2, 3], 1)"
    );
}

#[test]
fn native_tsql_single_arg_log_keeps_natural_log_semantics() {
    let out = transpile("SELECT LOG(x) FROM t", DialectType::TSQL, DialectType::TSQL)
        .expect("T-SQL LOG should transpile")
        .into_iter()
        .next()
        .expect("expected one statement");
    assert_eq!(out, "SELECT LOG(x) FROM t");
}

#[test]
fn postgres_extract_and_date_part_fields_map_to_valid_tsql_dateparts() {
    let cases = [
        (
            "SELECT EXTRACT(doy FROM ts) AS c FROM t",
            "SELECT DATEPART(DAYOFYEAR, ts) AS c FROM t",
        ),
        (
            "SELECT date_part('dow', ts) AS c FROM t",
            "SELECT DATEPART(WEEKDAY, ts) AS c FROM t",
        ),
        (
            "SELECT date_part('hour', ts) AS c FROM t",
            "SELECT DATEPART(hour, ts) AS c FROM t",
        ),
        (
            "SELECT date_part('isoweek', ts) AS c FROM t",
            "SELECT DATEPART(ISO_WEEK, ts) AS c FROM t",
        ),
        (
            "SELECT EXTRACT(epoch FROM ts) AS c FROM t",
            "SELECT DATEDIFF(SECOND, CAST('1970-01-01' AS DATETIME2), ts) AS c FROM t",
        ),
        (
            "SELECT date_part('epoch', ts) AS c FROM t",
            "SELECT DATEDIFF(SECOND, CAST('1970-01-01' AS DATETIME2), ts) AS c FROM t",
        ),
        (
            "SELECT EXTRACT(isodow FROM ts) AS c FROM t",
            "SELECT (((DATEPART(WEEKDAY, ts) + @@DATEFIRST - 2) % 7) + 1) AS c FROM t",
        ),
        (
            "SELECT date_part('isodow', ts) AS c FROM t",
            "SELECT (((DATEPART(WEEKDAY, ts) + @@DATEFIRST - 2) % 7) + 1) AS c FROM t",
        ),
        (
            "SELECT date_part('year'::text, ts) AS c FROM t",
            "SELECT DATEPART(year, ts) AS c FROM t",
        ),
        (
            "SELECT date_part('month'::text, ts) AS c FROM t",
            "SELECT DATEPART(month, ts) AS c FROM t",
        ),
        (
            "SELECT date_part('dow'::text, ts) AS c FROM t",
            "SELECT DATEPART(WEEKDAY, ts) AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_date_trunc_fields_map_to_valid_tsql_datetrunc_dateparts() {
    let cases = [
        (
            "SELECT date_trunc('day', ts) AS c FROM t",
            "SELECT DATETRUNC(DAY, ts) AS c FROM t",
        ),
        (
            "SELECT date_trunc('month', ts) AS c FROM t",
            "SELECT DATETRUNC(MONTH, ts) AS c FROM t",
        ),
        (
            "SELECT date_trunc('year', ts) AS c FROM t",
            "SELECT DATETRUNC(YEAR, ts) AS c FROM t",
        ),
        (
            "SELECT date_trunc('hour', ts) AS c FROM t",
            "SELECT DATETRUNC(HOUR, ts) AS c FROM t",
        ),
        (
            "SELECT date_trunc('week', ts) AS c FROM t",
            "SELECT DATETRUNC(WEEK, ts) AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_date_bin_maps_to_tsql_date_bucket_for_simple_strides() {
    let out = pg_to_tsql_strict(
        "SELECT date_bin('15 minutes', completed_at, TIMESTAMP '2001-01-01') AS b FROM order_fulfillment_history",
    );
    assert_eq!(
        out,
        "SELECT DATE_BUCKET(MINUTE, 15, completed_at, CAST('2001-01-01' AS DATETIME2)) AS b FROM order_fulfillment_history"
    );
}

#[test]
fn postgres_to_timestamp_and_to_date_formats_map_to_valid_tsql_convert_signatures() {
    let cases = [
        (
            "SELECT make_date(2020, 1, 1) AS c FROM t",
            "SELECT DATEFROMPARTS(2020, 1, 1) AS c FROM t",
        ),
        (
            "SELECT to_timestamp('2020-01-01', 'YYYY-MM-DD') AS c FROM t",
            "SELECT CONVERT(DATETIME2, '2020-01-01', 23) AS c FROM t",
        ),
        (
            "SELECT to_timestamp('2020-01-01 12:13:14', 'YYYY-MM-DD HH24:MI:SS') AS c FROM t",
            "SELECT CONVERT(DATETIME2, '2020-01-01 12:13:14', 120) AS c FROM t",
        ),
        (
            "SELECT to_timestamp('2020-01-01T12:13:14.123456', 'YYYY-MM-DDTHH24:MI:SS.US') AS c FROM t",
            "SELECT CONVERT(DATETIME2, '2020-01-01T12:13:14.123456', 126) AS c FROM t",
        ),
        (
            "SELECT to_date('2020-01-01', 'YYYY-MM-DD') AS c FROM t",
            "SELECT CONVERT(DATE, '2020-01-01', 23) AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_unsupported_date_time_functions_fail_for_tsql_in_strict_mode() {
    let cases = [
        "SELECT age(ts) AS c FROM t",
        "SELECT age(d1, timestamp '2000-01-01') AS c FROM t",
    ];

    for sql in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL date/time functions");

        assert!(
            err.to_string().contains("AGE"),
            "unexpected error for PostgreSQL AGE in {sql}: {err}"
        );
    }
}

#[test]
fn postgres_to_char_formats_map_to_tsql_dotnet_format_strings() {
    let cases = [
        (
            "SELECT to_char(ts, 'YYYY-MM-DD') AS c FROM t",
            "SELECT FORMAT(ts, 'yyyy-MM-dd') AS c FROM t",
        ),
        (
            "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') AS c FROM t",
            "SELECT FORMAT(ts, 'yyyy-MM-dd HH:mm:ss.ffffff') AS c FROM t",
        ),
        (
            "SELECT to_char(ts, 'YYYY-MM-DD'::text) AS c FROM t",
            "SELECT FORMAT(ts, 'yyyy-MM-dd') AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_numeric_to_char_formats_fail_for_tsql_in_strict_mode() {
    let cases = [
        "SELECT to_char(val, '9G999G999') FROM t",
        "SELECT to_char(val, '9G999G999'::text) FROM t",
        "SELECT to_char(123, 'S')",
    ];

    for sql in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject PostgreSQL numeric TO_CHAR formats");

        assert!(
            err.to_string().contains("PostgreSQL TO_CHAR"),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_format_string_interpolation_maps_to_tsql_concat() {
    let cases = [
        (
            "SELECT format('%s-%s', a, b) FROM t",
            "SELECT CONCAT(a, '-', b) FROM t",
        ),
        (
            "SELECT format('x=%s', f1) FROM text_tbl",
            "SELECT CONCAT('x=', f1) FROM text_tbl",
        ),
        (
            "SELECT format('%s', f1) FROM text_tbl",
            "SELECT CONCAT(f1, '') FROM text_tbl",
        ),
        (
            "SELECT format('literal %% %s', f1) FROM text_tbl",
            "SELECT CONCAT('literal % ', f1) FROM text_tbl",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_advanced_format_specifiers_fail_for_tsql() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile(
            "SELECT format('%I', table_name) FROM metadata",
            DialectType::TSQL,
        )
        .expect_err("PostgreSQL identifier quoting format should not map to T-SQL FORMAT");

    assert!(
        err.to_string().contains("PostgreSQL format()"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_uuid_generators_map_to_tsql_newid() {
    let cases = [
        ("SELECT gen_random_uuid()", "SELECT NEWID()"),
        ("SELECT uuid_generate_v4()", "SELECT NEWID()"),
        ("SELECT uuidv4()", "SELECT NEWID()"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_unsupported_to_timestamp_format_fails_for_tsql_in_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT to_timestamp('2020-01', 'YYYY-MM') AS c FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject unsupported T-SQL timestamp parse formats");

    assert!(
        err.to_string().contains("no CONVERT style mapping"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_unsupported_extract_fields_fail_for_tsql_in_strict_mode() {
    let cases = [
        "SELECT EXTRACT(millennium FROM ts) AS c FROM t",
        "SELECT date_part('millennium'::text, ts) AS c FROM t",
    ];

    for sql in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported T-SQL dateparts");

        assert!(
            err.to_string().contains("MILLENNIUM"),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_unsupported_date_trunc_fields_fail_for_tsql_in_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT date_trunc('millennium', ts) AS c FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject unsupported T-SQL DATETRUNC dateparts");

    assert!(
        err.to_string().contains("MILLENNIUM"),
        "unexpected error: {err}"
    );
}

const TRY_CATCH_SQL: &str = r#"BEGIN TRY
    INSERT INTO orders (id, amount) VALUES (1, 100.00);
    UPDATE inventory SET qty = qty - 1 WHERE product_id = 42;
END TRY
BEGIN CATCH
    INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE());
END CATCH"#;

// ---------------------------------------------------------------------------
// T-SQL hex literals
// ---------------------------------------------------------------------------

#[test]
fn tsql_hex_literals_tokenize_as_hex_strings() {
    let dialect = Dialect::get(DialectType::TSQL);
    let tokens = dialect
        .tokenize("SELECT * FROM Employees WHERE EmployeeId = 0x1")
        .expect("T-SQL hex literal should tokenize");

    let hex = tokens
        .iter()
        .find(|token| token.token_type == TokenType::HexString)
        .expect("expected a HexString token for 0x1");

    assert_eq!(hex.text, "1");
}

#[test]
fn tsql_hex_literals_parse_and_roundtrip() {
    for sql in [
        "SELECT * FROM Employees WHERE EmployeeId = 0x1",
        "INSERT INTO t (bin) VALUES (0xFF)",
        "SELECT 0xCAFE + 1",
    ] {
        let ast = parse(sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("T-SQL hex literal should parse for {sql:?}: {error}"));
        assert_eq!(ast.len(), 1);
        assert_eq!(generate_tsql(&ast[0]), sql);

        let roundtrip =
            transpile(sql, DialectType::TSQL, DialectType::TSQL).unwrap_or_else(|error| {
                panic!("T-SQL hex literal should transpile for {sql:?}: {error}")
            });
        assert_eq!(roundtrip, vec![sql.to_string()]);
    }
}

#[test]
fn tsql_max_length_types_parse_generate_and_transpile() {
    for type_name in ["VARBINARY(MAX)", "VARCHAR(MAX)", "NVARCHAR(MAX)"] {
        let data_type = parse_data_type(type_name, DialectType::TSQL).unwrap_or_else(|error| {
            panic!("{type_name} should parse as a standalone type: {error}")
        });
        assert_eq!(
            data_type,
            DataType::Custom {
                name: type_name.to_string()
            }
        );
        assert_eq!(
            generate_data_type(&data_type, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{type_name} should generate as T-SQL: {error}")),
            type_name
        );

        let create_sql = format!("CREATE TABLE t (c {type_name})");
        let ast = parse(&create_sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("{create_sql} should parse: {error}"));
        let Expression::CreateTable(create) = &ast[0] else {
            panic!(
                "expected CreateTable for {create_sql}, got {}",
                ast[0].variant_name()
            );
        };
        assert_eq!(create.columns.len(), 1);
        assert_eq!(
            create.columns[0].data_type,
            DataType::Custom {
                name: type_name.to_string()
            }
        );
        assert_eq!(generate_tsql(&ast[0]), create_sql);
        assert_eq!(
            transpile(&create_sql, DialectType::TSQL, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{create_sql} should identity-transpile: {error}")),
            vec![create_sql]
        );

        let cast_sql = format!("SELECT CAST(x AS {type_name})");
        let ast = parse(&cast_sql, DialectType::TSQL)
            .unwrap_or_else(|error| panic!("{cast_sql} should parse: {error}"));
        assert_eq!(generate_tsql(&ast[0]), cast_sql);
        assert_eq!(
            transpile(&cast_sql, DialectType::TSQL, DialectType::TSQL)
                .unwrap_or_else(|error| panic!("{cast_sql} should identity-transpile: {error}")),
            vec![cast_sql]
        );
    }

    let postgres = transpile(
        "SELECT CAST(x AS VARBINARY(MAX))",
        DialectType::TSQL,
        DialectType::PostgreSQL,
    )
    .expect("VARBINARY(MAX) should no longer fail before target generation");
    assert_eq!(postgres.len(), 1);
    assert!(!postgres[0].is_empty());
}

#[test]
fn postgres_positional_parameters_render_as_tsql_style_placeholders() {
    assert_eq!(pg_to_tsql("SELECT $1, $2"), "SELECT @P1, @P2");
}

#[test]
fn postgres_positional_parameters_render_as_tsql_style_placeholders_in_predicates() {
    assert_eq!(
        pg_to_tsql("SELECT id FROM t WHERE a = $1 AND b < $2"),
        "SELECT id FROM t WHERE a = @P1 AND b < @P2"
    );
}

#[test]
fn tsql_datepart_dayofweek_generates_valid_tsql_datepart() {
    let cases = [
        ("WEEKDAY", "WEEKDAY"),
        ("dw", "WEEKDAY"),
        ("DAYOFWEEK", "WEEKDAY"),
        ("doy", "DAYOFYEAR"),
        ("hour", "hour"),
        ("isowk", "ISO_WEEK"),
        ("WEEKISO", "ISO_WEEK"),
        ("tz", "TZOFFSET"),
        ("TIMEZONE_MINUTE", "TZOFFSET"),
    ];

    for dialect in [DialectType::TSQL, DialectType::Fabric] {
        for (input, expected) in cases {
            let sql = format!("SELECT DATEPART({input}, o.order_date) AS dow FROM orders o");
            let ast = parse(&sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should parse: {error}"));
            assert_eq!(ast.len(), 1);

            let generated = generate(&ast[0], dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should generate: {error}"));

            assert_eq!(
                generated,
                format!("SELECT DATEPART({expected}, o.order_date) AS dow FROM orders AS o"),
                "failed for {dialect:?} DATEPART({input}, ...)"
            );
        }
    }
}

#[test]
fn tsql_datepart_dayofweek_transpiles_to_valid_tsql_datepart() {
    let sql = "SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders o";

    assert_eq!(
        transpile(sql, DialectType::TSQL, DialectType::TSQL).expect("TSQL identity transpile"),
        vec!["SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders AS o"]
    );
    assert_eq!(
        transpile(sql, DialectType::TSQL, DialectType::Fabric).expect("TSQL to Fabric transpile"),
        vec!["SELECT DATEPART(WEEKDAY, o.order_date) AS dow FROM orders AS o"]
    );
}

#[test]
fn tsql_datepart_epoch_and_isodow_generate_tsql_expressions() {
    let cases = [
        (
            "SELECT DATEPART(epoch, o.order_date) AS epoch FROM orders o",
            "SELECT DATEDIFF(SECOND, CAST('1970-01-01' AS DATETIME2), o.order_date) AS epoch FROM orders AS o",
        ),
        (
            "SELECT DATEPART(isodow, o.order_date) AS isodow FROM orders o",
            "SELECT (((DATEPART(WEEKDAY, o.order_date) + @@DATEFIRST - 2) % 7) + 1) AS isodow FROM orders AS o",
        ),
    ];

    for dialect in [DialectType::TSQL, DialectType::Fabric] {
        for (sql, expected) in cases {
            let ast = parse(sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should parse: {error}"));
            let generated = generate(&ast[0], dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATEPART should generate: {error}"));

            assert_eq!(generated, expected, "failed for {dialect:?}: {sql}");
        }
    }
}

#[test]
fn tsql_datetrunc_string_dateparts_generate_unquoted_tsql_dateparts() {
    let cases = [
        (
            "SELECT DATETRUNC('day', o.order_date) AS c FROM orders o",
            "SELECT DATETRUNC(DAY, o.order_date) AS c FROM orders AS o",
        ),
        (
            "SELECT DATETRUNC(doy, o.order_date) AS c FROM orders o",
            "SELECT DATETRUNC(DAYOFYEAR, o.order_date) AS c FROM orders AS o",
        ),
        (
            "SELECT DATETRUNC(isowk, o.order_date) AS c FROM orders o",
            "SELECT DATETRUNC(ISO_WEEK, o.order_date) AS c FROM orders AS o",
        ),
    ];

    for dialect in [DialectType::TSQL, DialectType::Fabric] {
        for (sql, expected) in cases {
            let ast = parse(sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATETRUNC should parse: {error}"));
            let generated = generate(&ast[0], dialect)
                .unwrap_or_else(|error| panic!("{dialect:?} DATETRUNC should generate: {error}"));

            assert_eq!(generated, expected, "failed for {dialect:?}: {sql}");
        }
    }
}

// ---------------------------------------------------------------------------
// T-SQL SET options
// ---------------------------------------------------------------------------

#[test]
fn tsql_set_statistics_options_parse_as_commands() {
    for sql in [
        "SET STATISTICS TIME ON",
        "SET STATISTICS TIME OFF",
        "SET STATISTICS IO ON",
        "SET STATISTICS IO OFF",
        "SET STATISTICS XML ON",
        "SET STATISTICS XML OFF",
        "SET STATISTICS PROFILE ON",
        "SET STATISTICS PROFILE OFF",
        "SET STATISTICS IO, TIME ON",
    ] {
        let ast = parse(sql, DialectType::TSQL).expect("SET STATISTICS should parse");
        assert_eq!(ast.len(), 1);
        assert!(
            matches!(ast[0], Expression::Command(_)),
            "expected Command for {sql}, got {}",
            ast[0].variant_name()
        );
        assert_eq!(generate_tsql(&ast[0]), sql);
    }
}

#[test]
fn tsql_set_statistics_consumes_only_current_statement() {
    let ast =
        parse("SET STATISTICS TIME ON; SELECT 1", DialectType::TSQL).expect("batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Command(_)));
    assert!(matches!(ast[1], Expression::Select(_)));
    assert_eq!(generate_tsql(&ast[0]), "SET STATISTICS TIME ON");
}

#[test]
fn tsql_simple_set_options_remain_structured() {
    for sql in ["SET NOCOUNT ON", "SET XACT_ABORT ON", "SET ANSI_NULLS OFF"] {
        let ast = parse(sql, DialectType::TSQL).expect("T-SQL SET option should parse");
        assert_eq!(ast.len(), 1);
        assert!(
            matches!(ast[0], Expression::SetStatement(_)),
            "expected SetStatement for {sql}, got {}",
            ast[0].variant_name()
        );
        assert_eq!(generate_tsql(&ast[0]), sql);
    }
}

#[test]
fn tsql_set_identity_insert_remains_structured() {
    for state in ["ON", "OFF"] {
        let sql = format!("SET IDENTITY_INSERT dbo.Employees {state}");
        let sql_with_semicolon = format!("{sql};");
        let ast = parse(&sql_with_semicolon, DialectType::TSQL)
            .expect("SET IDENTITY_INSERT should parse");
        assert_eq!(ast.len(), 1);

        let Expression::SetStatement(set) = &ast[0] else {
            panic!(
                "expected SetStatement for {sql}, got {}",
                ast[0].variant_name()
            );
        };

        assert_eq!(set.items.len(), 1);
        let item = &set.items[0];
        assert_eq!(item.name.to_string(), "IDENTITY_INSERT");
        assert!(item.no_equals);

        let Expression::Tuple(value) = &item.value else {
            panic!("expected tuple value for SET IDENTITY_INSERT");
        };
        assert_eq!(value.expressions.len(), 2);

        let Expression::Table(table) = &value.expressions[0] else {
            panic!("expected table target for SET IDENTITY_INSERT");
        };
        assert_eq!(
            table.schema.as_ref().map(|schema| schema.name.as_str()),
            Some("dbo")
        );
        assert_eq!(table.name.name, "Employees");
        assert_eq!(value.expressions[1].to_string(), state);

        assert_eq!(generate_tsql(&ast[0]), sql);
        assert_eq!(
            transpile(&sql_with_semicolon, DialectType::TSQL, DialectType::TSQL)
                .expect("transpile")[0],
            sql
        );
        assert!(validate(&sql_with_semicolon, DialectType::TSQL).valid);
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL LATERAL joins -> T-SQL APPLY
// ---------------------------------------------------------------------------

#[test]
fn postgres_lateral_joins_map_to_tsql_apply() {
    let lateral_subquery = "(SELECT v FROM lineitem WHERE l_orderkey = o.id)";
    let cross_apply =
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t";
    let outer_apply =
        "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t";

    let cases = [
        (
            format!("SELECT o.id, t.v FROM orders o CROSS JOIN LATERAL {lateral_subquery} t"),
            cross_apply,
        ),
        (
            format!("SELECT o.id, t.v FROM orders o JOIN LATERAL {lateral_subquery} t ON true"),
            cross_apply,
        ),
        (
            format!(
                "SELECT o.id, t.v FROM orders o INNER JOIN LATERAL {lateral_subquery} t ON true"
            ),
            cross_apply,
        ),
        (
            format!(
                "SELECT o.id, t.v FROM orders o LEFT JOIN LATERAL {lateral_subquery} t ON true"
            ),
            outer_apply,
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(&sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_parenthesized_lateral_joins_map_to_tsql_apply() {
    let cases = [
        (
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS x) a ON true)",
            "SELECT * FROM (orders AS o CROSS APPLY (SELECT 1 AS x) AS a)",
        ),
        (
            "SELECT * FROM (orders o LEFT JOIN LATERAL (SELECT 1 AS x) a ON true)",
            "SELECT * FROM (orders AS o OUTER APPLY (SELECT 1 AS x) AS a)",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_comma_lateral_maps_to_tsql_cross_apply() {
    assert_eq!(
        pg_to_tsql("SELECT o.id, t.v FROM orders o, LATERAL (SELECT v FROM lineitem WHERE l_orderkey = o.id) t"),
        "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT v AS v FROM lineitem WHERE l_orderkey = o.id) AS t"
    );
}

#[test]
fn postgres_lateral_join_on_predicate_pushes_into_tsql_apply_rhs() {
    let cases = [
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.v FROM orders o LEFT JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON t.v > 0",
            "SELECT o.id, t.v FROM orders AS o OUTER APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE _polyglot_lateral_source.v > 0) AS t",
        ),
        (
            "SELECT o.id, t.x FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t(x) ON t.x > 0",
            "SELECT o.id, t.x FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source(x) WHERE _polyglot_lateral_source.x > 0) AS t(x)",
        ),
        (
            "SELECT o.id, t.v FROM orders o JOIN LATERAL (SELECT v FROM lineitem WHERE lineitem.l_orderkey = o.id) t ON o.id > 0",
            "SELECT o.id, t.v FROM orders AS o CROSS APPLY (SELECT * FROM (SELECT v AS v FROM lineitem WHERE lineitem.l_orderkey = o.id) AS _polyglot_lateral_source WHERE o.id > 0) AS t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_lateral_using_inside_joined_table_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT * FROM (orders o JOIN LATERAL (SELECT 1 AS id) a USING (id))",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject residual LATERAL");

    assert!(
        err.to_string().contains("LATERAL joins and subqueries"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_framed_named_window_inlines_frame_stripped_copy_for_tsql_ranking_function() {
    let out = pg_to_tsql(
        "SELECT row_number() OVER w AS rn, avg(o_totalprice) OVER w AS av \
         FROM orders \
         WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    );

    assert_eq!(
        out,
        "SELECT ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn, AVG(o_totalprice) OVER w AS av FROM orders WINDOW w AS (PARTITION BY o_custkey ORDER BY o_orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    );
}

#[test]
fn postgres_inline_window_frame_is_stripped_for_tsql_ranking_function() {
    let out = pg_to_tsql(
        "SELECT row_number() OVER (PARTITION BY o_custkey ORDER BY o_orderdate NULLS FIRST \
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn \
         FROM orders",
    );

    assert_eq!(
        out,
        "SELECT ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn FROM orders"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL NULLS FIRST/LAST -> T-SQL CASE sort key
// ---------------------------------------------------------------------------

#[test]
fn postgres_null_ordering_rewrites_for_tsql() {
    let cases = [
        (
            "SELECT id FROM t ORDER BY id ASC",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id ASC NULLS LAST",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END, id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id ASC NULLS FIRST",
            "SELECT id FROM t ORDER BY id ASC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC NULLS FIRST",
            "SELECT id FROM t ORDER BY CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC",
        ),
        (
            "SELECT id FROM t ORDER BY id DESC NULLS LAST",
            "SELECT id FROM t ORDER BY id DESC",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_random_ordering_does_not_add_null_sort_key_for_tsql() {
    let out = pg_to_tsql(r#"SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5"#);
    assert_eq!(out, "SELECT TOP 5 * FROM [test_table] ORDER BY RAND()");
}

#[test]
fn fetch_first_with_ties_uses_top_with_ties_for_tsql() {
    let out = pg_to_tsql_strict(
        "SELECT store_id FROM orders ORDER BY promised_at FETCH FIRST 5 ROWS WITH TIES",
    );
    assert_eq!(
        out,
        "SELECT TOP (5) WITH TIES store_id FROM orders ORDER BY CASE WHEN promised_at IS NULL THEN 1 ELSE 0 END, promised_at"
    );
}

#[test]
fn postgres_overlaps_predicate_rewrites_to_tsql_range_predicate() {
    let out = pg_to_tsql_strict(
        "SELECT 1 WHERE (DATE '2020-01-01', DATE '2020-02-01') OVERLAPS (DATE '2020-01-15', DATE '2020-03-01')",
    );

    assert!(
        !out.contains("OVERLAPS"),
        "T-SQL output should not contain OVERLAPS: {out}"
    );
    assert!(
        out.contains("CASE WHEN CAST('2020-01-01' AS DATE) <= CAST('2020-02-01' AS DATE)"),
        "expected normalized left range start in output: {out}"
    );
    assert!(
        out.contains(" AND "),
        "expected conjunctive range predicate in output: {out}"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL no-op LIMIT -> omitted for T-SQL
// ---------------------------------------------------------------------------

#[test]
fn postgres_noop_limits_are_omitted_for_tsql() {
    let cases = [
        ("SELECT f1 FROM t LIMIT NULL", "SELECT f1 FROM t"),
        ("SELECT f1 FROM t LIMIT ALL", "SELECT f1 FROM t"),
        ("SELECT f1 FROM t LIMIT (NULL)", "SELECT f1 FROM t"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_noop_limits_keep_offset_only_for_tsql() {
    let cases = [
        (
            "SELECT f1 FROM t ORDER BY f1 LIMIT NULL OFFSET 2",
            "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1 OFFSET 2 ROWS",
        ),
        (
            "SELECT f1 FROM t ORDER BY f1 LIMIT ALL OFFSET 2",
            "SELECT f1 FROM t ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1 OFFSET 2 ROWS",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_set_operation_modifiers_wrap_for_tsql() {
    let cases = [
        (
            "SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer UNION ALL SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer UNION ALL SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer INTERSECT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100",
            "SELECT TOP 100 * FROM (SELECT c_custkey FROM customer INTERSECT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey",
        ),
        (
            "SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders ORDER BY c_custkey LIMIT 100 OFFSET 2",
            "SELECT * FROM (SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders) AS _l_0 ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey OFFSET 2 ROWS FETCH NEXT 100 ROWS ONLY",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL WITH RECURSIVE -> T-SQL WITH
// ---------------------------------------------------------------------------

#[test]
fn postgres_recursive_cte_omits_recursive_keyword_for_tsql() {
    let out = pg_to_tsql(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r",
    );
    assert_eq!(
        out,
        "WITH r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL MOD function -> T-SQL modulo operator
// ---------------------------------------------------------------------------

#[test]
fn postgres_mod_function_maps_to_tsql_percent_operator() {
    let cases = [
        ("SELECT mod(a, 7) AS m FROM t", "SELECT a % 7 AS m FROM t"),
        (
            "SELECT mod(a + 1, b * 2) AS m FROM t",
            "SELECT (a + 1) % (b * 2) AS m FROM t",
        ),
        ("SELECT a % 7 AS m FROM t", "SELECT a % 7 AS m FROM t"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL row-value IN subquery -> T-SQL correlated EXISTS
// ---------------------------------------------------------------------------

#[test]
fn postgres_row_value_in_subquery_maps_to_tsql_exists() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.x = t.a AND u.y = t.b AND q < 10)"
    );
}

#[test]
fn postgres_row_value_not_in_subquery_is_not_rewritten_for_tsql() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) NOT IN (SELECT x, y FROM u WHERE q < 10)");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE NOT EXISTS(SELECT 1 FROM u WHERE (u.x = t.a OR u.x IS NULL OR t.a IS NULL) AND (u.y = t.b OR u.y IS NULL OR t.b IS NULL) AND q < 10)"
    );
}

#[test]
fn postgres_row_value_in_values_maps_to_tsql_exists() {
    let out = pg_to_tsql_strict(
        "SELECT * FROM onek WHERE (unique1, ten) IN (VALUES (1,1),(20,0),(99,9),(17,99)) ORDER BY unique1",
    );
    assert_eq!(
        out,
        "SELECT * FROM onek WHERE EXISTS(SELECT 1 FROM (VALUES (1, 1), (20, 0), (99, 9), (17, 99)) AS _polyglot_values(_polyglot_value_1, _polyglot_value_2) WHERE _polyglot_values._polyglot_value_1 = onek.unique1 AND _polyglot_values._polyglot_value_2 = onek.ten) ORDER BY CASE WHEN unique1 IS NULL THEN 1 ELSE 0 END, unique1"
    );
}

#[test]
fn postgres_row_value_not_in_values_maps_to_tsql_not_exists() {
    let out = pg_to_tsql_strict("SELECT a FROM t WHERE (a, b) NOT IN (VALUES (1, 2), (3, NULL))");
    assert_eq!(
        out,
        "SELECT a FROM t WHERE NOT EXISTS(SELECT 1 FROM (VALUES (1, 2), (3, NULL)) AS _polyglot_values(_polyglot_value_1, _polyglot_value_2) WHERE (_polyglot_values._polyglot_value_1 = t.a OR _polyglot_values._polyglot_value_1 IS NULL OR t.a IS NULL) AND (_polyglot_values._polyglot_value_2 = t.b OR _polyglot_values._polyglot_value_2 IS NULL OR t.b IS NULL))"
    );
}

#[test]
fn postgres_row_value_in_values_rewrites_inside_scalar_boolean_for_tsql() {
    let out = pg_to_tsql_strict("SELECT (a, b) IN (VALUES (1, 2), (3, 4)) AS matched FROM t");
    assert_eq!(
        out,
        "SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM (VALUES (1, 2), (3, 4)) AS _polyglot_values(_polyglot_value_1, _polyglot_value_2) WHERE _polyglot_values._polyglot_value_1 = t.a AND _polyglot_values._polyglot_value_2 = t.b) THEN 1 ELSE 0 END AS BIT) AS matched FROM t"
    );
}

#[test]
fn postgres_row_value_in_values_arity_mismatch_fails_tsql_strict_mode() {
    for sql in [
        "SELECT a FROM t WHERE (a, b) IN (VALUES (1), (2))",
        "SELECT a FROM t WHERE (a, b) IN (VALUES (1, 2), (3))",
    ] {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject mismatched row-value VALUES membership");

        assert!(
            err.to_string()
                .contains("row-value VALUES membership comparisons"),
            "unexpected error for {sql}: {err}"
        );
    }
}

#[test]
fn postgres_row_value_in_subquery_arity_mismatch_is_not_rewritten_for_tsql() {
    let out = pg_to_tsql("SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
    assert_eq!(out, "SELECT a FROM t WHERE (a, b) IN (SELECT x FROM u)");
}

#[test]
fn postgres_row_value_in_subquery_rewrites_cast_projections_for_tsql() {
    let out = pg_to_tsql_strict(
        "SELECT f1 FROM t WHERE (f1, f2) IN (SELECT CAST(a AS BIGINT), b FROM t2 WHERE q < 10)",
    );
    assert_eq!(
        out,
        "SELECT f1 FROM t WHERE EXISTS(SELECT 1 FROM t2 WHERE CAST(t2.a AS BIGINT) = t.f1 AND t2.b = t.f2 AND q < 10)"
    );
}

#[test]
fn postgres_row_value_in_subquery_rewrites_wrapped_subqueries_for_tsql() {
    let out = pg_to_tsql_strict("SELECT f1 FROM t WHERE (f1, f2) IN ((SELECT a, b FROM t2))");
    assert_eq!(
        out,
        "SELECT f1 FROM t WHERE EXISTS(SELECT 1 FROM t2 WHERE t2.a = t.f1 AND t2.b = t.f2)"
    );
}

#[test]
fn postgres_row_value_in_subquery_preserves_qualified_join_projections_for_tsql() {
    let out = pg_to_tsql_strict(
        "SELECT f1 FROM t WHERE (f1, f2) IN (SELECT u.a, v.b FROM u JOIN v ON u.id = v.id)",
    );
    assert_eq!(
        out,
        "SELECT f1 FROM t WHERE EXISTS(SELECT 1 FROM u JOIN v ON u.id = v.id WHERE u.a = t.f1 AND v.b = t.f2)"
    );
}

#[test]
fn postgres_row_value_equality_subquery_rewrites_in_predicate_contexts_for_tsql() {
    let out = pg_to_tsql_strict("SELECT f1 FROM t WHERE ROW(f1, f2) = (SELECT a, b FROM t2)");
    assert_eq!(
        out,
        "SELECT f1 FROM t WHERE EXISTS(SELECT 1 FROM t2 WHERE t2.a = t.f1 AND t2.b = t.f2)"
    );
}

#[test]
fn postgres_row_value_equality_subquery_rewrites_inside_scalar_boolean_for_tsql() {
    let out = pg_to_tsql_strict("SELECT ROW(1, 2) = (SELECT f1, f2) AS eq FROM t");
    assert_eq!(
        out,
        "SELECT CAST(CASE WHEN EXISTS(SELECT 1 WHERE f1 = 1 AND f2 = 2) THEN 1 ELSE 0 END AS BIT) AS eq FROM t"
    );
}

#[test]
fn postgres_row_value_subquery_arity_mismatch_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT f1 FROM t WHERE (f1, f2) IN (SELECT a FROM t2)",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject unrewriteable row-value comparisons");

    assert!(
        err.to_string().contains("row-value subquery comparisons"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL statistical aggregates -> T-SQL names
// ---------------------------------------------------------------------------

#[test]
fn postgres_statistical_aggregates_map_to_tsql_names() {
    let cases = [
        (
            "SELECT stddev_samp(x) AS s FROM t",
            "SELECT STDEV(x) AS s FROM t",
        ),
        (
            "SELECT stddev_pop(x) AS s FROM t",
            "SELECT STDEVP(x) AS s FROM t",
        ),
        (
            "SELECT var_samp(x) AS s FROM t",
            "SELECT VAR(x) AS s FROM t",
        ),
        (
            "SELECT var_pop(x) AS s FROM t",
            "SELECT VARP(x) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL boolean aggregates -> T-SQL CASE aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_boolean_aggregates_map_to_tsql_case_aggregates() {
    let cases = [
        (
            "SELECT bool_and(x > 0) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_or(x > 0) AS s FROM t",
            "SELECT CAST(MAX(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT every(x > 0) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_and(x) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x <> 0 THEN 1 WHEN NOT x <> 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_or(x > 0) FILTER (WHERE y > 0) AS s FROM t",
            "SELECT CAST(MAX(CASE WHEN y > 0 AND x > 0 THEN 1 WHEN y > 0 AND NOT x > 0 THEN 0 ELSE NULL END) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_and(x > 0) OVER (PARTITION BY p) AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) OVER (PARTITION BY p) AS BIT) AS s FROM t",
        ),
        (
            "SELECT bool_or(x > 0) FILTER (WHERE y > 0) OVER (PARTITION BY p) AS s FROM t",
            "SELECT CAST(MAX(CASE WHEN y > 0 AND x > 0 THEN 1 WHEN y > 0 AND NOT x > 0 THEN 0 ELSE NULL END) OVER (PARTITION BY p) AS BIT) AS s FROM t",
        ),
        (
            "SELECT every(x > 0) OVER () AS s FROM t",
            "SELECT CAST(MIN(CASE WHEN x > 0 THEN 1 WHEN NOT x > 0 THEN 0 ELSE NULL END) OVER () AS BIT) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_unsupported_aggregates_fail_for_tsql_in_strict_mode() {
    let cases = [
        ("SELECT bit_and(i4) FROM t", "BIT_AND"),
        ("SELECT bit_or(i4) FROM t", "BIT_OR"),
        ("SELECT bit_xor(i4) FROM t", "BIT_XOR"),
        ("SELECT regr_avgx(y, x) FROM t", "REGR_AVGX"),
        ("SELECT regr_avgy(y, x) FROM t", "REGR_AVGY"),
        ("SELECT regr_count(y, x) FROM t", "REGR_COUNT"),
        ("SELECT regr_intercept(y, x) FROM t", "REGR_INTERCEPT"),
        ("SELECT regr_r2(y, x) FROM t", "REGR_R2"),
        ("SELECT regr_slope(y, x) FROM t", "REGR_SLOPE"),
        ("SELECT regr_sxx(y, x) FROM t", "REGR_SXX"),
        ("SELECT regr_sxy(y, x) FROM t", "REGR_SXY"),
        ("SELECT regr_syy(y, x) FROM t", "REGR_SYY"),
        ("SELECT covar_pop(y, x) FROM t", "COVAR_POP"),
        ("SELECT covar_samp(y, x) FROM t", "COVAR_SAMP"),
        ("SELECT corr(y, x) FROM t", "CORR"),
    ];

    for (sql, expected) in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict mode should reject unsupported PostgreSQL aggregate");
        assert!(
            err.to_string().contains(expected),
            "expected {expected} in error for {sql}, got: {err}"
        );
    }
}

#[test]
fn postgres_scalar_boolean_values_map_to_tsql_case_values() {
    let cases = [
        (
            "SELECT (l_quantity > 30) AS b FROM tpch.lineitem",
            "SELECT CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT) AS b FROM tpch.lineitem",
        ),
        (
            "SELECT COUNT(*) AS c FROM tpch.lineitem GROUP BY (l_quantity > 30)",
            "SELECT COUNT_BIG(*) AS c FROM tpch.lineitem GROUP BY CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT)",
        ),
        (
            "SELECT (l_quantity > 30) AS b, COUNT(*) AS c FROM tpch.lineitem WHERE l_orderkey < 1000 GROUP BY (l_quantity > 30) ORDER BY b",
            "SELECT CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT) AS b, COUNT_BIG(*) AS c FROM tpch.lineitem WHERE l_orderkey < 1000 GROUP BY CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT) ORDER BY CASE WHEN b IS NULL THEN 1 ELSE 0 END, b",
        ),
        (
            "SELECT l_quantity FROM tpch.lineitem ORDER BY (l_quantity > 30)",
            "SELECT l_quantity FROM tpch.lineitem ORDER BY CASE WHEN CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT) IS NULL THEN 1 ELSE 0 END, CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT)",
        ),
        (
            "SELECT COUNT(*) OVER (PARTITION BY (l_quantity > 30)) AS c FROM tpch.lineitem",
            "SELECT COUNT_BIG(*) OVER (PARTITION BY CAST(CASE WHEN (l_quantity > 30) THEN 1 ELSE 0 END AS BIT)) AS c FROM tpch.lineitem",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_absolute_value_operator_maps_to_tsql_abs_function() {
    let cases = [
        (
            "SELECT f1, @ f1 AS abs_f1 FROM float4_tbl",
            "SELECT f1, ABS(f1) AS abs_f1 FROM float4_tbl",
        ),
        ("SELECT @ -5.0 AS x", "SELECT ABS(-5.0) AS x"),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_top_level_values_map_to_tsql_derived_table() {
    assert_eq!(
        pg_to_tsql("VALUES (1, 2), (3, 8)"),
        "SELECT * FROM (VALUES (1, 2), (3, 8)) AS _v(column1, column2)"
    );
}

#[test]
fn postgres_values_set_operation_arms_map_to_tsql_derived_tables() {
    let cases = [
        (
            "VALUES (1, 2), (3, 8) UNION ALL SELECT 4, 57",
            "SELECT * FROM (VALUES (1, 2), (3, 8)) AS _v(column1, column2) UNION ALL SELECT 4, 57",
        ),
        (
            "SELECT 4, 57 UNION ALL VALUES (1, 2), (3, 8)",
            "SELECT 4, 57 UNION ALL SELECT * FROM (VALUES (1, 2), (3, 8)) AS _v(column1, column2)",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_from_values_for_tsql_stays_derived_table() {
    assert_eq!(
        pg_to_tsql("SELECT * FROM (VALUES (1, 2), (3, 8)) AS v(a, b)"),
        "SELECT * FROM (VALUES (1, 2), (3, 8)) AS v(a, b)"
    );
}

#[test]
fn postgres_from_values_for_tsql_adds_required_column_aliases() {
    assert_eq!(
        pg_to_tsql_strict("SELECT * FROM (VALUES (1, 2), (3, 8)) AS v"),
        "SELECT * FROM (VALUES (1, 2), (3, 8)) AS v(column1, column2)"
    );
}

#[test]
fn postgres_predicate_boolean_contexts_stay_predicates_for_tsql() {
    let out = pg_to_tsql(
        "SELECT COUNT(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT(*) > 0",
    );
    assert_eq!(
        out,
        "SELECT COUNT_BIG(*) AS c FROM tpch.lineitem WHERE l_quantity > 30 HAVING COUNT_BIG(*) > 0"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL aggregate FILTER clauses -> T-SQL conditional aggregates
// ---------------------------------------------------------------------------

#[test]
fn postgres_aggregate_filters_map_to_tsql_conditional_aggregates() {
    let cases = [
        (
            "SELECT count(*) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN x > 5 THEN 1 END) AS c FROM t",
        ),
        (
            "SELECT count(value) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN x > 5 THEN value END) AS c FROM t",
        ),
        (
            "SELECT count(DISTINCT value) FILTER (WHERE x > 5) AS c FROM t",
            "SELECT COUNT_BIG(DISTINCT CASE WHEN x > 5 THEN value END) AS c FROM t",
        ),
        (
            "SELECT sum(v) FILTER (WHERE x > 5) AS s FROM t",
            "SELECT SUM(CASE WHEN x > 5 THEN v END) AS s FROM t",
        ),
        (
            "SELECT avg(v) FILTER (WHERE x > 5) AS a FROM t",
            "SELECT AVG(CASE WHEN x > 5 THEN v END) AS a FROM t",
        ),
        (
            "SELECT count(*) FILTER (WHERE flag = 'R') OVER (PARTITION BY g) AS c FROM t",
            "SELECT COUNT_BIG(CASE WHEN flag = 'R' THEN 1 END) OVER (PARTITION BY g) AS c FROM t",
        ),
        (
            "SELECT sum(v) FILTER (WHERE x > 5) OVER (PARTITION BY g) AS s FROM t",
            "SELECT SUM(CASE WHEN x > 5 THEN v END) OVER (PARTITION BY g) AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL STRING_AGG inline ORDER BY -> T-SQL WITHIN GROUP
// ---------------------------------------------------------------------------

#[test]
fn postgres_string_agg_order_by_maps_to_tsql_within_group() {
    let cases = [
        (
            "SELECT string_agg(name, ', ' ORDER BY name) AS s FROM t",
            "SELECT STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY CASE WHEN name IS NULL THEN 1 ELSE 0 END, name) AS s FROM t",
        ),
        (
            "SELECT string_agg(name, ', ' ORDER BY name DESC) AS s FROM t",
            "SELECT STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY CASE WHEN name IS NULL THEN 1 ELSE 0 END DESC, name DESC) AS s FROM t",
        ),
        (
            "SELECT string_agg(name, ', ') AS s FROM t",
            "SELECT STRING_AGG(name, ', ') AS s FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_from_only_drops_inheritance_modifier_for_tsql() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let sql = "SELECT AVG(gpa) FROM ONLY student AS s";

    assert_eq!(
        pg.transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .unwrap(),
        vec!["SELECT AVG(gpa) FROM student AS s"]
    );
    assert_eq!(
        pg.transpile(sql, DialectType::PostgreSQL).unwrap(),
        vec!["SELECT AVG(gpa) FROM ONLY student AS s"]
    );
}

#[test]
fn strict_postgres_distinct_string_agg_is_rejected_for_tsql() {
    let sql = "SELECT string_agg(DISTINCT f1, ',' ORDER BY f1) FROM t";
    let pg = Dialect::get(DialectType::PostgreSQL);
    let err = pg
        .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
        .expect_err("strict T-SQL transpilation should reject DISTINCT STRING_AGG");

    assert!(
        err.to_string().contains("STRING_AGG with DISTINCT"),
        "unexpected error: {err}"
    );
    assert_eq!(
        pg_to_tsql(sql),
        "SELECT STRING_AGG(DISTINCT f1, ',') WITHIN GROUP (ORDER BY CASE WHEN f1 IS NULL THEN 1 ELSE 0 END, f1) FROM t"
    );
    assert!(pg
        .transpile_with(
            "SELECT string_agg(f1, ',' ORDER BY f1) FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .is_ok());
}

#[test]
fn postgres_typed_string_agg_separator_maps_to_tsql_literal() {
    let cases = [
        (
            "SELECT string_agg(s, ','::text ORDER BY g) FROM t",
            "SELECT STRING_AGG(s, ',') WITHIN GROUP (ORDER BY CASE WHEN g IS NULL THEN 1 ELSE 0 END, g) FROM t",
        ),
        (
            "SELECT string_agg(s, ','::varchar) FROM t",
            "SELECT STRING_AGG(s, ',') FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_grouped_ordered_set_percentiles_map_to_tsql_windows() {
    let cases = [
        (
            "SELECT store_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY pick_seconds) AS med FROM order_fulfillment_history GROUP BY store_id",
            "SELECT DISTINCT store_id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pick_seconds) OVER (PARTITION BY store_id) AS med FROM order_fulfillment_history",
        ),
        (
            "SELECT store_id, percentile_disc(0.9) WITHIN GROUP (ORDER BY pick_seconds) AS p90 FROM order_fulfillment_history GROUP BY store_id",
            "SELECT DISTINCT store_id, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY pick_seconds) OVER (PARTITION BY store_id) AS p90 FROM order_fulfillment_history",
        ),
        (
            "SELECT store_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY pick_seconds) AS med, percentile_disc(0.9) WITHIN GROUP (ORDER BY pick_seconds DESC) AS p90 FROM order_fulfillment_history GROUP BY store_id",
            "SELECT DISTINCT store_id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pick_seconds) OVER (PARTITION BY store_id) AS med, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY pick_seconds DESC) OVER (PARTITION BY store_id) AS p90 FROM order_fulfillment_history",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_mode_within_group_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT store_id, mode() WITHIN GROUP (ORDER BY status) AS mode_status FROM order_fulfillment_history GROUP BY store_id",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject MODE() ordered-set aggregates for T-SQL");

    assert!(
        err.to_string().contains("MODE ordered-set aggregates"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// T-SQL TRY/CATCH structured traversal
// ---------------------------------------------------------------------------

#[test]
fn try_catch_parses_structured_bodies_and_generates_sql() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    assert_eq!(ast.len(), 1);

    let Expression::TryCatch(try_catch) = &ast[0] else {
        panic!("expected TRY/CATCH expression, got {:?}", ast[0]);
    };

    assert_eq!(try_catch.try_body.len(), 2);
    assert_eq!(try_catch.catch_body.as_ref().map(Vec::len), Some(1));

    let sql = Generator::sql(&ast[0]).expect("TRY/CATCH should generate");
    assert_eq!(
        sql,
        "BEGIN TRY INSERT INTO orders (id, amount) VALUES (1, 100.00); UPDATE inventory SET qty = qty - 1 WHERE product_id = 42; END TRY BEGIN CATCH INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE()); END CATCH"
    );
}

#[test]
fn try_catch_children_include_inner_statements() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    let children = ast[0].children();

    assert_eq!(children.len(), 3);
    assert!(matches!(children[0], Expression::Insert(_)));
    assert!(matches!(children[1], Expression::Update(_)));
    assert!(matches!(children[2], Expression::Insert(_)));
}

#[test]
fn try_catch_get_all_tables_finds_try_and_catch_tables() {
    let ast = Parser::parse_sql(TRY_CATCH_SQL).expect("TRY/CATCH should parse");
    let names: Vec<String> = get_all_tables(&ast[0])
        .into_iter()
        .filter_map(|table| match table {
            Expression::Table(table) => Some(table.name.name),
            _ => None,
        })
        .collect();

    assert_eq!(names, vec!["orders", "inventory", "error_log"]);
}

// ---------------------------------------------------------------------------
// DECLARE statement boundaries
// ---------------------------------------------------------------------------

#[test]
fn declare_table_variable_keeps_following_insert_as_second_statement() {
    let sql = "DECLARE @tmp TABLE (id INT, name VARCHAR(50)); \
               INSERT INTO @tmp SELECT id, name FROM employees;";
    let ast = parse(sql, DialectType::TSQL).expect("DECLARE TABLE batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Declare(_)));
    assert!(matches!(ast[1], Expression::Insert(_)));
    assert_eq!(
        generate_tsql(&ast[0]),
        "DECLARE @tmp TABLE (id INTEGER, name VARCHAR(50))"
    );
    assert_eq!(
        generate_tsql(&ast[1]),
        "INSERT INTO @tmp SELECT id, name FROM employees"
    );

    let names: Vec<String> = get_all_tables(&ast[1])
        .into_iter()
        .filter_map(|table| match table {
            Expression::Table(table) => Some(table.name.name),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["@tmp", "employees"]);
}

#[test]
fn declare_scalar_keeps_following_select_as_second_statement() {
    let ast = parse("DECLARE @x INT; SELECT @x;", DialectType::TSQL)
        .expect("DECLARE scalar batch should parse");

    assert_eq!(ast.len(), 2);
    assert!(matches!(ast[0], Expression::Declare(_)));
    assert!(matches!(ast[1], Expression::Select(_)));
    assert_eq!(generate_tsql(&ast[0]), "DECLARE @x INTEGER");
    assert_eq!(generate_tsql(&ast[1]), "SELECT @x");
}

// ---------------------------------------------------------------------------
// PostgreSQL function-style type casts -> T-SQL CAST
// ---------------------------------------------------------------------------

#[test]
fn postgres_approximate_numeric_casts_map_to_tsql_float_and_real() {
    let sql = "SELECT x::float8, y::double precision, z::real, w::float4 FROM t";
    let expected =
        "SELECT CAST(x AS FLOAT), CAST(y AS FLOAT), CAST(z AS REAL), CAST(w AS REAL) FROM t";

    assert_eq!(pg_to_tsql(sql), expected);
    assert_eq!(pg_to_tsql_strict(sql), expected);
}

#[test]
fn postgres_single_arg_round_uses_tsql_length_argument() {
    let cases = [
        (
            "SELECT f1, round(f1) AS round_f1 FROM float8_tbl",
            "SELECT f1, ROUND(f1, 0) AS round_f1 FROM float8_tbl",
        ),
        (
            "SELECT round(f1::numeric, 0) AS r FROM float8_tbl",
            "SELECT ROUND(CAST(f1 AS NUMERIC), 0) AS r FROM float8_tbl",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql_strict(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_bitwise_xor_maps_to_tsql_caret_operator() {
    let sql = "SELECT a # b FROM t";
    let expected = "SELECT a ^ b FROM t";

    assert_eq!(pg_to_tsql(sql), expected);
    assert_eq!(pg_to_tsql_strict(sql), expected);
}

#[test]
fn postgres_type_function_casts_map_to_tsql_casts() {
    let cases = [
        (
            "SELECT s, sum(v) + numeric(sum(g)) AS mixed FROM t GROUP BY s",
            "SELECT s, SUM(v) + CAST(SUM(g) AS NUMERIC) AS mixed FROM t GROUP BY s",
        ),
        (
            "SELECT decimal(5) AS d, numeric(5) AS n",
            "SELECT CAST(5 AS NUMERIC) AS d, CAST(5 AS NUMERIC) AS n",
        ),
        (
            "SELECT int4(1.5) AS a, float8(4) AS b",
            "SELECT CAST(1.5 AS INTEGER) AS a, CAST(4 AS FLOAT) AS b",
        ),
        (
            "SELECT int8(1) AS a, float4(4) AS b, bool(1) AS c, text(1) AS d",
            "SELECT CAST(1 AS BIGINT) AS a, CAST(4 AS REAL) AS b, CAST(1 AS BIT) AS c, CAST(1 AS VARCHAR(MAX)) AS d",
        ),
        (
            "SELECT smallint(1) AS s, integer(1) AS i, varchar(1) AS v, uuid(id) AS u FROM t",
            "SELECT CAST(1 AS SMALLINT) AS s, CAST(1 AS INTEGER) AS i, CAST(1 AS VARCHAR) AS v, CAST(id AS UNIQUEIDENTIFIER) AS u FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn postgres_type_function_cast_with_unsafe_arity_fails_tsql_strict_mode() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT numeric(1, 2) AS n",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict mode should reject residual PostgreSQL type-name function casts");

    assert!(
        err.to_string()
            .contains("PostgreSQL type-name function casts"),
        "unexpected error: {err}"
    );
}

#[test]
fn postgres_qualified_or_quoted_type_named_functions_are_not_casts_for_tsql() {
    let out = pg_to_tsql(r#"SELECT public.numeric(1) AS q, "numeric"(1) AS quoted"#);
    assert_eq!(out, "SELECT public.numeric(1) AS q, NUMERIC(1) AS quoted");
}

// ---------------------------------------------------------------------------
// BPCHAR → CHAR normalisation
// ---------------------------------------------------------------------------

#[test]
fn bpchar_cast_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR)");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_cast_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT CAST(x AS BPCHAR(3))");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_double_colon_no_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar");
    assert_eq!(out, "SELECT CAST(x AS CHAR)");
}

#[test]
fn bpchar_double_colon_with_length_maps_to_char() {
    let out = pg_to_tsql("SELECT x::bpchar(3)");
    assert_eq!(out, "SELECT CAST(x AS CHAR(3))");
}

#[test]
fn bpchar_ddl_column_no_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR)");
    assert_eq!(out, "CREATE TABLE t (x CHAR)");
}

#[test]
fn bpchar_ddl_column_with_length_maps_to_char() {
    let out = pg_to_tsql("CREATE TABLE t (x BPCHAR(3))");
    assert_eq!(out, "CREATE TABLE t (x CHAR(3))");
}

// ---------------------------------------------------------------------------
// PostgreSQL interval arithmetic -> T-SQL DATEADD
// ---------------------------------------------------------------------------

#[test]
fn date_minus_date_rewrites_to_datediff() {
    let cases = [
        (
            "SELECT '2020-01-01'::date - '2019-01-01'::date",
            "SELECT DATEDIFF(DAY, CAST('2019-01-01' AS DATE), CAST('2020-01-01' AS DATE))",
        ),
        (
            "SELECT DATE '2020-01-01' - DATE '2019-01-01'",
            "SELECT DATEDIFF(DAY, CAST('2019-01-01' AS DATE), CAST('2020-01-01' AS DATE))",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn strict_date_subtraction_with_unresolved_column_type_is_rejected() {
    let cases = [
        "SELECT (f1 - '2000-01-01'::date) AS days FROM date_tbl",
        "SELECT ('2020-01-01'::date - f1) AS days FROM t",
    ];

    for sql in cases {
        let err = Dialect::get(DialectType::PostgreSQL)
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict T-SQL transpilation should require the column type");
        assert!(
            err.to_string()
                .contains("date subtraction with an unresolved column type"),
            "unexpected error for {sql}: {err}"
        );
    }

    assert_eq!(
        pg_to_tsql("SELECT (f1 - '2000-01-01'::date) AS days FROM date_tbl"),
        "SELECT (f1 - CAST('2000-01-01' AS DATE)) AS days FROM date_tbl"
    );
    assert_eq!(
        pg_to_tsql("SELECT ('2020-01-01'::date - f1) AS days FROM t"),
        "SELECT (CAST('2020-01-01' AS DATE) - f1) AS days FROM t"
    );
}

#[test]
fn date_integer_arithmetic_rewrites_to_day_dateadd() {
    let cases = [
        (
            "SELECT DATE '2020-01-01' + 7",
            "SELECT DATEADD(DAY, 7, CAST('2020-01-01' AS DATE))",
        ),
        (
            "SELECT 7 + DATE '2020-01-01'",
            "SELECT DATEADD(DAY, 7, CAST('2020-01-01' AS DATE))",
        ),
        (
            "SELECT DATE '2020-01-01' - 7",
            "SELECT DATEADD(DAY, -7, CAST('2020-01-01' AS DATE))",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn schema_less_column_integer_arithmetic_is_not_treated_as_date_arithmetic() {
    let cases = [
        (
            "SELECT shipdate + 7 FROM lineitem",
            "SELECT shipdate + 7 FROM lineitem",
        ),
        (
            "SELECT shipdate - 7 FROM lineitem",
            "SELECT shipdate - 7 FROM lineitem",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn date_plus_abbreviated_time_intervals_rewrite_to_dateadd() {
    let cases = [
        (
            "SELECT ts + interval '30 min' AS c FROM t",
            "SELECT DATEADD(MINUTE, 30, ts) AS c FROM t",
        ),
        (
            "SELECT now() + interval '30 min' AS c FROM t",
            "SELECT DATEADD(MINUTE, 30, GETDATE()) AS c FROM t",
        ),
        (
            "SELECT now() - interval '30 min' AS c FROM t",
            "SELECT DATEADD(MINUTE, -30, GETDATE()) AS c FROM t",
        ),
        (
            "SELECT ts + interval '5 sec' AS c FROM t",
            "SELECT DATEADD(SECOND, 5, ts) AS c FROM t",
        ),
        (
            "SELECT now() + interval '3 hrs' AS c FROM t",
            "SELECT DATEADD(HOUR, 3, GETDATE()) AS c FROM t",
        ),
        (
            "SELECT now() + interval '4 mins' AS c FROM t",
            "SELECT DATEADD(MINUTE, 4, GETDATE()) AS c FROM t",
        ),
        (
            "SELECT now() + interval '5 secs' AS c FROM t",
            "SELECT DATEADD(SECOND, 5, GETDATE()) AS c FROM t",
        ),
    ];

    for (sql, expected) in cases {
        assert_eq!(pg_to_tsql(sql), expected, "failed for {sql}");
    }
}

#[test]
fn date_plus_cast_abbreviated_interval_rewrites_to_dateadd() {
    let out = pg_to_tsql("SELECT shipdate + CAST('4 mins' AS INTERVAL) FROM lineitem");
    assert_eq!(out, "SELECT DATEADD(MINUTE, 4, shipdate) FROM lineitem");
    assert_eq!(
        pg_to_tsql_strict("SELECT shipdate + CAST('4 mins' AS INTERVAL) FROM lineitem"),
        "SELECT DATEADD(MINUTE, 4, shipdate) FROM lineitem"
    );
}

#[test]
fn scalar_interval_cast_maps_to_tsql_varchar_max_by_default() {
    let out = pg_to_tsql("SELECT x::interval FROM t");
    assert_eq!(out, "SELECT CAST(x AS VARCHAR(MAX)) FROM t");
}

#[test]
fn strict_scalar_interval_cast_rejects_for_tsql() {
    let err = Dialect::get(DialectType::PostgreSQL)
        .transpile_with(
            "SELECT x::interval FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict T-SQL transpilation should reject scalar INTERVAL casts");

    assert!(err.to_string().contains("INTERVAL casts"));
}

// ---------------------------------------------------------------------------
// = ANY(ARRAY[...]) / = ANY((...)) → IN
// ---------------------------------------------------------------------------

#[test]
fn any_eq_array_brackets_rewrites_to_in() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(ARRAY['a', 'b', 'c'])");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_tuple_rewrites_to_in() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(('a', 'b', 'c'))");
    assert_eq!(out, "SELECT * FROM t WHERE col IN ('a', 'b', 'c')");
}

#[test]
fn any_eq_empty_array_rewrites_to_always_false() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(ARRAY[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_outer_array_cast_rewrites_to_in_with_element_casts() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY((ARRAY['a', 'b'])::text[])");
    assert_eq!(
        out,
        "SELECT * FROM t WHERE col IN (CAST('a' AS VARCHAR(MAX)), CAST('b' AS VARCHAR(MAX)))"
    );
}

#[test]
fn any_eq_outer_array_cast_empty_rewrites_to_always_false() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY((ARRAY[])::text[])");
    assert_eq!(out, "SELECT * FROM t WHERE 1 = 0");
}

#[test]
fn any_eq_ruleutils_outer_array_cast_rewrites_to_in() {
    let out = pg_to_tsql(
        "SELECT s FROM t WHERE ((s)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))",
    );
    assert_eq!(
        out,
        "SELECT s FROM t WHERE (CAST((s) AS VARCHAR(MAX)) IN (CAST(CAST('a' AS VARCHAR) AS VARCHAR(MAX)), CAST(CAST('b' AS VARCHAR) AS VARCHAR(MAX))))"
    );
}

#[test]
fn any_neq_array_not_rewritten() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
    assert_eq!(out, "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])");
}

#[test]
fn strict_any_neq_array_rejects_non_subquery_rhs() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let err = pg
        .transpile_with(
            "SELECT * FROM t WHERE col <> ANY(ARRAY['a', 'b'])",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict T-SQL transpilation should reject ANY over array literals");

    assert!(err
        .to_string()
        .contains("ANY over non-subquery expressions"));
}

#[test]
fn strict_any_array_agg_in_case_wrapper_rejects_unsupported_array_semantics() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let err = pg
        .transpile_with(
            "SELECT (1 = any(array_agg(f1))) = any (SELECT false) FROM t",
            DialectType::TSQL,
            TranspileOptions::strict(),
        )
        .expect_err("strict T-SQL transpilation should reject ANY over ARRAY_AGG");

    let message = err.to_string();
    assert!(message.contains("ARRAY_AGG"), "unexpected error: {message}");
    assert!(
        message.contains("ANY over non-subquery expressions"),
        "unexpected error: {message}"
    );
}

#[test]
fn strict_postgres_aggregate_support_functions_and_array_casts_are_rejected() {
    let pg = Dialect::get(DialectType::PostgreSQL);
    let cases = [
        (
            "SELECT float8_accum('{4,140,2900}'::float8[], 100) FROM t",
            "FLOAT8_ACCUM",
        ),
        (
            "SELECT float8_regr_accum('{4,140,2900}'::float8[], 100, 10) FROM t",
            "FLOAT8_REGR_ACCUM",
        ),
        (
            "SELECT float8_combine('{4,140,2900}'::float8[], '{1,2,3}'::float8[]) FROM t",
            "FLOAT8_COMBINE",
        ),
        (
            "SELECT float8_regr_combine('{4,140,2900}'::float8[], '{1,2,3}'::float8[]) FROM t",
            "FLOAT8_REGR_COMBINE",
        ),
        (
            "SELECT booland_statefunc(true, true) FROM t",
            "BOOLAND_STATEFUNC",
        ),
        (
            "SELECT boolor_statefunc(false, true) FROM t",
            "BOOLOR_STATEFUNC",
        ),
        ("SELECT '{1,2,3}'::float8[]", "array data types"),
    ];

    for (sql, expected) in cases {
        let err = pg
            .transpile_with(sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict T-SQL transpilation should reject PostgreSQL aggregate state SQL");
        assert!(
            err.to_string().contains(expected),
            "unexpected error for {sql}: {err}"
        );
    }

    assert_eq!(
        pg_to_tsql("SELECT booland_statefunc(true, true) FROM t"),
        "SELECT BOOLAND_STATEFUNC(1, 1) FROM t"
    );
}

#[test]
fn strict_postgres_collations_are_rejected_for_tsql() {
    let pg = Dialect::get(DialectType::PostgreSQL);

    for collation in ["C", "POSIX"] {
        let sql = format!(r#"SELECT foo COLLATE "{collation}" FROM v"#);
        let err = pg
            .transpile_with(&sql, DialectType::TSQL, TranspileOptions::strict())
            .expect_err("strict T-SQL transpilation should reject PostgreSQL collations");
        assert!(
            err.to_string()
                .contains(&format!(r#"PostgreSQL collation "{collation}""#)),
            "unexpected error for {sql}: {err}"
        );
    }

    assert_eq!(
        pg_to_tsql(r#"SELECT foo COLLATE "C" FROM v"#),
        r#"SELECT foo COLLATE "C" FROM v"#
    );
}

#[test]
fn postgres_any_value_lowers_to_max_for_tsql() {
    assert_eq!(
        pg_to_tsql("SELECT ANY_VALUE(v) FROM t"),
        "SELECT MAX(v) FROM t"
    );
    assert_eq!(
        pg_to_tsql_strict("SELECT ANY_VALUE(v) FROM t"),
        "SELECT MAX(v) FROM t"
    );
}

#[test]
fn any_eq_subquery_not_rewritten() {
    let out = pg_to_tsql("SELECT * FROM t WHERE col = ANY(SELECT id FROM s)");
    assert_eq!(out, "SELECT * FROM t WHERE col = ANY (SELECT id FROM s)");
}
