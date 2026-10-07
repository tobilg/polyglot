//! DuckDB `//` integer division: tokenizes to a single operator (so `7 / / 2`
//! and separated/commented slashes are rejected, matching DuckDB), parses to
//! `IntDiv` with correct precedence, and round-trips or lowers per target --
//! preserving fractional operands, exact integer arithmetic, and NULL on zero.
use polyglot_sql::{parse_one, transpile, transpile_with_by_name, DialectType, TranspileOptions};

#[test]
fn duckdb_int_div_round_trips() {
    let out = transpile(
        "SELECT 7 // 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 // 2 AS v"]);
}

#[test]
fn duckdb_int_div_transpiles_like_other_integer_division() {
    // Preserve DuckDB's NULL-on-zero semantics as well as truncation.
    for target in [DialectType::PostgreSQL, DialectType::BigQuery] {
        let out = transpile("SELECT 7 // 2 AS v", DialectType::DuckDB, target).unwrap();
        assert_eq!(
            out,
            vec!["SELECT DIV(7, NULLIF(2, 0)) AS v"],
            "target {target:?}"
        );
    }
}

#[test]
fn duckdb_int_div_emulated_for_sqlite() {
    // SQLite has no DIV function, so // is emulated as truncating division.
    let out = transpile(
        "SELECT 7 // 2 AS v",
        DialectType::DuckDB,
        DialectType::SQLite,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT CAST(7 / NULLIF(2, 0) AS INTEGER) AS v"]);
}

#[test]
fn duckdb_int_div_binds_tighter_than_addition() {
    let out = transpile(
        "SELECT 1 + 7 // 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 1 + 7 // 2 AS v"]);
}

#[test]
fn duckdb_int_div_precedence_is_unambiguous_in_the_ast() {
    // `1 + 7 // 2` round-trips the same either way `+`/`//` group (7), so it
    // doesn't actually prove precedence -- assert the AST shape directly,
    // and use `2 + 7 // 2` (5 if // binds tighter, 4 if + does) to show the
    // two groupings aren't interchangeable.
    use polyglot_sql::Expression;
    let expr = parse_one("SELECT 2 + 7 // 2 AS v", DialectType::DuckDB).unwrap();
    let Expression::Select(select) = &expr else {
        panic!("expected a SELECT");
    };
    let Expression::Alias(alias) = &select.expressions[0] else {
        panic!("expected an aliased projection");
    };
    let Expression::Add(add) = &alias.this else {
        panic!(
            "expected the top-level operator to be +, got {:?}",
            alias.this
        );
    };
    assert!(
        matches!(add.right, Expression::IntDiv(_)),
        "expected // to bind tighter than + (Add(2, IntDiv(7, 2))), got {add:?}"
    );
}

#[test]
fn duckdb_int_div_rejects_separated_slashes() {
    // DuckDB requires a contiguous `//`; `a / / b` and `a / /* c */ / b` are
    // invalid DuckDB SQL and must not be accepted as integer division.
    assert!(parse_one("SELECT 7 / / 2", DialectType::DuckDB).is_err());
    assert!(parse_one("SELECT 7 / /* c */ / 2", DialectType::DuckDB).is_err());
}

#[test]
fn duckdb_int_div_on_float_operand_lowers_to_float_division() {
    // DuckDB's // is ordinary float division when either operand is
    // non-integer (`7.0 // 2` is `3.5`, not `3`), so it lowers to `/` --
    // never to a truncating DIV/intDiv/CAST(... AS INTEGER) -- on every
    // target, including Vertica's own `//`. Negated, parenthesized, and
    // exponent-form literals count too.
    for target in [
        DialectType::PostgreSQL,
        DialectType::BigQuery,
        DialectType::SQLite,
        DialectType::Vertica,
        DialectType::ClickHouse,
        DialectType::MySQL,
        DialectType::DataFusion,
    ] {
        for sql in [
            "SELECT 7.0 // 2 AS v",
            "SELECT -7.0 // 2 AS v",
            "SELECT (7.0) // 2 AS v",
            "SELECT 7e0 // 2 AS v",
            "SELECT 7 // 2.0 AS v",
            "SELECT CAST(7 AS DOUBLE) // 2 AS v",
            "SELECT CAST(7 AS FLOAT) // 2 AS v",
            "SELECT CAST(7 AS DECIMAL(10, 1)) // 2 AS v",
            "SELECT (7.0 + 0) // 2 AS v",
            "SELECT (7 // 2.0) // 2 AS v",
        ] {
            if target == DialectType::Vertica && sql == "SELECT CAST(7 AS FLOAT) // 2 AS v" {
                // Vertica rejects narrowing numeric casts independently of //.
                let error = transpile(sql, DialectType::DuckDB, target).unwrap_err();
                assert!(error.to_string().contains("Narrow numeric CAST"));
                continue;
            }
            let out = transpile(sql, DialectType::DuckDB, target)
                .unwrap_or_else(|e| panic!("{sql} -> {target:?}: {e}"))
                .remove(0);
            assert!(out.contains(" / "), "{sql} -> {target:?}: got {out}");
            for truncating in ["DIV", "intDiv", "AS INTEGER", "//"] {
                assert!(
                    !out.contains(truncating),
                    "{sql} -> {target:?}: still truncates: {out}"
                );
            }
        }
    }
    // The int/int case is unaffected.
    assert_eq!(
        transpile(
            "SELECT 7 // 2 AS v",
            DialectType::DuckDB,
            DialectType::Vertica
        )
        .unwrap(),
        vec!["SELECT 7 // NULLIF(2, 0) AS v"]
    );
}

#[test]
fn duckdb_int_div_protects_zero_divisors() {
    for target in [
        DialectType::PostgreSQL,
        DialectType::BigQuery,
        DialectType::ClickHouse,
        DialectType::SQLite,
    ] {
        for divisor in [
            "0",
            "(0)",
            "-0",
            "CAST(0 AS INTEGER)",
            "(1 - 1)",
            "CAST(z AS INTEGER)",
        ] {
            let sql = format!("SELECT 7 // {divisor} AS v FROM t");
            let out = transpile(&sql, DialectType::DuckDB, target).unwrap();
            assert!(out[0].contains("NULLIF("), "{sql} -> {target:?}: {out:?}");
        }
    }
}

#[test]
fn duckdb_int_div_rejects_unresolved_types_in_every_mode() {
    for options in [TranspileOptions::default(), TranspileOptions::strict()] {
        for target in ["postgresql", "bigquery", "clickhouse", "sqlite"] {
            for sql in [
                "SELECT x // 2 FROM t",
                "SELECT x // 2 FROM (SELECT 7.0 AS x) t",
                "SELECT 7.0 // x FROM t",
                "SELECT (x + 7.0) // 2 FROM t",
            ] {
                let error = transpile_with_by_name(sql, "duckdb", target, &options).unwrap_err();
                assert!(
                    error.to_string().contains("unresolved operand types"),
                    "{sql}: {error}"
                );
            }
        }
    }
    // Explicitly typed columns remain usable without requiring a schema.
    let out = transpile_with_by_name(
        "SELECT CAST(x AS DOUBLE) // CAST(y AS INTEGER) FROM t",
        "duckdb",
        "sqlite",
        &TranspileOptions::strict(),
    )
    .unwrap();
    assert!(!out[0].starts_with("SELECT CAST(CAST("), "{out:?}");
    assert!(out[0].contains("NULLIF("), "{out:?}");
}

#[test]
fn duckdb_int_div_preserves_nested_operators_and_large_integer_sql() {
    for sql in [
        "SELECT 8 // 2 // 2 AS v",
        "SELECT 8 // (4 // 2) AS v",
        "SELECT ROUND(8 // 2 // 2, 0) AS v",
        "SELECT 9007199254740995 // 2 AS v",
        "SELECT -9007199254740995 // 2 AS v",
    ] {
        let out =
            transpile_with_by_name(sql, "duckdb", "sqlite", &TranspileOptions::strict()).unwrap();
        assert!(!out[0].contains("DIV("), "{sql}: {out:?}");
        assert!(!out[0].contains("AS REAL"), "{sql}: {out:?}");
        parse_one(&out[0], DialectType::SQLite).unwrap();
    }
    let out = transpile_with_by_name(
        "SELECT 8 // (4 // 0) AS v",
        "duckdb",
        "postgresql",
        &TranspileOptions::strict(),
    )
    .unwrap();
    assert_eq!(
        out,
        ["SELECT DIV(8, NULLIF((DIV(4, NULLIF(0, 0))), 0)) AS v"]
    );
}

#[test]
fn duckdb_fractional_int_div_protects_zero_for_clickhouse() {
    for sql in [
        "SELECT 7.0 // 0",
        "SELECT 7 // 0.0",
        "SELECT 7e0 // -0",
        "SELECT CAST(7 AS DOUBLE) // CAST(z AS DOUBLE) FROM t",
    ] {
        let out = transpile_with_by_name(sql, "duckdb", "clickhouse", &TranspileOptions::strict())
            .unwrap();
        assert!(out[0].contains(" / NULLIF("), "{sql}: {out:?}");
        assert!(!out[0].contains("intDiv"), "{sql}: {out:?}");
    }
}

#[test]
fn duckdb_integer_division_rejects_targets_without_a_lowering() {
    for target in [
        DialectType::MySQL,
        DialectType::DataFusion,
        DialectType::Snowflake,
    ] {
        assert!(transpile("SELECT 7 // 2", DialectType::DuckDB, target).is_err());
    }
}

#[test]
#[ignore = "requires POLYGLOT_DUCKDB and POLYGLOT_SQLITE CLI paths"]
fn duckdb_integer_division_sqlite_execution() {
    use std::process::Command;
    let duckdb = std::env::var("POLYGLOT_DUCKDB").expect("set POLYGLOT_DUCKDB");
    let sqlite = std::env::var("POLYGLOT_SQLITE").expect("set POLYGLOT_SQLITE");
    for (sql, expected) in [
        ("SELECT 7 // 2 AS v", "3"),
        ("SELECT CAST(7 AS DOUBLE) // 2 AS v", "3.5"),
        ("SELECT CAST(7 AS DECIMAL(10, 1)) // 2 AS v", "3.5"),
        ("SELECT (7.0 + 0) // 2 AS v", "3.5"),
        ("SELECT (7 // 2.0) // 2 AS v", "1.75"),
        (
            "SELECT CAST(x AS DOUBLE) // 2 AS v FROM (SELECT 7.0 AS x) t",
            "3.5",
        ),
        ("SELECT 8 // 2 // 2 AS v", "2"),
        ("SELECT 8 // (4 // 2) AS v", "4"),
        ("SELECT 9007199254740995 // 2 AS v", "4503599627370497"),
        ("SELECT -9007199254740995 // 2 AS v", "-4503599627370497"),
        (
            "SELECT 9223372036854775807 // 2 AS v",
            "4611686018427387903",
        ),
        (
            "SELECT -9223372036854775808 // 2 AS v",
            "-4611686018427387904",
        ),
        ("SELECT 7 // 0 AS v", "NULL"),
        ("SELECT 7 // (0) AS v", "NULL"),
        ("SELECT 7 // -0 AS v", "NULL"),
        ("SELECT 7 // CAST(0 AS INTEGER) AS v", "NULL"),
        ("SELECT 7 // (1 - 1) AS v", "NULL"),
        ("SELECT 7.0 // 0 AS v", "NULL"),
        ("SELECT 7 // 0.0 AS v", "NULL"),
        ("SELECT 8 // (4 // 0) AS v", "NULL"),
        (
            "SELECT 7 // CAST(z AS INTEGER) AS v FROM (SELECT 0 AS z) t",
            "NULL",
        ),
    ] {
        let generated =
            transpile_with_by_name(sql, "duckdb", "sqlite", &TranspileOptions::strict()).unwrap();
        for (engine, query, args) in [
            (
                &duckdb,
                sql,
                vec![
                    "-init",
                    "/dev/null",
                    "-noheader",
                    "-csv",
                    "-nullvalue",
                    "NULL",
                    ":memory:",
                ],
            ),
            (
                &sqlite,
                generated[0].as_str(),
                vec!["-noheader", "-csv", "-nullvalue", "NULL", ":memory:"],
            ),
        ] {
            let result = Command::new(engine).args(args).arg(query).output().unwrap();
            assert!(
                result.status.success(),
                "{query}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert_eq!(
                String::from_utf8_lossy(&result.stdout).trim(),
                expected,
                "{engine}: {query}"
            );
        }
    }
}

#[test]
fn other_dialects_integer_division_is_unaffected() {
    // The float/zero guards are about DuckDB's // semantics specifically.
    // MySQL's and Vertica's integer division genuinely truncates float
    // operands, so their existing lowering must keep working.
    assert_eq!(
        transpile("SELECT 7.5 DIV 2", DialectType::MySQL, DialectType::MySQL).unwrap(),
        vec!["SELECT DIV(7.5, 2)"]
    );
    assert_eq!(
        transpile(
            "SELECT 7.5 // 2",
            DialectType::Vertica,
            DialectType::PostgreSQL
        )
        .unwrap(),
        vec!["SELECT DIV(7.5, 2)"]
    );
}

#[test]
fn plain_division_is_unchanged_in_duckdb_and_elsewhere() {
    let out = transpile(
        "SELECT 7 / 2 AS v",
        DialectType::DuckDB,
        DialectType::DuckDB,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 / 2 AS v"]);
    let out = transpile(
        "SELECT 7 / 2 AS v",
        DialectType::PostgreSQL,
        DialectType::PostgreSQL,
    )
    .unwrap();
    assert_eq!(out, vec!["SELECT 7 / 2 AS v"]);
}
