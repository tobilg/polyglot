//! Impala cases that do not fit the JSON identity fixtures.
//!
//! Negative parses and AST checks live here. A live Impala 4.5.3 run is
//! enabled only when `IMPALA_LIVE=1` is set; otherwise the test returns
//! without touching Docker.

use polyglot_sql::dialects::Dialect;
use polyglot_sql::expressions::{Expression, HintExpression};
use polyglot_sql::DialectType;

fn dialect() -> Dialect {
    Dialect::get(DialectType::Impala)
}

fn generated(sql: &str) -> String {
    let dialect = dialect();
    let parsed = dialect.parse(sql).expect("Impala SQL should parse");
    let statement = parsed.first().expect("expected one statement");
    let transformed = dialect
        .transform(statement.clone())
        .expect("Impala transform should succeed");
    dialect
        .generate_with_source(&transformed, DialectType::Impala)
        .expect("Impala generation should succeed")
}

#[test]
fn numeric_literal_without_suffix_is_an_alias() {
    let sql = generated("SELECT 1 d");
    let upper = sql.to_ascii_uppercase();
    assert!(!upper.contains("DOUBLE"), "{sql}");
    assert!(upper.contains("SELECT 1"), "{sql}");
    assert!(upper.split_whitespace().any(|part| part == "D"), "{sql}");
}

#[test]
fn char_and_varchar_keep_their_length() {
    let varchar_sql = generated("SELECT CAST(a AS VARCHAR(10))");
    assert!(
        varchar_sql.to_ascii_uppercase().contains("VARCHAR(10)"),
        "{varchar_sql}"
    );
    let char_sql = generated("SELECT CAST(a AS CHAR(4))");
    assert!(
        char_sql.to_ascii_uppercase().contains("CHAR(4)"),
        "{char_sql}"
    );
}

#[test]
fn lateral_view_is_rejected() {
    let err = dialect()
        .parse("SELECT a FROM t LATERAL VIEW EXPLODE(arr) x AS y")
        .expect_err("LATERAL VIEW is not Impala syntax");
    let message = err.to_string().to_ascii_uppercase();
    assert!(message.contains("LATERAL VIEW"), "{err}");
}

#[test]
fn query_organize_clauses_are_not_accepted() {
    for sql in [
        "SELECT a FROM t SORT BY a",
        "SELECT a FROM t CLUSTER BY a",
        "SELECT a FROM t DISTRIBUTE BY a",
    ] {
        assert!(
            dialect().parse(sql).is_err(),
            "{sql} should not parse as an Impala query"
        );
    }
}

#[test]
fn create_table_sort_by_still_parses() {
    generated("CREATE TABLE t (id INT) SORT BY (id)");
}

#[test]
fn upsert_into_table_uses_the_table_name() {
    let parsed = dialect()
        .parse("UPSERT INTO TABLE t SELECT 1")
        .expect("UPSERT should parse");
    let Expression::Insert(insert) = &parsed[0] else {
        panic!("expected Insert, got {:?}", parsed[0]);
    };
    assert!(insert.is_upsert);
    assert!(insert.explicit_table_keyword);
    assert_eq!(insert.table.name.name, "t");
    assert_eq!(generated("UPSERT INTO TABLE t SELECT 1"), "UPSERT INTO TABLE t SELECT 1");
    assert_eq!(generated("UPSERT INTO t SELECT 1"), "UPSERT INTO t SELECT 1");
}

#[test]
fn join_bracket_hint_stays_on_that_join() {
    let parsed = dialect()
        .parse("SELECT a FROM t1 JOIN [SHUFFLE] t2 ON t1.id = t2.id")
        .expect("join hint should parse");
    let Expression::Select(select) = &parsed[0] else {
        panic!("expected Select");
    };
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].join_hint.as_deref(), Some("SHUFFLE"));
    assert_eq!(
        generated("SELECT a FROM t1 JOIN [SHUFFLE] t2 ON t1.id = t2.id"),
        "SELECT a FROM t1 JOIN [SHUFFLE] t2 ON t1.id = t2.id"
    );
}

#[test]
fn join_noshuffle_is_rejected() {
    let err = dialect()
        .parse("SELECT a FROM t1 JOIN [NOSHUFFLE] t2 ON t1.id = t2.id")
        .expect_err("NOSHUFFLE is not valid on JOIN");
    assert!(err.to_string().contains("NOSHUFFLE"), "{err}");
}

#[test]
fn select_bracket_hint_round_trips() {
    assert_eq!(generated("SELECT [SHUFFLE] a FROM t"), "SELECT [SHUFFLE] a FROM t");
    assert_eq!(
        generated("SELECT DISTINCT STRAIGHT_JOIN a FROM t"),
        "SELECT DISTINCT STRAIGHT_JOIN a FROM t"
    );
    assert_eq!(
        generated("SELECT a FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.id"),
        "SELECT STRAIGHT_JOIN a FROM t1 JOIN t2 ON t1.id = t2.id"
    );
}

#[test]
fn insert_table_keyword_and_hints() {
    assert_eq!(
        generated("INSERT INTO t SELECT 1"),
        "INSERT INTO TABLE t SELECT 1"
    );
    assert_eq!(
        generated("INSERT [SHUFFLE] INTO TABLE t SELECT 1"),
        "INSERT [SHUFFLE] INTO TABLE t SELECT 1"
    );
    assert_eq!(
        generated("INSERT INTO TABLE t [NOSHUFFLE] SELECT 1"),
        "INSERT INTO TABLE t [NOSHUFFLE] SELECT 1"
    );
}

#[test]
fn native_functions_keep_impala_spelling_or_canonical_form() {
    assert_eq!(generated("SELECT NDV(x) FROM t"), "SELECT NDV(x) FROM t");
    assert_eq!(
        generated("SELECT ZEROIFNULL(x) FROM t"),
        "SELECT COALESCE(x, 0) FROM t"
    );
    assert_eq!(
        generated("SELECT NULLIFZERO(x) FROM t"),
        "SELECT NULLIF(x, 0) FROM t"
    );
    assert_eq!(
        generated("SELECT ISNULL(a, b) FROM t"),
        "SELECT COALESCE(a, b) FROM t"
    );
    assert_eq!(generated("SELECT APPX_MEDIAN(x) FROM t"), "SELECT APPX_MEDIAN(x) FROM t");
    assert_eq!(
        generated("SELECT DATE_SUB(ts, 6) FROM t"),
        "SELECT DATE_ADD(ts, 6 * -1) FROM t"
    );
    assert_eq!(generated("SELECT a ILIKE b"), "SELECT a ILIKE b");
    assert_eq!(generated("SELECT a RLIKE b"), "SELECT a RLIKE b");
    assert_eq!(generated("SELECT a IREGEXP b"), "SELECT a IREGEXP b");
}

#[test]
fn bracket_hint_is_impala_scoped() {
    let parsed = dialect()
        .parse("SELECT [SHUFFLE] a FROM t")
        .expect("select hint should parse");
    let Expression::Select(select) = &parsed[0] else {
        panic!("expected Select");
    };
    let hint = select.hint.as_ref().expect("hint");
    assert_eq!(hint.source_dialect, Some(DialectType::Impala));
    assert!(matches!(
        hint.expressions.first(),
        Some(HintExpression::Identifier(name)) if name == "SHUFFLE"
    ));
}

#[test]
fn set_with_unit_suffix_keeps_the_suffix() {
    assert_eq!(generated("SET MEM_LIMIT=128g"), "SET MEM_LIMIT=128g");
}

/// Sends a tiny statement to the shared Impala 4.5.3 coordinator when
/// `IMPALA_LIVE=1`. Does not start, reset, or reconfigure the cluster.
#[test]
fn live_impala_accepts_a_literal_select_when_enabled() {
    if std::env::var("IMPALA_LIVE").ok().as_deref() != Some("1") {
        return;
    }
    let output = std::process::Command::new("docker")
        .args([
            "exec",
            "mktdm-impala-shell",
            "impala-shell",
            "--protocol=hs2",
            "-i",
            "impalad:21050",
            "--query_option=NUM_NODES=1",
            "--query_option=DISABLE_CODEGEN=1",
            "-q",
            "SELECT 1",
        ])
        .output()
        .expect("docker exec mktdm-impala-shell");
    let text = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "live Impala SELECT 1 failed\nstdout: {text}\nstderr: {err}"
    );
    assert!(text.contains("4.5.3") || text.contains('1'), "{text}");
}
