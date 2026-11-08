//! EXPLAIN options are syntax and execution semantics, not disposable metadata.
use polyglot_sql::expressions::{Explain, ExplainExecution, ExplainOptionKind, Expression};
use polyglot_sql::{transpile, Dialect, DialectType};

fn explain(sql: &str) -> Explain {
    let mut ast = Dialect::get(DialectType::PostgreSQL)
        .parse(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let Expression::Explain(node) = ast.remove(0) else {
        panic!("Expected Explain: {sql}")
    };
    *node
}

fn pg(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::PostgreSQL)
        .unwrap()
        .join("; ")
}

#[test]
fn preserves_all_postgres_options_and_values() {
    let sql = "EXPLAIN (ANALYZE FALSE, VERBOSE, COSTS OFF, BUFFERS ON, SETTINGS TRUE, WAL FALSE, TIMING 0, SUMMARY 1, MEMORY, GENERIC_PLAN OFF, SERIALIZE BINARY, FORMAT JSON) SELECT * FROM t";
    assert_eq!(pg(sql), sql);
    let node = explain(sql);
    assert_eq!(node.options.len(), 12);
    assert_eq!(node.execution_mode(), ExplainExecution::PlanOnly);
    assert!(matches!(node.statement(), Some(Expression::Select(_))));
    assert_eq!(node.options[0].boolean_value(), Some(false));
    assert_eq!(node.options[10].kind, ExplainOptionKind::Serialize);
    let ast = Expression::Explain(Box::new(node));
    let json = serde_json::to_string(&ast).unwrap();
    assert_eq!(serde_json::from_str::<Expression>(&json).unwrap(), ast);
}

#[test]
fn analyze_execution_mode_includes_explicit_false_and_legacy_syntax() {
    for (clause, executes) in [
        ("", false),
        ("ANALYZE", true),
        ("ANALYZE VERBOSE", true),
        ("VERBOSE", false),
        ("(ANALYZE)", true),
        ("(ANALYZE TRUE)", true),
        ("(ANALYZE ON)", true),
        ("(ANALYZE 1)", true),
        ("(ANALYZE FALSE)", false),
        ("(ANALYZE OFF)", false),
        ("(ANALYZE 0)", false),
    ] {
        let sql = format!("EXPLAIN {clause} SELECT 1");
        assert_eq!(
            explain(&sql).execution_mode(),
            if executes {
                ExplainExecution::Executes
            } else {
                ExplainExecution::PlanOnly
            },
            "{sql}"
        );
        let generated = pg(&sql);
        assert_eq!(
            explain(&generated).execution_mode(),
            explain(&sql).execution_mode()
        );
    }
    assert!(Dialect::get(DialectType::PostgreSQL)
        .parse("EXPLAIN (ANALYZE invalid) SELECT 1")
        .is_err());
    assert!(Dialect::get(DialectType::PostgreSQL)
        .parse("EXPLAIN (VERBOSE FALSE extra) SELECT 1")
        .is_err());
}

#[test]
fn statement_targets_are_structured_including_writes_and_ctes() {
    for (target, kind) in [
        ("SELECT 1", "select"),
        ("INSERT INTO t VALUES (1)", "insert"),
        ("UPDATE t SET x = 1", "update"),
        ("DELETE FROM t", "delete"),
        (
            "MERGE INTO t USING s ON t.x = s.x WHEN MATCHED THEN UPDATE SET x = s.x",
            "merge",
        ),
        (
            "WITH changed AS (UPDATE t SET x = 1 RETURNING x) SELECT * FROM changed",
            "select",
        ),
        ("CREATE TABLE result AS SELECT 1 AS x", "create_table"),
        ("VALUES (1), (2)", "values"),
        ("TABLE t", "select"),
        ("DECLARE c CURSOR FOR SELECT * FROM t", "declare"),
        ("(SELECT 1)", "subquery"),
        ("(VALUES (1))", "subquery"),
        ("(TABLE t)", "subquery"),
        ("EXECUTE prepared_query(1)", "execute"),
    ] {
        let sql = format!("EXPLAIN ANALYZE {target}");
        let node = explain(&sql);
        assert_eq!(node.statement().unwrap().variant_name(), kind, "{sql}");
        assert_eq!(node.execution_mode(), ExplainExecution::Executes);
        let generated = pg(&sql);
        assert_eq!(
            explain(&generated).statement().unwrap().variant_name(),
            kind,
            "{generated}"
        );
    }
}

#[test]
fn explain_and_describe_have_distinct_ast_and_sql() {
    let dialect = Dialect::get(DialectType::PostgreSQL);
    let plans = dialect.parse("EXPLAIN SELECT 1").unwrap();
    let columns = dialect.parse("DESCRIBE SELECT 1").unwrap();
    assert_ne!(plans, columns);
    assert_eq!(pg("EXPLAIN SELECT 1"), "EXPLAIN SELECT 1");
    assert_eq!(pg("DESCRIBE SELECT 1"), "DESCRIBE SELECT 1");
}

#[test]
fn cross_dialect_explain_preserves_execution_and_rejects_lossy_options() {
    use DialectType::*;
    assert_eq!(
        transpile("EXPLAIN SELECT 1", PostgreSQL, DuckDB).unwrap(),
        ["EXPLAIN SELECT 1"]
    );
    assert_eq!(
        transpile("EXPLAIN ANALYZE SELECT 1", PostgreSQL, DuckDB).unwrap(),
        ["EXPLAIN ANALYZE SELECT 1"]
    );
    // DuckDB enables ANALYZE by presence, even with an explicit FALSE argument.
    assert_eq!(
        transpile(
            "EXPLAIN (ANALYZE FALSE, FORMAT JSON) SELECT 1",
            PostgreSQL,
            DuckDB
        )
        .unwrap(),
        ["EXPLAIN (FORMAT JSON) SELECT 1"]
    );
    assert_eq!(
        transpile(
            "EXPLAIN (ANALYZE FALSE, FORMAT JSON) SELECT 1",
            DuckDB,
            PostgreSQL
        )
        .unwrap(),
        ["EXPLAIN (ANALYZE, FORMAT JSON) SELECT 1"]
    );
    assert!(transpile("EXPLAIN ANALYZE SELECT 1", PostgreSQL, Snowflake).is_err());
    assert!(transpile("EXPLAIN (COSTS OFF) SELECT 1", PostgreSQL, DuckDB).is_err());
    assert!(transpile(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT 1",
        PostgreSQL,
        ClickHouse
    )
    .is_err());
    assert_eq!(
        transpile("EXPLAIN (FORMAT JSON) SELECT 1", PostgreSQL, Snowflake).unwrap(),
        ["EXPLAIN USING JSON SELECT 1"]
    );
    assert_eq!(
        transpile("EXPLAIN SELECT 1", PostgreSQL, Oracle).unwrap(),
        ["EXPLAIN PLAN FOR SELECT 1"]
    );
    for target in [BigQuery, Fabric, Solr, Tableau, TSQL] {
        assert!(
            transpile("EXPLAIN SELECT 1", PostgreSQL, target).is_err(),
            "{target:?}"
        );
    }
}

#[test]
fn unknown_options_remain_visible_and_conservative() {
    let node = explain("EXPLAIN (FUTURE_OPTION ON) SELECT 1");
    assert_eq!(node.execution_mode(), ExplainExecution::Unknown);
    assert_eq!(
        pg("EXPLAIN (FUTURE_OPTION ON) SELECT 1"),
        "EXPLAIN (FUTURE_OPTION ON) SELECT 1"
    );
    assert!(transpile(
        "EXPLAIN (FUTURE_OPTION ON) SELECT 1",
        DialectType::PostgreSQL,
        DialectType::DuckDB
    )
    .is_err());
    assert!(transpile(
        "EXPLAIN ANALYZE FUTURE_EXTENSION value",
        DialectType::ClickHouse,
        DialectType::DuckDB,
    )
    .is_err());
}
