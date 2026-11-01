//! Oracle flashback query and partition selection on a table reference.
//!
//! Oracle reads a table as of an earlier point in time with
//! `AS OF SCN <expr>` or `AS OF TIMESTAMP <expr>`, and reads a single
//! partition with `PARTITION (<name>)`. Both attach to the table reference
//! rather than to the query, and both have to survive a parse and a
//! regeneration unchanged for a transpiler to be usable as a rewriter.

use polyglot_sql::expressions::Expression;
use polyglot_sql::{generate, parse, parse_one, DialectType};

/// Returns the single table reference of a one-table SELECT.
fn table_of(sql: &str) -> polyglot_sql::expressions::TableRef {
    let expr =
        parse_one(sql, DialectType::Oracle).unwrap_or_else(|e| panic!("parse {sql:?} failed: {e}"));
    let select = match expr {
        Expression::Select(select) => select,
        other => panic!("expected a SELECT for {sql:?}, got {other:?}"),
    };
    let from = select.from.as_ref().expect("expected a FROM clause");
    match from.expressions.first().expect("expected one table") {
        Expression::Table(table) => (**table).clone(),
        other => panic!("expected a table reference for {sql:?}, got {other:?}"),
    }
}

fn roundtrip(sql: &str) -> String {
    let ast =
        parse(sql, DialectType::Oracle).unwrap_or_else(|e| panic!("parse {sql:?} failed: {e}"));
    generate(&ast[0], DialectType::Oracle)
        .unwrap_or_else(|e| panic!("generate for {sql:?} failed: {e}"))
}

#[test]
fn flashback_as_of_scn_roundtrips() {
    for sql in [
        "SELECT a FROM t AS OF SCN 12345",
        "SELECT a, b FROM s.t AS OF SCN 12345",
        "SELECT a FROM t AS OF SCN 12345 WHERE a > 1",
        // The alias follows the flashback clause, which is where a parser
        // that mistook AS OF for an alias would go wrong.
        "SELECT x.a FROM t AS OF SCN 12345 x WHERE x.a > 1",
        "SELECT a FROM t AS OF SCN :scn",
    ] {
        assert_eq!(roundtrip(sql), sql, "round trip changed {sql:?}");
    }
}

#[test]
fn flashback_as_of_timestamp_roundtrips() {
    for sql in [
        "SELECT a FROM t AS OF TIMESTAMP SYSTIMESTAMP",
        "SELECT a FROM t AS OF TIMESTAMP SYSTIMESTAMP - INTERVAL '1' HOUR",
        "SELECT a FROM t AS OF TIMESTAMP TO_TIMESTAMP('2026-01-01', 'YYYY-MM-DD')",
    ] {
        assert_eq!(roundtrip(sql), sql, "round trip changed {sql:?}");
    }
}

#[test]
fn flashback_populates_the_version_field() {
    let table = table_of("SELECT a FROM t AS OF SCN 12345");
    let version = table
        .version
        .as_ref()
        .expect("AS OF SCN should populate TableRef::version");

    assert_eq!(version.kind, "AS OF");
    match version.this.as_ref() {
        Expression::Identifier(ident) => assert_eq!(ident.name, "SCN"),
        other => panic!("expected the identifier SCN, got {other:?}"),
    }
    assert!(
        version.expression.is_some(),
        "the SCN value should be parsed as an expression"
    );
    // The clause must not be mistaken for an alias.
    assert!(
        table.alias.is_none(),
        "AS OF SCN was read as an alias: {:?}",
        table.alias
    );
}

/// Oracle has no AS keyword for table aliases, but the shared alias parser
/// accepts one, so `AS <something>` has to stay an alias unless the something
/// is really a flashback keyword. Consuming `AS OF` unconditionally would
/// break every query whose alias merely starts with those letters.
#[test]
fn as_alias_is_not_mistaken_for_flashback() {
    for (sql, expected_alias) in [
        ("SELECT a FROM t AS OFFICE", "OFFICE"),
        ("SELECT a FROM t AS x", "x"),
        ("SELECT a FROM t AS scn", "scn"),
    ] {
        let table = table_of(sql);
        assert!(
            table.version.is_none(),
            "{sql:?} was read as a flashback query"
        );
        assert_eq!(
            table.alias.as_ref().map(|a| a.name.as_str()),
            Some(expected_alias),
            "{sql:?} lost its alias"
        );
    }
}

/// Oracle partition selection used to fall through to the alias parser and
/// come back as a table aliased `PARTITION` carrying a column alias list.
/// That regenerated correctly, so only an AST assertion catches it.
#[test]
fn partition_selection_populates_the_partitions_field() {
    let table = table_of("SELECT a FROM t PARTITION (P1)");

    assert_eq!(
        table.partitions.len(),
        1,
        "PARTITION (P1) should populate TableRef::partitions, got {:?}",
        table.partitions
    );
    assert_eq!(table.partitions[0].name, "P1");
    assert!(
        table.alias.is_none(),
        "PARTITION was read as an alias: {:?}",
        table.alias
    );
    assert!(
        table.column_aliases.is_empty(),
        "the partition name was read as a column alias list: {:?}",
        table.column_aliases
    );
}

/// Oracle requires PARTITION before AS OF SCN and rejects the reverse with
/// ORA-03048, so the generated order is a correctness requirement.
#[test]
fn partition_precedes_flashback() {
    let sql = "SELECT a FROM t PARTITION (P1) AS OF SCN 12345";
    let table = table_of(sql);
    assert_eq!(table.partitions.len(), 1, "the partition was lost");
    assert!(table.version.is_some(), "the flashback clause was lost");

    // Oracle accepts the no-space PARTITION(P1) spelling the generator emits.
    let out = roundtrip(sql);
    assert_eq!(out, "SELECT a FROM t PARTITION(P1) AS OF SCN 12345");

    let partition_at = out.find("PARTITION").expect("PARTITION missing");
    let as_of_at = out.find("AS OF").expect("AS OF missing");
    assert!(
        partition_at < as_of_at,
        "AS OF SCN was emitted before PARTITION, which Oracle rejects: {out}"
    );
}

/// The combination a parallel reader actually emits: schema qualified, one
/// partition, pinned to an SCN, aliased, and filtered.
#[test]
fn full_slice_shape_roundtrips() {
    let sql = "SELECT a, b FROM s.t PARTITION(P1) AS OF SCN 12345 x WHERE x.a > 1";
    assert_eq!(roundtrip(sql), sql);
}

/// Flashback is Oracle-only syntax, so other dialects must not start
/// accepting it.
#[test]
fn flashback_is_not_accepted_by_other_dialects() {
    for dialect in [
        DialectType::PostgreSQL,
        DialectType::MySQL,
        DialectType::Snowflake,
    ] {
        assert!(
            parse("SELECT a FROM t AS OF SCN 12345", dialect).is_err(),
            "{dialect:?} accepted Oracle flashback syntax"
        );
    }
}
