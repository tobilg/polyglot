//! Oracle flashback query and partition selection on a table reference.
//!
//! Oracle reads a table as of an earlier point in time with
//! `AS OF SCN <expr>` or `AS OF TIMESTAMP <expr>`, and reads a single
//! partition with `PARTITION (<name>)`. Both attach to the table reference
//! rather than to the query, and both have to survive a parse and a
//! regeneration unchanged for a transpiler to be usable as a rewriter.

use polyglot_sql::dialects::{Dialect, TranspileOptions};
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

/// Parsing must never accept a query and then drop the snapshot it asked for.
/// A subquery and a table function have nowhere to put the clause, so the
/// clause was consumed and discarded: the query succeeded and read current
/// data instead of the requested snapshot, which is wrong data rather than an
/// error. Refusing is the honest answer until the AST can carry it there.
#[test]
fn flashback_on_an_unsupported_relation_is_refused_not_dropped() {
    for sql in [
        "SELECT * FROM (SELECT * FROM t) AS OF SCN 12345",
        "SELECT * FROM TABLE(f()) AS OF SCN 12345",
        "SELECT * FROM (SELECT * FROM t) AS OF TIMESTAMP SYSTIMESTAMP",
    ] {
        match parse(sql, DialectType::Oracle) {
            Err(_) => {}
            Ok(ast) => {
                let out = generate(&ast[0], DialectType::Oracle).unwrap_or_default();
                panic!(
                    "{sql:?} was accepted and its snapshot silently dropped, generating {out:?}"
                );
            }
        }
    }
}

/// A successful parse must never lose an explicitly requested snapshot. This
/// is the general form of the case above: whatever shapes are accepted, the
/// clause has to survive into the output.
#[test]
fn an_accepted_flashback_always_reaches_the_output() {
    for sql in [
        "SELECT a FROM t AS OF SCN 12345",
        "SELECT a FROM s.t AS OF SCN 12345",
        "SELECT a FROM t PARTITION (P1) AS OF SCN 12345",
        "SELECT a FROM t AS OF SCN 12345 x",
        "SELECT a FROM t SAMPLE (10) AS OF SCN 12345",
        "SELECT a FROM t AS OF TIMESTAMP SYSTIMESTAMP",
    ] {
        let Ok(ast) = parse(sql, DialectType::Oracle) else {
            continue; // refusing is fine; losing the clause is not
        };
        let out = generate(&ast[0], DialectType::Oracle).expect("generate");
        assert!(
            out.contains("AS OF"),
            "{sql:?} parsed but generated {out:?}, which has no snapshot clause"
        );
    }
}

/// PARTITION is a legal table alias, and recognising partition selection must
/// not consume it. The parenthesis is part of the lookahead for that reason:
/// `FROM t partition` lost its alias while `partition.a` in the projection
/// kept referring to it.
#[test]
fn an_alias_named_partition_survives() {
    for sql in [
        "SELECT partition.a FROM t partition",
        "SELECT a FROM t partition WHERE partition.a > 1",
    ] {
        let out = {
            let ast =
                parse(sql, DialectType::Oracle).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            generate(&ast[0], DialectType::Oracle)
                .unwrap_or_else(|e| panic!("generate {sql:?}: {e}"))
        };
        assert_eq!(out, sql, "the alias was consumed by partition recognition");
    }

    // The MySQL family is deliberately untouched. Requiring the parenthesis
    // for them too would be more correct, but `FROM t partition` parses there
    // today and would start failing, since they reject PARTITION as an alias.
    // What must not change is that partition selection still works and that
    // input they accepted still parses.
    for dialect in [
        DialectType::MySQL,
        DialectType::StarRocks,
        DialectType::Doris,
    ] {
        let selection = "SELECT a FROM t PARTITION (p0)";
        let ast = parse(selection, dialect).unwrap_or_else(|e| panic!("{dialect:?} parse: {e}"));
        let out =
            generate(&ast[0], dialect).unwrap_or_else(|e| panic!("{dialect:?} generate: {e}"));
        assert!(
            out.to_uppercase().contains("PARTITION("),
            "{dialect:?} lost its partition selection: {out}"
        );
        assert!(
            parse("SELECT partition.a FROM t partition", dialect).is_ok(),
            "{dialect:?} stopped accepting an input it used to accept"
        );
    }
}

/// Oracle's grammar puts the sample clause inside query_table_expression and
/// the flashback clause after it, so the order is
/// table, PARTITION, SAMPLE, AS OF, alias.
///
/// Verified against Oracle Free 23ai: `SAMPLE (10) AS OF SCN n` runs, and the
/// reverse order is rejected with ORA-03048.
#[test]
fn sample_precedes_flashback() {
    let sql = "SELECT a FROM t SAMPLE (10) AS OF SCN 12345";
    assert_eq!(roundtrip(sql), sql);

    let table = table_of(sql);
    assert!(table.table_sample.is_some(), "the sample clause was lost");
    assert!(table.version.is_some(), "the flashback clause was lost");

    let out = roundtrip(sql);
    let sample_at = out.find("SAMPLE").expect("SAMPLE missing");
    let as_of_at = out.find("AS OF").expect("AS OF missing");
    assert!(
        sample_at < as_of_at,
        "AS OF was emitted before SAMPLE, which Oracle rejects with ORA-03048: {out}"
    );

    // The reverse order is not valid Oracle, so it must not be accepted.
    assert!(
        parse(
            "SELECT a FROM t AS OF SCN 12345 SAMPLE (10)",
            DialectType::Oracle
        )
        .is_err(),
        "the reverse order was accepted, and Oracle rejects it with ORA-03048"
    );
}

/// The full table reference shape, with every clause present at once.
#[test]
fn partition_sample_flashback_and_alias_together() {
    let sql = "SELECT x.a FROM s.t PARTITION(P1) SAMPLE (10) AS OF SCN 12345 x WHERE x.a > 1";
    assert_eq!(roundtrip(sql), sql);

    let table = table_of(sql);
    assert_eq!(table.partitions.len(), 1, "the partition was lost");
    assert_eq!(table.partitions[0].name, "P1");
    assert!(table.table_sample.is_some(), "the sample was lost");
    assert!(table.version.is_some(), "the flashback clause was lost");
    assert_eq!(
        table.alias.as_ref().map(|a| a.name.as_str()),
        Some("x"),
        "the alias was lost"
    );
    assert_eq!(
        table.schema.as_ref().map(|s| s.name.as_str()),
        Some("s"),
        "the schema qualifier was lost"
    );
}

/// An Oracle SCN is a database change number. It is not a timestamp and not
/// another system's snapshot identifier, so there is nothing to convert it
/// into, and converting it anyway is worse than refusing: PostgreSQL and Trino
/// received invalid SQL, and Dremio received valid SQL with a different
/// meaning because its branch treats every kind other than VERSION as a
/// timestamp.
#[test]
fn scn_is_not_converted_to_another_dialects_time_travel() {
    let sql = "SELECT a FROM t AS OF SCN 12345";
    let oracle = Dialect::get(DialectType::Oracle);

    for target in [
        DialectType::PostgreSQL,
        DialectType::Trino,
        DialectType::Dremio,
        DialectType::Spark,
        DialectType::BigQuery,
        DialectType::Snowflake,
    ] {
        // Strict mode has to fail rather than emit something.
        let strict = oracle.transpile_with(sql, target, TranspileOptions::strict());
        assert!(
            strict.is_err(),
            "{target:?} accepted an SCN in strict mode: {strict:?}"
        );

        // The default policy continues, but must not fabricate target syntax
        // and must never reinterpret the change number as a timestamp.
        let lenient = oracle
            .transpile_with(sql, target, TranspileOptions::default())
            .unwrap_or_else(|e| panic!("{target:?} default: {e}"));
        let out = lenient.join("");
        assert!(
            !out.contains("12345"),
            "{target:?} still carries the change number into its own time travel syntax: {out}"
        );
        assert!(
            out.contains("no equivalent"),
            "{target:?} produced no diagnostic: {out}"
        );
    }
}

/// Oracle to Oracle is the supported conversion and must keep working.
#[test]
fn scn_survives_an_oracle_to_oracle_transpile() {
    let sql = "SELECT a FROM t AS OF SCN 12345";
    let out = Dialect::get(DialectType::Oracle)
        .transpile_with(sql, DialectType::Oracle, TranspileOptions::strict())
        .expect("Oracle to Oracle should be supported");
    assert_eq!(out, vec![sql.to_string()]);
}

/// AS OF TIMESTAMP is a timestamp on both sides, so unlike SCN it has a real
/// equivalent and must not be caught by the SCN guard.
#[test]
fn timestamp_flashback_still_converts() {
    let out = Dialect::get(DialectType::Oracle).transpile_with(
        "SELECT a FROM t AS OF TIMESTAMP SYSTIMESTAMP",
        DialectType::Trino,
        TranspileOptions::default(),
    );
    let rendered = out.unwrap_or_default().join("");
    assert!(
        !rendered.contains("no equivalent"),
        "AS OF TIMESTAMP was treated as unconvertible: {rendered}"
    );
}
