//! Regression tests for CREATE VIEW with both COMMENT and SECURITY clauses.
//!
//! Trino/Presto document the canonical ordering as COMMENT before SECURITY:
//!   https://trino.io/docs/current/sql/create-view.html
//!
//!   CREATE [ OR REPLACE ] VIEW view_name
//!   [ COMMENT view_comment ]
//!   [ SECURITY { DEFINER | INVOKER } ]
//!   AS query
//!
//! The parser previously consumed the SECURITY clause before COMMENT, so once a
//! COMMENT preceded SECURITY the statement failed with
//! "Invalid expression / Unexpected token". Both orderings must now parse.

use polyglot_sql::dialects::DialectType;
use polyglot_sql::transpile;

#[test]
fn trino_view_comment_before_security_definer_parses() {
    let out = transpile(
        "CREATE VIEW v COMMENT 'c' SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::Trino,
        DialectType::Trino,
    )
    .expect("COMMENT before SECURITY DEFINER should parse");

    let sql = &out[0];
    assert!(sql.contains("SECURITY DEFINER"), "got: {sql}");
    assert!(sql.to_uppercase().contains("COMMENT"), "got: {sql}");
}

#[test]
fn trino_view_comment_before_security_invoker_parses() {
    transpile(
        "CREATE VIEW v COMMENT 'c' SECURITY INVOKER AS SELECT 1 AS a",
        DialectType::Trino,
        DialectType::Trino,
    )
    .expect("COMMENT before SECURITY INVOKER should parse");
}

#[test]
fn presto_view_comment_before_security_parses() {
    transpile(
        "CREATE VIEW v COMMENT 'c' SECURITY INVOKER AS SELECT 1 AS a",
        DialectType::Presto,
        DialectType::Presto,
    )
    .expect("Presto COMMENT before SECURITY should parse");
}

#[test]
fn trino_view_security_before_comment_still_parses() {
    // The previously-accepted ordering (SECURITY before COMMENT) must keep working.
    transpile(
        "CREATE VIEW v SECURITY DEFINER COMMENT 'c' AS SELECT 1 AS a",
        DialectType::Trino,
        DialectType::Trino,
    )
    .expect("SECURITY before COMMENT should still parse");
}

#[test]
fn trino_view_security_only_and_comment_only_still_parse() {
    transpile(
        "CREATE VIEW v SECURITY DEFINER AS SELECT 1 AS a",
        DialectType::Trino,
        DialectType::Trino,
    )
    .expect("SECURITY only should parse");

    transpile(
        "CREATE VIEW v COMMENT 'c' AS SELECT 1 AS a",
        DialectType::Trino,
        DialectType::Trino,
    )
    .expect("COMMENT only should parse");
}
