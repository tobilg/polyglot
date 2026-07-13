//! Regression tests: PostgreSQL regex-match operators transpiled to ClickHouse.
//!
//! ClickHouse has no `REGEXP_LIKE` function and no case-insensitive regex operator; the
//! regex-match primitive is `match(haystack, pattern)`. Previously the PostgreSQL operators
//! `~`, `~*`, `!~`, `!~*` were emitted as `REGEXP_LIKE(...)` / an invalid `x REGEXP_ILIKE y`
//! infix form, both of which ClickHouse rejects. They now map to `match`, with case-insensitive
//! matching expressed via an inline `(?i)` flag.

use polyglot_sql::{transpile, DialectType};

fn pg_ch(sql: &str) -> String {
    transpile(sql, DialectType::PostgreSQL, DialectType::ClickHouse)
        .expect("PostgreSQL -> ClickHouse transpilation should succeed")
        .join("; ")
}

#[test]
fn regex_match_operator_maps_to_match() {
    let cases = [
        ("SELECT x ~ 'ab.*' FROM t", "SELECT match(x, 'ab.*') FROM t"),
        (
            "SELECT * FROM t WHERE name ~ '^A'",
            "SELECT * FROM t WHERE match(name, '^A')",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(pg_ch(input), expected, "input: {input}");
    }
}

#[test]
fn case_insensitive_regex_operator_uses_inline_flag() {
    // A string-literal pattern gets the (?i) flag inlined for readable output.
    assert_eq!(
        pg_ch("SELECT x ~* 'ab.*' FROM t"),
        "SELECT match(x, '(?i)ab.*') FROM t"
    );
    // A non-literal pattern falls back to concat() so the flag still applies.
    assert_eq!(
        pg_ch("SELECT x ~* y FROM t"),
        "SELECT match(x, concat('(?i)', y)) FROM t"
    );
}

#[test]
fn negated_regex_operators_map_to_not_match() {
    assert_eq!(
        pg_ch("SELECT x !~ 'ab.*' FROM t"),
        "SELECT NOT (match(x, 'ab.*')) FROM t"
    );
    assert_eq!(
        pg_ch("SELECT x !~* 'ab.*' FROM t"),
        "SELECT NOT (match(x, '(?i)ab.*')) FROM t"
    );
}

#[test]
fn mysql_rlike_also_maps_to_clickhouse_match() {
    // The mapping is on the RegexpLike node, so RLIKE/REGEXP sources benefit too.
    assert_eq!(
        transpile(
            "SELECT x RLIKE 'a' FROM t",
            DialectType::MySQL,
            DialectType::ClickHouse
        )
        .expect("MySQL -> ClickHouse transpilation should succeed")
        .join("; "),
        "SELECT match(x, 'a') FROM t"
    );
}
