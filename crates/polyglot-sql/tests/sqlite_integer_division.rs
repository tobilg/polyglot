//! SQLite and PostgreSQL truncate the division of two integers, so their `/`
//! is kept as is; a cast is only needed from dialects that divide as floats.
use polyglot_sql::{transpile, DialectType};

#[test]
fn integer_division_is_kept_from_truncating_dialects() {
    for (read, write) in [
        (DialectType::SQLite, DialectType::SQLite),
        (DialectType::PostgreSQL, DialectType::SQLite),
        (DialectType::SQLite, DialectType::PostgreSQL),
    ] {
        let out = transpile("SELECT 7 / 2, a / b FROM t", read, write).unwrap();
        assert_eq!(
            out,
            vec!["SELECT 7 / 2, a / b FROM t"],
            "{read:?} -> {write:?}"
        );
    }
}

#[test]
fn float_division_to_sqlite_still_casts() {
    let out = transpile("SELECT 7 / 2", DialectType::MySQL, DialectType::SQLite).unwrap();
    assert_eq!(out, vec!["SELECT CAST(7 AS REAL) / 2"]);
}
