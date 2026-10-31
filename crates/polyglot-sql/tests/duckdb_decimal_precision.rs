use polyglot_sql::expressions::Literal;
use polyglot_sql::{generate, parse_one, transpile, DialectType, Expression};

const MAX_PRECISION_FRACTION: &str = ".12345678901234567890123456789012345678";

#[test]
fn duckdb_preserves_leading_dot_literal_spelling() {
    for number in [
        ".5",
        ".50",
        ".0001",
        ".5_0",
        ".5e2",
        ".50E-2",
        MAX_PRECISION_FRACTION,
    ] {
        for sign in ["", "-", "+"] {
            let sql = format!("SELECT {sign}{number} AS value");
            // Unary plus is discarded by the parser; it does not affect precision.
            let expected_sign = if sign == "+" { "" } else { sign };
            let expected_number = number.replace('_', "");
            let expected = format!("SELECT {expected_sign}{expected_number} AS value");
            for prefix in ["", "/* π */ "] {
                let expression = parse_one(&format!("{prefix}{sql}"), DialectType::DuckDB).unwrap();
                let generated = generate(&expression, DialectType::DuckDB).unwrap();
                assert_eq!(generated, format!("{prefix}{expected}"));
                let reparsed = parse_one(&generated, DialectType::DuckDB).unwrap();
                assert_eq!(generate(&reparsed, DialectType::DuckDB).unwrap(), generated);
            }
        }
    }
}

#[test]
fn duckdb_preserves_decimal_case_results_with_adjacent_end() {
    for number in [".5", "-.50", MAX_PRECISION_FRACTION] {
        let expected = format!("SELECT CASE WHEN TRUE THEN {number} END AS value");
        for separator in ["", " "] {
            let sql = format!("SELECT CASE WHEN TRUE THEN {number}{separator}END AS value");
            assert_eq!(
                transpile(&sql, DialectType::DuckDB, DialectType::DuckDB).unwrap(),
                vec![expected.clone()]
            );
        }
    }
}

#[test]
fn duckdb_preserves_programmatically_constructed_numeric_literals() {
    for number in [".50", "-.50", MAX_PRECISION_FRACTION] {
        let expression = Expression::Literal(Box::new(Literal::Number(number.to_string())));
        assert_eq!(generate(&expression, DialectType::DuckDB).unwrap(), number);
    }
}

#[test]
fn leading_dot_normalization_is_unchanged_for_other_targets() {
    for target in [
        DialectType::Generic,
        DialectType::PostgreSQL,
        DialectType::MySQL,
        DialectType::BigQuery,
        DialectType::ClickHouse,
    ] {
        assert_eq!(
            transpile("SELECT .50, -.50", DialectType::DuckDB, target).unwrap(),
            vec!["SELECT 0.50, -0.50"],
            "{target:?}"
        );
    }
}

#[test]
fn explicit_leading_zeros_are_preserved_in_duckdb() {
    let sql = "SELECT 0.50, -0.50, 1.50, 13, 1e3";
    assert_eq!(
        transpile(sql, DialectType::DuckDB, DialectType::DuckDB).unwrap(),
        vec![sql]
    );
}
