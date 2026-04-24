use polyglot_sql::{parse, DialectType};

fn assert_clickhouse_parse(sql: &str) {
    let parsed = parse(sql, DialectType::ClickHouse).expect("ClickHouse SQL should parse");
    assert_eq!(parsed.len(), 1);
}

#[test]
fn clickhouse_parses_array_literal_with_nested_subscript_and_cast() {
    assert_clickhouse_parse("SELECT [[1][1]]::Array(UInt32)");
}

#[test]
fn clickhouse_parses_array_literal_elements_with_type_casts() {
    assert_clickhouse_parse("SELECT [1::UInt32, 2::UInt32]::Array(UInt64)");
    assert_clickhouse_parse("SELECT [[1, 2]::Array(UInt32), [3]]::Array(Array(UInt64))");
}

#[test]
fn clickhouse_parses_nested_array_literal_subscript_chain() {
    assert_clickhouse_parse(
        "SELECT [[10, 2, 13, 15][toNullable(toLowCardinality(1))]][materialize(toLowCardinality(1))]",
    );
}

#[test]
fn clickhouse_parses_array_literal_followed_by_multiple_casts() {
    assert_clickhouse_parse("SELECT [[[1, 2, 3]::Array(UInt64)::Dynamic]]");
}
