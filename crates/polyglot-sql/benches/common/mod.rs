//! SQL inputs shared by the latency and allocation hotspot benchmarks.
//! Dialect selection stays in the callers, so both suites use identical fixtures.

pub(super) const SHORT_ASCII: &str =
    "SELECT a, b, SUM(c) AS total FROM events WHERE created_at >= '2025-01-01' GROUP BY a, b";

pub(super) const UNICODE: &str =
    "SELECT \"Kundennummer\", 'Gr\u{00fc}\u{00df}e aus Z\u{00fc}rich' AS \"Mitteilung\" FROM \"Bestellungen\" WHERE \"Stadt\" = 'M\u{00fc}nchen'";

pub(super) const COMMENT_AND_STRING_HEAVY: &str = r#"
-- leading comment
SELECT 'alpha''beta' AS value, "quoted name", E'line\nvalue'
FROM events /* source comment */
WHERE payload = '{"key":"value"}' -- trailing comment
"#;

pub(super) fn large_token_list() -> String {
    let values = (0..20_000)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT * FROM events WHERE event_id IN ({values})")
}

pub(super) fn many_columns() -> String {
    format!(
        "SELECT {} FROM t",
        (0..1_000)
            .map(|index| format!("c{index}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(super) fn nested_functions() -> String {
    format!(
        "SELECT {}x{} FROM t",
        "COALESCE(".repeat(20),
        ", NULL)".repeat(20)
    )
}

pub(super) fn large_strings() -> String {
    format!(
        "SELECT {} FROM t",
        vec![format!("'{}'", "x".repeat(100)); 500].join(", ")
    )
}

pub(super) fn many_numbers() -> String {
    format!(
        "SELECT {} FROM t",
        (0..10_000)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(feature = "transpile")]
pub(super) fn transpilation_queries() -> Vec<(&'static str, String)> {
    vec![
        ("short", "SELECT COALESCE(x, 0) FROM t".to_owned()),
        (
            "wide",
            format!(
                "SELECT {} FROM t",
                (0..100)
                    .map(|i| format!("COALESCE(c{i}, 0) AS v{i}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        (
            "nested",
            format!(
                "SELECT {}x{} FROM t",
                "COALESCE(".repeat(20),
                ", 0)".repeat(20)
            ),
        ),
        (
            "casts",
            format!(
                "SELECT {}",
                (0..100)
                    .map(|i| format!("CAST({i}.123 AS DECIMAL(10, 2)) AS v{i}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
    ]
}
