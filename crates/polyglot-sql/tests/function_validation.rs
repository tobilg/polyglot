//! Tests for dialect-specific function validation.

use polyglot_sql::{validate, DialectType, ValidationSeverity};

#[test]
fn test_unknown_function_warning() {
    let result = validate(
        "SELECT completely_fake_function(1)",
        DialectType::ClickHouse,
    );
    // Should still be valid (warnings don't invalidate)
    assert!(result.valid);
    let warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001")
        .collect();
    assert!(!warnings.is_empty(), "Expected W001 warning for unknown function");
    assert_eq!(warnings[0].severity, ValidationSeverity::Warning);
    assert!(warnings[0].message.contains("completely_fake_function"));
}

#[test]
fn test_arity_mismatch_warning() {
    // toDate takes exactly 1 arg
    let result = validate(
        "SELECT toDate(1, 2, 3, 4)",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    let warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W002")
        .collect();
    assert!(!warnings.is_empty(), "Expected W002 warning for arity mismatch");
    assert!(warnings[0].message.contains("toDate"));
}

#[test]
fn test_valid_function_no_warnings() {
    let result = validate(
        "SELECT toDate('2023-01-01')",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    assert!(
        result.errors.is_empty(),
        "Expected no warnings for valid function call, got: {:?}",
        result.errors
    );
}

#[test]
fn test_typed_variants_no_warnings() {
    // COUNT, SUM, etc. are typed expression variants, not Expression::Function
    let result = validate(
        "SELECT COUNT(*), SUM(x), AVG(y) FROM t",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    // Filter out only function-validation warnings (W001/W002)
    let func_warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001" || e.code == "W002")
        .collect();
    assert!(
        func_warnings.is_empty(),
        "Expected no function warnings for typed variants, got: {:?}",
        func_warnings
    );
}

#[test]
fn test_case_insensitive_lookup() {
    // toDate exists — TODATE should also match
    let result = validate(
        "SELECT TODATE('2023-01-01')",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    let func_warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001" || e.code == "W002")
        .collect();
    assert!(
        func_warnings.is_empty(),
        "Expected case-insensitive match, got warnings: {:?}",
        func_warnings
    );
}

#[test]
fn test_non_clickhouse_dialect_no_validation() {
    // Non-ClickHouse dialects should not produce function validation warnings
    let result = validate(
        "SELECT completely_fake_function(1)",
        DialectType::PostgreSQL,
    );
    assert!(result.valid);
    let func_warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001" || e.code == "W002")
        .collect();
    assert!(
        func_warnings.is_empty(),
        "Expected no function validation for PostgreSQL"
    );
}

#[test]
fn test_valid_remains_true_with_warnings() {
    // Even with unknown functions, `valid` should be true (only severity=Error sets it false)
    let result = validate(
        "SELECT fake1(1), fake2(2, 3)",
        DialectType::ClickHouse,
    );
    assert!(result.valid, "Warnings should not set valid=false");
    assert!(!result.errors.is_empty(), "Expected some warnings");
    for w in &result.errors {
        assert_eq!(w.severity, ValidationSeverity::Warning);
    }
}

#[test]
fn test_variadic_function_accepts_many_args() {
    // concat is variadic (min 1, no max)
    let result = validate(
        "SELECT concat('a', 'b', 'c', 'd', 'e')",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    let func_warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001" || e.code == "W002")
        .collect();
    assert!(
        func_warnings.is_empty(),
        "concat is variadic, should accept many args, got: {:?}",
        func_warnings
    );
}

#[test]
fn test_aggregate_function_validation() {
    // sumIf takes exactly 2 args in ClickHouse
    let result = validate(
        "SELECT sumIf(amount, status = 'active') FROM orders",
        DialectType::ClickHouse,
    );
    assert!(result.valid);
    let func_warnings: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "W001" || e.code == "W002")
        .collect();
    assert!(
        func_warnings.is_empty(),
        "sumIf(amount, cond) should be valid, got: {:?}",
        func_warnings
    );
}

#[test]
fn test_known_clickhouse_functions() {
    // Spot check several well-known ClickHouse functions
    let functions = [
        "SELECT toString(42)",
        "SELECT arrayJoin([1, 2, 3])",
        "SELECT now()",
        "SELECT abs(-5)",
    ];
    for sql in &functions {
        let result = validate(sql, DialectType::ClickHouse);
        let func_warnings: Vec<_> = result
            .errors
            .iter()
            .filter(|e| e.code == "W001" || e.code == "W002")
            .collect();
        assert!(
            func_warnings.is_empty(),
            "Expected no warnings for '{sql}', got: {func_warnings:?}"
        );
    }
}
