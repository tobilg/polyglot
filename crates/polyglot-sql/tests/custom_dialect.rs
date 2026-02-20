//! Tests for custom dialect support.
//!
//! These tests validate the `CustomDialectBuilder` API, the global custom dialect
//! registry, and name-based dialect lookup and transpilation.

use polyglot_sql::dialects::{CustomDialectBuilder, Dialect, DialectType};
use polyglot_sql::generator::NormalizeFunctions;
use polyglot_sql::{generate_by_name, parse_by_name, transpile_by_name, unregister_custom_dialect};

/// Helper to ensure cleanup after each test, even on panic.
struct DialectGuard(&'static str);

impl Drop for DialectGuard {
    fn drop(&mut self) {
        unregister_custom_dialect(self.0);
    }
}

#[test]
fn test_custom_dialect_lowercase_functions() {
    let _guard = DialectGuard("test_lower_funcs");

    CustomDialectBuilder::new("test_lower_funcs")
        .based_on(DialectType::PostgreSQL)
        .generator_config_modifier(|gc| {
            gc.normalize_functions = NormalizeFunctions::Lower;
        })
        .register()
        .unwrap();

    let d = Dialect::get_by_name("test_lower_funcs").unwrap();
    let exprs = d.parse("SELECT COUNT(*), MAX(id) FROM users").unwrap();
    let sql = d.generate(&exprs[0]).unwrap();

    assert!(
        sql.contains("count("),
        "Expected lowercase 'count(' in: {}",
        sql
    );
    assert!(
        sql.contains("max("),
        "Expected lowercase 'max(' in: {}",
        sql
    );
}

#[test]
fn test_custom_dialect_uppercase_keywords() {
    let _guard = DialectGuard("test_upper_kw");

    CustomDialectBuilder::new("test_upper_kw")
        .based_on(DialectType::Generic)
        .generator_config_modifier(|gc| {
            gc.uppercase_keywords = true;
        })
        .register()
        .unwrap();

    let d = Dialect::get_by_name("test_upper_kw").unwrap();
    let exprs = d.parse("select 1").unwrap();
    let sql = d.generate(&exprs[0]).unwrap();

    assert!(
        sql.starts_with("SELECT"),
        "Expected uppercase SELECT in: {}",
        sql
    );
}

#[test]
fn test_custom_dialect_transform() {
    let _guard = DialectGuard("test_transform");

    // Custom transform that renames MY_FUNC to COALESCE
    CustomDialectBuilder::new("test_transform")
        .based_on(DialectType::Generic)
        .transform_fn(|expr| {
            if let polyglot_sql::Expression::Function(ref f) = expr {
                if f.name.eq_ignore_ascii_case("MY_FUNC") {
                    let mut f = f.clone();
                    f.name = "COALESCE".to_string();
                    return Ok(polyglot_sql::Expression::Function(f));
                }
            }
            Ok(expr)
        })
        .register()
        .unwrap();

    let d = Dialect::get_by_name("test_transform").unwrap();
    let exprs = d.parse("SELECT MY_FUNC(a, b)").unwrap();
    let transformed = d.transform(exprs[0].clone()).unwrap();
    let sql = d.generate(&transformed).unwrap();

    assert!(sql.contains("COALESCE"), "Expected COALESCE in: {}", sql);
    assert!(!sql.contains("MY_FUNC"), "Expected no MY_FUNC in: {}", sql);
}

#[test]
fn test_custom_dialect_preprocess() {
    let _guard = DialectGuard("test_preprocess");

    // Custom preprocess that is a no-op (just passes through)
    CustomDialectBuilder::new("test_preprocess")
        .based_on(DialectType::PostgreSQL)
        .preprocess_fn(|expr| {
            // No-op preprocessing — overrides the base dialect's preprocessing
            Ok(expr)
        })
        .register()
        .unwrap();

    let d = Dialect::get_by_name("test_preprocess").unwrap();
    let exprs = d.parse("SELECT 1").unwrap();
    let transformed = d.transform(exprs[0].clone()).unwrap();
    let sql = d.generate(&transformed).unwrap();

    assert_eq!(sql, "SELECT 1");
}

#[test]
fn test_builtin_name_collision() {
    let result = CustomDialectBuilder::new("postgresql")
        .based_on(DialectType::Generic)
        .register();

    assert!(
        result.is_err(),
        "Should reject built-in dialect name 'postgresql'"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("collides with built-in"),
        "Error should mention collision: {}",
        err_msg
    );
}

#[test]
fn test_builtin_name_collision_alias() {
    // "postgres" is an alias for PostgreSQL
    let result = CustomDialectBuilder::new("postgres")
        .based_on(DialectType::Generic)
        .register();

    assert!(
        result.is_err(),
        "Should reject built-in dialect alias 'postgres'"
    );
}

#[test]
fn test_duplicate_registration() {
    let _guard = DialectGuard("test_dup");

    CustomDialectBuilder::new("test_dup").register().unwrap();

    let result = CustomDialectBuilder::new("test_dup").register();

    assert!(result.is_err(), "Should reject duplicate registration");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("already registered"),
        "Error should mention duplicate: {}",
        err_msg
    );
}

#[test]
fn test_get_by_name_builtin() {
    let d = Dialect::get_by_name("postgresql");
    assert!(d.is_some(), "Should find built-in dialect 'postgresql'");
    assert_eq!(d.unwrap().dialect_type(), DialectType::PostgreSQL);

    let d2 = Dialect::get_by_name("mysql");
    assert!(d2.is_some(), "Should find built-in dialect 'mysql'");
    assert_eq!(d2.unwrap().dialect_type(), DialectType::MySQL);
}

#[test]
fn test_get_by_name_unknown() {
    let d = Dialect::get_by_name("nonexistent_dialect_xyz");
    assert!(d.is_none(), "Should return None for unknown dialect");
}

#[test]
fn test_transpile_by_name() {
    let _guard = DialectGuard("test_transpile_custom");

    CustomDialectBuilder::new("test_transpile_custom")
        .based_on(DialectType::DuckDB)
        .generator_config_modifier(|gc| {
            gc.normalize_functions = NormalizeFunctions::Lower;
        })
        .register()
        .unwrap();

    // Transpile from built-in DuckDB to custom dialect
    let result = transpile_by_name("SELECT COUNT(*)", "duckdb", "test_transpile_custom").unwrap();
    assert_eq!(result.len(), 1);
    assert!(
        result[0].contains("count("),
        "Expected lowercase count in: {}",
        result[0]
    );

    // Also test transpile_by_name with two built-in dialects
    let result2 = transpile_by_name("SELECT 1", "postgresql", "mysql").unwrap();
    assert_eq!(result2.len(), 1);
}

#[test]
fn test_transpile_by_name_unknown_dialect() {
    let result = transpile_by_name("SELECT 1", "no_such_dialect", "mysql");
    assert!(result.is_err(), "Should error on unknown source dialect");

    let result2 = transpile_by_name("SELECT 1", "mysql", "no_such_dialect");
    assert!(result2.is_err(), "Should error on unknown target dialect");
}

#[test]
fn test_unregister() {
    CustomDialectBuilder::new("test_unreg").register().unwrap();

    assert!(Dialect::get_by_name("test_unreg").is_some());

    let removed = unregister_custom_dialect("test_unreg");
    assert!(removed, "Should return true when dialect was found");

    assert!(Dialect::get_by_name("test_unreg").is_none());

    // Double unregister should return false
    let removed2 = unregister_custom_dialect("test_unreg");
    assert!(
        !removed2,
        "Should return false when dialect already removed"
    );
}

#[test]
fn test_parse_by_name() {
    let exprs = parse_by_name("SELECT 1; SELECT 2", "postgresql").unwrap();
    assert_eq!(exprs.len(), 2);

    let err = parse_by_name("SELECT 1", "no_such_dialect");
    assert!(err.is_err());
}

#[test]
fn test_generate_by_name() {
    let exprs = parse_by_name("SELECT 1", "generic").unwrap();
    let sql = generate_by_name(&exprs[0], "generic").unwrap();
    assert_eq!(sql, "SELECT 1");

    let err = generate_by_name(&exprs[0], "no_such_dialect");
    assert!(err.is_err());
}

#[test]
fn test_custom_dialect_inherits_base_parsing() {
    let _guard = DialectGuard("test_inherit_parse");

    // Based on BigQuery (which supports backtick identifiers)
    CustomDialectBuilder::new("test_inherit_parse")
        .based_on(DialectType::BigQuery)
        .register()
        .unwrap();

    let d = Dialect::get_by_name("test_inherit_parse").unwrap();
    // BigQuery uses backtick identifiers — parsing should work
    let exprs = d.parse("SELECT `my_column` FROM `my_table`").unwrap();
    assert_eq!(exprs.len(), 1);
}
