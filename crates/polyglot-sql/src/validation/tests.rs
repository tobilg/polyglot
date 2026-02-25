use super::*;
use crate::function_catalog::{FunctionNameCase, FunctionSignature, HashMapFunctionCatalog};
use std::sync::Arc;

#[test]
fn test_canonical_type_family_aliases() {
    assert_eq!(canonical_type_family("INT4"), TypeFamily::Integer);
    assert_eq!(
        canonical_type_family("double precision"),
        TypeFamily::Numeric
    );
    assert_eq!(canonical_type_family("VARCHAR(255)"), TypeFamily::String);
    assert_eq!(
        canonical_type_family("timestamp with time zone"),
        TypeFamily::Timestamp
    );
    assert_eq!(canonical_type_family("JSONB"), TypeFamily::Json);
    assert_eq!(canonical_type_family("UUID"), TypeFamily::Uuid);
}

#[test]
fn test_canonical_type_family_wrappers_and_collections() {
    assert_eq!(
        canonical_type_family("Nullable(Int64)"),
        TypeFamily::Integer
    );
    assert_eq!(
        canonical_type_family("LowCardinality(String)"),
        TypeFamily::String
    );
    assert_eq!(canonical_type_family("Array(String)"), TypeFamily::Array);
    assert_eq!(canonical_type_family("list(varchar)"), TypeFamily::Array);
    assert_eq!(canonical_type_family("Map(String, Int64)"), TypeFamily::Map);
    assert_eq!(canonical_type_family("STRUCT<a INT>"), TypeFamily::Struct);
    assert_eq!(canonical_type_family(""), TypeFamily::Unknown);
}

fn base_schema() -> ValidationSchema {
    ValidationSchema {
        tables: vec![
            SchemaTable {
                name: "users".to_string(),
                schema: None,
                columns: vec![
                    SchemaColumn {
                        name: "id".to_string(),
                        data_type: "integer".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    SchemaColumn {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    SchemaColumn {
                        name: "email".to_string(),
                        data_type: "varchar".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    SchemaColumn {
                        name: "age".to_string(),
                        data_type: "integer".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                ],
                aliases: vec![],
                primary_key: vec![],
                unique_keys: vec![],
                foreign_keys: vec![],
            },
            SchemaTable {
                name: "orders".to_string(),
                schema: None,
                columns: vec![
                    SchemaColumn {
                        name: "id".to_string(),
                        data_type: "integer".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    SchemaColumn {
                        name: "user_id".to_string(),
                        data_type: "integer".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    SchemaColumn {
                        name: "total".to_string(),
                        data_type: "decimal".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                ],
                aliases: vec![],
                primary_key: vec![],
                unique_keys: vec![],
                foreign_keys: vec![],
            },
        ],
        strict: Some(true),
    }
}

fn attach_column_fk(
    schema: &mut ValidationSchema,
    table_name: &str,
    column_name: &str,
    target_table: &str,
    target_column: &str,
) {
    if let Some(table) = schema.tables.iter_mut().find(|t| t.name == table_name) {
        if let Some(column) = table.columns.iter_mut().find(|c| c.name == column_name) {
            column.references = Some(SchemaColumnReference {
                table: target_table.to_string(),
                column: target_column.to_string(),
                schema: None,
            });
        }
    }
}

fn mark_primary_key(schema: &mut ValidationSchema, table_name: &str, column_name: &str) {
    if let Some(table) = schema.tables.iter_mut().find(|t| t.name == table_name) {
        table.primary_key = vec![column_name.to_string()];
        if let Some(column) = table.columns.iter_mut().find(|c| c.name == column_name) {
            column.primary_key = true;
        }
    }
}

fn test_function_catalog() -> Arc<HashMapFunctionCatalog> {
    let mut catalog = HashMapFunctionCatalog::default();
    catalog.register(
        DialectType::Generic,
        "abs",
        vec![FunctionSignature::exact(1)],
    );
    catalog.register(
        DialectType::Generic,
        "coalesce",
        vec![FunctionSignature::variadic(1)],
    );
    catalog.register(
        DialectType::Generic,
        "foo",
        vec![FunctionSignature::exact(1)],
    );
    Arc::new(catalog)
}

#[test]
fn test_validate_with_schema_known_table_column() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT id, name FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result.errors.is_empty());
}

#[test]
fn test_validate_with_schema_unknown_table() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT * FROM nonexistent",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_TABLE && e.message.contains("nonexistent")));
}

#[test]
fn test_validate_with_schema_unknown_column() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT unknown_col FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.code == validation_codes::E_UNKNOWN_COLUMN
                && e.message.contains("unknown_col"))
    );
}

#[test]
fn test_validate_with_schema_function_catalog_unknown_function() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(test_function_catalog()),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT made_up_fn(id) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_FUNCTION));
}

#[test]
fn test_validate_with_schema_function_catalog_invalid_arity() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(test_function_catalog()),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT FOO(id, age) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_FUNCTION_ARITY));
}

#[test]
fn test_validate_with_schema_function_catalog_valid_variadic() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(test_function_catalog()),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT COALESCE(name, email, 'fallback') FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid, "{:?}", result.errors);
}

#[test]
fn test_validate_with_schema_function_catalog_dialect_case_sensitive() {
    let schema = base_schema();
    let mut catalog = HashMapFunctionCatalog::default();
    catalog.set_dialect_name_case(DialectType::Generic, FunctionNameCase::Sensitive);
    catalog.register(
        DialectType::Generic,
        "Foo",
        vec![FunctionSignature::exact(1)],
    );

    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(Arc::new(catalog)),
        ..Default::default()
    };

    let valid = validate_with_schema("SELECT Foo(id) FROM users", DialectType::Generic, &schema, &opts);
    assert!(valid.valid, "{:?}", valid.errors);

    let invalid =
        validate_with_schema("SELECT FOO(id) FROM users", DialectType::Generic, &schema, &opts);
    assert!(!invalid.valid);
    assert!(invalid
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_FUNCTION));
}

#[test]
fn test_validate_with_schema_function_catalog_function_case_override() {
    let schema = base_schema();
    let mut catalog = HashMapFunctionCatalog::default();
    catalog.set_dialect_name_case(DialectType::Generic, FunctionNameCase::Insensitive);
    catalog.register(
        DialectType::Generic,
        "Bar",
        vec![FunctionSignature::exact(1)],
    );
    catalog.set_function_name_case(
        DialectType::Generic,
        "bar",
        FunctionNameCase::Sensitive,
    );

    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(Arc::new(catalog)),
        ..Default::default()
    };

    let valid = validate_with_schema("SELECT Bar(id) FROM users", DialectType::Generic, &schema, &opts);
    assert!(valid.valid, "{:?}", valid.errors);

    let invalid =
        validate_with_schema("SELECT BAR(id) FROM users", DialectType::Generic, &schema, &opts);
    assert!(!invalid.valid);
    assert!(invalid
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_FUNCTION));
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-all-dialects"
))]
#[test]
fn test_validate_with_schema_uses_embedded_function_catalog_when_unset() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT made_up_fn(id) FROM users",
        DialectType::ClickHouse,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_FUNCTION));
}

#[cfg(any(
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
#[test]
fn test_validate_with_schema_uses_embedded_duckdb_function_catalog_when_unset() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT made_up_fn(id) FROM users",
        DialectType::DuckDB,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_UNKNOWN_FUNCTION));
}

#[test]
fn test_validate_with_schema_cte_projected_alias_column() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "WITH my_cte AS (SELECT id AS emp_id FROM users) SELECT emp_id FROM my_cte",
        DialectType::ClickHouse,
        &schema,
        &opts,
    );
    assert!(result.valid, "{:?}", result.errors);
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

#[test]
fn test_validate_with_schema_cte_projected_alias_column_non_strict() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        strict: Some(false),
        check_types: true,
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "WITH my_cte AS (SELECT id AS emp_id FROM users) SELECT emp_id FROM my_cte",
        DialectType::ClickHouse,
        &schema,
        &opts,
    );
    assert!(result.valid, "{:?}", result.errors);
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

#[test]
fn test_validate_with_schema_unknown_cte_projected_alias_column() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "WITH my_cte AS (SELECT id AS emp_id FROM users) SELECT missing_col FROM my_cte",
        DialectType::ClickHouse,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result.errors.iter().any(|e| {
        e.code == validation_codes::E_UNKNOWN_COLUMN && e.message.contains("missing_col")
    }));
}

#[test]
fn test_validate_with_schema_join_columns() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
}

#[test]
fn test_validate_with_schema_non_strict_is_warning() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT unknown FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .all(|e| e.severity == crate::ValidationSeverity::Warning));
}

#[test]
fn test_validate_with_schema_semantic_warnings() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        semantic: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT * FROM users LIMIT 10",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_SELECT_STAR));
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_LIMIT_WITHOUT_ORDER_BY));
}

#[test]
fn test_validate_with_schema_reference_check_valid_column_fk() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema("SELECT 1", DialectType::Generic, &schema, &opts);
    assert!(result.valid, "errors: {:?}", result.errors);
    assert!(result.errors.is_empty());
}

#[test]
fn test_validate_with_schema_reference_check_unknown_target_table() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "missing_users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema("SELECT 1", DialectType::Generic, &schema, &opts);
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_FOREIGN_KEY_REFERENCE));
}

#[test]
fn test_validate_with_schema_reference_check_unknown_target_column() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "missing_id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema("SELECT 1", DialectType::Generic, &schema, &opts);
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_FOREIGN_KEY_REFERENCE));
}

#[test]
fn test_validate_with_schema_reference_check_type_mismatch() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    if let Some(orders) = schema.tables.iter_mut().find(|t| t.name == "orders") {
        if let Some(user_id) = orders.columns.iter_mut().find(|c| c.name == "user_id") {
            user_id.data_type = "varchar".to_string();
        }
    }
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema("SELECT 1", DialectType::Generic, &schema, &opts);
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_FOREIGN_KEY_REFERENCE));
}

#[test]
fn test_validate_with_schema_reference_check_non_strict_warning() {
    let mut schema = base_schema();
    attach_column_fk(&mut schema, "orders", "user_id", "missing_users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema("SELECT 1", DialectType::Generic, &schema, &opts);
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_WEAK_REFERENCE_INTEGRITY));
}

#[test]
fn test_validate_with_schema_reference_check_ambiguous_unqualified_column() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_AMBIGUOUS_COLUMN_REFERENCE));
}

#[test]
fn test_validate_with_schema_reference_check_ambiguous_unqualified_column_non_strict() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_WEAK_REFERENCE_INTEGRITY));
}

#[test]
fn test_validate_with_schema_reference_check_cartesian_join_warning() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT users.id FROM users CROSS JOIN orders",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_CARTESIAN_JOIN));
}

#[test]
fn test_validate_with_schema_reference_check_join_not_using_declared_fk_warning() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT users.id FROM users JOIN orders ON users.age = orders.total",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_JOIN_NOT_USING_DECLARED_REFERENCE));
}

#[test]
fn test_validate_with_schema_reference_check_join_using_declared_fk_no_warning() {
    let mut schema = base_schema();
    mark_primary_key(&mut schema, "users", "id");
    attach_column_fk(&mut schema, "orders", "user_id", "users", "id");

    let opts = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(!result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_JOIN_NOT_USING_DECLARED_REFERENCE));
}

#[test]
fn test_validate_with_schema_type_check_comparison_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT id FROM users WHERE age = 'abc'",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES));
}

#[test]
fn test_validate_with_schema_type_check_arithmetic_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT age + name FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_ARITHMETIC_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_function_argument_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT ABS(name) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_FUNCTION_ARGUMENT_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_predicate_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT id FROM users WHERE age + 1",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_PREDICATE_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_non_strict_warnings() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT ABS(name) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_FUNCTION_ARGUMENT_COERCION));
}

#[test]
fn test_validate_with_schema_type_check_setop_arity_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT id FROM users UNION SELECT id, total FROM orders",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_SETOP_ARITY_MISMATCH));
}

#[test]
fn test_validate_with_schema_type_check_setop_type_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT age FROM users UNION SELECT name FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_SETOP_TYPE_MISMATCH));
}

#[test]
fn test_validate_with_schema_type_check_insert_values_assignment_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "INSERT INTO users (age) VALUES ('abc')",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_ASSIGNMENT_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_insert_query_assignment_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "INSERT INTO users (age) SELECT name FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_ASSIGNMENT_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_update_assignment_mismatch() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    let result = validate_with_schema(
        "UPDATE users SET age = name",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::E_INVALID_ASSIGNMENT_TYPE));
}

#[test]
fn test_validate_with_schema_type_check_update_non_strict_warning() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        check_types: true,
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema(
        "UPDATE users SET age = name",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == validation_codes::W_IMPLICIT_CAST_ASSIGNMENT));
}

#[test]
fn test_validate_with_schema_unresolved_table_alias_in_join_on() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = q.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result.errors.iter().any(|e| {
        e.code == validation_codes::E_UNRESOLVED_REFERENCE && e.message.contains("q")
    }));
}

#[test]
fn test_validate_with_schema_unresolved_table_alias_in_join_on_non_strict() {
    let schema = base_schema();
    let opts = SchemaValidationOptions {
        strict: Some(false),
        ..Default::default()
    };
    let result = validate_with_schema(
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = q.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    // Non-strict: valid but with a warning
    assert!(result.valid);
    assert!(result.errors.iter().any(|e| {
        e.code == validation_codes::E_UNRESOLVED_REFERENCE
            && e.message.contains("q")
            && e.severity == crate::ValidationSeverity::Warning
    }));
}

#[test]
fn test_validate_with_schema_valid_aliases_in_join_on() {
    let schema = base_schema();
    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(result.valid, "{:?}", result.errors);
}
