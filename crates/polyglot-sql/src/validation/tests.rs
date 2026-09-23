use super::*;
use crate::function_catalog::{FunctionNameCase, FunctionSignature, HashMapFunctionCatalog};
use std::sync::Arc;

fn review_schema() -> ValidationSchema {
    serde_json::from_value(serde_json::json!({"tables": [
        {"name": "items", "columns": [{"name":"quantity","type":"INTEGER"},{"name":"active","type":"BOOLEAN"}]},
        {"name": "other", "columns": [{"name":"quantity","type":"BOOLEAN"},{"name":"active","type":"INTEGER"}]}
    ]})).unwrap()
}

#[test]
fn review_scoped_type_validation() {
    let schema = review_schema();
    let options = SchemaValidationOptions {
        check_types: true,
        check_references: true,
        ..Default::default()
    };
    for sql in [
        "SELECT active + 1 FROM items",
        "WITH q AS (SELECT active AS flag FROM items) SELECT flag + 1 FROM q",
        "SELECT flag + 1 FROM (SELECT active AS flag FROM items) q",
        "SELECT quantity FROM (SELECT active AS flag, quantity FROM items) q WHERE flag + 1 > 0",
        "SELECT (SELECT t.quantity + 1 FROM other t) FROM items t",
        "WITH q AS (SELECT true AS \"Camel\") SELECT \"Camel\" + 1 FROM q",
        "WITH \"Query\" AS (SELECT true AS \"Camel\") SELECT \"Query\".\"Camel\" + 1 FROM \"Query\"",
    ] {
        let result = validate_with_schema(sql, DialectType::PostgreSQL, &schema, &options);
        assert!(!result.valid && result.errors.iter().any(|e| e.code == "E212"), "{sql}: {:?}", result.errors);
    }
    for sql in [
        "WITH q AS (SELECT quantity AS n FROM items) SELECT n + 1 FROM q",
        "SELECT (SELECT t.quantity + 1 FROM items t) FROM other t",
    ] {
        let result = validate_with_schema(sql, DialectType::PostgreSQL, &schema, &options);
        assert!(result.valid, "{sql}: {:?}", result.errors);
    }
}

#[test]
fn review_dml_reference_checks_do_not_require_types() {
    let schema = review_schema();
    for check_types in [false, true] {
        for strict in [false, true] {
            let options = SchemaValidationOptions {
                check_types,
                check_references: true,
                strict: Some(strict),
                ..Default::default()
            };
            for (sql, code) in [
                ("UPDATE nonexistent SET x = 1", "E200"),
                ("DELETE FROM nonexistent", "E200"),
                ("INSERT INTO nonexistent(x) VALUES(1)", "E200"),
                ("UPDATE items SET quantity = missing", "E201"),
                ("UPDATE items SET missing = 1", "E201"),
                ("DELETE FROM items WHERE missing = 1", "E201"),
                ("INSERT INTO items(missing) VALUES(1)", "E201"),
            ] {
                let result = validate_with_schema(sql, DialectType::PostgreSQL, &schema, &options);
                assert_eq!(result.valid, !strict, "{sql}: {:?}", result.errors);
                assert!(
                    result.errors.iter().any(|e| e.code == code),
                    "{sql}: {:?}",
                    result.errors
                );
            }
            for sql in [
                "UPDATE items SET quantity = quantity + 1 WHERE active",
                "DELETE FROM items WHERE quantity = 1",
                "INSERT INTO items(quantity) VALUES(1)",
            ] {
                let result = validate_with_schema(sql, DialectType::PostgreSQL, &schema, &options);
                assert!(result.valid, "{sql}: {:?}", result.errors);
            }
        }
    }
}

#[test]
fn review_name_aligned_set_operation_validation() {
    let schema = review_schema();
    let options = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    for dialect in [
        DialectType::Snowflake,
        DialectType::DuckDB,
        DialectType::BigQuery,
    ] {
        let sql = "SELECT quantity AS a, active AS b FROM items UNION ALL BY NAME SELECT active AS b, quantity AS a FROM items";
        let result = validate_with_schema(sql, dialect, &schema, &options);
        assert!(result.valid, "{dialect}: {:?}", result.errors);
        let result = validate_with_schema("SELECT quantity AS a FROM items UNION ALL BY NAME SELECT quantity AS a, active AS b FROM items", dialect, &schema, &options);
        assert_eq!(
            result.valid,
            dialect != DialectType::BigQuery,
            "{dialect}: {:?}",
            result.errors
        );
    }
    for sql in [
        "SELECT * FROM items UNION ALL BY NAME SELECT active, quantity FROM items",
        "(SELECT quantity AS a FROM items UNION ALL BY NAME SELECT active AS b FROM items) UNION ALL BY NAME SELECT quantity AS a, active AS b FROM items",
    ] {
        let result = validate_with_schema(sql, DialectType::DuckDB, &schema, &options);
        assert!(result.valid, "{sql}: {:?}", result.errors);
    }
    assert!(
        !validate_with_schema(
            "SELECT * FROM items UNION ALL BY NAME SELECT * FROM other",
            DialectType::DuckDB,
            &schema,
            &options
        )
        .valid
    );
}

#[test]
fn review_dml_clause_boundaries() {
    let schema = review_schema();
    let captured = validate_with_schema(
        "UPDATE items AS x SET quantity = TRANSFORM(ARRAY_CONSTRUCT(quantity), x -> x.missing)",
        DialectType::Snowflake,
        &schema,
        &SchemaValidationOptions::default(),
    );
    assert!(
        captured.errors.iter().any(|e| e.code == "E201"),
        "{:?}",
        captured.errors
    );
    for check_types in [false, true] {
        let options = SchemaValidationOptions {
            check_types,
            check_references: true,
            ..Default::default()
        };
        for (dialect, sql, valid) in [
            (DialectType::PostgreSQL, "INSERT INTO items(quantity) VALUES(quantity)", false),
            (DialectType::PostgreSQL, "INSERT INTO items(quantity) VALUES(1) ON CONFLICT(quantity) DO UPDATE SET quantity = excluded.quantity + quantity", true),
            (DialectType::PostgreSQL, "INSERT INTO items(quantity) VALUES(1) RETURNING excluded.quantity", false),
            (DialectType::TSQL, "UPDATE items SET quantity = quantity + 1 OUTPUT inserted.quantity", true),
            (DialectType::TSQL, "UPDATE items SET quantity = inserted.quantity OUTPUT inserted.quantity", false),
            (DialectType::TSQL, "DELETE FROM items OUTPUT deleted.quantity", true),
            (DialectType::TSQL, "DELETE FROM items OUTPUT inserted.quantity", false),
            (DialectType::PostgreSQL, "WITH q AS(SELECT 1 AS n) INSERT INTO items(quantity) SELECT n FROM q", true),
            (DialectType::Snowflake, "MERGE INTO items t USING other s ON t.quantity=s.active WHEN MATCHED THEN UPDATE SET quantity=s.active", true),
            (DialectType::Snowflake, "MERGE INTO items t USING other s ON t.quantity=s.active WHEN MATCHED THEN UPDATE SET missing=1", false),
            (DialectType::Snowflake, "MERGE INTO items t USING other s ON t.quantity=s.active WHEN NOT MATCHED THEN INSERT (missing) VALUES(s.active)", false),
            (DialectType::PostgreSQL, "CREATE TABLE new_items AS SELECT quantity FROM items", true),
        ] {
            let result = validate_with_schema(sql, dialect, &schema, &options);
            assert_eq!(result.valid, valid, "{sql}, types={check_types}: {:?}", result.errors);
        }
    }
    let options = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    for sql in [
        "UPDATE items SET quantity=1 WHERE quantity",
        "DELETE FROM items WHERE quantity",
    ] {
        assert!(
            !validate_with_schema(sql, DialectType::PostgreSQL, &schema, &options).valid,
            "{sql}"
        );
        assert!(
            validate_with_schema(sql, DialectType::MySQL, &schema, &options).valid,
            "{sql}"
        );
    }
}

#[test]
fn review_grouping_aliases_and_windows() {
    let options = SchemaValidationOptions {
        semantic: true,
        strict: Some(false),
        ..Default::default()
    };
    for (sql, valid) in [
        ("SELECT quantity AS a, SUM(active) FROM items GROUP BY quantity", true),
        ("SELECT quantity + 1 AS a FROM items GROUP BY a", true),
        ("SELECT SUM(quantity) AS a FROM items WHERE a > 0", false),
        ("SELECT SUM(quantity) AS a, SUM(a) FROM items", false),
        ("SELECT quantity AS a, a, SUM(quantity) FROM items", false),
        ("SELECT i.quantity, SUM(o.active) FROM items i JOIN other o ON TRUE GROUP BY o.quantity", false),
        ("SELECT i.quantity, SUM(o.active) FROM items i JOIN other o ON TRUE GROUP BY i.quantity", true),
        ("SELECT SUM(quantity) FROM items HAVING active", false),
        ("SELECT SUM(quantity) FROM items ORDER BY active", false),
        ("SELECT SUM(quantity) AS total FROM items HAVING total > 0 ORDER BY total", true),
        ("SELECT quantity, SUM(quantity) OVER (PARTITION BY active) FROM items GROUP BY quantity", false),
        ("SELECT SUM(quantity) FROM items GROUP BY 1", false),
        ("SELECT quantity, SUM(quantity) OVER() FROM items", true),
    ] {
        let result = validate_with_schema(sql, DialectType::Snowflake, &review_schema(), &options);
        assert_eq!(result.valid, valid, "{sql}: {:?}", result.errors);
    }
}

#[test]
fn review_dialect_aliases_and_quoting() {
    let schema = review_schema();
    for dialect in [DialectType::Snowflake, DialectType::DuckDB] {
        for check_types in [false, true] {
            let options = SchemaValidationOptions {
                check_types,
                check_references: true,
                ..Default::default()
            };
            for sql in [
                "SELECT quantity + 1 AS a, a * 2 AS b FROM items",
                "SELECT quantity + 1 AS a FROM items WHERE a > 0",
                "SELECT quantity + 1 AS a FROM items GROUP BY a",
                "SELECT SUM(quantity) AS a FROM items HAVING a > 0",
                "SELECT ROW_NUMBER() OVER(ORDER BY quantity) AS rn FROM items QUALIFY rn = 1",
            ] {
                let result = validate_with_schema(sql, dialect, &schema, &options);
                assert!(result.valid, "{dialect}: {sql}: {:?}", result.errors);
            }
        }
    }
    for (name, valid) in [("Camel", true), ("camel", false), ("CAMEL", false)] {
        let sql = format!("WITH q AS (SELECT 1 AS \"Camel\") SELECT \"{name}\" FROM q");
        let result = validate_with_schema(
            &sql,
            DialectType::PostgreSQL,
            &schema,
            &SchemaValidationOptions::default(),
        );
        assert_eq!(result.valid, valid, "{sql}: {:?}", result.errors);
    }
    let options = SchemaValidationOptions {
        check_types: true,
        ..Default::default()
    };
    assert!(
        validate_with_schema(
            "SELECT quantity FROM items WHERE quantity",
            DialectType::MySQL,
            &schema,
            &options
        )
        .valid
    );
    assert!(
        !validate_with_schema(
            "SELECT quantity FROM items WHERE quantity",
            DialectType::PostgreSQL,
            &schema,
            &options
        )
        .valid
    );
}

#[test]
fn review_semantic_errors_are_scope_local_and_opt_in() {
    for (sql, code) in [
        ("SELECT quantity AS n, SUM(active) FROM items", "E230"),
        ("SELECT quantity FROM items WHERE SUM(quantity)>0", "E231"),
        ("SELECT SUM(SUM(quantity)) FROM items", "E231"),
        (
            "SELECT SUM(SUM(quantity)) OVER(), quantity FROM items",
            "E230",
        ),
        (
            "SELECT quantity AS quantity, SUM(quantity) FROM items",
            "E230",
        ),
        (
            "SELECT quantity FROM items WHERE ROW_NUMBER() OVER(ORDER BY quantity)=1",
            "E232",
        ),
    ] {
        assert!(crate::validate(sql, DialectType::Snowflake).valid);
        let result = crate::validate_with_options(
            sql,
            DialectType::Snowflake,
            &crate::ValidationOptions {
                semantic: true,
                ..Default::default()
            },
        );
        assert!(
            !result.valid && result.errors.iter().any(|e| e.code == code),
            "{sql}: {:?}",
            result.errors
        );
    }
    for sql in [
        "SELECT SUM(quantity) OVER(), quantity FROM items",
        "SELECT quantity, (SELECT SUM(quantity) FROM other) FROM items",
        "SELECT quantity, SUM(SUM(quantity)) OVER() FROM items GROUP BY quantity",
        "SELECT quantity + 1 AS a FROM items GROUP BY a",
        "SELECT SUM(quantity) AS a FROM items HAVING a > 0",
        "SELECT TRANSFORM(ARRAY_CONSTRUCT(1), x -> x + 1), COUNT(*) FROM items",
        "WITH q AS (SELECT quantity FROM items LIMIT 1) SELECT quantity FROM q",
    ] {
        let result = crate::validate_with_options(
            sql,
            DialectType::Snowflake,
            &crate::ValidationOptions {
                semantic: true,
                ..Default::default()
            },
        );
        assert!(result.valid, "{sql}: {:?}", result.errors);
    }
}

#[test]
fn review_strict_configuration_and_catalog_specs() {
    assert!(serde_json::from_value::<crate::AnalyzeQueryOptions>(
        serde_json::json!({"scheam": {"tables": []}})
    )
    .is_err());
    assert!(serde_json::from_value::<ValidationSchema>(
        serde_json::json!({"tables":[{"name":"t","columns":[{"name":"x","dataType":"INT"}]}]})
    )
    .is_err());
    use crate::function_catalog::FunctionCatalogSpec;
    let spec: FunctionCatalogSpec = serde_json::from_value(serde_json::json!({"functions":[{"name":"Foo","nameCase":"sensitive","signatures":[{"minArity":1,"maxArity":2}]}]})).unwrap();
    let options = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(Arc::new(spec.build(DialectType::Generic).unwrap())),
        ..Default::default()
    };
    for (sql, valid) in [
        ("SELECT Foo(1)", true),
        ("SELECT Foo(1,2)", true),
        ("SELECT Foo(1,2,3)", false),
        ("SELECT FOO(1)", false),
        ("SELECT Missing(1)", false),
    ] {
        let result = validate_with_schema(sql, DialectType::Generic, &review_schema(), &options);
        assert_eq!(result.valid, valid, "{sql}: {:?}", result.errors);
    }
}

#[test]
fn test_schema_validation_options_json_names() {
    for json in [
        r#"{"check_types":true,"check_references":true,"strict_syntax":true,"strict":false}"#,
        r#"{"checkTypes":true,"checkReferences":true,"strictSyntax":true,"strict":false}"#,
    ] {
        let options: SchemaValidationOptions = serde_json::from_str(json).unwrap();
        assert!(options.check_types && options.check_references && options.strict_syntax);
        assert_eq!(options.strict, Some(false));
    }
    assert!(serde_json::from_str::<SchemaValidationOptions>(r#"{"checkType":true}"#).is_err());
}

#[test]
fn test_schema_validation_lexical_scopes() {
    let schema = base_schema();
    for check_references in [false, true] {
        let options = SchemaValidationOptions {
            check_references,
            ..Default::default()
        };
        for sql in [
            "WITH a AS (SELECT id AS k FROM users), b AS (SELECT k FROM a) SELECT k FROM b",
            "WITH a(k) AS (SELECT id FROM users), b AS (SELECT k FROM a) SELECT b.k FROM b",
            "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = age)",
            "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE EXISTS (SELECT 1 WHERE u.id = o.user_id))",
            "SELECT u.id FROM users u WHERE EXISTS (SELECT u.id FROM orders u)",
            "WITH q AS (SELECT id FROM users) SELECT q.id FROM q WHERE EXISTS (WITH q AS (SELECT total FROM orders) SELECT total FROM q)",
            "WITH unused AS (SELECT id FROM orders) SELECT id FROM users",
            "SELECT q.id FROM (SELECT id FROM users) q",
            "SELECT u.id FROM users u WHERE EXISTS (SELECT u.id UNION ALL SELECT u.id)",
            "SELECT u.id FROM users u JOIN orders o ON EXISTS (SELECT 1 WHERE o.user_id = u.id)",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert!(result.valid, "{sql}: {:?}", result.errors);
        }
        for (sql, code) in [
            ("SELECT id FROM q WHERE EXISTS (WITH q AS (SELECT id FROM users) SELECT id FROM q)", "E200"),
            ("WITH q AS (SELECT id FROM users) SELECT q.id FROM users", "E222"),
            ("WITH q AS (SELECT id FROM users) SELECT age FROM q", "E201"),
            ("SELECT u.id FROM users u WHERE EXISTS (SELECT u.age FROM orders u)", "E201"),
            ("SELECT u.id FROM users u JOIN (SELECT u.id) q ON TRUE", "E222"),
            ("SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE u.missing = o.id)", "E201"),
            ("WITH q AS (SELECT id FROM users) SELECT id FROM public.q", "E200"),
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert!(!result.valid && result.errors.iter().any(|error| error.code == code), "{sql}: {:?}", result.errors);
        }
    }
}

#[test]
fn test_schema_validation_issue_441_nested_scopes() {
    let schema = ValidationSchema {
        tables: ["t1", "t2"]
            .into_iter()
            .map(|name| SchemaTable {
                name: name.to_string(),
                schema: None,
                columns: vec![SchemaColumn {
                    name: "id".to_string(),
                    data_type: "NUMBER".to_string(),
                    nullable: None,
                    primary_key: false,
                    unique: false,
                    references: None,
                }],
                aliases: vec![],
                primary_key: vec![],
                unique_keys: vec![],
                foreign_keys: vec![],
            })
            .collect(),
        strict: Some(true),
    };
    let options = SchemaValidationOptions {
        check_types: false,
        check_references: true,
        strict: Some(true),
        semantic: false,
        strict_syntax: false,
        ..Default::default()
    };

    for (case, sql) in [
        (
            "qualified_cte_column_is_not_ambiguous",
            "WITH a AS (SELECT id FROM t1), b AS (SELECT id FROM t2) \
             SELECT a.id FROM a JOIN b ON a.id = b.id",
        ),
        (
            "correlated_subquery_resolves_outer_alias",
            "SELECT outer_table.id FROM t1 outer_table \
             WHERE NOT EXISTS ( \
               SELECT 1 FROM t2 inner_table \
               WHERE inner_table.id = outer_table.id \
             )",
        ),
    ] {
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert!(result.valid, "{case}: {:#?}", result.errors);
        assert!(result.errors.is_empty(), "{case}: {:#?}", result.errors);
    }
}

#[test]
fn test_schema_validation_issue_442_prior_cte_outputs() {
    let schema = ValidationSchema {
        tables: vec![SchemaTable {
            name: "t1".to_string(),
            schema: None,
            columns: ["id", "value"]
                .into_iter()
                .map(|name| SchemaColumn {
                    name: name.to_string(),
                    data_type: "NUMBER".to_string(),
                    nullable: None,
                    primary_key: false,
                    unique: false,
                    references: None,
                })
                .collect(),
            aliases: vec![],
            primary_key: vec![],
            unique_keys: vec![],
            foreign_keys: vec![],
        }],
        strict: Some(true),
    };
    let options = SchemaValidationOptions {
        check_types: false,
        check_references: true,
        strict: Some(true),
        semantic: false,
        strict_syntax: false,
        ..Default::default()
    };

    for (case, sql) in [
        (
            "subsequent_cte_resolves_prior_cte_projection",
            "WITH derived AS (SELECT value AS derived_value FROM t1), \
             next AS (SELECT derived_value FROM derived) \
             SELECT derived_value FROM next",
        ),
        (
            "window_order_by_resolves_prior_cte_projection",
            "WITH scored AS (SELECT id, value AS info_score FROM t1), \
             ranked AS ( \
               SELECT id, ROW_NUMBER() OVER ( \
                 PARTITION BY id ORDER BY info_score DESC \
               ) AS rn \
               FROM scored \
             ) \
             SELECT id FROM ranked",
        ),
    ] {
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert!(result.valid, "{case}: {:#?}", result.errors);
        assert!(result.errors.is_empty(), "{case}: {:#?}", result.errors);
    }
}

#[test]
fn test_schema_validation_snowflake_projection_aliases() {
    let schema = projection_alias_schema();
    for check_references in [false, true] {
        for check_types in [false, true] {
            let options = SchemaValidationOptions {
                check_references,
                check_types,
                ..Default::default()
            };
            for sql in [
                // Exact issue #460 example.
                "SELECT quantity + 1 AS adjusted_quantity, adjusted_quantity * 2 AS doubled_quantity FROM items",
                "SELECT quantity + 1 AS a, a * 2 AS b, b + a AS c FROM items",
                "SELECT 1 AS a, a + 1 AS b",
                "SELECT quantity + 1 AS quantity, quantity * 2 AS doubled FROM items",
                "SELECT quantity AS Adjusted, ADJUSTED + adjusted AS doubled FROM items",
                "SELECT quantity AS \"Adjusted\", \"Adjusted\" + 1 AS doubled FROM items",
                "SELECT quantity AS adjusted, \"ADJUSTED\" + 1 AS doubled FROM items",
                "SELECT quantity AS \"数量\", \"数量\" + 1 AS doubled FROM items",
                "SELECT quantity AS a, CASE WHEN a > 0 THEN COALESCE(a, 0) ELSE 0 END AS b FROM items",
                "SELECT SUM(quantity) AS a, a * 2 AS b FROM items",
                "SELECT ROW_NUMBER() OVER (ORDER BY quantity) AS a, a + 1 AS b FROM items",
                "SELECT quantity AS a, a + 1 AS b FROM items ORDER BY b, a + 1",
                "WITH q AS (SELECT quantity AS a, a + 1 AS b FROM items) SELECT b FROM q",
                "WITH q(n) AS (SELECT quantity FROM items) SELECT n AS a, a + 1 AS b FROM q",
                "SELECT q.b FROM (SELECT quantity AS a, a + 1 AS b FROM items) q",
                "SELECT quantity AS a, a + 1 AS b FROM items UNION ALL SELECT quantity AS a, a + 2 AS b FROM other",
                "SELECT i.quantity FROM items i WHERE EXISTS (SELECT o.quantity AS a, a + 1 AS b FROM other o WHERE o.quantity = i.quantity)",
                "SELECT quantity AS a, TRANSFORM(ARRAY_CONSTRUCT(quantity), x INT -> x + a) AS b FROM items",
                "SELECT quantity > 0 AS x, TRANSFORM(ARRAY_CONSTRUCT(quantity), x INT -> x + 1) AS b FROM items",
                "SELECT quantity AS a, TRANSFORM(ARRAY_CONSTRUCT(quantity), x INT -> TRANSFORM(ARRAY_CONSTRUCT(x), y INT -> y + a)) AS b FROM items",
                "SELECT OBJECT_CONSTRUCT('n', quantity) AS obj, obj:n AS n FROM items",
            ] {
                let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
                assert!(result.valid && result.errors.is_empty(), "{sql}, refs={check_references}, types={check_types}: {:?}", result.errors);
            }
        }
    }
}

#[test]
fn test_schema_validation_projection_alias_diagnostics_and_boundaries() {
    let schema = projection_alias_schema();
    for strict in [false, true] {
        let options = SchemaValidationOptions {
            check_references: true,
            strict: Some(strict),
            ..Default::default()
        };
        for (sql, code, token) in [
            ("SELECT missing AS a, a + 1 AS b FROM items", "E201", "missing"),
            ("SELECT a + 1 AS b, quantity AS a FROM items", "E201", "a"),
            ("SELECT a + 1 AS a FROM items", "E201", "a"),
            ("SELECT quantity AS a, items.a + 1 AS b FROM items", "E201", "a"),
            ("SELECT quantity AS a, absent.a + 1 AS b FROM items", "E222", "absent"),
            ("SELECT quantity AS \"Adjusted\", adjusted + 1 AS b FROM items", "E201", "adjusted"),
            ("SELECT quantity AS adjusted, \"adjusted\" + 1 AS b FROM items", "E201", "\"adjusted\""),
            ("SELECT quantity AS a, (SELECT a + 1) AS b FROM items", "E201", "a"),
            ("SELECT quantity AS a, quantity + 1 AS a, a * 2 AS b FROM items", "E201", "a"),
            ("SELECT i.quantity AS quantity, quantity + 1 AS b FROM items i JOIN other o ON i.quantity=o.quantity", "E221", "quantity"),
            ("SELECT quantity AS a, a + 1 AS b FROM items UNION ALL SELECT a FROM other", "E201", "a"),
            ("SELECT quantity AS a, TRANSFORM(ARRAY_CONSTRUCT(quantity), x -> x + missing) AS b FROM items", "E201", "missing"),
            ("SELECT '😀', quantity AS \"数量\", \"数量\" + míssing AS b FROM items", "E201", "míssing"),
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert_eq!(result.valid, !strict, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors.len(), 1, "{sql}: {:?}", result.errors);
            let error = &result.errors[0];
            assert_eq!(error.code, if !strict && code == "E221" { "W222" } else { code });
            assert_eq!(error.severity, if strict { crate::ValidationSeverity::Error } else { crate::ValidationSeverity::Warning });
            let actual: String = sql.chars().skip(error.start.unwrap()).take(error.end.unwrap() - error.start.unwrap()).collect();
            assert_eq!(actual, token, "{sql}");
        }
        let result = validate_with_schema(
            "SELECT b + 1 AS a, a + 1 AS b FROM items",
            DialectType::Snowflake,
            &schema,
            &options,
        );
        assert_eq!(result.valid, !strict);
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        assert_eq!(result.errors[0].code, "E201");
    }
    for dialect in [
        DialectType::PostgreSQL,
        DialectType::BigQuery,
        DialectType::TSQL,
        DialectType::MySQL,
        DialectType::Oracle,
    ] {
        let result = validate_with_schema(
            "SELECT quantity AS a, a + 1 AS b FROM items",
            dialect,
            &schema,
            &SchemaValidationOptions::default(),
        );
        assert!(
            !result.valid,
            "out-of-scope dialect {dialect}: {:?}",
            result.errors
        );
        assert_eq!(result.errors[0].code, "E201");
    }
}

#[test]
fn test_schema_validation_projection_alias_types_and_input_precedence() {
    let mut schema = projection_alias_schema();
    schema.tables[0].columns.push(
        serde_json::from_value(serde_json::json!({"name": "active", "type": "BOOLEAN"})).unwrap(),
    );
    for strict in [false, true] {
        let options = SchemaValidationOptions {
            check_types: true,
            check_references: true,
            strict: Some(strict),
            ..Default::default()
        };
        for sql in [
            "SELECT active AS a, NOT a AS b FROM items",
            "SELECT quantity AS a, a + 1 AS b, b * 2 AS c FROM items",
            // The BOOLEAN input wins over the numeric alias.
            "SELECT quantity AS active, NOT active AS b FROM items",
            "SELECT active AS quantity, quantity + 1 AS b FROM items",
            "SELECT quantity AS \"active\", \"active\" + 1 AS b FROM items",
            "SELECT quantity AS \"active\", \"active\" + 1 AS b FROM (SELECT active, quantity FROM items) q",
            "SELECT quantity AS a, a + 1 AS b FROM items WHERE EXISTS (SELECT active AS a, NOT a AS b FROM items)",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert!(result.valid && result.errors.is_empty(), "{sql}: {:?}", result.errors);
        }
        for sql in [
            "SELECT quantity > 0 AS a, a + 1 AS b FROM items",
            "SELECT active AS a, a AS b, b + 1 AS c FROM items",
            "SELECT quantity AS active, active + 1 AS b FROM items",
            "SELECT quantity AS \"ACTIVE\", \"ACTIVE\" + 1 AS b FROM items",
            "SELECT quantity AS \"active\", \"active\" + 1 AS b FROM (SELECT active AS \"active\", quantity FROM items) q",
            "SELECT active AS a, TRANSFORM(ARRAY_CONSTRUCT(quantity), x INT -> x + a) AS b FROM items",
            "WITH q AS (SELECT active AS a, a + 1 AS b FROM items) SELECT b FROM q",
            "WITH q AS (SELECT active AS flag FROM items), r AS (SELECT flag AS a, a + 1 AS b FROM q) SELECT b FROM r",
            "WITH q AS (SELECT active AS a, a AS b FROM items) SELECT b AS flag, flag + 1 AS n FROM q",
            "SELECT q.b AS flag, flag + 1 AS n FROM (SELECT active AS a, a AS b FROM items) q",
            "SELECT (SELECT b FROM (SELECT active AS a, a AS b FROM items) q LIMIT 1) AS flag, flag + 1 AS n",
            "SELECT q.b FROM (SELECT active AS a, a + 1 AS b FROM items) q",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert_eq!(result.valid, !strict, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors.len(), 1, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors[0].code, if strict { "E212" } else { "W211" });
        }
    }
}

#[test]
fn test_schema_validation_projection_alias_chains_are_bounded() {
    let schema = projection_alias_schema();
    let mut sql = String::from("SELECT quantity AS a0");
    for i in 1..128 {
        sql.push_str(&format!(", a{} + a{} AS a{i}", i - 1, i - 1));
    }
    sql.push_str(" FROM items");
    let options = SchemaValidationOptions {
        check_types: true,
        check_references: true,
        ..Default::default()
    };
    let original = Dialect::get(DialectType::Snowflake)
        .parse(&sql)
        .unwrap()
        .remove(0);
    let scope = build_scope(&original);
    let mut selected = selected_validation_scope(&scope);
    let mut bindings = ProjectionAliasBindings::new();
    bind_scope_projection_aliases(
        &mut selected,
        &[],
        &build_resolver_schema(&schema),
        DialectType::Snowflake,
        true,
        &mut bindings,
    );
    assert_eq!(bindings.len(), 254);
    let bound = apply_projection_alias_bindings(original.clone(), &bindings);
    assert!(bound.dfs().count() < original.dfs().count() * 2);
    assert_eq!(
        original,
        Dialect::get(DialectType::Snowflake)
            .parse(&sql)
            .unwrap()
            .remove(0)
    );
    let result = validate_with_schema(&sql, DialectType::Snowflake, &schema, &options);
    assert!(
        result.valid && result.errors.is_empty(),
        "{:?}",
        result.errors
    );
    let options: SchemaValidationOptions =
        serde_json::from_value(serde_json::json!({"complexity_guard": {"maxInputBytes": 10}}))
            .unwrap();
    assert!(!validate_with_schema(&sql, DialectType::Snowflake, &schema, &options).valid);
}

fn projection_alias_schema() -> ValidationSchema {
    serde_json::from_value(serde_json::json!({"strict": true, "tables": [
        {"name": "items", "columns": [{"name": "quantity", "type": "NUMBER"}]},
        {"name": "other", "columns": [{"name": "quantity", "type": "NUMBER"}]}
    ]}))
    .unwrap()
}

#[test]
fn test_schema_validation_lambda_parameters() {
    let schema = lambda_validation_schema();
    for check_types in [false, true] {
        let options = SchemaValidationOptions {
            check_references: true,
            check_types,
            strict: Some(true),
            ..Default::default()
        };
        for sql in [
            // Exact issue #459 example.
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) FROM items",
            "SELECT FILTER(ARRAY_CONSTRUCT(item_id), value -> value > 0) FROM items",
            "SELECT REDUCE(ARRAY_CONSTRUCT(item_id), 0, (acc, value) -> acc + value) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value INT -> value + 1) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + item_id) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + items.item_id) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), items -> items + items.item_id) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x -> TRANSFORM(ARRAY_CONSTRUCT(x), y -> x + y + item_id)) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x -> TRANSFORM(ARRAY_CONSTRUCT(x), x -> x + 1)) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(i.item_id), item_id -> item_id + 1) FROM items i JOIN other o ON i.item_id = o.item_id",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), Value -> VALUE + value) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), \"Value\" -> \"Value\" + 1) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), \"Value\" INT -> \"Value\" + 1) FROM items",
            "SELECT FILTER(ARRAY_CONSTRUCT(item_id), \"Value\" -> \"Value\" > 0) FROM items",
            "SELECT FILTER(ARRAY_CONSTRUCT(item_id), \"Value\" INT -> \"Value\" > 0) FROM items",
            "SELECT REDUCE(ARRAY_CONSTRUCT(item_id), 0, (\"Acc\", \"Value\") -> \"Acc\" + \"Value\") FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> \"VALUE\" + 1) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('amount', item_id)), value -> value:amount) FROM items",
            "SELECT item_id FROM items WHERE ARRAY_SIZE(FILTER(ARRAY_CONSTRUCT(item_id), value -> value > 0)) > 0",
            "SELECT item_id AS merged FROM items ORDER BY TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1), merged",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + (SELECT MAX(item_id) FROM other)) FROM items",
            "WITH q AS (SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) AS values_array FROM items) SELECT values_array FROM q",
            "SELECT q.values_array FROM (SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) AS values_array FROM items) q",
            "SELECT i.item_id FROM items i WHERE EXISTS (SELECT TRANSFORM(ARRAY_CONSTRUCT(o.item_id), value -> value + i.item_id) FROM other o)",
            "SELECT i.item_id FROM items i WHERE EXISTS (SELECT 1 FROM (SELECT TRANSFORM(ARRAY_CONSTRUCT(1), i INT -> i + i.item_id)) q)",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) FROM items UNION ALL SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) FROM other",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert!(result.valid && result.errors.is_empty(), "{sql}, types={check_types}: {:?}", result.errors);
        }
    }
}

#[test]
fn test_schema_validation_lambda_dialects_and_fields() {
    let schema = lambda_validation_schema();
    let options = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    for (dialect, sql) in [
        (DialectType::DuckDB, "SELECT list_transform([item_id], value -> value + 1) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([item_id], lambda value: value + 1) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([item_id], lambda value, idx: value + idx + item_id) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([{'amount': item_id}], value -> value.amount) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([{'a': {'b': item_id}}], value -> value.a.b) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([item_id], \"Value\" -> value + 1) FROM items"),
        (DialectType::DuckDB, "SELECT list_transform([item_id], value -> value + q.item_id) FROM (SELECT item_id FROM items) q"),
        (DialectType::DuckDB, "SELECT list_reduce([item_id], lambda items, acc: items + acc + items.item_id, 0) FROM items"),
        (DialectType::DuckDB, "WITH q AS (SELECT list_transform([{'a': item_id}], q -> q.a) AS vals FROM items) SELECT vals FROM q"),
        (DialectType::DuckDB, "SELECT q.vals FROM (SELECT list_transform([{'a': item_id}], q -> q.a) AS vals FROM items) q"),
        (DialectType::Spark, "SELECT transform(array(item_id), value -> value + 1) FROM items"),
        (DialectType::Databricks, "SELECT transform(array(item_id), (value, idx) -> value + idx) FROM items"),
        (DialectType::Trino, "SELECT transform(ARRAY[item_id], value -> value + 1) FROM items"),
        (DialectType::Presto, "SELECT transform(ARRAY[item_id], value -> value + 1) FROM items"),
        (DialectType::ClickHouse, "SELECT arrayMap(value -> value + 1, [item_id]) FROM items"),
    ] {
        let result = validate_with_schema(sql, dialect, &schema, &options);
        assert!(result.valid && result.errors.is_empty(), "{dialect}: {sql}: {:?}", result.errors);
    }
}

#[test]
fn test_schema_validation_lambda_captures_and_boundaries() {
    let schema = lambda_validation_schema();
    for strict in [false, true] {
        let options = SchemaValidationOptions {
            check_references: true,
            strict: Some(strict),
            ..Default::default()
        };
        for (sql, code, token) in [
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + missing) FROM items", "E201", "missing"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(missing), value -> value + 1) FROM items", "E201", "missing"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + items.missing) FROM items", "E201", "missing"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), items -> items + items.missing) FROM items", "E201", "missing"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + absent.item_id) FROM items", "E222", "absent"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1), value FROM items", "E201", "value"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x -> x + 1), TRANSFORM(ARRAY_CONSTRUCT(item_id), y -> x + y) FROM items", "E201", "x"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + (SELECT MAX(value) FROM other)) FROM items", "E201", "value"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + (SELECT MAX(missing) FROM other)) FROM items", "E201", "missing"),
            ("SELECT i.item_id FROM items i WHERE EXISTS (SELECT 1 FROM (SELECT TRANSFORM(ARRAY_CONSTRUCT(1), i INT -> i + i.missing)) q)", "E201", "missing"),
            ("SELECT i.item_id FROM items i WHERE EXISTS (WITH q AS (SELECT TRANSFORM(ARRAY_CONSTRUCT(1), i INT -> i + i.missing)) SELECT * FROM q)", "E201", "missing"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), \"Value\" -> value + 1) FROM items", "E201", "value"),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> \"value\" + 1) FROM items", "E201", "\"value\""),
            ("SELECT TRANSFORM(ARRAY_CONSTRUCT(i.item_id), value -> value + item_id) FROM items i JOIN other o ON i.item_id = o.item_id", "E221", "item_id"),
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert_eq!(result.valid, !strict, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors.len(), 1, "{sql}: {:?}", result.errors);
            let error = &result.errors[0];
            assert_eq!(error.code, if !strict && code == "E221" { "W222" } else { code });
            assert_eq!(error.severity, if strict { crate::ValidationSeverity::Error } else { crate::ValidationSeverity::Warning });
            assert_eq!(&sql[error.start.unwrap()..error.end.unwrap()], token, "{sql}");
        }
    }
}

#[test]
fn test_schema_validation_lambda_parameter_types() {
    let mut schema = lambda_validation_schema();
    schema.tables[0].columns.push(
        serde_json::from_value(serde_json::json!({"name": "value", "type": "BOOLEAN"})).unwrap(),
    );
    for strict in [false, true] {
        let options = SchemaValidationOptions {
            check_types: true,
            check_references: true,
            strict: Some(strict),
            ..Default::default()
        };
        for sql in [
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value INT -> value + 1) FROM items",
            // Untyped bindings remain unknown, not the shadowed BOOLEAN type.
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value -> value + 1) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value INT -> value + item_id) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x INT -> ARRAY_CONSTRUCT(TRANSFORM(ARRAY_CONSTRUCT(TRUE), x BOOLEAN -> NOT x), x + 1)) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(TRUE), x BOOLEAN -> TRANSFORM(ARRAY_CONSTRUCT(item_id), x -> x + 1)) FROM items",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert!(result.valid && result.errors.is_empty(), "{sql}: {:?}", result.errors);
        }
        for sql in [
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(TRUE), item_id BOOLEAN -> item_id + 1) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x INT -> x + value) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value INT -> value + items.value) FROM items",
            "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), x INT -> TRANSFORM(ARRAY_CONSTRUCT(TRUE), x BOOLEAN -> x + 1)) FROM items",
        ] {
            let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
            assert_eq!(result.valid, !strict, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors.len(), 1, "{sql}: {:?}", result.errors);
            assert_eq!(result.errors[0].code, if strict { "E212" } else { "W211" });
        }
    }
}

#[test]
fn test_validation_lambda_binding_keeps_public_ast_and_guards() {
    let dialect = Dialect::get(DialectType::Snowflake);
    let sql = "SELECT TRANSFORM(ARRAY_CONSTRUCT(item_id), value INT -> value + 1) FROM items";
    let original = dialect.parse(sql).unwrap().remove(0);
    let bound = bind_validation_lambdas(original.clone(), DialectType::Snowflake);
    assert!(original
        .dfs()
        .any(|node| matches!(node, Expression::Column(column) if column.name.name == "value")));
    assert!(!bound
        .dfs()
        .any(|node| matches!(node, Expression::Column(column) if column.name.name == "value")));
    assert_eq!(original, dialect.parse(sql).unwrap().remove(0));

    // The parser keeps quoting and source locations on typed and untyped
    // parameters in both specialized and generic function-argument paths.
    for function in ["TRANSFORM", "FILTER"] {
        for parameter_type in ["", " INT"] {
            let sql = format!(
                "SELECT {function}(ARRAY_CONSTRUCT(item_id), \"Value\"{parameter_type} -> \"Value\" + 1) FROM items"
            );
            let parsed = dialect.parse(&sql).unwrap().remove(0);
            let parameter = parsed
                .dfs()
                .find_map(|node| match node {
                    Expression::Lambda(lambda) => lambda.parameters.first(),
                    _ => None,
                })
                .unwrap();
            assert!(parameter.quoted, "{sql}");
            assert_eq!(parameter.name, "Value");
            let span = parameter.span.unwrap();
            assert_eq!(&sql[span.start..span.end], "\"Value\"");
        }
    }

    let sql = format!(
        "SELECT {}item_id{} FROM items",
        "TRANSFORM(ARRAY_CONSTRUCT(item_id), x -> ".repeat(20),
        ")".repeat(20)
    );
    let schema = lambda_validation_schema();
    let result = validate_with_schema(
        &sql,
        DialectType::Snowflake,
        &schema,
        &SchemaValidationOptions::default(),
    );
    assert!(result.valid, "{:?}", result.errors);
    let options: SchemaValidationOptions = serde_json::from_value(
        serde_json::json!({"complexity_guard": {"maxFunctionCallDepth": 4}}),
    )
    .unwrap();
    let result = validate_with_schema(&sql, DialectType::Snowflake, &schema, &options);
    assert!(!result.valid);
    assert!(result.errors[0]
        .message
        .contains("E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED"));
}

fn lambda_validation_schema() -> ValidationSchema {
    serde_json::from_value(serde_json::json!({
        "strict": true,
        "tables": [
            {"name": "items", "columns": [{"name": "item_id", "type": "NUMBER"}]},
            {"name": "other", "columns": [{"name": "item_id", "type": "NUMBER"}]}
        ]
    }))
    .unwrap()
}

#[test]
fn test_schema_validation_order_by_output_names() {
    let schema: ValidationSchema = serde_json::from_value(serde_json::json!({
        "strict": true,
        "tables": [
            {"name": "current_items", "columns": [{"name": "item_id", "type": "NUMBER"}]},
            {"name": "archived_items", "columns": [{"name": "item_id", "type": "NUMBER"}]}
        ]
    }))
    .unwrap();
    for dialect in [
        DialectType::Snowflake,
        DialectType::DuckDB,
        DialectType::PostgreSQL,
        DialectType::BigQuery,
        DialectType::TSQL,
        DialectType::Fabric,
    ] {
        for check_references in [false, true] {
            for strict in [false, true] {
                let options = SchemaValidationOptions {
                    check_references,
                    strict: Some(strict),
                    ..Default::default()
                };
                for (projection, ordering) in [
                    // Exact issue #458 projection and ordering.
                    ("COALESCE(c.item_id, a.item_id) AS item_id", "item_id"),
                    ("COALESCE(c.item_id, a.item_id) AS merged_id", "merged_id"),
                    ("c.item_id AS item_id", "item_id"),
                    ("c.item_id", "item_id"),
                    ("c.item_id AS merged_id", "merged_id DESC, c.item_id"),
                    ("COALESCE(c.item_id, a.item_id) AS item_id", "1"),
                ] {
                    let sql = format!(
                        "SELECT {projection} FROM current_items AS c \
                         FULL JOIN archived_items AS a ON c.item_id = a.item_id ORDER BY {ordering}"
                    );
                    let result = validate_with_schema(&sql, dialect, &schema, &options);
                    assert!(result.valid, "{dialect}: {sql}: {:?}", result.errors);
                    assert!(
                        result.errors.is_empty(),
                        "{dialect}: {sql}: {:?}",
                        result.errors
                    );
                }
            }
        }
    }
}

#[test]
fn test_schema_validation_order_by_alias_expression_dialects() {
    let schema = base_schema();
    let options = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    for dialect in [
        DialectType::Snowflake,
        DialectType::DuckDB,
        DialectType::BigQuery,
        DialectType::PostgreSQL,
        DialectType::TSQL,
        DialectType::Fabric,
    ] {
        let scalar_aliases = matches!(
            dialect,
            DialectType::Snowflake | DialectType::DuckDB | DialectType::BigQuery
        );
        for ordering in ["merged_id + 1", "ABS(merged_id)", "COALESCE(merged_id, 0)"] {
            let sql = format!("SELECT u.id AS merged_id FROM users u ORDER BY {ordering}");
            let result = validate_with_schema(&sql, dialect, &schema, &options);
            assert_eq!(
                result.valid, scalar_aliases,
                "{dialect}: {sql}: {:?}",
                result.errors
            );
            if !scalar_aliases {
                assert!(result.errors.iter().any(|error| error.code == "E201"));
            }
        }
        // In dialects requiring standalone aliases, compound expressions can
        // still reference actual input columns of the same name.
        let sql = "SELECT u.age AS id FROM users u ORDER BY id + 1";
        assert!(validate_with_schema(sql, dialect, &schema, &options).valid);
    }
    // DuckDB allows aliases in expressions only as a fallback to input names.
    // The bare name binds the output; the compound expression is ambiguous.
    for (ordering, valid) in [
        ("id", true),
        ("(id)", true),
        ("id + 1", false),
        ("ABS(id)", false),
    ] {
        let sql = format!(
            "SELECT COALESCE(u.id, o.id) AS id FROM users u \
             FULL JOIN orders o ON u.id = o.id ORDER BY {ordering}"
        );
        let result = validate_with_schema(&sql, DialectType::DuckDB, &schema, &options);
        assert_eq!(result.valid, valid, "{sql}: {:?}", result.errors);
        if !valid {
            assert!(result.errors.iter().any(|error| error.code == "E221"));
        }
    }
}

#[test]
fn test_schema_validation_order_by_alias_quoting() {
    let schema = base_schema();
    for (dialect, alias, reference, valid) in [
        (DialectType::Snowflake, "\"MergedId\"", "\"MergedId\"", true),
        (DialectType::Snowflake, "\"MergedId\"", "mergedid", false),
        (DialectType::Snowflake, "mergedid", "\"MERGEDID\"", true),
        (DialectType::Snowflake, "mergedid", "\"mergedid\"", false),
        (
            DialectType::PostgreSQL,
            "\"MergedId\"",
            "\"MergedId\"",
            true,
        ),
        (DialectType::PostgreSQL, "\"MergedId\"", "mergedid", false),
        (DialectType::PostgreSQL, "MERGEDID", "\"mergedid\"", true),
        (DialectType::DuckDB, "\"MergedId\"", "mergedid", true),
        (DialectType::BigQuery, "`MergedId`", "mergedid", true),
    ] {
        let sql = format!("SELECT u.id AS {alias} FROM users u ORDER BY {reference}");
        let result = validate_with_schema(
            sql.as_str(),
            dialect,
            &schema,
            &SchemaValidationOptions::default(),
        );
        assert_eq!(result.valid, valid, "{dialect}: {sql}: {:?}", result.errors);
    }
}

#[test]
fn test_schema_validation_order_by_alias_scope_boundaries() {
    let schema = base_schema();
    let options = SchemaValidationOptions {
        check_references: true,
        ..Default::default()
    };
    for sql in [
        "WITH q AS (SELECT COALESCE(u.id, o.id) AS id FROM users u FULL JOIN orders o ON u.id = o.id ORDER BY id) SELECT id FROM q",
        "SELECT q.id FROM (SELECT COALESCE(u.id, o.id) AS id FROM users u FULL JOIN orders o ON u.id = o.id ORDER BY id) q",
        "SELECT u.id FROM users u ORDER BY (SELECT o.id AS merged_id FROM orders o ORDER BY merged_id LIMIT 1)",
        "SELECT u.id AS merged_id FROM users u ORDER BY (SELECT o.id FROM orders o LIMIT 1), merged_id",
        "SELECT u.id FROM users u WHERE EXISTS (SELECT o.id AS id FROM orders o ORDER BY id)",
    ] {
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert!(result.valid, "{sql}: {:?}", result.errors);
    }
    for (sql, code) in [
        ("SELECT id AS id FROM users u JOIN orders o ON u.id = o.id ORDER BY id", "E221"),
        ("SELECT u.id AS id, o.id AS id FROM users u JOIN orders o ON u.id = o.id ORDER BY id", "E221"),
        ("SELECT u.id AS merged_id FROM users u ORDER BY u.merged_id", "E201"),
        ("SELECT u.id AS merged_id FROM users u ORDER BY absent.merged_id", "E222"),
        ("SELECT u.missing AS merged_id FROM users u ORDER BY merged_id", "E201"),
        ("SELECT u.id AS merged_id FROM users u ORDER BY missing", "E201"),
        ("SELECT u.id AS merged_id FROM users u ORDER BY (SELECT merged_id FROM orders)", "E201"),
        ("SELECT u.id FROM users u ORDER BY (SELECT missing AS merged_id FROM orders ORDER BY merged_id LIMIT 1)", "E201"),
        ("SELECT *, u.id AS id FROM users u JOIN orders o ON u.id = o.id ORDER BY id", "E221"),
    ] {
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert!(!result.valid && result.errors.iter().any(|error| error.code == code), "{sql}: {:?}", result.errors);
    }
    // These clauses bind inputs independently of query-level ORDER BY.
    for (dialect, sql, code) in [
        (DialectType::PostgreSQL, "SELECT u.id AS id FROM users u JOIN orders o ON id = o.id ORDER BY id", "E221"),
        (DialectType::PostgreSQL, "SELECT u.id AS id FROM users u JOIN orders o ON u.id = o.id WHERE id > 0 ORDER BY id", "E221"),
        (DialectType::PostgreSQL, "SELECT u.id AS id FROM users u JOIN orders o ON u.id = o.id GROUP BY id ORDER BY id", "E221"),
        (DialectType::DuckDB, "SELECT u.id AS id FROM users u JOIN orders o ON u.id = o.id ORDER BY ROW_NUMBER() OVER (ORDER BY id)", "E221"),
        (DialectType::DuckDB, "SELECT u.id AS merged_id FROM users u ORDER BY SUM(merged_id)", "E201"),
        (DialectType::DuckDB, "SELECT u.id AS merged_id FROM users u ORDER BY STRING_AGG(u.name, ',' ORDER BY merged_id)", "E201"),
        (DialectType::DuckDB, "SELECT u.id AS merged_id FROM users u ORDER BY SUM(u.id) FILTER (WHERE merged_id > 0)", "E201"),
        (DialectType::PostgreSQL, "SELECT u.id AS merged_id FROM users u ORDER BY PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY merged_id)", "E201"),
    ] {
        let result = validate_with_schema(sql, dialect, &schema, &options);
        assert!(!result.valid && result.errors.iter().any(|error| error.code == code), "{dialect}: {sql}: {:?}", result.errors);
    }
}

#[test]
fn test_schema_validation_order_by_alias_diagnostics() {
    let schema = base_schema();
    for strict in [false, true] {
        let options = SchemaValidationOptions {
            check_references: true,
            strict: Some(strict),
            ..Default::default()
        };
        // The output reference is valid, but must not hide an ambiguous input
        // in the projection. Preserve severity and original source location.
        let sql = "SELECT id AS id FROM users u JOIN orders o ON u.id = o.id ORDER BY id";
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert_eq!(result.valid, !strict);
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        let error = &result.errors[0];
        assert_eq!(error.code, if strict { "E221" } else { "W222" });
        assert_eq!((error.start, error.end), (Some(7), Some(9)));

        let sql = "SELECT u.id AS merged_id FROM users u ORDER BY missing";
        let result = validate_with_schema(sql, DialectType::Snowflake, &schema, &options);
        assert_eq!(result.valid, !strict);
        let error = &result.errors[0];
        assert_eq!(error.code, "E201");
        assert_eq!(error.start, Some(sql.find("missing").unwrap()));
        assert_eq!(error.end, Some(sql.len()));
    }
}

#[test]
fn test_schema_validation_open_sources() {
    for columns in [serde_json::json!([]), serde_json::json!([{"name": "*"}])] {
        let mut schema = base_schema();
        schema.tables[0].columns = serde_json::from_value(columns).unwrap();
        for sql in [
            "SELECT missing FROM users",
            "SELECT u.missing FROM users u",
            "SELECT payload.field FROM users",
            "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM users u WHERE u.missing = o.id)",
            "SELECT missing FROM users u JOIN orders o ON TRUE",
            "SELECT missing FROM orders o JOIN users u ON TRUE",
            "WITH q AS (SELECT * FROM users) SELECT missing FROM q",
            "SELECT q.missing FROM (SELECT * FROM users) q",
        ] {
            let result = validate_with_schema(
                sql,
                DialectType::Snowflake,
                &schema,
                &SchemaValidationOptions {
                    check_references: true,
                    ..Default::default()
                },
            );
            assert!(result.valid, "{sql}: {:?}", result.errors);
        }
        let result = validate_with_schema(
            "SELECT o.missing FROM users u JOIN orders o ON TRUE",
            DialectType::Snowflake,
            &schema,
            &SchemaValidationOptions::default(),
        );
        assert!(!result.valid);
    }
}

#[test]
fn test_schema_reference_spans_and_ambiguity_options() {
    let schema = base_schema();
    for (sql, code, token) in [
        (
            "SELECT u.id FROM users u WHERE u.missing = TRUE",
            "E201",
            "missing",
        ),
        ("SELECT x.id FROM users", "E222", "x"),
        ("SELECT * FROM absent", "E200", "absent"),
        (
            "SELECT '😀', u.\"míssing\" FROM users u",
            "E201",
            "\"míssing\"",
        ),
        (
            "SELECT id FROM users u JOIN orders o ON u.id = o.user_id",
            "E221",
            "id",
        ),
    ] {
        let result = validate_with_schema(
            sql,
            DialectType::Snowflake,
            &schema,
            &SchemaValidationOptions {
                check_references: true,
                ..Default::default()
            },
        );
        let error = result
            .errors
            .iter()
            .find(|error| error.code == code)
            .unwrap_or_else(|| panic!("{sql}: {:?}", result.errors));
        let start = sql[..sql.find(token).unwrap()].chars().count();
        assert_eq!(error.start, Some(start), "{sql}");
        assert_eq!(error.end, Some(start + token.chars().count()), "{sql}");
        assert_eq!(error.line, Some(1));
        assert!(error.column.is_some());
    }
    let sql = "SELECT missing, missing FROM users";
    let result = validate_with_schema(
        sql,
        DialectType::Snowflake,
        &schema,
        &SchemaValidationOptions::default(),
    );
    let starts: Vec<_> = result
        .errors
        .iter()
        .filter(|e| e.code == "E201")
        .map(|e| e.start)
        .collect();
    assert_eq!(starts, vec![Some(7), Some(16)]);
    let sql = "SELECT id FROM users u JOIN orders o ON u.id = o.user_id";
    assert!(
        validate_with_schema(
            sql,
            DialectType::Snowflake,
            &schema,
            &SchemaValidationOptions::default()
        )
        .valid
    );
    let result = validate_with_schema(
        sql,
        DialectType::Snowflake,
        &schema,
        &SchemaValidationOptions {
            check_references: true,
            strict: Some(false),
            ..Default::default()
        },
    );
    assert!(result.valid);
    assert!(result
        .errors
        .iter()
        .any(|e| e.code == "W222" && e.start == Some(7)));
    let error = reference_diagnostic("Unknown column".into(), "E201", true, None);
    assert_eq!(
        (error.start, error.end, error.line, error.column),
        (None, None, None, None)
    );
}

#[test]
fn test_canonical_type_family_aliases() {
    assert_eq!(canonical_type_family("INT4"), TypeFamily::Integer);
    for name in ["HUGEINT", "INT128", "LARGEINT", "Nullable(Int128)"] {
        assert_eq!(canonical_type_family(name), TypeFamily::Integer);
    }
    assert_eq!(data_type_family(&DataType::Int128), TypeFamily::Integer);
    for name in [
        "UTINYINT",
        "UINT8",
        "USMALLINT",
        "UINT16",
        "UINTEGER",
        "UINT32",
        "UBIGINT",
        "UINT64",
        "UHUGEINT",
        "UINT128",
    ] {
        assert_eq!(canonical_type_family(name), TypeFamily::Integer);
        let dt = crate::parse_data_type(name, DialectType::DuckDB).unwrap();
        assert_eq!(data_type_family(&dt), TypeFamily::Integer);
    }
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
fn test_validate_with_schema_partial_schema_stays_strict() {
    let mut schema = base_schema();
    let users = schema
        .tables
        .iter_mut()
        .find(|table| table.name == "users")
        .expect("users table");
    users.columns.retain(|column| column.name == "id");

    let opts = SchemaValidationOptions::default();
    let result = validate_with_schema(
        "SELECT id, name FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(!result.valid);
    assert!(result.errors.iter().any(|error| {
        error.code == validation_codes::E_UNKNOWN_COLUMN && error.message.contains("name")
    }));
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

    let valid = validate_with_schema(
        "SELECT Foo(id) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(valid.valid, "{:?}", valid.errors);

    let invalid = validate_with_schema(
        "SELECT FOO(id) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
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
    catalog.set_function_name_case(DialectType::Generic, "bar", FunctionNameCase::Sensitive);

    let opts = SchemaValidationOptions {
        check_types: true,
        function_catalog: Some(Arc::new(catalog)),
        ..Default::default()
    };

    let valid = validate_with_schema(
        "SELECT Bar(id) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
    assert!(valid.valid, "{:?}", valid.errors);

    let invalid = validate_with_schema(
        "SELECT BAR(id) FROM users",
        DialectType::Generic,
        &schema,
        &opts,
    );
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
fn test_semantic_warning_uses_column_source_span() {
    let sql = "SELECT customer_id, SUM(amount) FROM orders";
    let result = crate::validate_with_options(
        sql,
        DialectType::Snowflake,
        &crate::ValidationOptions {
            semantic: true,
            ..Default::default()
        },
    );
    let warning = result
        .errors
        .iter()
        .find(|error| error.code == "E230")
        .unwrap();
    assert_eq!((warning.start, warning.end), (Some(7), Some(18)));
    assert_eq!((warning.line, warning.column), (Some(1), Some(19)));
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
fn test_basic_and_schema_validation_share_semantic_diagnostics() {
    let sql = "SELECT * FROM users LIMIT 10";
    let basic = crate::validate_with_options(
        sql,
        DialectType::Generic,
        &crate::ValidationOptions {
            semantic: true,
            ..Default::default()
        },
    );
    let schema = validate_with_schema(
        sql,
        DialectType::Generic,
        &base_schema(),
        &SchemaValidationOptions {
            semantic: true,
            ..Default::default()
        },
    );

    let diagnostics = |result: &ValidationResult| {
        result
            .errors
            .iter()
            .filter(|error| error.code.starts_with('W'))
            .map(|error| {
                (
                    error.code.clone(),
                    error.message.clone(),
                    error.line,
                    error.column,
                    error.start,
                    error.end,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(diagnostics(&basic), diagnostics(&schema));
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

#[cfg(feature = "dialect-hana")]
#[test]
fn hana_integer_casts_participate_in_type_validation() {
    let schema = ValidationSchema {
        tables: vec![],
        strict: Some(true),
    };
    let opts = SchemaValidationOptions {
        check_types: true,
        strict: Some(true),
        ..Default::default()
    };
    for data_type in ["TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"] {
        for (sql, code) in [
            (
                format!("SELECT CAST(1 AS {data_type}) + 'x'"),
                validation_codes::E_INVALID_ARITHMETIC_TYPE,
            ),
            (
                format!("SELECT CAST(1 AS {data_type}) = DATE '2024-01-01'"),
                validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES,
            ),
            (
                format!("SELECT LENGTH(CAST(1 AS {data_type}))"),
                validation_codes::E_INVALID_FUNCTION_ARGUMENT_TYPE,
            ),
        ] {
            let result = validate_with_schema(&sql, DialectType::HANA, &schema, &opts);
            assert!(!result.valid, "{sql}");
            assert!(
                result.errors.iter().any(|e| e.code == code),
                "{sql}: {result:?}"
            );
        }
        for sql in [
            format!("SELECT CAST(1 AS {data_type}) + 2"),
            format!("SELECT CAST(1 AS {data_type}) = 2"),
            format!("SELECT ABS(CAST(1 AS {data_type}))"),
        ] {
            let result = validate_with_schema(&sql, DialectType::HANA, &schema, &opts);
            assert!(result.valid, "{sql}: {result:?}");
        }
    }
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

#[test]
fn test_validate_with_schema_accepts_struct_field_access_issue_408() {
    let schema: ValidationSchema = serde_json::from_value(serde_json::json!({
        "tables": [{
            "name": "source_table",
            "columns": [
                {"name": "nested_items", "type": "STRUCT(field_value VARCHAR)[]"},
                {"name": "composite_value", "type": "STRUCT(field_value VARCHAR, label VARCHAR)"}
            ]
        }]
    }))
    .expect("schema");
    let options = SchemaValidationOptions::default();

    for sql in [
        "SELECT composite_value.field_value AS output_value FROM source_table",
        "SELECT item.field_value AS output_value FROM source_table s \
         CROSS JOIN UNNEST(s.nested_items) AS expanded(item)",
    ] {
        let result = validate_with_schema(sql, DialectType::DuckDB, &schema, &options);
        assert!(
            result.valid,
            "validation failed for {sql:?}: {:?}",
            result.errors
        );
    }

    let negative = validate_with_schema(
        "SELECT missing.field_value FROM source_table",
        DialectType::DuckDB,
        &schema,
        &options,
    );
    assert!(!negative.valid);
    assert!(negative.errors.iter().any(|error| {
        error.code == validation_codes::E_UNRESOLVED_REFERENCE && error.message.contains("missing")
    }));
}
#[test]
fn schema_validation_propagates_complexity_guard_to_both_parses() {
    let schema: ValidationSchema = serde_json::from_value(serde_json::json!({
        "tables": [{"name": "records", "columns": [{"name": "value", "type": "INTEGER"}]}]
    }))
    .unwrap();
    let sql = format!(
        "SELECT {}value{} FROM records",
        "COALESCE(".repeat(65),
        ", 0)".repeat(65)
    );
    assert!(
        !validate_with_schema(
            &sql,
            DialectType::Snowflake,
            &schema,
            &SchemaValidationOptions::default()
        )
        .valid
    );
    for limit in [Some(128), None] {
        let options = SchemaValidationOptions {
            complexity_guard: Some(crate::ComplexityGuardOptions {
                max_function_call_depth: limit,
                ..Default::default()
            }),
            check_types: true,
            check_references: true,
            ..Default::default()
        };
        let result = validate_with_schema(&sql, DialectType::Snowflake, &schema, &options);
        assert!(result.valid, "{:?}", result.errors);
        let invalid = validate_with_schema(
            &sql.replace("value", "missing"),
            DialectType::Snowflake,
            &schema,
            &options,
        );
        assert!(!invalid.valid);
        assert!(invalid.errors.iter().any(|error| error.code == "E201"));
    }
}
