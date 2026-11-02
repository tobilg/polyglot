use polyglot_sql::traversal::ExpressionWalk;
use polyglot_sql::{
    analyze_query, parse_one, validate_with_schema, AnalyzeQueryOptions, Dialect, DialectType,
    SchemaValidationOptions, ValidationSchema,
};
use std::hint::black_box;
use std::time::{Duration, Instant};

fn measure<T>(name: &str, n: usize, mut f: impl FnMut() -> T) {
    drop(black_box(f()));
    let mut samples = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let mut count = 0;
        loop {
            drop(black_box(f()));
            count += 1;
            if start.elapsed() >= Duration::from_millis(100) {
                break;
            }
        }
        samples.push(start.elapsed().as_secs_f64() * 1e6 / count as f64);
    }
    samples.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"operation": name, "size": n, "median_us": samples[2], "samples_us": samples})
    );
}

fn main() {
    let pg = DialectType::PostgreSQL;
    let dialect = Dialect::get(pg);
    let schema: ValidationSchema =
        serde_json::from_str(r#"{"tables":[{"name":"t","columns":[{"name":"x","type":"INT"}]}]}"#)
            .unwrap();
    measure("type_annotator_new", 1, || {
        polyglot_sql::optimizer::annotate_types::TypeAnnotator::new(None, Some(pg))
    });
    for n in [1000, 10000] {
        let ascii = format!(
            "SELECT {}",
            (0..n).map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
        );
        let unicode = format!("{ascii} /* π */");
        measure("parse_ascii_numbers", n, || dialect.parse(&ascii).unwrap());
        measure("parse_mixed_unicode_numbers", n, || {
            dialect.parse(&unicode).unwrap()
        });
        measure("tokenize_mixed_unicode_numbers", n, || {
            dialect.tokenize(&unicode).unwrap()
        });
    }
    for n in [25, 50, 100, 200] {
        let sql = format!(
            "SELECT {} FROM t",
            (0..n)
                .map(|i| format!("x + {i} AS c{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let other = sql.replace("x + 0", "x + 1");
        let ast = parse_one(&sql, pg).unwrap();
        let target = parse_one(&other, pg).unwrap();
        let validation = validate_with_schema(
            &sql,
            pg,
            &schema,
            &SchemaValidationOptions {
                check_types: true,
                ..Default::default()
            },
        );
        assert!(validation.valid, "{:?}", validation.errors);
        measure("parse_width", n, || dialect.parse(&sql).unwrap());
        measure("diff_width", n, || {
            polyglot_sql::diff::diff(&ast, &target, true)
        });
        measure("analysis_width", n, || {
            analyze_query(
                &sql,
                AnalyzeQueryOptions {
                    dialect: pg,
                    schema: Some(schema.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
        });
        measure("openlineage_width", n, || {
            polyglot_sql::openlineage::openlineage_column_lineage(
                &sql,
                &polyglot_sql::openlineage::OpenLineageOptions {
                    dialect: pg,
                    producer: "review".into(),
                    dataset_namespace: Some("review".into()),
                    output_dataset: Some(polyglot_sql::openlineage::OpenLineageDatasetId::new(
                        "review", "out",
                    )),
                    ..Default::default()
                },
            )
            .unwrap()
        });
        measure("validation_width", n, || {
            validate_with_schema(
                &sql,
                pg,
                &schema,
                &SchemaValidationOptions {
                    check_types: true,
                    ..Default::default()
                },
            )
        });
    }
    for n in [8, 16, 32, 64] {
        let sql = format!(
            "WITH {} SELECT x FROM c{}",
            (0..n)
                .map(|i| if i == 0 {
                    "c0 AS (SELECT x FROM t)".to_owned()
                } else {
                    format!("c{i} AS (SELECT x FROM c{})", i - 1)
                })
                .collect::<Vec<_>>()
                .join(", "),
            n - 1
        );
        let ast = parse_one(&sql, pg).unwrap();
        measure("parse_ctes", n, || dialect.parse(&sql).unwrap());
        measure("build_scope_ctes", n, || {
            polyglot_sql::scope::build_scope(&ast)
        });
    }
    for n in [32, 64, 128, 256] {
        let sql = format!("SELECT {} FROM t", vec!["x"; n].join(" + "));
        let ast = parse_one(&sql, pg).unwrap();
        measure("borrowed_walk", n, || ast.dfs().count());
        measure("cloned_walk_core_equivalent", n, || {
            ast.dfs().cloned().collect::<Vec<_>>()
        });
        let typed_sql = format!("SELECT {}", vec!["1"; n].join(" + "));
        let typed_ast = parse_one(&typed_sql, pg).unwrap();
        measure("annotate_arithmetic", n, || {
            let mut copy = typed_ast.clone();
            polyglot_sql::optimizer::annotate_types::annotate_types(&mut copy, None, Some(pg));
            copy
        });
    }
    for n in [50, 100, 200] {
        let sql = format!(
            "WITH c AS (SELECT {} FROM t) SELECT {} FROM c",
            (0..100)
                .map(|i| format!("x + {i} AS v{i}"))
                .collect::<Vec<_>>()
                .join(", "),
            (0..n)
                .map(|i| format!("v0 + {i} AS a{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let validation = validate_with_schema(
            &sql,
            pg,
            &schema,
            &SchemaValidationOptions {
                check_types: true,
                ..Default::default()
            },
        );
        assert!(validation.valid, "{:?}", validation.errors);
        measure("validation_cte_aliases", n, || {
            validate_with_schema(
                &sql,
                pg,
                &schema,
                &SchemaValidationOptions {
                    check_types: true,
                    ..Default::default()
                },
            )
        });
    }
    for n in [100, 1000] {
        let sql = format!(
            "SELECT {} FROM t",
            (0..n)
                .map(|i| format!("x + {i} AS c{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let ast = parse_one(&sql, pg).unwrap();
        let json = serde_json::to_string(&ast).unwrap();
        measure("parse_pipeline", n, || dialect.parse(&sql).unwrap());
        measure("format_pipeline", n, || {
            polyglot_sql::format(&sql, pg).unwrap()
        });
        measure("generate_pipeline", n, || dialect.generate(&ast).unwrap());
        measure("transpile_pg_duckdb", n, || {
            dialect.transpile(&sql, DialectType::DuckDB).unwrap()
        });
        measure("transpile_pg_tsql", n, || {
            dialect.transpile(&sql, DialectType::TSQL).unwrap()
        });
        measure("ast_json_compat_decode", n, || {
            polyglot_sql::ast_json::expression_from_str(&json).unwrap()
        });
        measure("ast_json_direct_decode", n, || {
            serde_json::from_str::<polyglot_sql::Expression>(&json).unwrap()
        });
    }
}

// Run with: cargo run -p polyglot-sql --example performance_review --profile performance
// Timings include result destruction, exclude fixture construction, and use five
// independently timed batches after warm-up. Each JSON line retains all samples.
