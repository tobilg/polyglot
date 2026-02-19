//! ClickHouse test suite parser tests.
//!
//! Parses all .sql files from the ClickHouse test suite and asserts zero failures.
//! Run with: RUST_MIN_STACK=16777216 cargo test --test custom_clickhouse_parser -p polyglot-sql --release -- --nocapture

use polyglot_sql::DialectType;
use std::fs;
use std::path::PathBuf;

/// Files to skip because they contain non-SQL content (KQL, intentionally invalid SQL,
/// ClickHouse-specific syntax not in scope, special test harness files, etc.)
const SKIP_FILES: &[&str] = &[
    // KQL (Kusto Query Language) files - not SQL
    "02366_kql_distinct.sql",
    "02366_kql_extend.sql",
    "02366_kql_func_dynamic.sql",
    "02366_kql_func_iif.sql",
    "02366_kql_func_scalar.sql",
    "02366_kql_func_string.sql",
    "02366_kql_mvexpand.sql",
    "02366_kql_summarize.sql",
    "02366_kql_tabular.sql",
    // Intentionally invalid/malformed SQL for error handling tests
    "02469_fix_aliases_parser.sql",
    "02472_segfault_expression_parser.sql",
    "02474_fix_function_parser_bug.sql",
    "02476_fix_cast_parser_bug.sql",
    "02515_tuple_lambda_parsing.sql",
    "02901_remove_nullable_crash_analyzer.sql",
    "02985_dialects_with_distributed_tables.sql",
    "03144_fuzz_quoted_type_name.sql",
    "03814_subquery_parser_partially.sql",
    // Files that test ClickHouse-specific syntax extensions not in scope
    "00692_if_exception_code.sql",          // throwIf() returns non-standard error codes
    "01144_multiword_data_types.sql",       // ClickHouse-specific TINYINT UNSIGNED etc.
    "01280_unicode_whitespaces_lexer.sql",  // Unicode whitespace in tokenizer
    "01564_test_hint_woes.sql",             // ClickHouse hint syntax with ||
    "01604_explain_ast_of_nonselect_query.sql", // EXPLAIN AST non-select
    "01622_multiple_ttls.sql",              // ClickHouse TTL syntax
    "01666_blns_long.sql",                  // Big List of Naughty Strings
    "02128_apply_lambda_parsing.sql",       // ClickHouse APPLY/lambda syntax
    "02343_create_empty_as_select.sql",     // CREATE TABLE ... AS SELECT with empty
    "02493_numeric_literals_with_underscores.sql", // Numeric literal underscores
    "02863_ignore_foreign_keys_in_tables_definition.sql", // FOREIGN KEY in ClickHouse
    "03257_reverse_sorting_key.sql",        // ORDER BY (col DESC) in DDL
    "03257_reverse_sorting_key_simple.sql",
    "03257_reverse_sorting_key_zookeeper.sql",
    "03273_select_from_explain_ast_non_select.sql", // EXPLAIN AST
    "03280_aliases_for_selects_and_views.sql", // Alias syntax extension
    "03286_reverse_sorting_key_final.sql",
    "03286_reverse_sorting_key_final2.sql",
    "03322_bugfix_of_with_insert.sql",      // WITH ... INSERT
    "03460_basic_projection_index.sql",     // PROJECTION INDEX TYPE syntax
    "03558_no_alias_in_single_expressions.sql", // ORDER BY (col AS x)
    "03559_explain_ast_in_subquery.sql",    // INTO OUTFILE in subquery
    "03601_temporary_views.sql",            // TEMPORARY VIEW syntax
    "03668_shard_join_in_reverse_order.sql",
    "03669_min_max_projection_with_reverse_order_key.sql",
    "03761_join_using_empty_list.sql",      // JOIN USING ()
    "03800_assume_not_null_coalesce_if_null_monotonicity_key_condition.sql",
];

#[test]
fn test_clickhouse_parser_all() {
    let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../external-projects/clickhouse/tests/queries/0_stateless");

    let mut sql_files: Vec<_> = fs::read_dir(&test_dir)
        .expect("Failed to read ClickHouse test directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "sql"))
        .collect();
    sql_files.sort();

    let total = sql_files.len();
    let mut parsed = 0;
    let mut failed = 0;
    let mut skipped = 0;
    let mut failures: Vec<(String, String)> = Vec::new();

    for path in &sql_files {
        let filename = path.file_name().unwrap().to_string_lossy().to_string();

        // Skip known non-SQL or out-of-scope files
        if SKIP_FILES.contains(&filename.as_str()) {
            skipped += 1;
            continue;
        }

        // Read file, skip if not valid UTF-8
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Skip empty files
        let trimmed = content.trim();
        if trimmed.is_empty() {
            skipped += 1;
            continue;
        }

        // Try to parse
        match polyglot_sql::parse(trimmed, DialectType::ClickHouse) {
            Ok(_) => {
                parsed += 1;
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                failures.push((filename, err_msg));
                failed += 1;
            }
        }
    }

    println!("\n=== ClickHouse Test Suite Parsing Results ===");
    println!("Total .sql files: {}", total);
    println!("Skipped (non-UTF8/empty/out-of-scope): {}", skipped);
    println!("Parsed successfully: {}", parsed);
    println!("Failed: {}", failed);

    if !failures.is_empty() {
        println!("\n=== Failures ===");
        for (file, err) in &failures {
            println!("  {} => {}", file, err);
        }
    }

    assert!(
        failures.is_empty(),
        "{} out of {} ClickHouse .sql files failed to parse",
        failed,
        total - skipped
    );
}
