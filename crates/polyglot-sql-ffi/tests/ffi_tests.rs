use polyglot_sql_ffi::{
    polyglot_dialect_count, polyglot_dialect_list, polyglot_diff, polyglot_format,
    polyglot_format_with_options, polyglot_free_result, polyglot_free_string,
    polyglot_free_validation_result, polyglot_generate, polyglot_lineage,
    polyglot_lineage_with_schema, polyglot_openlineage_column_lineage,
    polyglot_openlineage_job_event, polyglot_openlineage_run_event, polyglot_optimize,
    polyglot_parse, polyglot_parse_one, polyglot_qualify_tables,
    polyglot_rename_tables_with_options, polyglot_source_tables, polyglot_transpile,
    polyglot_transpile_with_options, polyglot_validate, polyglot_version, PolyglotResult,
    PolyglotValidationResult,
};
use serde_json::Value;
use std::ffi::{CStr, CString};
use std::ptr;

fn c(s: &str) -> CString {
    CString::new(s).expect("CString conversion failed")
}

unsafe fn opt_string(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

fn consume_result(result: PolyglotResult) -> (i32, Option<String>, Option<String>) {
    let status = result.status;
    let data = unsafe { opt_string(result.data) };
    let error = unsafe { opt_string(result.error) };
    polyglot_free_result(result);
    (status, data, error)
}

fn consume_validation(
    result: PolyglotValidationResult,
) -> (i32, i32, Option<String>, Option<String>) {
    let status = result.status;
    let valid = result.valid;
    let errors_json = unsafe { opt_string(result.errors_json) };
    let error = unsafe { opt_string(result.error) };
    polyglot_free_validation_result(result);
    (status, valid, errors_json, error)
}

#[test]
fn test_transpile_happy_path() {
    let sql = c("SELECT IFNULL(a, b) FROM t");
    let from = c("mysql");
    let to = c("postgres");

    let (status, data, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let json = data.expect("missing data");
    let statements: Vec<String> = serde_json::from_str(&json).expect("invalid JSON");
    assert_eq!(statements.len(), 1);
    assert!(
        statements[0].to_uppercase().contains("COALESCE"),
        "expected COALESCE in output, got: {}",
        statements[0]
    );
}

#[test]
fn test_transpile_tsql_identity_preserves_nvarchar() {
    let sql = c("SELECT CAST(x AS NVARCHAR(MAX))");
    let from = c("tsql");
    let to = c("tsql");

    let (status, data, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let statements: Vec<String> =
        serde_json::from_str(&data.expect("missing data")).expect("invalid JSON");
    assert_eq!(statements, vec!["SELECT CAST(x AS NVARCHAR(MAX))"]);
}

#[test]
fn test_transpile_tsql_to_fabric_maps_nvarchar_to_varchar() {
    let sql = c("SELECT CAST(x AS NVARCHAR(MAX))");
    let from = c("tsql");
    let to = c("fabric");

    let (status, data, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let statements: Vec<String> =
        serde_json::from_str(&data.expect("missing data")).expect("invalid JSON");
    assert_eq!(statements, vec!["SELECT CAST(x AS VARCHAR(MAX))"]);
}

#[test]
fn test_transpile_snowflake_timestamp_variant_names_match_sqlglot_aliases() {
    let sql = c("SELECT CURRENT_TIMESTAMP()::TIMESTAMPNTZ");
    let from = c("snowflake");
    let to = c("snowflake");

    let (status, data, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let statements: Vec<String> =
        serde_json::from_str(&data.expect("missing data")).expect("invalid JSON");
    assert_eq!(
        statements,
        vec!["SELECT CAST(CURRENT_TIMESTAMP() AS TIMESTAMPNTZ)"]
    );
}

#[test]
fn test_transpile_invalid_sql() {
    let sql = c("SELECT FROM");
    let from = c("mysql");
    let to = c("postgres");

    let (status, data, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 3);
    assert!(data.is_none());
    assert!(error.is_some());
}

#[test]
fn test_transpile_invalid_dialect() {
    let sql = c("SELECT 1");
    let from = c("not_a_dialect");
    let to = c("postgres");

    let (status, _, error) =
        consume_result(polyglot_transpile(sql.as_ptr(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 5);
    assert!(error.is_some());
}

#[test]
fn test_transpile_null_pointer_input() {
    let from = c("mysql");
    let to = c("postgres");
    let (status, _, error) =
        consume_result(polyglot_transpile(ptr::null(), from.as_ptr(), to.as_ptr()));
    assert_eq!(status, 5);
    assert!(error.is_some());
}

#[test]
fn test_transpile_with_options_default() {
    // Empty options JSON should behave identically to polyglot_transpile.
    let sql = c("SELECT IFNULL(a, b) FROM t");
    let from = c("mysql");
    let to = c("postgres");
    let opts = c("{}");

    let (status, data, error) = consume_result(polyglot_transpile_with_options(
        sql.as_ptr(),
        from.as_ptr(),
        to.as_ptr(),
        opts.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let json = data.expect("missing data");
    let statements: Vec<String> = serde_json::from_str(&json).expect("invalid JSON");
    assert_eq!(statements.len(), 1);
    assert!(statements[0].to_uppercase().contains("COALESCE"));
    // Default (pretty=false) → single-line output
    assert!(!statements[0].contains('\n'));
}

#[test]
fn test_transpile_with_options_pretty() {
    // pretty:true should yield multi-line output.
    let sql = c("SELECT a, b, c, d FROM t UNION SELECT a, b, c, d FROM u");
    let from = c("postgres");
    let to = c("postgres");
    let opts = c(r#"{"pretty": true}"#);

    let (status, data, error) = consume_result(polyglot_transpile_with_options(
        sql.as_ptr(),
        from.as_ptr(),
        to.as_ptr(),
        opts.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let json = data.expect("missing data");
    let statements: Vec<String> = serde_json::from_str(&json).expect("invalid JSON");
    assert_eq!(statements.len(), 1);
    // Pretty output should contain newlines
    assert!(
        statements[0].contains('\n'),
        "expected pretty-printed (multi-line) output, got: {}",
        statements[0]
    );
}

#[test]
fn test_transpile_with_options_invalid_json() {
    let sql = c("SELECT 1");
    let from = c("postgres");
    let to = c("postgres");
    let opts = c("not-json");

    let (status, _, error) = consume_result(polyglot_transpile_with_options(
        sql.as_ptr(),
        from.as_ptr(),
        to.as_ptr(),
        opts.as_ptr(),
    ));
    assert_eq!(status, 6); // STATUS_SERIALIZATION_ERROR
    assert!(error
        .unwrap_or_default()
        .contains("Invalid transpile options JSON"));
}

#[test]
fn test_transpile_with_options_null_options() {
    let sql = c("SELECT 1");
    let from = c("postgres");
    let to = c("postgres");

    let (status, _, error) = consume_result(polyglot_transpile_with_options(
        sql.as_ptr(),
        from.as_ptr(),
        to.as_ptr(),
        ptr::null(),
    ));
    assert_eq!(status, 5); // STATUS_INVALID_ARGUMENT
    assert!(error.is_some());
}

#[test]
fn test_null_pointer_inputs_on_other_apis() {
    let dialect = c("generic");
    let sql = c("SELECT 1");
    let column = c("a");

    let (status, _, _) = consume_result(polyglot_parse(ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_generate(ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_format(ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);
    let options_json = c("{}");
    let (status, _, _) = consume_result(polyglot_format_with_options(
        ptr::null(),
        dialect.as_ptr(),
        options_json.as_ptr(),
    ));
    assert_eq!(status, 5);

    let (status, _, _, _) = consume_validation(polyglot_validate(ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_optimize(ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_lineage(
        column.as_ptr(),
        ptr::null(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_source_tables(
        column.as_ptr(),
        ptr::null(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 5);

    let schema_json = c(r#"{"tables":[]}"#);
    let (status, _, _) = consume_result(polyglot_lineage_with_schema(
        column.as_ptr(),
        ptr::null(),
        schema_json.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 5);

    let (status, _, _) = consume_result(polyglot_diff(sql.as_ptr(), ptr::null(), dialect.as_ptr()));
    assert_eq!(status, 5);

    let openlineage_options = c(r#"{"producer":"test"}"#);
    let (status, _, _) = consume_result(polyglot_openlineage_column_lineage(
        ptr::null(),
        openlineage_options.as_ptr(),
    ));
    assert_eq!(status, 5);
    let (status, _, _) = consume_result(polyglot_openlineage_job_event(
        ptr::null(),
        openlineage_options.as_ptr(),
    ));
    assert_eq!(status, 5);
    let (status, _, _) = consume_result(polyglot_openlineage_run_event(
        ptr::null(),
        openlineage_options.as_ptr(),
    ));
    assert_eq!(status, 5);
}

#[test]
fn test_parse_returns_json_array() {
    let sql = c("SELECT 1");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_parse(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let payload = data.expect("missing parse payload");
    let parsed: Value = serde_json::from_str(&payload).expect("invalid parse json");
    assert!(parsed.is_array());
    assert_eq!(parsed.as_array().expect("array").len(), 1);
}

#[test]
fn test_parse_one_multiple_statements_fails() {
    let sql = c("SELECT 1; SELECT 2");
    let dialect = c("generic");
    let (status, _, error) = consume_result(polyglot_parse_one(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 1);
    assert!(error.is_some());
}

#[test]
fn test_parse_generate_roundtrip() {
    let sql = c("SELECT a + 1 FROM t");
    let dialect = c("generic");

    let (parse_status, parse_data, parse_error) =
        consume_result(polyglot_parse(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(parse_status, 0, "parse_error={parse_error:?}");

    let ast_json = c(&parse_data.expect("missing parse result"));
    let (gen_status, gen_data, gen_error) =
        consume_result(polyglot_generate(ast_json.as_ptr(), dialect.as_ptr()));
    assert_eq!(gen_status, 0, "gen_error={gen_error:?}");

    let statements: Vec<String> =
        serde_json::from_str(&gen_data.expect("missing generate output")).expect("invalid json");
    assert_eq!(statements.len(), 1);
    assert!(statements[0].to_uppercase().contains("SELECT"));
}

#[test]
fn test_parse_generate_postgres_prepare_and_execute() {
    let dialect = c("postgres");
    let prepare_sql = c("PREPARE leak (int) AS SELECT id FROM sensitive_table WHERE id = $1");

    let (parse_status, parse_data, parse_error) =
        consume_result(polyglot_parse(prepare_sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(parse_status, 0, "parse_error={parse_error:?}");
    let payload = parse_data.expect("missing parse result");
    let parsed: Value = serde_json::from_str(&payload).expect("invalid parse json");
    assert!(parsed[0].get("prepare").is_some(), "payload={parsed}");

    let ast_json = c(&payload);
    let (gen_status, gen_data, gen_error) =
        consume_result(polyglot_generate(ast_json.as_ptr(), dialect.as_ptr()));
    assert_eq!(gen_status, 0, "gen_error={gen_error:?}");
    let statements: Vec<String> =
        serde_json::from_str(&gen_data.expect("missing generate output")).expect("invalid json");
    assert!(statements[0].starts_with("PREPARE leak (INT) AS SELECT"));

    let execute_sql = c("EXECUTE leak(1)");
    let (exec_status, exec_data, exec_error) =
        consume_result(polyglot_parse(execute_sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(exec_status, 0, "exec_error={exec_error:?}");
    let parsed_execute: Value =
        serde_json::from_str(&exec_data.expect("missing execute parse result"))
            .expect("invalid parse json");
    assert_eq!(parsed_execute[0]["execute"]["prepared"], true);
    assert_eq!(
        parsed_execute[0]["execute"]["arguments"]
            .as_array()
            .expect("arguments")
            .len(),
        1
    );
}

#[test]
fn test_qualify_tables_ast_transform() {
    let sql =
        c("SELECT * FROM (SELECT * FROM tab_1) UNION ALL SELECT * FROM (SELECT * FROM tab_1)");
    let dialect = c("generic");

    let (parse_status, parse_data, parse_error) =
        consume_result(polyglot_parse(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(parse_status, 0, "parse_error={parse_error:?}");

    let ast_json = c(&parse_data.expect("missing parse result"));
    let options_json = c("{}");
    let (qualify_status, qualify_data, qualify_error) = consume_result(polyglot_qualify_tables(
        ast_json.as_ptr(),
        options_json.as_ptr(),
    ));
    assert_eq!(qualify_status, 0, "qualify_error={qualify_error:?}");

    let qualified_ast = c(&qualify_data.expect("missing qualify result"));
    let (gen_status, gen_data, gen_error) =
        consume_result(polyglot_generate(qualified_ast.as_ptr(), dialect.as_ptr()));
    assert_eq!(gen_status, 0, "gen_error={gen_error:?}");

    let statements: Vec<String> =
        serde_json::from_str(&gen_data.expect("missing generate output")).expect("invalid json");
    assert_eq!(
        statements[0],
        "SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _0 UNION ALL SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _1"
    );
}

#[test]
fn test_rename_tables_with_options_ast_transform() {
    let sql = c("SELECT a FROM old_table");
    let dialect = c("generic");

    let (parse_status, parse_data, parse_error) =
        consume_result(polyglot_parse(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(parse_status, 0, "parse_error={parse_error:?}");

    let ast_json = c(&parse_data.expect("missing parse result"));
    let mapping_json = c(r#"{"old_table":"new_table"}"#);
    let options_json = c(r#"{"aliasRenamedTables":true}"#);
    let (rename_status, rename_data, rename_error) =
        consume_result(polyglot_rename_tables_with_options(
            ast_json.as_ptr(),
            mapping_json.as_ptr(),
            options_json.as_ptr(),
        ));
    assert_eq!(rename_status, 0, "rename_error={rename_error:?}");

    let renamed_ast = c(&rename_data.expect("missing rename result"));
    let (gen_status, gen_data, gen_error) =
        consume_result(polyglot_generate(renamed_ast.as_ptr(), dialect.as_ptr()));
    assert_eq!(gen_status, 0, "gen_error={gen_error:?}");

    let statements: Vec<String> =
        serde_json::from_str(&gen_data.expect("missing generate output")).expect("invalid json");
    assert_eq!(statements[0], "SELECT a FROM new_table AS new_table");
}

#[test]
fn test_generate_invalid_json() {
    let ast = c("{not-json");
    let dialect = c("generic");
    let (status, _, error) = consume_result(polyglot_generate(ast.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 6);
    assert!(error.is_some());
}

#[test]
fn test_format_sql() {
    let sql = c("SELECT a,b FROM t WHERE x=1 AND y=2");
    let dialect = c("postgres");
    let (status, data, error) = consume_result(polyglot_format(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");

    let payload = data.expect("missing payload");
    let statements: Vec<String> = serde_json::from_str(&payload).expect("invalid json");
    assert_eq!(statements.len(), 1);
    assert!(statements[0].contains('\n') || statements[0].contains("SELECT"));
}

#[test]
fn test_format_sql_with_options_guard_trigger() {
    let sql = c("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3");
    let dialect = c("generic");
    let options = c(r#"{"maxSetOpChain":1}"#);
    let (status, _, error) = consume_result(polyglot_format_with_options(
        sql.as_ptr(),
        dialect.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 2, "error={error:?}");
    let err = error.expect("expected error message");
    assert!(err.contains("E_GUARD_SET_OP_CHAIN_EXCEEDED"), "error={err}");
}

#[test]
fn test_format_sql_with_options_invalid_json() {
    let sql = c("SELECT 1");
    let dialect = c("generic");
    let options = c("{not-json");
    let (status, _, error) = consume_result(polyglot_format_with_options(
        sql.as_ptr(),
        dialect.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 6);
    assert!(error.is_some());
}

#[test]
fn test_validate_valid_sql() {
    let sql = c("SELECT 1");
    let dialect = c("generic");
    let (status, valid, errors_json, top_error) =
        consume_validation(polyglot_validate(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 0, "top_error={top_error:?}");
    assert_eq!(valid, 1);
    let errors: Vec<Value> =
        serde_json::from_str(&errors_json.expect("missing errors_json")).expect("invalid json");
    assert!(errors.is_empty());
}

#[test]
fn test_validate_invalid_sql() {
    let sql = c("SELECT FROM");
    let dialect = c("generic");
    let (status, valid, errors_json, _) =
        consume_validation(polyglot_validate(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 4);
    assert_eq!(valid, 0);
    let errors: Vec<Value> =
        serde_json::from_str(&errors_json.expect("missing errors_json")).expect("invalid json");
    assert!(!errors.is_empty());
}

#[test]
fn test_optimize_sql() {
    let sql = c("SELECT a FROM t WHERE NOT (NOT (b = 1))");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_optimize(sql.as_ptr(), dialect.as_ptr()));
    assert_eq!(status, 0, "error={error:?}");
    let statements: Vec<String> =
        serde_json::from_str(&data.expect("missing optimize output")).expect("invalid json");
    assert_eq!(statements.len(), 1);
    assert!(statements[0].to_uppercase().contains("SELECT"));
}

#[test]
fn test_lineage_happy_path() {
    let column = c("total");
    let sql = c("SELECT o.total FROM orders o");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_lineage(
        column.as_ptr(),
        sql.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let node: Value = serde_json::from_str(&data.expect("missing lineage")).expect("invalid json");
    assert!(node.is_object());
}

#[test]
fn test_source_tables() {
    let column = c("total");
    let sql = c("SELECT o.total FROM orders o");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_source_tables(
        column.as_ptr(),
        sql.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let tables: Vec<String> =
        serde_json::from_str(&data.expect("missing source tables")).expect("invalid json");
    assert!(tables.iter().any(|t| t.eq_ignore_ascii_case("orders")));
}

#[test]
fn test_source_tables_postgres_prepare_body() {
    let column = c("id");
    let sql = c("PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1");
    let dialect = c("postgres");
    let (status, data, error) = consume_result(polyglot_source_tables(
        column.as_ptr(),
        sql.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let tables: Vec<String> =
        serde_json::from_str(&data.expect("missing source tables")).expect("invalid json");
    assert!(
        tables
            .iter()
            .any(|table| table.eq_ignore_ascii_case("sensitive_table")),
        "tables={tables:?}"
    );
}

#[test]
fn test_lineage_with_schema_resolves_ambiguous_column() {
    let column = c("id");
    let sql = c("SELECT id FROM users u JOIN orders o ON u.id = o.user_id");
    let dialect = c("generic");
    let schema = c(r#"{
            "tables": [
                {
                    "name": "users",
                    "columns": [{"name": "id", "type": "INT"}, {"name": "name", "type": "TEXT"}]
                },
                {
                    "name": "orders",
                    "columns": [{"name": "order_id", "type": "INT"}, {"name": "user_id", "type": "INT"}]
                }
            ]
        }"#);
    let (status, data, error) = consume_result(polyglot_lineage_with_schema(
        column.as_ptr(),
        sql.as_ptr(),
        schema.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let node: Value = serde_json::from_str(&data.expect("missing lineage")).expect("invalid json");
    let payload = node.to_string();
    assert!(
        payload.contains("u.id"),
        "expected qualified lineage edge u.id, got: {}",
        payload
    );
}

#[test]
fn test_openlineage_column_lineage() {
    let sql = c("SELECT a FROM input_table");
    let options = c(r#"{
            "dialect":"generic",
            "producer":"https://github.com/tobilg/polyglot",
            "datasetNamespace":"warehouse",
            "outputDataset":{"namespace":"warehouse","name":"output_table"}
        }"#);

    let (status, data, error) = consume_result(polyglot_openlineage_column_lineage(
        sql.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let payload: Value =
        serde_json::from_str(&data.expect("missing openlineage payload")).expect("invalid json");
    assert!(payload["facet"]["fields"]["a"]["inputFields"].is_array());
    assert!(payload["inputs"].is_array());
    assert!(payload["outputs"].is_array());
}

#[test]
fn test_openlineage_job_event() {
    let sql = c("SELECT a FROM input_table");
    let options = c(r#"{
            "dialect":"generic",
            "producer":"https://github.com/tobilg/polyglot",
            "datasetNamespace":"warehouse",
            "outputDataset":{"namespace":"warehouse","name":"output_table"},
            "jobNamespace":"jobs",
            "jobName":"daily_sql",
            "eventTime":"2026-05-21T00:00:00Z"
        }"#);

    let (status, data, error) = consume_result(polyglot_openlineage_job_event(
        sql.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let payload: Value =
        serde_json::from_str(&data.expect("missing openlineage event")).expect("invalid json");
    assert_eq!(payload["event"]["job"]["name"], "daily_sql");
    assert!(payload["event"]["outputs"].is_array());
}

#[test]
fn test_openlineage_run_event() {
    let sql = c("SELECT a FROM input_table");
    let options = c(r#"{
            "dialect":"generic",
            "producer":"https://github.com/tobilg/polyglot",
            "datasetNamespace":"warehouse",
            "outputDataset":{"namespace":"warehouse","name":"output_table"},
            "jobNamespace":"jobs",
            "jobName":"daily_sql",
            "eventTime":"2026-05-21T00:00:00Z",
            "runId":"run-1",
            "eventType":"COMPLETE"
        }"#);

    let (status, data, error) = consume_result(polyglot_openlineage_run_event(
        sql.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let payload: Value =
        serde_json::from_str(&data.expect("missing openlineage run event")).expect("invalid json");
    assert_eq!(payload["event"]["eventType"], "COMPLETE");
    assert_eq!(payload["event"]["run"]["runId"], "run-1");
}

#[test]
fn test_openlineage_invalid_options_json() {
    let sql = c("SELECT 1");
    let options = c("{not-json");

    let (status, data, error) = consume_result(polyglot_openlineage_column_lineage(
        sql.as_ptr(),
        options.as_ptr(),
    ));
    assert_eq!(status, 6);
    assert!(data.is_none());
    assert!(error
        .unwrap_or_default()
        .contains("Invalid OpenLineage options JSON"));
}

#[test]
fn test_diff_with_changes() {
    let sql1 = c("SELECT a FROM t");
    let sql2 = c("SELECT b FROM t");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_diff(
        sql1.as_ptr(),
        sql2.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let edits: Vec<Value> =
        serde_json::from_str(&data.expect("missing diff")).expect("invalid json");
    assert!(!edits.is_empty());
}

#[test]
fn test_diff_identical_sql() {
    let sql1 = c("SELECT a FROM t");
    let sql2 = c("SELECT a FROM t");
    let dialect = c("generic");
    let (status, data, error) = consume_result(polyglot_diff(
        sql1.as_ptr(),
        sql2.as_ptr(),
        dialect.as_ptr(),
    ));
    assert_eq!(status, 0, "error={error:?}");
    let edits: Vec<Value> =
        serde_json::from_str(&data.expect("missing diff")).expect("invalid json");
    assert!(edits.is_empty());
}

#[test]
fn test_dialect_list_and_count() {
    let list_ptr = polyglot_dialect_list();
    assert!(!list_ptr.is_null());
    let json = unsafe { CStr::from_ptr(list_ptr).to_string_lossy().into_owned() };
    polyglot_free_string(list_ptr);

    let list: Vec<String> = serde_json::from_str(&json).expect("invalid dialect list json");
    let count = polyglot_dialect_count();
    assert_eq!(list.len() as i32, count);
    assert!(count >= 32);
    assert!(list.iter().any(|d| d == "generic"));
}

#[test]
fn test_version_non_empty() {
    let ptr = polyglot_version();
    assert!(!ptr.is_null());
    let version = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    assert!(!version.trim().is_empty());
}

#[test]
fn test_memory_free_functions_null_safe() {
    polyglot_free_string(ptr::null_mut());

    let result = PolyglotResult {
        data: ptr::null_mut(),
        error: ptr::null_mut(),
        status: 0,
    };
    polyglot_free_result(result);

    let validation = PolyglotValidationResult {
        valid: 0,
        errors_json: ptr::null_mut(),
        error: ptr::null_mut(),
        status: 0,
    };
    polyglot_free_validation_result(validation);
}
