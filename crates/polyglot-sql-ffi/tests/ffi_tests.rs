use polyglot_sql_ffi::{
    polyglot_dialect_count, polyglot_dialect_list, polyglot_diff, polyglot_format,
    polyglot_format_with_options, polyglot_free_result, polyglot_free_string,
    polyglot_free_validation_result, polyglot_generate, polyglot_lineage, polyglot_optimize,
    polyglot_parse, polyglot_parse_one, polyglot_source_tables, polyglot_transpile,
    polyglot_validate, polyglot_version, PolyglotResult, PolyglotValidationResult,
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

    let (status, _, _) = consume_result(polyglot_diff(sql.as_ptr(), ptr::null(), dialect.as_ptr()));
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
