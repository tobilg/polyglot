use crate::helpers::{
    dialect_by_name, ok_json_result, panic_result, parse_single_statement, required_arg,
};
use crate::types::PolyglotResult;
use polyglot_sql::diff::diff as compute_diff;
use std::os::raw::c_char;

/// Compute AST diff between two SQL statements.
#[no_mangle]
pub extern "C" fn polyglot_diff(
    sql1: *const c_char,
    sql2: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| diff_impl(sql1, sql2, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn diff_impl(sql1: *const c_char, sql2: *const c_char, dialect: *const c_char) -> PolyglotResult {
    let sql1 = match unsafe { required_arg(sql1, "sql1") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let sql2 = match unsafe { required_arg(sql2, "sql2") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let dialect_name = match unsafe { required_arg(dialect, "dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let dialect = match dialect_by_name(&dialect_name) {
        Ok(dialect) => dialect,
        Err(result) => return result,
    };

    let source = match parse_single_statement(&sql1, &dialect, &dialect_name) {
        Ok(expression) => expression,
        Err(result) => return result,
    };
    let target = match parse_single_statement(&sql2, &dialect, &dialect_name) {
        Ok(expression) => expression,
        Err(result) => return result,
    };

    let edits = compute_diff(&source, &target, true);
    ok_json_result(&edits)
}
