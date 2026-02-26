use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_PARSE_ERROR};
use std::os::raw::c_char;

/// Tokenize SQL into a JSON array of tokens.
#[no_mangle]
pub extern "C" fn polyglot_tokenize(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| tokenize_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn tokenize_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
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

    match dialect.tokenize(&sql) {
        Ok(tokens) => ok_json_result(&tokens),
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}
