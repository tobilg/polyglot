use crate::helpers::{err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_GENERATE_ERROR, STATUS_PARSE_ERROR};
use polyglot_sql::Error;
use std::os::raw::c_char;

/// Pretty-print SQL for a dialect.
#[no_mangle]
pub extern "C" fn polyglot_format(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| format_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn format_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let dialect_name = match unsafe { required_arg(dialect, "dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    match polyglot_sql::format_by_name(&sql, &dialect_name) {
        Ok(formatted) => ok_json_result(&formatted),
        Err(error @ (Error::Tokenize { .. } | Error::Parse { .. } | Error::Syntax { .. })) => {
            err_result(STATUS_PARSE_ERROR, error.to_string())
        }
        Err(error) => err_result(STATUS_GENERATE_ERROR, error.to_string()),
    }
}
