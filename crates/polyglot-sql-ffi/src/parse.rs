use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_PARSE_ERROR};
use std::os::raw::c_char;

/// Parse SQL into an AST JSON string.
#[no_mangle]
pub extern "C" fn polyglot_parse(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| parse_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Parse a single SQL statement into a JSON AST object.
#[no_mangle]
pub extern "C" fn polyglot_parse_one(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| parse_one_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn parse_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
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

    match dialect.parse(&sql) {
        Ok(expressions) => ok_json_result(&expressions),
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}

fn parse_one_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
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

    match dialect.parse(&sql) {
        Ok(mut expressions) => {
            if expressions.len() != 1 {
                return err_result(
                    STATUS_PARSE_ERROR,
                    format!("Expected 1 statement, found {}", expressions.len()),
                );
            }
            ok_json_result(&expressions.remove(0))
        }
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}
