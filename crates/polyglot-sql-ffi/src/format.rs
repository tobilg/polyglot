use crate::helpers::{err_result, ok_json_result, panic_result, required_arg};
use crate::types::{
    PolyglotResult, STATUS_GENERATE_ERROR, STATUS_PARSE_ERROR, STATUS_SERIALIZATION_ERROR,
};
use polyglot_sql::{Error, FormatGuardOptions};
use std::os::raw::c_char;

/// Pretty-print SQL for a dialect.
#[no_mangle]
pub extern "C" fn polyglot_format(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| format_impl(sql, dialect, None)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Pretty-print SQL for a dialect with explicit formatting guard options.
///
/// `options_json` must be a JSON object compatible with `FormatGuardOptions`, e.g.
/// `{"maxInputBytes": 16777216, "maxTokens": 1000000, "maxAstNodes": 1000000, "maxSetOpChain": 256}`.
#[no_mangle]
pub extern "C" fn polyglot_format_with_options(
    sql: *const c_char,
    dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| format_with_options_impl(sql, dialect, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn format_with_options_impl(
    sql: *const c_char,
    dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    let options_json = match unsafe { required_arg(options_json, "options_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let options: FormatGuardOptions = match serde_json::from_str(&options_json) {
        Ok(value) => value,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid format options JSON: {error}"),
            );
        }
    };

    format_impl(sql, dialect, Some(&options))
}

fn format_impl(
    sql: *const c_char,
    dialect: *const c_char,
    options: Option<&FormatGuardOptions>,
) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let dialect_name = match unsafe { required_arg(dialect, "dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let result = match options {
        Some(guard_options) => {
            polyglot_sql::format_with_options_by_name(&sql, &dialect_name, guard_options)
        }
        None => polyglot_sql::format_by_name(&sql, &dialect_name),
    };

    match result {
        Ok(formatted) => ok_json_result(&formatted),
        Err(error @ (Error::Tokenize { .. } | Error::Parse { .. } | Error::Syntax { .. })) => {
            err_result(STATUS_PARSE_ERROR, error.to_string())
        }
        Err(error) => err_result(STATUS_GENERATE_ERROR, error.to_string()),
    }
}
