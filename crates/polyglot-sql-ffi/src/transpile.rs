use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_SERIALIZATION_ERROR, STATUS_TRANSPILE_ERROR};
use polyglot_sql::TranspileOptions;
use std::os::raw::c_char;

/// Transpile SQL between dialects using dialect names.
#[no_mangle]
pub extern "C" fn polyglot_transpile(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| transpile_impl(sql, from_dialect, to_dialect, None)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Transpile SQL between dialects with explicit transpile options.
///
/// `options_json` must be a JSON object compatible with `TranspileOptions`, e.g.
/// `{"pretty": true}`. Unknown fields are ignored; omitted fields use their defaults.
#[no_mangle]
pub extern "C" fn polyglot_transpile_with_options(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| {
        transpile_with_options_impl(sql, from_dialect, to_dialect, options_json)
    }) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn transpile_with_options_impl(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    let options_json = match unsafe { required_arg(options_json, "options_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let options: TranspileOptions = match serde_json::from_str(&options_json) {
        Ok(value) => value,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid transpile options JSON: {error}"),
            );
        }
    };

    transpile_impl(sql, from_dialect, to_dialect, Some(&options))
}

fn transpile_impl(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
    options: Option<&TranspileOptions>,
) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let from_dialect = match unsafe { required_arg(from_dialect, "from_dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let to_dialect = match unsafe { required_arg(to_dialect, "to_dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    if let Err(result) = dialect_by_name(&from_dialect) {
        return result;
    }
    if let Err(result) = dialect_by_name(&to_dialect) {
        return result;
    }

    let result = match options {
        Some(opts) => {
            polyglot_sql::transpile_with_by_name(&sql, &from_dialect, &to_dialect, opts)
        }
        None => polyglot_sql::transpile_by_name(&sql, &from_dialect, &to_dialect),
    };

    match result {
        Ok(transpiled) => ok_json_result(&transpiled),
        Err(error) => err_result(STATUS_TRANSPILE_ERROR, error.to_string()),
    }
}
