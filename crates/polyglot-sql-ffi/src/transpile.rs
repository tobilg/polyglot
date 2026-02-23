use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_TRANSPILE_ERROR};
use std::os::raw::c_char;

/// Transpile SQL between dialects using dialect names.
#[no_mangle]
pub extern "C" fn polyglot_transpile(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| transpile_impl(sql, from_dialect, to_dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn transpile_impl(
    sql: *const c_char,
    from_dialect: *const c_char,
    to_dialect: *const c_char,
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

    match polyglot_sql::transpile_by_name(&sql, &from_dialect, &to_dialect) {
        Ok(transpiled) => ok_json_result(&transpiled),
        Err(error) => err_result(STATUS_TRANSPILE_ERROR, error.to_string()),
    }
}
