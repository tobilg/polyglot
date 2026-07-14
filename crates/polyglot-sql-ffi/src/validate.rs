use crate::helpers::{
    dialect_by_name, err_validation_result, panic_validation_result, required_arg_validation,
    validation_result_from_core,
};
use crate::types::{PolyglotValidationResult, STATUS_INVALID_ARGUMENT, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::ValidationOptions;
use std::os::raw::c_char;

/// Validate SQL syntax for a dialect.
#[no_mangle]
pub extern "C" fn polyglot_validate(
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotValidationResult {
    match std::panic::catch_unwind(|| validate_impl(sql, dialect, None)) {
        Ok(result) => result,
        Err(panic) => panic_validation_result(panic),
    }
}

/// Validate SQL syntax and optional semantic warnings for a dialect.
///
/// `options_json` must be a JSON object compatible with `ValidationOptions`, e.g.
/// `{"strictSyntax": true, "semantic": true}`.
#[no_mangle]
pub extern "C" fn polyglot_validate_with_options(
    sql: *const c_char,
    dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotValidationResult {
    match std::panic::catch_unwind(|| validate_with_options_impl(sql, dialect, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_validation_result(panic),
    }
}

fn validate_with_options_impl(
    sql: *const c_char,
    dialect: *const c_char,
    options_json: *const c_char,
) -> PolyglotValidationResult {
    let options_json = match unsafe { required_arg_validation(options_json, "options_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options: ValidationOptions = match serde_json::from_str(&options_json) {
        Ok(value) => value,
        Err(error) => {
            return err_validation_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid validation options JSON: {error}"),
            );
        }
    };

    validate_impl(sql, dialect, Some(&options))
}

fn validate_impl(
    sql: *const c_char,
    dialect: *const c_char,
    options: Option<&ValidationOptions>,
) -> PolyglotValidationResult {
    let sql = match unsafe { required_arg_validation(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let dialect_name = match unsafe { required_arg_validation(dialect, "dialect") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let dialect = match dialect_by_name(&dialect_name) {
        Ok(dialect) => dialect,
        Err(_) => {
            return err_validation_result(
                STATUS_INVALID_ARGUMENT,
                format!("Unknown dialect: {dialect_name}"),
            )
        }
    };

    let default_options = ValidationOptions::default();
    let result =
        polyglot_sql::validate_with_dialect(&sql, &dialect, options.unwrap_or(&default_options));

    validation_result_from_core(result)
}
