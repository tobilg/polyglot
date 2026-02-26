use crate::helpers::{
    dialect_by_name, err_validation_result, panic_validation_result, required_arg_validation,
    validation_result_from_core,
};
use crate::types::{PolyglotValidationResult, STATUS_INVALID_ARGUMENT};
use polyglot_sql::{Error, ValidationError, ValidationResult};
use std::os::raw::c_char;

/// Validate SQL syntax for a dialect.
#[no_mangle]
pub extern "C" fn polyglot_validate(
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotValidationResult {
    match std::panic::catch_unwind(|| validate_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_validation_result(panic),
    }
}

fn validate_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotValidationResult {
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

    let result = match dialect.parse(&sql) {
        Ok(expressions) => {
            for expression in &expressions {
                if !expression.is_statement() {
                    return validation_result_from_core(ValidationResult::with_errors(vec![
                        ValidationError::error("Invalid expression / Unexpected token", "E004"),
                    ]));
                }
            }
            ValidationResult::success()
        }
        Err(error) => ValidationResult::with_errors(vec![to_validation_error(error)]),
    };

    validation_result_from_core(result)
}

fn to_validation_error(error: Error) -> ValidationError {
    match error {
        Error::Syntax {
            message,
            line,
            column,
            start,
            end,
        } => ValidationError::error(message, "E001")
            .with_location(line, column)
            .with_span(Some(start), Some(end)),
        Error::Tokenize {
            message,
            line,
            column,
            start,
            end,
        } => ValidationError::error(message, "E002")
            .with_location(line, column)
            .with_span(Some(start), Some(end)),
        Error::Parse {
            message,
            line,
            column,
            start,
            end,
        } => ValidationError::error(message, "E003")
            .with_location(line, column)
            .with_span(Some(start), Some(end)),
        _ => ValidationError::error(error.to_string(), "E000"),
    }
}
