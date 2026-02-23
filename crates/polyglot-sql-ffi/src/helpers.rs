use crate::types::{
    PolyglotResult, PolyglotValidationResult, STATUS_GENERATE_ERROR, STATUS_INTERNAL_ERROR,
    STATUS_INVALID_ARGUMENT, STATUS_PARSE_ERROR, STATUS_SERIALIZATION_ERROR,
    STATUS_VALIDATION_ERROR,
};
use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::{Error, Expression, ValidationResult};
use serde::Serialize;
use std::any::Any;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

fn string_to_c_ptr_internal(text: String) -> *mut c_char {
    match CString::new(text) {
        Ok(cstr) => cstr.into_raw(),
        Err(err) => {
            let filtered: Vec<u8> = err.into_vec().into_iter().filter(|b| *b != 0).collect();
            CString::new(filtered)
                .expect("CString::new on filtered bytes must succeed")
                .into_raw()
        }
    }
}

pub fn string_to_c_ptr(text: impl Into<String>) -> *mut c_char {
    string_to_c_ptr_internal(text.into())
}

pub unsafe fn required_arg(ptr: *const c_char, name: &str) -> Result<String, PolyglotResult> {
    if ptr.is_null() {
        return Err(err_result(
            STATUS_INVALID_ARGUMENT,
            format!("{name} must not be NULL"),
        ));
    }

    let value = unsafe { CStr::from_ptr(ptr) };
    match value.to_str() {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Err(err_result(
            STATUS_INVALID_ARGUMENT,
            format!("{name} must be valid UTF-8"),
        )),
    }
}

pub unsafe fn required_arg_validation(
    ptr: *const c_char,
    name: &str,
) -> Result<String, PolyglotValidationResult> {
    if ptr.is_null() {
        return Err(err_validation_result(
            STATUS_INVALID_ARGUMENT,
            format!("{name} must not be NULL"),
        ));
    }

    let value = unsafe { CStr::from_ptr(ptr) };
    match value.to_str() {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Err(err_validation_result(
            STATUS_INVALID_ARGUMENT,
            format!("{name} must be valid UTF-8"),
        )),
    }
}

pub fn ok_result(data: impl Into<String>) -> PolyglotResult {
    PolyglotResult::success(string_to_c_ptr(data.into()))
}

pub fn err_result(status: i32, message: impl Into<String>) -> PolyglotResult {
    PolyglotResult::error(status, string_to_c_ptr(message.into()))
}

pub fn ok_json_result<T: Serialize>(value: &T) -> PolyglotResult {
    match serde_json::to_string(value) {
        Ok(json) => ok_result(json),
        Err(err) => err_result(
            STATUS_SERIALIZATION_ERROR,
            format!("JSON serialization error: {err}"),
        ),
    }
}

pub fn err_validation_result(status: i32, message: impl Into<String>) -> PolyglotValidationResult {
    PolyglotValidationResult::error(status, string_to_c_ptr(message.into()))
}

pub fn validation_result_from_core(result: ValidationResult) -> PolyglotValidationResult {
    match serde_json::to_string(&result.errors) {
        Ok(errors_json) => PolyglotValidationResult {
            valid: if result.valid { 1 } else { 0 },
            errors_json: string_to_c_ptr(errors_json),
            error: std::ptr::null_mut(),
            status: if result.valid {
                crate::types::STATUS_SUCCESS
            } else {
                STATUS_VALIDATION_ERROR
            },
        },
        Err(err) => err_validation_result(
            STATUS_SERIALIZATION_ERROR,
            format!("JSON serialization error: {err}"),
        ),
    }
}

pub fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return message.clone();
    }
    "Internal panic".to_string()
}

pub fn panic_result(panic: Box<dyn Any + Send>) -> PolyglotResult {
    err_result(
        STATUS_INTERNAL_ERROR,
        format!("Internal panic: {}", panic_message(panic)),
    )
}

pub fn panic_validation_result(panic: Box<dyn Any + Send>) -> PolyglotValidationResult {
    err_validation_result(
        STATUS_INTERNAL_ERROR,
        format!("Internal panic: {}", panic_message(panic)),
    )
}

pub fn map_polyglot_error(error: &Error, fallback: i32) -> i32 {
    match error {
        Error::Parse { .. } | Error::Tokenize { .. } | Error::Syntax { .. } => STATUS_PARSE_ERROR,
        Error::Generate(_) => STATUS_GENERATE_ERROR,
        _ => fallback,
    }
}

pub fn dialect_by_name(name: &str) -> Result<Dialect, PolyglotResult> {
    Dialect::get_by_name(name)
        .ok_or_else(|| err_result(STATUS_INVALID_ARGUMENT, format!("Unknown dialect: {name}")))
}

pub fn parse_single_statement(
    sql: &str,
    dialect: &Dialect,
    dialect_name: &str,
) -> Result<Expression, PolyglotResult> {
    match dialect.parse(sql) {
        Ok(mut expressions) => {
            if expressions.len() != 1 {
                return Err(err_result(
                    STATUS_PARSE_ERROR,
                    format!(
                        "Expected exactly 1 statement for dialect '{dialect_name}', found {}",
                        expressions.len()
                    ),
                ));
            }
            Ok(expressions.remove(0))
        }
        Err(error) => Err(err_result(
            map_polyglot_error(&error, STATUS_PARSE_ERROR),
            error.to_string(),
        )),
    }
}

pub fn dialect_option_for_lineage(dialect: &Dialect) -> Option<DialectType> {
    let dialect_type = dialect.dialect_type();
    if dialect_type == DialectType::Generic {
        None
    } else {
        Some(dialect_type)
    }
}
