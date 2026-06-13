use crate::helpers::{
    dialect_by_name, err_result, ok_json_result, ok_result, panic_result, required_arg,
};
use crate::types::{PolyglotResult, STATUS_GENERATE_ERROR, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::{ast_json, DataType, Expression};
use std::os::raw::c_char;

/// Generate SQL from AST JSON.
///
/// `ast_json` must encode `Vec<Expression>`.
#[no_mangle]
pub extern "C" fn polyglot_generate(
    ast_json: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| generate_impl(ast_json, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Generate SQL from a standalone DataType JSON object.
#[no_mangle]
pub extern "C" fn polyglot_generate_data_type(
    data_type_json: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| generate_data_type_impl(data_type_json, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn generate_impl(ast_json: *const c_char, dialect: *const c_char) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
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

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };

    let mut generated = Vec::with_capacity(expressions.len());
    for expression in &expressions {
        match dialect.generate(expression) {
            Ok(sql) => generated.push(sql),
            Err(error) => return err_result(STATUS_GENERATE_ERROR, error.to_string()),
        }
    }

    ok_json_result(&generated)
}

fn generate_data_type_impl(
    data_type_json: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    let data_type_json = match unsafe { required_arg(data_type_json, "data_type_json") } {
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

    let data_type: DataType = match serde_json::from_str(&data_type_json) {
        Ok(data_type) => data_type,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid DataType JSON: {error}"),
            )
        }
    };

    match dialect.generate(&Expression::DataType(data_type)) {
        Ok(sql) => ok_result(sql),
        Err(error) => err_result(STATUS_GENERATE_ERROR, error.to_string()),
    }
}
