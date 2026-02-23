use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_GENERATE_ERROR, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::Expression;
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

    let expressions: Vec<Expression> = match serde_json::from_str(&ast_json) {
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
