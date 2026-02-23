use crate::helpers::{dialect_by_name, err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_GENERATE_ERROR, STATUS_PARSE_ERROR};
use polyglot_sql::optimizer::{optimize, OptimizerConfig};
use std::os::raw::c_char;

/// Parse, optimize (full pipeline), and generate SQL.
#[no_mangle]
pub extern "C" fn polyglot_optimize(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
    match std::panic::catch_unwind(|| optimize_impl(sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn optimize_impl(sql: *const c_char, dialect: *const c_char) -> PolyglotResult {
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

    let expressions = match dialect.parse(&sql) {
        Ok(expressions) => expressions,
        Err(error) => return err_result(STATUS_PARSE_ERROR, error.to_string()),
    };

    let config = OptimizerConfig {
        dialect: Some(dialect.dialect_type()),
        ..Default::default()
    };

    let mut optimized_sql = Vec::with_capacity(expressions.len());
    for expression in expressions {
        let optimized = optimize(expression, &config);
        match dialect.generate(&optimized) {
            Ok(sql) => optimized_sql.push(sql),
            Err(error) => return err_result(STATUS_GENERATE_ERROR, error.to_string()),
        }
    }

    ok_json_result(&optimized_sql)
}
