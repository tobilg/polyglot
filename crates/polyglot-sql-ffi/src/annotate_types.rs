use crate::helpers::{
    dialect_by_name, dialect_option_for_lineage, err_result, ok_json_result, panic_result,
    required_arg,
};
use crate::types::{PolyglotResult, STATUS_PARSE_ERROR, STATUS_SERIALIZATION_ERROR};
use std::os::raw::c_char;

/// Parse SQL and annotate the AST with inferred type information.
///
/// Returns a JSON array of Expression objects with `inferred_type` fields
/// populated on value-producing nodes.
///
/// If `schema_json` is NULL or empty, type annotation runs without schema
/// (only literal and operator types are inferred). With a schema, column
/// types are resolved from the provided table definitions.
#[no_mangle]
pub extern "C" fn polyglot_annotate_types(
    sql: *const c_char,
    dialect: *const c_char,
    schema_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| annotate_types_impl(sql, dialect, schema_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn annotate_types_impl(
    sql: *const c_char,
    dialect: *const c_char,
    schema_json: *const c_char,
) -> PolyglotResult {
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

    let mut expressions = match dialect.parse(&sql) {
        Ok(exprs) => exprs,
        Err(error) => {
            return err_result(STATUS_PARSE_ERROR, error.to_string());
        }
    };

    let dialect_opt = dialect_option_for_lineage(&dialect);

    // Parse schema if provided
    let schema = if !schema_json.is_null() {
        let schema_str = match unsafe { required_arg(schema_json, "schema_json") } {
            Ok(value) => value,
            Err(_) => String::new(),
        };
        if !schema_str.is_empty() {
            match serde_json::from_str::<polyglot_sql::ValidationSchema>(&schema_str) {
                Ok(vs) => Some(polyglot_sql::mapping_schema_from_validation_schema(&vs)),
                Err(error) => {
                    return err_result(
                        STATUS_SERIALIZATION_ERROR,
                        format!("Invalid schema JSON: {}", error),
                    );
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Annotate types on each expression
    for expr in &mut expressions {
        polyglot_sql::annotate_types(
            expr,
            schema.as_ref().map(|s| s as &dyn polyglot_sql::Schema),
            dialect_opt,
        );
    }

    ok_json_result(&expressions)
}
