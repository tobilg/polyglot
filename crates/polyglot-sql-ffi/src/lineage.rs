use crate::helpers::{
    dialect_by_name, dialect_option_for_lineage, err_result, ok_json_result, panic_result,
    parse_single_statement, required_arg,
};
use crate::types::{PolyglotResult, STATUS_PARSE_ERROR, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::lineage::{get_source_tables, lineage as compute_lineage};
use polyglot_sql::mapping_schema_from_validation_schema;
use std::os::raw::c_char;

/// Trace column lineage in a SQL statement.
#[no_mangle]
pub extern "C" fn polyglot_lineage(
    column_name: *const c_char,
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| lineage_impl(column_name, sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Trace column lineage in a SQL statement using schema metadata.
#[no_mangle]
pub extern "C" fn polyglot_lineage_with_schema(
    column_name: *const c_char,
    sql: *const c_char,
    schema_json: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| {
        lineage_with_schema_impl(column_name, sql, schema_json, dialect)
    }) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Get source tables that contribute to a column.
#[no_mangle]
pub extern "C" fn polyglot_source_tables(
    column_name: *const c_char,
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| source_tables_impl(column_name, sql, dialect)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn lineage_impl(
    column_name: *const c_char,
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    let column_name = match unsafe { required_arg(column_name, "column_name") } {
        Ok(value) => value,
        Err(result) => return result,
    };
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
    let expression = match parse_single_statement(&sql, &dialect, &dialect_name) {
        Ok(expression) => expression,
        Err(result) => return result,
    };

    match compute_lineage(
        &column_name,
        &expression,
        dialect_option_for_lineage(&dialect),
        false,
    ) {
        Ok(node) => ok_json_result(&node),
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}

fn source_tables_impl(
    column_name: *const c_char,
    sql: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    let column_name = match unsafe { required_arg(column_name, "column_name") } {
        Ok(value) => value,
        Err(result) => return result,
    };
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
    let expression = match parse_single_statement(&sql, &dialect, &dialect_name) {
        Ok(expression) => expression,
        Err(result) => return result,
    };

    match compute_lineage(
        &column_name,
        &expression,
        dialect_option_for_lineage(&dialect),
        false,
    ) {
        Ok(node) => {
            let tables = get_source_tables(&node);
            let mut sorted: Vec<String> = tables.into_iter().collect();
            sorted.sort();
            ok_json_result(&sorted)
        }
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}

fn lineage_with_schema_impl(
    column_name: *const c_char,
    sql: *const c_char,
    schema_json: *const c_char,
    dialect: *const c_char,
) -> PolyglotResult {
    let column_name = match unsafe { required_arg(column_name, "column_name") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let schema_json = match unsafe { required_arg(schema_json, "schema_json") } {
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
    let expression = match parse_single_statement(&sql, &dialect, &dialect_name) {
        Ok(expression) => expression,
        Err(result) => return result,
    };
    let validation_schema =
        match serde_json::from_str::<polyglot_sql::ValidationSchema>(&schema_json) {
            Ok(schema) => schema,
            Err(error) => {
                return err_result(
                    STATUS_SERIALIZATION_ERROR,
                    format!("Invalid schema JSON: {}", error),
                );
            }
        };
    let mapping_schema = mapping_schema_from_validation_schema(&validation_schema);

    match polyglot_sql::lineage::lineage_with_schema(
        &column_name,
        &expression,
        Some(&mapping_schema),
        dialect_option_for_lineage(&dialect),
        false,
    ) {
        Ok(node) => ok_json_result(&node),
        Err(error) => err_result(STATUS_PARSE_ERROR, error.to_string()),
    }
}
