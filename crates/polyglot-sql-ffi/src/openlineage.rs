use crate::helpers::{err_result, map_polyglot_error, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_PARSE_ERROR, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::openlineage::{
    openlineage_column_lineage as core_openlineage_column_lineage,
    openlineage_job_event as core_openlineage_job_event,
    openlineage_run_event as core_openlineage_run_event, OpenLineageOptions,
};
use std::os::raw::c_char;

/// Build an OpenLineage columnLineage facet and dataset payload.
///
/// `options_json` must be a JSON object compatible with `OpenLineageOptions`.
#[no_mangle]
pub extern "C" fn polyglot_openlineage_column_lineage(
    sql: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| openlineage_column_lineage_impl(sql, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Build an OpenLineage JobEvent payload. This does not send the event.
///
/// `options_json` must be a JSON object compatible with `OpenLineageOptions`.
#[no_mangle]
pub extern "C" fn polyglot_openlineage_job_event(
    sql: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| openlineage_job_event_impl(sql, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

/// Build an OpenLineage RunEvent payload. This does not send the event.
///
/// `options_json` must be a JSON object compatible with `OpenLineageOptions`.
#[no_mangle]
pub extern "C" fn polyglot_openlineage_run_event(
    sql: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| openlineage_run_event_impl(sql, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn openlineage_column_lineage_impl(
    sql: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options = match parse_openlineage_options(options_json) {
        Ok(options) => options,
        Err(result) => return result,
    };

    match core_openlineage_column_lineage(&sql, &options) {
        Ok(result) => ok_json_result(&result),
        Err(error) => err_result(
            map_polyglot_error(&error, STATUS_PARSE_ERROR),
            error.to_string(),
        ),
    }
}

fn openlineage_job_event_impl(sql: *const c_char, options_json: *const c_char) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options = match parse_openlineage_options(options_json) {
        Ok(options) => options,
        Err(result) => return result,
    };

    match core_openlineage_job_event(&sql, &options) {
        Ok(result) => ok_json_result(&result),
        Err(error) => err_result(
            map_polyglot_error(&error, STATUS_PARSE_ERROR),
            error.to_string(),
        ),
    }
}

fn openlineage_run_event_impl(sql: *const c_char, options_json: *const c_char) -> PolyglotResult {
    let sql = match unsafe { required_arg(sql, "sql") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options = match parse_openlineage_options(options_json) {
        Ok(options) => options,
        Err(result) => return result,
    };

    match core_openlineage_run_event(&sql, &options) {
        Ok(result) => ok_json_result(&result),
        Err(error) => err_result(
            map_polyglot_error(&error, STATUS_PARSE_ERROR),
            error.to_string(),
        ),
    }
}

fn parse_openlineage_options(
    options_json: *const c_char,
) -> Result<OpenLineageOptions, PolyglotResult> {
    let options_json = unsafe { required_arg(options_json, "options_json") }?;

    serde_json::from_str::<OpenLineageOptions>(&options_json).map_err(|error| {
        err_result(
            STATUS_SERIALIZATION_ERROR,
            format!("Invalid OpenLineage options JSON: {error}"),
        )
    })
}
