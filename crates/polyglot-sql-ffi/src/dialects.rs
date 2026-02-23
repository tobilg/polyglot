use crate::helpers::{panic_message, string_to_c_ptr};
use polyglot_sql::dialects::DialectType;
use std::os::raw::c_char;
use std::ptr;

const DIALECTS: [DialectType; 34] = [
    DialectType::Generic,
    DialectType::PostgreSQL,
    DialectType::MySQL,
    DialectType::BigQuery,
    DialectType::Snowflake,
    DialectType::DuckDB,
    DialectType::SQLite,
    DialectType::Hive,
    DialectType::Spark,
    DialectType::Trino,
    DialectType::Presto,
    DialectType::Redshift,
    DialectType::TSQL,
    DialectType::Oracle,
    DialectType::ClickHouse,
    DialectType::Databricks,
    DialectType::Athena,
    DialectType::Teradata,
    DialectType::Doris,
    DialectType::StarRocks,
    DialectType::Materialize,
    DialectType::RisingWave,
    DialectType::SingleStore,
    DialectType::CockroachDB,
    DialectType::TiDB,
    DialectType::Druid,
    DialectType::Solr,
    DialectType::Tableau,
    DialectType::Dune,
    DialectType::Fabric,
    DialectType::Drill,
    DialectType::Dremio,
    DialectType::Exasol,
    DialectType::DataFusion,
];

/// Return supported dialect names as JSON.
#[no_mangle]
pub extern "C" fn polyglot_dialect_list() -> *mut c_char {
    match std::panic::catch_unwind(dialect_list_impl) {
        Ok(result) => result,
        Err(panic) => string_to_c_ptr(format!(
            r#"{{"error":"Internal panic: {}"}}"#,
            panic_message(panic)
        )),
    }
}

/// Return number of supported built-in dialects.
#[no_mangle]
pub extern "C" fn polyglot_dialect_count() -> i32 {
    DIALECTS.len() as i32
}

/// Return the library version.
///
/// Returned pointer is statically allocated and must not be freed.
#[no_mangle]
pub extern "C" fn polyglot_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    VERSION.as_ptr() as *const c_char
}

fn dialect_list_impl() -> *mut c_char {
    let names: Vec<String> = DIALECTS.iter().map(ToString::to_string).collect();
    match serde_json::to_string(&names) {
        Ok(json) => string_to_c_ptr(json),
        Err(_) => ptr::null_mut(),
    }
}
