use crate::errors::map_transpile_error;
use crate::helpers::{normalize_error_level, resolve_dialect, run_on_large_stack};
use polyglot_sql::dialects::{Dialect, TranspileOptions};
use pyo3::prelude::*;

#[pyfunction(signature = (sql, read = None, write = None, *, identity = true, error_level = None, pretty = false))]
pub fn transpile(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    write: Option<&str>,
    identity: bool,
    error_level: Option<&str>,
    pretty: bool,
) -> PyResult<Vec<String>> {
    let _ = normalize_error_level(error_level)?;
    let read = read.unwrap_or("generic");
    resolve_dialect(read)?;
    let write = if identity {
        write.unwrap_or(read)
    } else {
        write.unwrap_or("generic")
    };
    resolve_dialect(write)?;

    let sql_owned = sql.to_owned();
    let read_owned = read.to_owned();
    let write_owned = write.to_owned();
    run_on_large_stack(py, move || {
        let read_dialect = Dialect::get_by_name(&read_owned)
            .expect("dialect existence checked before entering stack");
        let write_dialect = Dialect::get_by_name(&write_owned)
            .expect("dialect existence checked before entering stack");
        let opts = if pretty {
            TranspileOptions::pretty()
        } else {
            TranspileOptions::default()
        };
        read_dialect.transpile_with(&sql_owned, &write_dialect, opts)
    })?
    .map_err(map_transpile_error)
}
