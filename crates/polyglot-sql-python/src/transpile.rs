use crate::errors::map_transpile_error;
use crate::helpers::{normalize_error_level, resolve_dialect};
use polyglot_sql::dialects::Dialect;
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

    let statements = py
        .detach(|| polyglot_sql::transpile_by_name(sql, read, write))
        .map_err(map_transpile_error)?;

    if !pretty {
        return Ok(statements);
    }

    py.detach(|| {
        let dialect =
            Dialect::get_by_name(write).expect("dialect existence checked before entering detach");

        statements
            .into_iter()
            .map(|statement| {
                let mut parsed = dialect.parse(&statement)?;
                if parsed.len() != 1 {
                    return Err(polyglot_sql::Error::parse(
                        format!("Expected 1 statement, found {}", parsed.len()),
                        0,
                        0,
                        0,
                        0,
                    ));
                }
                dialect.generate_pretty(&parsed.remove(0))
            })
            .collect::<polyglot_sql::Result<Vec<String>>>()
    })
    .map_err(map_transpile_error)
}
