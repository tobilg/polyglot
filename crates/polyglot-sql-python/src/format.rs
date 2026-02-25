use crate::errors::map_transpile_error;
use crate::helpers::join_sql;
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn format_sql(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<String> {
    let formatted = py.detach(|| polyglot_sql::format_by_name(sql, dialect));

    formatted
        .map(|statements| join_sql(&statements))
        .map_err(map_transpile_error)
}
