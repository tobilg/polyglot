use crate::errors::map_parse_error;
use crate::helpers::{resolve_dialect, to_python_object};
use pyo3::prelude::*;
use pyo3::types::PyAny;

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn tokenize(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<Py<PyAny>> {
    let d = resolve_dialect(dialect)?;
    let tokens = d.tokenize(sql).map_err(map_parse_error)?;
    to_python_object(py, &tokens)
}
