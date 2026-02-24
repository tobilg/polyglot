use crate::errors::{map_parse_error, parse_statement_count_error};
use crate::helpers::{resolve_dialect, to_python_object};
use pyo3::prelude::*;
use pyo3::types::PyAny;

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn parse(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;
    let expressions = py
        .detach(|| polyglot_sql::parse_by_name(sql, dialect))
        .map_err(map_parse_error)?;
    to_python_object(py, &expressions)
}

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn parse_one(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;
    let mut expressions = py
        .detach(|| polyglot_sql::parse_by_name(sql, dialect))
        .map_err(map_parse_error)?;

    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }

    let expression = expressions.remove(0);
    to_python_object(py, &expression)
}
