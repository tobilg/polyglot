use crate::errors::parse_statement_count_error;
use crate::expr_types::wrap_expression;
use crate::helpers::{
    normalize_error_level, parse_on_large_stack, reject_parse_into, resolve_read_or_dialect,
};
use pyo3::prelude::*;
use pyo3::types::PyAny;

#[pyfunction(signature = (sql, read = None, dialect = None, *, error_level = None))]
pub fn parse(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    dialect: Option<&str>,
    error_level: Option<&str>,
) -> PyResult<Vec<Py<PyAny>>> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    let _ = normalize_error_level(error_level)?;
    let expressions = parse_on_large_stack(py, &dialect, sql)?;
    expressions
        .into_iter()
        .map(|expr| wrap_expression(py, expr))
        .collect()
}

#[pyfunction(signature = (sql, read = None, dialect = None, *, into = None, error_level = None))]
pub fn parse_one(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    dialect: Option<&str>,
    into: Option<&Bound<'_, PyAny>>,
    error_level: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    reject_parse_into(into)?;
    let _ = normalize_error_level(error_level)?;
    let mut expressions = parse_on_large_stack(py, &dialect, sql)?;

    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }

    wrap_expression(py, expressions.remove(0))
}
