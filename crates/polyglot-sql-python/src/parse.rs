use crate::errors::parse_statement_count_error;
use crate::expr::PyExpression;
use crate::helpers::{
    normalize_error_level, parse_on_large_stack, reject_parse_into, resolve_read_or_dialect,
    to_python_object,
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
) -> PyResult<Py<PyAny>> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    let _ = normalize_error_level(error_level)?;
    let expressions = parse_on_large_stack(py, &dialect, sql)?;
    to_python_object(py, &expressions)
}

#[pyfunction(signature = (sql, read = None, dialect = None, *, error_level = None))]
pub fn parse_expr(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    dialect: Option<&str>,
    error_level: Option<&str>,
) -> PyResult<Vec<PyExpression>> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    let _ = normalize_error_level(error_level)?;
    parse_on_large_stack(py, &dialect, sql).map(|expressions| {
        expressions
            .into_iter()
            .map(PyExpression::from_inner)
            .collect()
    })
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

    let expression = expressions.remove(0);
    to_python_object(py, &expression)
}

#[pyfunction(signature = (sql, read = None, dialect = None, *, into = None, error_level = None))]
pub fn parse_one_expr(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    dialect: Option<&str>,
    into: Option<&Bound<'_, PyAny>>,
    error_level: Option<&str>,
) -> PyResult<PyExpression> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    reject_parse_into(into)?;
    let _ = normalize_error_level(error_level)?;
    let mut expressions = parse_on_large_stack(py, &dialect, sql)?;

    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }

    Ok(PyExpression::from_inner(expressions.remove(0)))
}
