use crate::errors::parse_statement_count_error;
use crate::expr_types::wrap_expression;
use crate::helpers::{
    normalize_error_level, parse_data_type_detached, parse_detached, resolve_read_or_dialect,
};
use polyglot_sql::Expression;
use pyo3::exceptions::PyNotImplementedError;
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
    let expressions = parse_detached(py, &dialect, sql)?;
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
    let _ = normalize_error_level(error_level)?;

    if let Some(into) = into {
        let data_type_class = py.get_type::<crate::expr_types::DataType>();
        if into.is(data_type_class.as_any()) {
            let data_type = parse_data_type_detached(py, &dialect, sql)?;
            return wrap_expression(py, Expression::DataType(data_type));
        }

        return Err(PyNotImplementedError::new_err(
            "parse_one(into=...) only supports polyglot_sql.DataType",
        ));
    }

    let mut expressions = parse_detached(py, &dialect, sql)?;

    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }

    wrap_expression(py, expressions.remove(0))
}

#[pyfunction(signature = (sql, read = None, dialect = None, *, error_level = None))]
pub fn parse_data_type(
    py: Python<'_>,
    sql: &str,
    read: Option<&str>,
    dialect: Option<&str>,
    error_level: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let dialect = resolve_read_or_dialect(read, dialect)?;
    let _ = normalize_error_level(error_level)?;
    let data_type = parse_data_type_detached(py, &dialect, sql)?;

    wrap_expression(py, Expression::DataType(data_type))
}
