use crate::errors::{parse_statement_count_error, unknown_dialect_error, GenerateError};
use crate::expr::PyExpression;
use polyglot_sql::dialects::Dialect;
use polyglot_sql::{ast_json, DataType, Expression, UnsupportedLevel};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use pythonize::{depythonize, pythonize};
use serde::Serialize;
use serde_json::Value;
/// Run native work without holding the GIL. Recursive core entry points use
/// `stacker`, so callers can execute concurrently on their Python threads.
pub fn run_detached<T, F>(py: Python<'_>, f: F) -> PyResult<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Ok(py.detach(f))
}

pub fn parse_detached(py: Python<'_>, dialect: &Dialect, sql: &str) -> PyResult<Vec<Expression>> {
    let dialect_type = dialect.dialect_type();
    let sql_owned = sql.to_owned();
    run_detached(py, move || {
        let d = Dialect::get(dialect_type);
        d.parse(&sql_owned)
    })?
    .map_err(crate::errors::map_parse_error)
}

pub fn parse_data_type_detached(
    py: Python<'_>,
    dialect: &Dialect,
    sql: &str,
) -> PyResult<DataType> {
    let dialect_type = dialect.dialect_type();
    let sql_owned = sql.to_owned();
    run_detached(py, move || {
        let d = Dialect::get(dialect_type);
        d.parse_data_type(&sql_owned)
    })?
    .map_err(crate::errors::map_parse_error)
}

pub fn resolve_dialect(name: &str) -> PyResult<Dialect> {
    Dialect::get_by_name(name).ok_or_else(|| unknown_dialect_error(name))
}

pub fn resolve_read_or_dialect(read: Option<&str>, dialect: Option<&str>) -> PyResult<Dialect> {
    resolve_dialect(read.or(dialect).unwrap_or("generic"))
}

pub fn normalize_error_level(error_level: Option<&str>) -> PyResult<Option<&str>> {
    match error_level.map(|level| level.to_ascii_lowercase()) {
        None => Ok(None),
        Some(level) if matches!(level.as_str(), "raise" | "immediate" | "warn" | "ignore") => {
            Ok(error_level)
        }
        Some(level) => Err(PyValueError::new_err(format!(
            "Unsupported error_level: {level}"
        ))),
    }
}

pub fn normalize_unsupported_level(
    unsupported_level: Option<&str>,
) -> PyResult<Option<UnsupportedLevel>> {
    match unsupported_level.map(|level| level.to_ascii_lowercase()) {
        None => Ok(None),
        Some(level) => match level.as_str() {
            "raise" => Ok(Some(UnsupportedLevel::Raise)),
            "immediate" => Ok(Some(UnsupportedLevel::Immediate)),
            "warn" => Ok(Some(UnsupportedLevel::Warn)),
            "ignore" => Ok(Some(UnsupportedLevel::Ignore)),
            _ => Err(PyValueError::new_err(format!(
                "Unsupported unsupported_level: {level}"
            ))),
        },
    }
}

pub fn to_python_object<T>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>>
where
    T: Serialize,
{
    let py_value = pythonize(py, value).map_err(|err| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Python conversion error: {err}"))
    })?;
    Ok(py_value.unbind())
}

pub fn parse_single_statement(sql: &str, dialect: &Dialect) -> PyResult<Expression> {
    let mut expressions = dialect.parse(sql).map_err(crate::errors::map_parse_error)?;
    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }
    Ok(expressions.remove(0))
}

pub fn join_sql(statements: &[String]) -> String {
    if statements.is_empty() {
        String::new()
    } else if statements.len() == 1 {
        statements[0].clone()
    } else {
        statements.join(";\n")
    }
}

pub fn ast_input_to_expressions(ast: &Bound<'_, PyAny>) -> PyResult<Vec<Expression>> {
    if let Ok(expr) = ast.extract::<PyRef<'_, PyExpression>>() {
        return Ok(vec![expr.inner.clone()]);
    }

    if ast.cast::<PyDict>().is_ok() {
        let value: Value = depythonize(ast).map_err(|err| {
            GenerateError::new_err(format!("Failed to decode AST expression: {err}"))
        })?;
        let expr = ast_json::expression_from_value(value).map_err(|err| {
            GenerateError::new_err(format!("Failed to decode AST expression: {err}"))
        })?;
        return Ok(vec![expr]);
    }

    if ast.cast::<PyList>().is_ok() {
        let list = ast.cast::<PyList>().expect("cast checked above");
        let mut expressions = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(expr) = item.extract::<PyRef<'_, PyExpression>>() {
                expressions.push(expr.inner.clone());
                continue;
            }

            let value: Value = depythonize(&item).map_err(|err| {
                GenerateError::new_err(format!("Failed to decode AST expression list item: {err}"))
            })?;
            let expr = ast_json::expression_from_value(value).map_err(|err| {
                GenerateError::new_err(format!("Failed to decode AST expression list item: {err}"))
            })?;
            expressions.push(expr);
        }
        return Ok(expressions);
    }

    Err(GenerateError::new_err(
        "AST must be an Expression, a dict (single expression), or a list of Expressions/dicts",
    ))
}
