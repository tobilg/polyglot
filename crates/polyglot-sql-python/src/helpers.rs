use crate::errors::{parse_statement_count_error, unknown_dialect_error, GenerateError};
use polyglot_sql::{dialects::Dialect, Expression};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use pythonize::{depythonize, pythonize};
use serde::Serialize;

pub fn resolve_dialect(name: &str) -> PyResult<Dialect> {
    Dialect::get_by_name(name).ok_or_else(|| unknown_dialect_error(name))
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
    if ast.cast::<PyDict>().is_ok() {
        let expr: Expression = depythonize(ast).map_err(|err| {
            GenerateError::new_err(format!("Failed to decode AST expression: {err}"))
        })?;
        return Ok(vec![expr]);
    }

    if ast.cast::<PyList>().is_ok() {
        let expressions: Vec<Expression> = depythonize(ast).map_err(|err| {
            GenerateError::new_err(format!("Failed to decode AST expression list: {err}"))
        })?;
        return Ok(expressions);
    }

    Err(GenerateError::new_err(
        "AST must be a dict (single expression) or list of dicts",
    ))
}
