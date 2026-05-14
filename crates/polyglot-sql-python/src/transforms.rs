use crate::expr_types::wrap_expression;
use crate::helpers::{ast_input_to_expressions, resolve_dialect};
use polyglot_sql::{QualifyTablesOptions, RenameTablesOptions};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList};
use pythonize::depythonize;
use std::collections::HashMap;

fn wrap_transform_result(
    py: Python<'_>,
    return_list: bool,
    mut expressions: Vec<polyglot_sql::Expression>,
) -> PyResult<Py<PyAny>> {
    if return_list {
        let py_list = PyList::empty(py);
        for expression in expressions {
            py_list.append(wrap_expression(py, expression)?)?;
        }
        return Ok(py_list.unbind().into());
    }

    if expressions.len() != 1 {
        return Err(PyRuntimeError::new_err(format!(
            "Expected 1 transformed expression, found {}",
            expressions.len()
        )));
    }

    wrap_expression(py, expressions.remove(0))
}

#[pyfunction(signature = (
    ast,
    *,
    db = None,
    catalog = None,
    dialect = None,
    canonicalize_table_aliases = false,
    alias_unaliased_tables = true,
    alias_unaliased_subqueries = true,
    alias_prefix = "_",
    normalize_set_operation_subqueries = true
))]
pub fn qualify_tables(
    py: Python<'_>,
    ast: &Bound<'_, PyAny>,
    db: Option<&str>,
    catalog: Option<&str>,
    dialect: Option<&str>,
    canonicalize_table_aliases: bool,
    alias_unaliased_tables: bool,
    alias_unaliased_subqueries: bool,
    alias_prefix: &str,
    normalize_set_operation_subqueries: bool,
) -> PyResult<Py<PyAny>> {
    let return_list = ast.cast::<PyList>().is_ok();
    let expressions = ast_input_to_expressions(ast)?;
    let dialect = dialect
        .map(resolve_dialect)
        .transpose()?
        .map(|dialect| dialect.dialect_type());

    let mut options = QualifyTablesOptions::new();
    options.db = db.map(str::to_string);
    options.catalog = catalog.map(str::to_string);
    options.dialect = dialect;
    options.canonicalize_table_aliases = canonicalize_table_aliases;
    options.alias_unaliased_tables = alias_unaliased_tables;
    options.alias_unaliased_subqueries = alias_unaliased_subqueries;
    options.alias_prefix = if alias_prefix.is_empty() {
        "_".to_string()
    } else {
        alias_prefix.to_string()
    };
    options.normalize_set_operation_subqueries = normalize_set_operation_subqueries;

    let transformed = py.detach(move || {
        expressions
            .into_iter()
            .map(|expression| polyglot_sql::qualify_tables(expression, &options))
            .collect()
    });

    wrap_transform_result(py, return_list, transformed)
}

#[pyfunction(signature = (
    ast,
    mapping,
    *,
    alias_renamed_tables = false,
    preserve_existing_aliases = true
))]
pub fn rename_tables(
    py: Python<'_>,
    ast: &Bound<'_, PyAny>,
    mapping: &Bound<'_, PyAny>,
    alias_renamed_tables: bool,
    preserve_existing_aliases: bool,
) -> PyResult<Py<PyAny>> {
    let return_list = ast.cast::<PyList>().is_ok();
    let expressions = ast_input_to_expressions(ast)?;
    let mapping: HashMap<String, String> = depythonize(mapping)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to decode table mapping: {err}")))?;

    let mut options = RenameTablesOptions::new();
    options.alias_renamed_tables = alias_renamed_tables;
    options.preserve_existing_aliases = preserve_existing_aliases;

    let transformed = py.detach(move || {
        expressions
            .into_iter()
            .map(|expression| {
                polyglot_sql::rename_tables_with_options(expression, &mapping, &options)
            })
            .collect()
    });

    wrap_transform_result(py, return_list, transformed)
}
