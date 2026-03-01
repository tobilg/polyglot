use crate::errors::map_transpile_error;
use crate::helpers::{parse_single_statement, resolve_dialect, to_python_object};
use polyglot_sql::lineage as core_lineage;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pythonize::depythonize;

#[pyfunction(signature = (column, sql, dialect = "generic"))]
pub fn lineage(py: Python<'_>, column: &str, sql: &str, dialect: &str) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;

    let node = py.detach(|| {
        let dialect_impl = polyglot_sql::dialects::Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let expression = parse_single_statement(sql, &dialect_impl)?;
        let dialect_type = dialect_impl.dialect_type();
        let dialect_option = if dialect_type == polyglot_sql::DialectType::Generic {
            None
        } else {
            Some(dialect_type)
        };
        core_lineage::lineage(column, &expression, dialect_option, false)
            .map_err(map_transpile_error)
    })?;

    to_python_object(py, &node)
}

#[pyfunction(signature = (column, sql, schema, dialect = "generic"))]
pub fn lineage_with_schema(
    py: Python<'_>,
    column: &str,
    sql: &str,
    schema: &Bound<'_, PyAny>,
    dialect: &str,
) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;
    let validation_schema: polyglot_sql::ValidationSchema = depythonize(schema).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid schema object (expected ValidationSchema shape): {err}"
        ))
    })?;
    let mapping_schema = polyglot_sql::mapping_schema_from_validation_schema(&validation_schema);

    let node = py.detach(|| {
        let dialect_impl = polyglot_sql::dialects::Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let expression = parse_single_statement(sql, &dialect_impl)?;
        let dialect_type = dialect_impl.dialect_type();
        let dialect_option = if dialect_type == polyglot_sql::DialectType::Generic {
            None
        } else {
            Some(dialect_type)
        };
        core_lineage::lineage_with_schema(
            column,
            &expression,
            Some(&mapping_schema),
            dialect_option,
            false,
        )
        .map_err(map_transpile_error)
    })?;

    to_python_object(py, &node)
}

#[pyfunction(signature = (column, sql, dialect = "generic"))]
pub fn source_tables(
    py: Python<'_>,
    column: &str,
    sql: &str,
    dialect: &str,
) -> PyResult<Vec<String>> {
    resolve_dialect(dialect)?;

    py.detach(|| {
        let dialect_impl = polyglot_sql::dialects::Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let expression = parse_single_statement(sql, &dialect_impl)?;
        let dialect_type = dialect_impl.dialect_type();
        let dialect_option = if dialect_type == polyglot_sql::DialectType::Generic {
            None
        } else {
            Some(dialect_type)
        };
        let node = core_lineage::lineage(column, &expression, dialect_option, false)
            .map_err(map_transpile_error)?;

        let mut tables: Vec<String> = core_lineage::get_source_tables(&node).into_iter().collect();
        tables.sort();
        Ok::<Vec<String>, PyErr>(tables)
    })
}
