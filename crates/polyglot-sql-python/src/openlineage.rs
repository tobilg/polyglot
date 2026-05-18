use crate::errors::map_transpile_error;
use crate::helpers::to_python_object;
use polyglot_sql::openlineage::{
    openlineage_column_lineage as core_openlineage_column_lineage,
    openlineage_job_event as core_openlineage_job_event,
    openlineage_run_event as core_openlineage_run_event, OpenLineageOptions,
};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pythonize::depythonize;

#[pyfunction(signature = (sql, options))]
pub fn openlineage_column_lineage(
    py: Python<'_>,
    sql: &str,
    options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let options = parse_options(options)?;
    let sql = sql.to_owned();
    let result = py.detach(move || {
        core_openlineage_column_lineage(&sql, &options).map_err(map_transpile_error)
    })?;

    to_python_object(py, &result)
}

#[pyfunction(signature = (sql, options))]
pub fn openlineage_job_event(
    py: Python<'_>,
    sql: &str,
    options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let options = parse_options(options)?;
    let sql = sql.to_owned();
    let result =
        py.detach(move || core_openlineage_job_event(&sql, &options).map_err(map_transpile_error))?;

    to_python_object(py, &result)
}

#[pyfunction(signature = (sql, options))]
pub fn openlineage_run_event(
    py: Python<'_>,
    sql: &str,
    options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let options = parse_options(options)?;
    let sql = sql.to_owned();
    let result =
        py.detach(move || core_openlineage_run_event(&sql, &options).map_err(map_transpile_error))?;

    to_python_object(py, &result)
}

fn parse_options(options: &Bound<'_, PyAny>) -> PyResult<OpenLineageOptions> {
    depythonize(options).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid OpenLineage options object: {err}"
        ))
    })
}
