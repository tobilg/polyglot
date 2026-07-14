use crate::helpers::resolve_dialect;
use crate::types::{validation_result_from_core, ValidationResult};
use polyglot_sql::ValidationOptions;
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = "generic", *, strict_syntax = false, semantic = false))]
pub fn validate(
    py: Python<'_>,
    sql: &str,
    dialect: &str,
    strict_syntax: bool,
    semantic: bool,
) -> PyResult<ValidationResult> {
    let dialect = resolve_dialect(dialect)?;
    let options = ValidationOptions {
        strict_syntax,
        semantic,
    };

    let result = py.detach(|| polyglot_sql::validate_with_dialect(sql, &dialect, &options));

    Ok(validation_result_from_core(result))
}
