use crate::helpers::resolve_dialect;
use crate::types::{validation_result_from_core, ValidationResult};
use polyglot_sql::{Error, ValidationError, ValidationResult as CoreValidationResult};
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn validate(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<ValidationResult> {
    resolve_dialect(dialect)?;

    let result = py.detach(|| {
        let dialect =
            polyglot_sql::dialects::Dialect::get_by_name(dialect).expect("dialect validated");
        match dialect.parse(sql) {
            Ok(expressions) => {
                for expression in &expressions {
                    if !expression.is_statement() {
                        return CoreValidationResult::with_errors(vec![ValidationError::error(
                            "Invalid expression / Unexpected token",
                            "E004",
                        )]);
                    }
                }
                CoreValidationResult::success()
            }
            Err(error) => CoreValidationResult::with_errors(vec![map_validation_error(error)]),
        }
    });

    Ok(validation_result_from_core(result))
}

fn map_validation_error(error: Error) -> ValidationError {
    match error {
        Error::Syntax {
            message,
            line,
            column,
        } => ValidationError::error(message, "E001").with_location(line, column),
        Error::Tokenize {
            message,
            line,
            column,
        } => ValidationError::error(message, "E002").with_location(line, column),
        Error::Parse {
            message,
            line,
            column,
        } => ValidationError::error(message, "E003").with_location(line, column),
        _ => ValidationError::error(error.to_string(), "E000"),
    }
}
