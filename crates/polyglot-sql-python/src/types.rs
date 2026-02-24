use polyglot_sql::{
    ValidationError as CoreValidationError, ValidationResult as CoreValidationResult,
    ValidationSeverity,
};
use pyo3::prelude::*;

#[pyclass(skip_from_py_object, module = "polyglot_sql")]
#[derive(Clone, Debug)]
pub struct ValidationErrorInfo {
    message: String,
    line: usize,
    col: usize,
    code: String,
    severity: String,
}

#[pymethods]
impl ValidationErrorInfo {
    #[getter]
    fn message(&self) -> &str {
        &self.message
    }

    #[getter]
    fn line(&self) -> usize {
        self.line
    }

    #[getter]
    fn col(&self) -> usize {
        self.col
    }

    #[getter]
    fn code(&self) -> &str {
        &self.code
    }

    #[getter]
    fn severity(&self) -> &str {
        &self.severity
    }

    fn __repr__(&self) -> String {
        format!(
            "ValidationErrorInfo(message={:?}, line={}, col={}, code={:?}, severity={:?})",
            self.message, self.line, self.col, self.code, self.severity
        )
    }
}

#[pyclass(skip_from_py_object, module = "polyglot_sql")]
#[derive(Clone, Debug)]
pub struct ValidationResult {
    valid: bool,
    errors: Vec<ValidationErrorInfo>,
}

#[pymethods]
impl ValidationResult {
    #[getter]
    fn valid(&self) -> bool {
        self.valid
    }

    #[getter]
    fn errors(&self) -> Vec<ValidationErrorInfo> {
        self.errors.clone()
    }

    fn __bool__(&self) -> bool {
        self.valid
    }

    fn __repr__(&self) -> String {
        format!(
            "ValidationResult(valid={}, errors={})",
            self.valid,
            self.errors.len()
        )
    }
}

fn validation_error_info_from_core(err: &CoreValidationError) -> ValidationErrorInfo {
    ValidationErrorInfo {
        message: err.message.clone(),
        line: err.line.unwrap_or(0),
        col: err.column.unwrap_or(0),
        code: err.code.clone(),
        severity: match err.severity {
            ValidationSeverity::Error => "error".to_string(),
            ValidationSeverity::Warning => "warning".to_string(),
        },
    }
}

pub fn validation_result_from_core(result: CoreValidationResult) -> ValidationResult {
    ValidationResult {
        valid: result.valid,
        errors: result
            .errors
            .iter()
            .map(validation_error_info_from_core)
            .collect(),
    }
}
