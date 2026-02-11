//! Error types for polyglot-sql

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The result type for polyglot operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during SQL parsing and generation
#[derive(Debug, Error)]
pub enum Error {
    /// Error during tokenization
    #[error("Tokenization error at line {line}, column {column}: {message}")]
    Tokenize {
        message: String,
        line: usize,
        column: usize,
    },

    /// Error during parsing
    #[error("Parse error: {0}")]
    Parse(String),

    /// Error during SQL generation
    #[error("Generation error: {0}")]
    Generate(String),

    /// Unsupported feature for the target dialect
    #[error("Unsupported: {feature} is not supported in {dialect}")]
    Unsupported {
        feature: String,
        dialect: String,
    },

    /// Invalid SQL syntax
    #[error("Syntax error at line {line}, column {column}: {message}")]
    Syntax {
        message: String,
        line: usize,
        column: usize,
    },

    /// Internal error (should not happen in normal usage)
    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Create a tokenization error
    pub fn tokenize(message: impl Into<String>, line: usize, column: usize) -> Self {
        Error::Tokenize {
            message: message.into(),
            line,
            column,
        }
    }

    /// Create a parse error
    pub fn parse(message: impl Into<String>) -> Self {
        Error::Parse(message.into())
    }

    /// Create a generation error
    pub fn generate(message: impl Into<String>) -> Self {
        Error::Generate(message.into())
    }

    /// Create an unsupported feature error
    pub fn unsupported(feature: impl Into<String>, dialect: impl Into<String>) -> Self {
        Error::Unsupported {
            feature: feature.into(),
            dialect: dialect.into(),
        }
    }

    /// Create a syntax error
    pub fn syntax(message: impl Into<String>, line: usize, column: usize) -> Self {
        Error::Syntax {
            message: message.into(),
            line,
            column,
        }
    }

    /// Create an internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Error::Internal(message.into())
    }
}

/// Severity level for validation errors
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ValidationSeverity {
    /// An error that prevents the query from being valid
    Error,
    /// A warning about potential issues
    Warning,
}

/// A single validation error or warning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// The error/warning message
    pub message: String,
    /// Line number where the error occurred (1-based)
    pub line: Option<usize>,
    /// Column number where the error occurred (1-based)
    pub column: Option<usize>,
    /// Severity of the validation issue
    pub severity: ValidationSeverity,
    /// Error code (e.g., "E001", "W001")
    pub code: String,
}

impl ValidationError {
    /// Create a new validation error
    pub fn error(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            line: None,
            column: None,
            severity: ValidationSeverity::Error,
            code: code.into(),
        }
    }

    /// Create a new validation warning
    pub fn warning(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            line: None,
            column: None,
            severity: ValidationSeverity::Warning,
            code: code.into(),
        }
    }

    /// Set the line number
    pub fn with_line(mut self, line: usize) -> Self {
        self.line = Some(line);
        self
    }

    /// Set the column number
    pub fn with_column(mut self, column: usize) -> Self {
        self.column = Some(column);
        self
    }

    /// Set both line and column
    pub fn with_location(mut self, line: usize, column: usize) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }
}

/// Result of validating SQL
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the SQL is valid (no errors, warnings are allowed)
    pub valid: bool,
    /// List of validation errors and warnings
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    /// Create a successful validation result
    pub fn success() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
        }
    }

    /// Create a validation result with errors
    pub fn with_errors(errors: Vec<ValidationError>) -> Self {
        let has_errors = errors.iter().any(|e| e.severity == ValidationSeverity::Error);
        Self {
            valid: !has_errors,
            errors,
        }
    }

    /// Add an error to the result
    pub fn add_error(&mut self, error: ValidationError) {
        if error.severity == ValidationSeverity::Error {
            self.valid = false;
        }
        self.errors.push(error);
    }
}
