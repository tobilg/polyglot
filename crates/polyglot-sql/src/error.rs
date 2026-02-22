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
    #[error("Parse error at line {line}, column {column}: {message}")]
    Parse {
        message: String,
        line: usize,
        column: usize,
    },

    /// Error during SQL generation
    #[error("Generation error: {0}")]
    Generate(String),

    /// Unsupported feature for the target dialect
    #[error("Unsupported: {feature} is not supported in {dialect}")]
    Unsupported { feature: String, dialect: String },

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

    /// Create a parse error with position information
    pub fn parse(message: impl Into<String>, line: usize, column: usize) -> Self {
        Error::Parse {
            message: message.into(),
            line,
            column,
        }
    }

    /// Get the line number if available
    pub fn line(&self) -> Option<usize> {
        match self {
            Error::Tokenize { line, .. }
            | Error::Parse { line, .. }
            | Error::Syntax { line, .. } => Some(*line),
            _ => None,
        }
    }

    /// Get the column number if available
    pub fn column(&self) -> Option<usize> {
        match self {
            Error::Tokenize { column, .. }
            | Error::Parse { column, .. }
            | Error::Syntax { column, .. } => Some(*column),
            _ => None,
        }
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
        let has_errors = errors
            .iter()
            .any(|e| e.severity == ValidationSeverity::Error);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_has_position() {
        let err = Error::parse("test message", 5, 10);
        assert_eq!(err.line(), Some(5));
        assert_eq!(err.column(), Some(10));
        assert!(err.to_string().contains("line 5"));
        assert!(err.to_string().contains("column 10"));
        assert!(err.to_string().contains("test message"));
    }

    #[test]
    fn test_tokenize_error_has_position() {
        let err = Error::tokenize("bad token", 3, 7);
        assert_eq!(err.line(), Some(3));
        assert_eq!(err.column(), Some(7));
    }

    #[test]
    fn test_generate_error_has_no_position() {
        let err = Error::generate("gen error");
        assert_eq!(err.line(), None);
        assert_eq!(err.column(), None);
    }

    #[test]
    fn test_parse_error_position_from_parser() {
        // Parse invalid SQL and verify the error carries position info
        use crate::dialects::{Dialect, DialectType};
        let d = Dialect::get(DialectType::Generic);
        let result = d.parse("SELECT 1 + 2)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.line().is_some(), "Parse error should have line: {:?}", err);
        assert!(err.column().is_some(), "Parse error should have column: {:?}", err);
        assert_eq!(err.line(), Some(1));
    }
}
