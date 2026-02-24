use polyglot_sql::Error as CoreError;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::{create_exception, PyErr};

create_exception!(_polyglot_sql, PolyglotError, PyException);
create_exception!(_polyglot_sql, ParseError, PolyglotError);
create_exception!(_polyglot_sql, GenerateError, PolyglotError);
create_exception!(_polyglot_sql, TranspileError, PolyglotError);
create_exception!(_polyglot_sql, ValidationError, PolyglotError);

pub fn unknown_dialect_error(name: &str) -> PyErr {
    PyValueError::new_err(format!("Unknown dialect: {name}"))
}

pub fn parse_statement_count_error(count: usize) -> PyErr {
    ParseError::new_err(format!("Expected 1 statement, found {count}"))
}

pub fn map_parse_error(err: CoreError) -> PyErr {
    match err {
        CoreError::Parse { .. } | CoreError::Tokenize { .. } | CoreError::Syntax { .. } => {
            ParseError::new_err(err.to_string())
        }
        _ => PolyglotError::new_err(err.to_string()),
    }
}

pub fn map_generate_error(err: CoreError) -> PyErr {
    match err {
        CoreError::Generate(_) => GenerateError::new_err(err.to_string()),
        CoreError::Parse { .. } | CoreError::Tokenize { .. } | CoreError::Syntax { .. } => {
            ParseError::new_err(err.to_string())
        }
        _ => GenerateError::new_err(err.to_string()),
    }
}

pub fn map_transpile_error(err: CoreError) -> PyErr {
    match err {
        CoreError::Generate(_) => GenerateError::new_err(err.to_string()),
        CoreError::Parse { .. } | CoreError::Tokenize { .. } | CoreError::Syntax { .. } => {
            ParseError::new_err(err.to_string())
        }
        _ => TranspileError::new_err(err.to_string()),
    }
}

pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PolyglotError", m.py().get_type::<PolyglotError>())?;
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    m.add("GenerateError", m.py().get_type::<GenerateError>())?;
    m.add("TranspileError", m.py().get_type::<TranspileError>())?;
    m.add("ValidationError", m.py().get_type::<ValidationError>())?;
    Ok(())
}
