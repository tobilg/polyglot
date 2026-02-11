//! Generic SQL Dialect

use super::{DialectImpl, DialectType};

/// Generic SQL dialect (ANSI SQL)
pub struct GenericDialect;

impl DialectImpl for GenericDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Generic
    }
}
