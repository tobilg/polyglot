//! Dremio SQL Dialect
//!
//! Dremio-specific SQL dialect based on sqlglot patterns.
//! Reference: https://docs.dremio.com/current/reference/sql/data-types/
//!
//! Key characteristics:
//! - NULLS LAST is default ordering
//! - DATE_ADD/DATE_SUB with INTERVAL CAST for non-DAY units
//! - CURRENT_DATE_UTC for current date in UTC
//! - ARRAY_GENERATE_RANGE for generating series
//! - No timezone-aware timestamps
//! - Comments support: --, //, /* */
//! - Type mappings: SMALLINT→INT, TINYINT→INT, ARRAY→LIST, etc.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Dremio dialect
pub struct DremioDialect;

impl DialectImpl for DremioDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Dremio
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Dremio uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Dremio supports multiple comment styles: --, //, /* */
        // Default tokenizer handles -- and /* */
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Dremio),
            // Dremio uses singular form for intervals (DAY not DAYS)
            interval_allows_plural_form: false,
            // Dremio requires literal values in LIMIT clause
            limit_only_literals: true,
            // Dremio doesn't support COUNT(DISTINCT a, b) - needs transformation
            multi_arg_distinct: false,
            // Dremio supports BETWEEN SYMMETRIC/ASYMMETRIC
            supports_between_flags: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DremioDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // GenerateSeries → ARRAY_GENERATE_RANGE
            "GENERATE_SERIES" => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_GENERATE_RANGE".to_string(),
                f.args,
            )))),

            // TimeToStr → TO_CHAR
            "DATE_FORMAT" | "TIME_TO_STR" | "STRFTIME" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_CHAR".to_string(),
                    f.args,
                ))))
            }

            // TO_DATE is native
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // DATE_ADD is native (with interval cast for non-day units)
            "DATE_ADD" => Ok(Expression::Function(Box::new(f))),

            // DATE_SUB is native (with interval cast for non-day units)
            "DATE_SUB" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_MATCHES → REGEXP_LIKE (Dremio uses REGEXP_LIKE)
            "REGEXP_MATCHES" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // REPEATSTR → REPEAT (Dremio uses REPEAT, not REPEATSTR)
            "REPEATSTR" => Ok(Expression::Function(Box::new(Function::new(
                "REPEAT".to_string(),
                f.args,
            )))),

            // DATE_PART → DATE_PART (native, same as EXTRACT)
            "DATE_PART" | "EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // DATETYPE constructor for date literals
            "DATETYPE" => Ok(Expression::Function(Box::new(f))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // BitwiseAndAgg → BIT_AND
            "BITWISE_AND_AGG" | "BIT_AND_AGG" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "BIT_AND".to_string(),
                    f.args,
                ))))
            }

            // BitwiseOrAgg → BIT_OR
            "BITWISE_OR_AGG" | "BIT_OR_AGG" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "BIT_OR".to_string(),
                    f.args,
                ))))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

// Note: Dremio type mappings (handled in generator if needed):
// - SMALLINT → INT
// - TINYINT → INT
// - BINARY → VARBINARY
// - TEXT → VARCHAR
// - NCHAR → VARCHAR
// - CHAR → VARCHAR
// - TIMESTAMPNTZ → TIMESTAMP
// - DATETIME → TIMESTAMP
// - ARRAY → LIST
// - BIT → BOOLEAN
//
// Dremio does not support timezone-aware TIMESTAMP types
