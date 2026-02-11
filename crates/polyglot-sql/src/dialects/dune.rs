//! Dune Analytics SQL Dialect
//!
//! Dune-specific SQL dialect based on sqlglot patterns.
//! Dune inherits from Trino with minor differences.
//!
//! Key characteristics:
//! - Based on Trino (uses Trino's tokenizer and generator configs)
//! - Hex strings use 0x prefix format: 0xABCD
//! - Supports X'...' hex string syntax for parsing

use super::{DialectImpl, DialectType, TrinoDialect};
use crate::error::Result;
use crate::expressions::Expression;
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Dune Analytics dialect (based on Trino)
pub struct DuneDialect;

impl DialectImpl for DuneDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Dune
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        // Inherit from Trino
        let trino = TrinoDialect;
        // Dune supports hex strings with 0x prefix and X'...' syntax
        // This is handled at the tokenizer level
        trino.tokenizer_config()
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        // Inherit from Trino with Dune dialect type
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Dune),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        // Delegate to Trino for most transformations
        let trino = TrinoDialect;

        // First apply Trino transformations
        let transformed = trino.transform_expr(expr)?;

        // Dune-specific transformations can be added here
        // Currently, the main difference is hex string format (handled in generator)
        Ok(transformed)
    }
}
