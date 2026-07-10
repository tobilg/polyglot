//! SAP HANA Cloud SQL Dialect
//!
//! SAP HANA is an in-memory, column-oriented, relational database management system.
//! Reference: SAP HANA Cloud SQL Reference Guide.
//!
//! Key characteristics (Phase 1 — identity round-trip only):
//! - Double-quote identifiers (case preserved, like Oracle)
//! - Standard SQL string quoting with single-quote escaping (`''`)
//! - No nested comment support
//! - Uppercase keyword generation
//! - No function transforms (added in Phase 2)

use super::{DialectImpl, DialectType};
#[cfg(feature = "generate")]
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// SAP HANA Cloud dialect
pub struct HanaDialect;

impl DialectImpl for HanaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::HANA
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // HANA uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // HANA does not support nested comments
        config.nested_comments = false;
        config
    }

    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, NormalizeFunctions};
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::HANA),
            // HANA uses the COLUMN keyword in ALTER TABLE ADD
            alter_table_include_column_keyword: true,
            // Preserve function name casing for identity round-trip (Phase 1)
            normalize_functions: NormalizeFunctions::None,
            ..Default::default()
        }
    }
}
