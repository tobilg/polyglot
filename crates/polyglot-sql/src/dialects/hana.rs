//! SAP HANA Cloud and Platform SQL dialect.
//!
//! Registration is based on Torsten Glunde's contribution in PR #276.
//! Syntax and semantic handling live in the shared parser, AST, and generator.

use super::{DialectImpl, DialectType};
#[cfg(feature = "generate")]
use crate::generator::{GeneratorConfig, IdentifierQuoteStyle, NormalizeFunctions};
use crate::tokens::{TokenType, TokenizerConfig};

pub struct HanaDialect;

impl DialectImpl for HanaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::HANA
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        config.identifiers.insert('"', '"');
        config.nested_comments = false;
        config
            .keywords
            .insert("UNKNOWN".to_owned(), TokenType::Null);
        config
    }

    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        GeneratorConfig {
            dialect: Some(DialectType::HANA),
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            normalize_functions: NormalizeFunctions::None,
            alter_table_include_column_keyword: false,
            ..Default::default()
        }
    }
}
