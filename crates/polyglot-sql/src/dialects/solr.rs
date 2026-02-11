//! Apache Solr SQL Dialect
//!
//! Solr-specific SQL dialect based on sqlglot patterns.
//! Reference: https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html
//!
//! Key characteristics:
//! - Case insensitive normalization
//! - Uses backticks for identifiers
//! - Single quotes for strings only
//! - Note: `||` is OR in Solr (not string concatenation)
//! - Does not support SEMI/ANTI joins

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{BinaryOp, Expression};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Apache Solr dialect
pub struct SolrDialect;

impl DialectImpl for SolrDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Solr
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Solr uses backticks for identifiers
        config.identifiers.insert('`', '`');
        // Single quotes only for strings
        // Note: Default tokenizer already handles single quotes for strings
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Solr),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        // Solr has limited SQL support
        // In Solr, || is OR, not string concatenation (DPIPE_IS_STRING_CONCAT = False in sqlglot)
        match expr {
            Expression::DPipe(dpipe) => {
                // Transform DPipe (||) to Or
                let left = self.transform_expr(*dpipe.this)?;
                let right = self.transform_expr(*dpipe.expression)?;
                Ok(Expression::Or(Box::new(BinaryOp::new(left, right))))
            }
            _ => Ok(expr),
        }
    }
}
