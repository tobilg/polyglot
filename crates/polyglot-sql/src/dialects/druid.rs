//! Apache Druid Dialect
//!
//! Druid-specific SQL dialect based on sqlglot patterns.
//! Reference: https://druid.apache.org/docs/latest/querying/sql-data-types/
//!
//! Key characteristics:
//! - Type mappings: NCHAR, NVARCHAR, TEXT, UUID → STRING
//! - CURRENT_TIMESTAMP without parentheses
//! - MOD function for modulo
//! - ARRAY[...] syntax for arrays

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Apache Druid dialect
pub struct DruidDialect;

impl DialectImpl for DruidDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Druid
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Druid uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Druid),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // CurrentTimestamp without args in Druid
            Expression::CurrentTimestamp(_) => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // Modulo -> MOD function in Druid
            Expression::Mod(op) => Ok(Expression::Function(Box::new(Function::new(
                "MOD".to_string(),
                vec![op.left, op.right],
            )))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DruidDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // CURRENT_TIMESTAMP without parentheses
            "CURRENT_TIMESTAMP" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }
}
