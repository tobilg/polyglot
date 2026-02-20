//! Tableau SQL Dialect
//!
//! Tableau-specific SQL dialect based on sqlglot patterns.
//!
//! Key characteristics:
//! - Uses square brackets for identifiers: [x]
//! - Single and double quotes for strings
//! - COALESCE → IFNULL
//! - COUNT(DISTINCT x) → COUNTD(x)
//! - No join hints, table hints, or query hints
//! - IF x THEN y ELSE z END syntax

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Tableau dialect
pub struct TableauDialect;

impl DialectImpl for TableauDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Tableau
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Tableau uses square brackets for identifiers
        config.identifiers.insert('[', ']');
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '[',
            identifier_quote_style: IdentifierQuoteStyle::BRACKET,
            dialect: Some(DialectType::Tableau),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // COALESCE → IFNULL in Tableau
            Expression::Coalesce(f) => {
                if f.expressions.len() == 2 {
                    Ok(Expression::Function(Box::new(Function::new(
                        "IFNULL".to_string(),
                        f.expressions,
                    ))))
                } else {
                    // For more than 2 args, keep as-is or nest IFNULL calls
                    Ok(Expression::Coalesce(f))
                }
            }

            // NVL → IFNULL in Tableau
            Expression::Nvl(f) => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                vec![f.this, f.expression],
            )))),

            // IfNull stays as IFNULL
            Expression::IfNull(f) => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                vec![f.this, f.expression],
            )))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Aggregate function transformations (for COUNT DISTINCT)
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl TableauDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // COALESCE → IFNULL
            "COALESCE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // NVL → IFNULL
            "NVL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // ISNULL → IFNULL
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // FIND is native to Tableau (similar to STRPOS/POSITION)
            "STRPOS" | "POSITION" | "INSTR" if f.args.len() >= 2 => Ok(Expression::Function(
                Box::new(Function::new("FIND".to_string(), f.args)),
            )),

            // CHARINDEX → FIND
            "CHARINDEX" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "FIND".to_string(),
                f.args,
            )))),

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
            // COUNT with DISTINCT → COUNTD in Tableau
            "COUNT" if f.distinct => Ok(Expression::Function(Box::new(Function::new(
                "COUNTD".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}
