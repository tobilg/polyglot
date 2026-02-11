//! Apache Drill SQL Dialect
//!
//! Drill-specific SQL dialect based on sqlglot patterns.
//!
//! Key characteristics:
//! - Uses backticks for identifiers
//! - Backslash string escapes
//! - No TRY_CAST support (must use CAST)
//! - NULLS LAST is default ordering
//! - Functions: REPEATED_COUNT (array size), REPEATED_CONTAINS (array contains)
//! - POW for power function
//! - Date format: 'yyyy-MM-dd'
//! - Type mappings: INT→INTEGER, TEXT→VARCHAR, etc.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function};
use crate::generator::{GeneratorConfig, NormalizeFunctions};
use crate::tokens::TokenizerConfig;

/// Apache Drill dialect
pub struct DrillDialect;

impl DialectImpl for DrillDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Drill
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Drill uses backticks for identifiers
        config.identifiers.insert('`', '`');
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Drill),
            // Drill: NORMALIZE_FUNCTIONS = False, PRESERVE_ORIGINAL_NAMES = True
            normalize_functions: NormalizeFunctions::None,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // TRY_CAST → CAST in Drill (no TRY_CAST support)
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast → CAST in Drill
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // CURRENT_TIMESTAMP without parentheses
            Expression::CurrentTimestamp(_) => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // ILIKE → `ILIKE` (backtick quoted function in Drill)
            // Drill supports ILIKE but it needs to be backtick-quoted
            Expression::ILike(op) => {
                // Just pass through - Drill supports ILIKE
                Ok(Expression::ILike(op))
            }

            // Power → POW in Drill
            Expression::Power(op) => Ok(Expression::Function(Box::new(Function::new(
                "POW".to_string(),
                vec![op.this, op.expression],
            )))),

            // ArrayContains → REPEATED_CONTAINS in Drill
            Expression::ArrayContains(f) => Ok(Expression::Function(Box::new(Function::new(
                "REPEATED_CONTAINS".to_string(),
                vec![f.this, f.expression],
            )))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DrillDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // CURRENT_TIMESTAMP without parentheses
            "CURRENT_TIMESTAMP" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // ARRAY_SIZE / ARRAY_LENGTH → REPEATED_COUNT
            "ARRAY_SIZE" | "ARRAY_LENGTH" | "CARDINALITY" | "SIZE" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "REPEATED_COUNT".to_string(),
                    f.args,
                ))))
            }

            // ARRAY_CONTAINS → REPEATED_CONTAINS
            "ARRAY_CONTAINS" | "CONTAINS" => Ok(Expression::Function(Box::new(Function::new(
                "REPEATED_CONTAINS".to_string(),
                f.args,
            )))),

            // POWER → POW
            "POWER" => Ok(Expression::Function(Box::new(Function::new(
                "POW".to_string(),
                f.args,
            )))),

            // LEVENSHTEIN → LEVENSHTEIN_DISTANCE
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(Function::new(
                "LEVENSHTEIN_DISTANCE".to_string(),
                f.args,
            )))),

            // REGEXP_LIKE → REGEXP_MATCHES
            "REGEXP_LIKE" | "RLIKE" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_MATCHES".to_string(),
                f.args,
            )))),

            // TO_TIMESTAMP → TO_TIMESTAMP (native, but for parsing)
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // TO_DATE → TO_DATE (native)
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT → TO_CHAR
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(Function::new(
                "TO_CHAR".to_string(),
                f.args,
            )))),

            // strftime → TO_CHAR
            "STRFTIME" => Ok(Expression::Function(Box::new(Function::new(
                "TO_CHAR".to_string(),
                f.args,
            )))),

            // UNIX_TIMESTAMP → native
            "UNIX_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // FROM_UNIXTIME → native (but named UNIX_TIMESTAMP_TO_TIMESTAMP in Drill)
            "FROM_UNIXTIME" => Ok(Expression::Function(Box::new(f))),

            // DATE_ADD with interval support
            "DATE_ADD" => Ok(Expression::Function(Box::new(f))),

            // DATE_SUB with interval support
            "DATE_SUB" => Ok(Expression::Function(Box::new(f))),

            // STRPOS → STRPOS (native in Drill)
            "STRPOS" => Ok(Expression::Function(Box::new(f))),

            // POSITION → STRPOS
            "POSITION" => Ok(Expression::Function(Box::new(Function::new(
                "STRPOS".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }
}
