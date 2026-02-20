//! Exasol SQL Dialect
//!
//! Exasol-specific SQL dialect based on sqlglot patterns.
//!
//! References:
//! - SQL Reference: https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm
//! - Data Types: https://docs.exasol.com/db/latest/sql_references/data_types/datatypesoverview.htm
//! - Functions: https://docs.exasol.com/db/latest/sql_references/functions/
//!
//! Key characteristics:
//! - Uppercase normalization for identifiers
//! - Identifiers: double quotes or square brackets
//! - Date functions: ADD_DAYS, ADD_MONTHS, DAYS_BETWEEN, MONTHS_BETWEEN
//! - Bitwise: BIT_AND, BIT_OR, BIT_XOR, BIT_NOT, BIT_LSHIFT, BIT_RSHIFT
//! - Functions: ZEROIFNULL, NULLIFZERO, SYSTIMESTAMP
//! - EVERY for ALL aggregate
//! - No SEMI/ANTI join support
//! - DATE_TRUNC for date truncation
//! - IF...THEN...ELSE...ENDIF syntax

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function, ListAggFunc, Literal, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Exasol dialect
pub struct ExasolDialect;

impl DialectImpl for ExasolDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Exasol
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Exasol uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Also supports square brackets
        config.identifiers.insert('[', ']');
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Exasol),
            supports_column_join_marks: true,
            // Exasol uses lowercase for window frame keywords (rows, preceding, following)
            lowercase_window_frame_keywords: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // SYSTIMESTAMP -> SYSTIMESTAMP() (with parentheses in Exasol)
            Expression::Systimestamp(_) => Ok(Expression::Function(Box::new(Function::new(
                "SYSTIMESTAMP".to_string(),
                vec![],
            )))),

            // WeekOfYear -> WEEK
            Expression::WeekOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "WEEK".to_string(),
                vec![f.this],
            )))),

            // COALESCE is native, but also support transformations from other forms
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Bitwise operations → BIT_* functions
            Expression::BitwiseAnd(op) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_AND".to_string(),
                vec![op.left, op.right],
            )))),

            Expression::BitwiseOr(op) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_OR".to_string(),
                vec![op.left, op.right],
            )))),

            Expression::BitwiseXor(op) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_XOR".to_string(),
                vec![op.left, op.right],
            )))),

            Expression::BitwiseNot(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_NOT".to_string(),
                vec![f.this],
            )))),

            Expression::BitwiseLeftShift(op) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_LSHIFT".to_string(),
                vec![op.left, op.right],
            )))),

            Expression::BitwiseRightShift(op) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_RSHIFT".to_string(),
                vec![op.left, op.right],
            )))),

            // Modulo → MOD function
            Expression::Mod(op) => Ok(Expression::Function(Box::new(Function::new(
                "MOD".to_string(),
                vec![op.left, op.right],
            )))),

            // GROUP_CONCAT -> LISTAGG in Exasol (with WITHIN GROUP for ORDER BY)
            Expression::GroupConcat(f) => Ok(Expression::ListAgg(Box::new(ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
            }))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl ExasolDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // SYSTIMESTAMP -> SYSTIMESTAMP() (with parentheses in Exasol)
            // Exasol requires parentheses even for no-arg functions
            // Preserve any arguments (like precision)
            "SYSTIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "SYSTIMESTAMP".to_string(),
                f.args,
            )))),

            // ALL → EVERY
            "ALL" => Ok(Expression::Function(Box::new(Function::new(
                "EVERY".to_string(),
                f.args,
            )))),

            // IFNULL/ISNULL/NVL → COALESCE (native in Exasol)
            "IFNULL" | "ISNULL" | "NVL" if f.args.len() == 2 => {
                Ok(Expression::Coalesce(Box::new(VarArgFunc {
                    original_name: None,
                    expressions: f.args,
                })))
            }

            // DateDiff → DAYS_BETWEEN (for DAY unit) or other *_BETWEEN functions
            "DATEDIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DAYS_BETWEEN".to_string(),
                f.args,
            )))),

            // DateAdd → ADD_DAYS (for DAY unit) or other ADD_* functions
            "DATEADD" | "DATE_ADD" => Ok(Expression::Function(Box::new(Function::new(
                "ADD_DAYS".to_string(),
                f.args,
            )))),

            // DateSub → Negate and use ADD_DAYS
            "DATESUB" | "DATE_SUB" => {
                // Would need to negate the interval, for now just use ADD_DAYS
                Ok(Expression::Function(Box::new(Function::new(
                    "ADD_DAYS".to_string(),
                    f.args,
                ))))
            }

            // DATE_TRUNC is native
            "DATE_TRUNC" | "TRUNC" => Ok(Expression::Function(Box::new(f))),

            // LEVENSHTEIN → EDIT_DISTANCE
            "LEVENSHTEIN" | "LEVENSHTEIN_DISTANCE" => Ok(Expression::Function(Box::new(
                Function::new("EDIT_DISTANCE".to_string(), f.args),
            ))),

            // REGEXP_EXTRACT → REGEXP_SUBSTR
            "REGEXP_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_SUBSTR".to_string(),
                f.args,
            )))),

            // SHA/SHA1 → HASH_SHA
            "SHA" | "SHA1" => Ok(Expression::Function(Box::new(Function::new(
                "HASH_SHA".to_string(),
                f.args,
            )))),

            // MD5 → HASH_MD5
            "MD5" => Ok(Expression::Function(Box::new(Function::new(
                "HASH_MD5".to_string(),
                f.args,
            )))),

            // SHA256 → HASH_SHA256
            "SHA256" | "SHA2" => {
                // SHA2 in some dialects takes a length parameter
                // HASH_SHA256 in Exasol just takes the value
                let arg = f
                    .args
                    .into_iter()
                    .next()
                    .unwrap_or(Expression::Null(crate::expressions::Null));
                Ok(Expression::Function(Box::new(Function::new(
                    "HASH_SHA256".to_string(),
                    vec![arg],
                ))))
            }

            // SHA512 → HASH_SHA512
            "SHA512" => Ok(Expression::Function(Box::new(Function::new(
                "HASH_SHA512".to_string(),
                f.args,
            )))),

            // VAR_POP is native
            "VAR_POP" | "VARIANCE_POP" => Ok(Expression::Function(Box::new(Function::new(
                "VAR_POP".to_string(),
                f.args,
            )))),

            // APPROX_DISTINCT → APPROXIMATE_COUNT_DISTINCT
            "APPROX_DISTINCT" | "APPROX_COUNT_DISTINCT" => Ok(Expression::Function(Box::new(
                Function::new("APPROXIMATE_COUNT_DISTINCT".to_string(), f.args),
            ))),

            // TO_CHAR is native for date formatting
            "TO_CHAR" | "DATE_FORMAT" | "STRFTIME" => Ok(Expression::Function(Box::new(
                Function::new("TO_CHAR".to_string(), f.args),
            ))),

            // TO_DATE is native but format specifiers need uppercasing
            "TO_DATE" => {
                if f.args.len() >= 2 {
                    // Uppercase format string if present
                    let mut new_args = f.args.clone();
                    if let Expression::Literal(Literal::String(fmt)) = &f.args[1] {
                        new_args[1] = Expression::Literal(Literal::String(
                            Self::uppercase_exasol_format(fmt),
                        ));
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_DATE".to_string(),
                        new_args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // TIME_TO_STR -> TO_CHAR with format conversion
            "TIME_TO_STR" => {
                if f.args.len() >= 2 {
                    let mut new_args = vec![f.args[0].clone()];
                    if let Expression::Literal(Literal::String(fmt)) = &f.args[1] {
                        new_args.push(Expression::Literal(Literal::String(
                            Self::convert_strptime_to_exasol_format(fmt),
                        )));
                    } else {
                        new_args.push(f.args[1].clone());
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_CHAR".to_string(),
                        new_args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_CHAR".to_string(),
                        f.args,
                    ))))
                }
            }

            // STR_TO_TIME -> TO_DATE with format conversion
            "STR_TO_TIME" => {
                if f.args.len() >= 2 {
                    let mut new_args = vec![f.args[0].clone()];
                    if let Expression::Literal(Literal::String(fmt)) = &f.args[1] {
                        new_args.push(Expression::Literal(Literal::String(
                            Self::convert_strptime_to_exasol_format(fmt),
                        )));
                    } else {
                        new_args.push(f.args[1].clone());
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_DATE".to_string(),
                        new_args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_DATE".to_string(),
                        f.args,
                    ))))
                }
            }

            // TO_TIMESTAMP is native
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // CONVERT_TZ for timezone conversion
            "CONVERT_TIMEZONE" | "AT_TIME_ZONE" => Ok(Expression::Function(Box::new(
                Function::new("CONVERT_TZ".to_string(), f.args),
            ))),

            // STRPOS/POSITION → INSTR
            "STRPOS" | "POSITION" | "CHARINDEX" | "LOCATE" => Ok(Expression::Function(Box::new(
                Function::new("INSTR".to_string(), f.args),
            ))),

            // WEEK_OF_YEAR → WEEK
            "WEEK_OF_YEAR" | "WEEKOFYEAR" => Ok(Expression::Function(Box::new(Function::new(
                "WEEK".to_string(),
                f.args,
            )))),

            // LAST_DAY is not native, would need complex transformation
            "LAST_DAY" => {
                // Exasol doesn't have LAST_DAY, but we can compute it
                // For now, pass through
                Ok(Expression::Function(Box::new(f)))
            }

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
            // ALL → EVERY
            "ALL" | "EVERY" => Ok(Expression::Function(Box::new(Function::new(
                "EVERY".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT / STRING_AGG → LISTAGG (native with WITHIN GROUP)
            "GROUP_CONCAT" | "STRING_AGG" => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // LISTAGG is native
            "LISTAGG" => Ok(Expression::AggregateFunction(f)),

            // APPROX_DISTINCT → APPROXIMATE_COUNT_DISTINCT
            "APPROX_DISTINCT" | "APPROX_COUNT_DISTINCT" => Ok(Expression::Function(Box::new(
                Function::new("APPROXIMATE_COUNT_DISTINCT".to_string(), f.args),
            ))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    /// Convert strptime format string to Exasol format string
    /// Exasol TIME_MAPPING (reverse of Python sqlglot):
    /// %Y -> YYYY, %y -> YY, %m -> MM, %d -> DD, %H -> HH, %M -> MI, %S -> SS, %a -> DY
    fn convert_strptime_to_exasol_format(format: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = format.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '%' && i + 1 < chars.len() {
                let spec = chars[i + 1];
                let exasol_spec = match spec {
                    'Y' => "YYYY",
                    'y' => "YY",
                    'm' => "MM",
                    'd' => "DD",
                    'H' => "HH",
                    'M' => "MI",
                    'S' => "SS",
                    'a' => "DY",    // abbreviated weekday name
                    'A' => "DAY",   // full weekday name
                    'b' => "MON",   // abbreviated month name
                    'B' => "MONTH", // full month name
                    'I' => "H12",   // 12-hour format
                    'u' => "ID",    // ISO weekday (1-7)
                    'V' => "IW",    // ISO week number
                    'G' => "IYYY",  // ISO year
                    'W' => "UW",    // Week number (Monday as first day)
                    'U' => "UW",    // Week number (Sunday as first day)
                    'z' => "Z",     // timezone offset
                    _ => {
                        // Unknown specifier, keep as-is
                        result.push('%');
                        result.push(spec);
                        i += 2;
                        continue;
                    }
                };
                result.push_str(exasol_spec);
                i += 2;
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }

    /// Uppercase Exasol format specifiers (DD, MM, YYYY, etc.)
    /// Converts lowercase format strings like 'dd-mm-yyyy' to 'DD-MM-YYYY'
    fn uppercase_exasol_format(format: &str) -> String {
        // Exasol format specifiers are always uppercase
        format.to_uppercase()
    }
}

// Note: Exasol type mappings (handled in generator if needed):
// - BLOB, LONGBLOB, etc. → VARCHAR
// - TEXT → LONG VARCHAR
// - VARBINARY → VARCHAR
// - TINYINT → SMALLINT
// - MEDIUMINT → INT
// - DECIMAL32/64/128/256 → DECIMAL
// - DATETIME → TIMESTAMP
// - TIMESTAMPTZ/TIMESTAMPLTZ/TIMESTAMPNTZ → TIMESTAMP
//
// Exasol also supports:
// - TIMESTAMP WITH LOCAL TIME ZONE (fixed precision of 3)
// - IF...THEN...ELSE...ENDIF syntax for conditionals
