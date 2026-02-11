//! Oracle Dialect
//!
//! Oracle-specific transformations based on sqlglot patterns.
//! Key differences:
//! - NVL is native (preferred over COALESCE)
//! - SYSDATE for current timestamp
//! - DBMS_RANDOM.VALUE for random numbers
//! - No ILIKE support (use LOWER + LIKE)
//! - SUBSTR instead of SUBSTRING
//! - TO_CHAR, TO_DATE, TO_TIMESTAMP for date/time formatting
//! - No TRY_CAST (must use CAST)
//! - INSTR instead of POSITION/STRPOS
//! - TRUNC for date truncation
//! - MINUS instead of EXCEPT

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{BinaryFunc, CeilFunc, Expression, Function, LikeOp, UnaryFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Oracle dialect
pub struct OracleDialect;

impl DialectImpl for OracleDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Oracle
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Oracle uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Oracle does not support nested comments
        config.nested_comments = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Oracle),
            supports_column_join_marks: true,
            // Oracle doesn't use COLUMN keyword in ALTER TABLE ADD
            alter_table_include_column_keyword: false,
            // Oracle uses SAMPLE instead of TABLESAMPLE
            tablesample_keywords: "SAMPLE",
            // Oracle places alias after the SAMPLE clause
            alias_post_tablesample: true,
            // Oracle uses TIMESTAMP WITH TIME ZONE syntax (not TIMESTAMPTZ)
            tz_to_with_time_zone: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> NVL in Oracle
            Expression::IfNull(f) => Ok(Expression::Nvl(f)),

            // COALESCE with 2 args -> NVL in Oracle (optimization)
            Expression::Coalesce(f) if f.expressions.len() == 2 => {
                let mut exprs = f.expressions;
                let second = exprs.pop().unwrap();
                let first = exprs.pop().unwrap();
                Ok(Expression::Nvl(Box::new(BinaryFunc { original_name: None,
                    this: first,
                    expression: second,
                })))
            }

            // NVL stays as NVL (native to Oracle)
            Expression::Nvl(f) => Ok(Expression::Nvl(f)),

            // TryCast -> CAST in Oracle (no TRY_CAST support)
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST in Oracle
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // ILIKE -> LOWER() LIKE LOWER() in Oracle (no ILIKE support)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left)));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right)));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier,
                })))
            }

            // RANDOM -> DBMS_RANDOM.VALUE in Oracle
            Expression::Random(_) => Ok(Expression::Function(Box::new(Function::new(
                "DBMS_RANDOM.VALUE".to_string(),
                vec![],
            )))),

            // Rand -> DBMS_RANDOM.VALUE in Oracle
            Expression::Rand(_) => Ok(Expression::Function(Box::new(Function::new(
                "DBMS_RANDOM.VALUE".to_string(),
                vec![],
            )))),

            // || (Concat) is native to Oracle
            Expression::Concat(op) => Ok(Expression::Concat(op)),

            // UNNEST -> Not directly supported in Oracle
            // Would need TABLE() with a collection type
            Expression::Unnest(f) => Ok(Expression::Function(Box::new(Function::new(
                "TABLE".to_string(),
                vec![f.this],
            )))),

            // EXPLODE -> TABLE in Oracle
            Expression::Explode(f) => Ok(Expression::Function(Box::new(Function::new(
                "TABLE".to_string(),
                vec![f.this],
            )))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl OracleDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> NVL
            "IFNULL" if f.args.len() == 2 => {
                let mut args = f.args;
                let second = args.pop().unwrap();
                let first = args.pop().unwrap();
                Ok(Expression::Nvl(Box::new(BinaryFunc { original_name: None,
                    this: first,
                    expression: second,
                })))
            }

            // ISNULL -> NVL
            "ISNULL" if f.args.len() == 2 => {
                let mut args = f.args;
                let second = args.pop().unwrap();
                let first = args.pop().unwrap();
                Ok(Expression::Nvl(Box::new(BinaryFunc { original_name: None,
                    this: first,
                    expression: second,
                })))
            }

            // NVL is native to Oracle
            "NVL" if f.args.len() == 2 => {
                let mut args = f.args;
                let second = args.pop().unwrap();
                let first = args.pop().unwrap();
                Ok(Expression::Nvl(Box::new(BinaryFunc { original_name: None,
                    this: first,
                    expression: second,
                })))
            }

            // NVL2 is native to Oracle
            "NVL2" => Ok(Expression::Function(Box::new(f))),

            // GROUP_CONCAT -> LISTAGG in Oracle
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Oracle
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // LISTAGG is native to Oracle
            "LISTAGG" => Ok(Expression::Function(Box::new(f))),

            // SUBSTRING -> SUBSTR in Oracle
            "SUBSTRING" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTR".to_string(),
                f.args,
            )))),

            // SUBSTR is native to Oracle
            "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native to Oracle
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // RANDOM -> DBMS_RANDOM.VALUE
            "RANDOM" | "RAND" => Ok(Expression::Function(Box::new(Function::new(
                "DBMS_RANDOM.VALUE".to_string(),
                vec![],
            )))),

            // NOW -> SYSDATE or CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> SYSDATE
            "GETDATE" => Ok(Expression::Function(Box::new(Function::new(
                "SYSDATE".to_string(),
                vec![],
            )))),

            // CURRENT_TIMESTAMP is native (or SYSDATE)
            // If it has arguments, keep as function to preserve them
            "CURRENT_TIMESTAMP" => {
                if f.args.is_empty() {
                    Ok(Expression::CurrentTimestamp(
                        crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
                    ))
                } else if f.args.len() == 1 {
                    // Check if the argument is a numeric literal
                    if let Expression::Literal(crate::expressions::Literal::Number(n)) = &f.args[0] {
                        if let Ok(precision) = n.parse::<u32>() {
                            return Ok(Expression::CurrentTimestamp(
                                crate::expressions::CurrentTimestamp { precision: Some(precision), sysdate: false },
                            ));
                        }
                    }
                    // Non-numeric argument, keep as function
                    Ok(Expression::Function(Box::new(f)))
                } else {
                    // Multiple args, keep as function
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // CURRENT_DATE is native
            "CURRENT_DATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // TO_DATE is native to Oracle
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP is native to Oracle
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // TO_CHAR is native to Oracle
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT -> TO_CHAR in Oracle
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(Function::new(
                "TO_CHAR".to_string(),
                f.args,
            )))),

            // strftime -> TO_CHAR in Oracle
            "STRFTIME" => Ok(Expression::Function(Box::new(Function::new(
                "TO_CHAR".to_string(),
                f.args,
            )))),

            // DATE_TRUNC -> TRUNC in Oracle
            "DATE_TRUNC" => Ok(Expression::Function(Box::new(Function::new(
                "TRUNC".to_string(),
                f.args,
            )))),

            // TRUNC with single date argument: add default 'DD' unit
            // In Oracle, TRUNC(date) defaults to truncating to day ('DD')
            // Python sqlglot: "TRUNC": lambda args: exp.DateTrunc(unit=seq_get(args, 1) or exp.Literal.string("DD"), ...)
            "TRUNC" if f.args.len() == 1 => {
                let mut args = f.args;
                args.push(Expression::Literal(crate::expressions::Literal::String("DD".to_string())));
                Ok(Expression::Function(Box::new(Function::new(
                    "TRUNC".to_string(),
                    args,
                ))))
            }

            // EXTRACT is native to Oracle
            "EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // POSITION -> INSTR in Oracle
            // INSTR(string, substring) - reversed arg order from POSITION
            "POSITION" if f.args.len() == 2 => {
                let mut args = f.args;
                let first = args.remove(0);
                let second = args.remove(0);
                // Oracle INSTR has args in order: (string, substring)
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![second, first],
                ))))
            }

            // STRPOS -> INSTR
            "STRPOS" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "INSTR".to_string(),
                f.args,
            )))),

            // CHARINDEX -> INSTR
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // Oracle INSTR: (string, substring, [start_pos])
                let mut instr_args = vec![string, substring];
                if !args.is_empty() {
                    instr_args.push(args.remove(0));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    instr_args,
                ))))
            }

            // INSTR is native to Oracle
            "INSTR" => Ok(Expression::Function(Box::new(f))),

            // CEILING -> CEIL
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // CEIL is native to Oracle
            "CEIL" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // LOG -> LN for natural log (Oracle LOG is different)
            // In Oracle, LOG(base, n) but LN(n) for natural log
            "LOG" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LN".to_string(),
                f.args,
            )))),

            // LN is native to Oracle
            "LN" => Ok(Expression::Function(Box::new(f))),

            // POWER is native to Oracle
            "POWER" | "POW" => Ok(Expression::Function(Box::new(Function::new(
                "POWER".to_string(),
                f.args,
            )))),

            // REGEXP_LIKE is native to Oracle
            "REGEXP_LIKE" => Ok(Expression::Function(Box::new(f))),

            // JSON_VALUE is native to Oracle 12c+
            "JSON_VALUE" => Ok(Expression::Function(Box::new(f))),

            // JSON_QUERY is native to Oracle 12c+
            "JSON_QUERY" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT -> JSON_VALUE
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_SCALAR -> JSON_VALUE
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE".to_string(),
                f.args,
            )))),

            // SPLIT -> Not directly available in Oracle
            // Would need REGEXP_SUBSTR or custom function
            "SPLIT" => {
                // For basic cases, use REGEXP_SUBSTR pattern
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_SUBSTR".to_string(),
                    f.args,
                ))))
            }

            // ADD_MONTHS is native to Oracle
            "ADD_MONTHS" => Ok(Expression::Function(Box::new(f))),

            // MONTHS_BETWEEN is native to Oracle
            "MONTHS_BETWEEN" => Ok(Expression::Function(Box::new(f))),

            // DATEADD -> Use arithmetic with INTERVAL or specific functions
            "DATEADD" => {
                // Pass through for now - complex transformation needed
                Ok(Expression::Function(Box::new(f)))
            }

            // DATEDIFF -> Complex in Oracle, might need MONTHS_BETWEEN or arithmetic
            "DATEDIFF" => Ok(Expression::Function(Box::new(f))),

            // DECODE is native to Oracle
            "DECODE" => Ok(Expression::Function(Box::new(f))),

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
            // GROUP_CONCAT -> LISTAGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> Not directly supported in Oracle
            // Would need COLLECT (for nested tables)
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}
