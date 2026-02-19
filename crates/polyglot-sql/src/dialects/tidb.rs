//! TiDB Dialect
//!
//! TiDB-specific transformations based on sqlglot patterns.
//! TiDB is MySQL-compatible with distributed database extensions.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, Case, Cast, Expression, Function, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// TiDB dialect (MySQL-compatible distributed database)
pub struct TiDBDialect;

impl DialectImpl for TiDBDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::TiDB
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // TiDB uses backticks for identifiers (MySQL-style)
        config.identifiers.insert('`', '`');
        config.nested_comments = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::TiDB),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL is native in TiDB (MySQL-style)
            Expression::IfNull(f) => Ok(Expression::IfNull(f)),

            // NVL -> IFNULL in TiDB
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // TryCast -> not directly supported, use CAST
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST in TiDB
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // RAND is native in TiDB
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl TiDBDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // NVL -> IFNULL
            "NVL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // ISNULL -> IFNULL
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // COALESCE is native in TiDB
            "COALESCE" => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NOW is native in TiDB
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> NOW
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GROUP_CONCAT is native in TiDB
            "GROUP_CONCAT" => Ok(Expression::Function(Box::new(f))),

            // STRING_AGG -> GROUP_CONCAT
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // LISTAGG -> GROUP_CONCAT
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // SUBSTR is native in TiDB
            "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // SUBSTRING is native in TiDB
            "SUBSTRING" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native in TiDB
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LENGTH".to_string(),
                f.args,
            )))),

            // CHARINDEX -> INSTR (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // STRPOS -> INSTR
            "STRPOS" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "INSTR".to_string(),
                f.args,
            )))),

            // LOCATE is native in TiDB
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // INSTR is native in TiDB
            "INSTR" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT is native in TiDB
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR -> DATE_FORMAT
            "TO_CHAR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // STR_TO_DATE is native in TiDB
            "STR_TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_DATE -> STR_TO_DATE
            "TO_DATE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT is native in TiDB
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // GET_JSON_OBJECT -> JSON_EXTRACT
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT".to_string(), f.args),
            ))),

            // REGEXP is native in TiDB
            "REGEXP" => Ok(Expression::Function(Box::new(f))),

            // RLIKE is native in TiDB
            "RLIKE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_LIKE -> REGEXP
            "REGEXP_LIKE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP".to_string(),
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
            // COUNT_IF -> SUM(CASE WHEN...)
            "COUNT_IF" if !f.args.is_empty() => {
                let condition = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // APPROX_COUNT_DISTINCT is native in TiDB
            "APPROX_COUNT_DISTINCT" => Ok(Expression::AggregateFunction(f)),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // TiDB type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
