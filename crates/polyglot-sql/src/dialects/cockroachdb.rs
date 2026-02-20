//! CockroachDB Dialect
//!
//! CockroachDB-specific transformations based on sqlglot patterns.
//! CockroachDB is PostgreSQL-compatible with distributed database extensions.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, Case, Cast, Expression, Function, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// CockroachDB dialect (PostgreSQL-compatible distributed database)
pub struct CockroachDBDialect;

impl DialectImpl for CockroachDBDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::CockroachDB
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // CockroachDB uses double quotes for identifiers (PostgreSQL-style)
        config.identifiers.insert('"', '"');
        // PostgreSQL-style nested comments supported
        config.nested_comments = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::CockroachDB),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in CockroachDB
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in CockroachDB
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // TryCast is not directly supported, use CAST
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST in CockroachDB
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // ILIKE is native in CockroachDB (PostgreSQL-style)
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // RAND -> RANDOM in CockroachDB (PostgreSQL-style)
            Expression::Rand(r) => {
                let _ = r.seed;
                Ok(Expression::Random(crate::expressions::Random))
            }

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

impl CockroachDBDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NOW is native in CockroachDB
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // GETDATE -> NOW
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // RAND -> RANDOM
            "RAND" => Ok(Expression::Random(crate::expressions::Random)),

            // STRING_AGG is native in CockroachDB (PostgreSQL-style)
            "STRING_AGG" => Ok(Expression::Function(Box::new(f))),

            // GROUP_CONCAT -> STRING_AGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // LISTAGG -> STRING_AGG
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "STRING_AGG".to_string(),
                f.args,
            )))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LENGTH is native in CockroachDB
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LENGTH".to_string(),
                f.args,
            )))),

            // CHARINDEX -> STRPOS (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // INSTR -> STRPOS
            "INSTR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "STRPOS".to_string(),
                f.args,
            )))),

            // LOCATE -> STRPOS (with swapped args)
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // STRPOS is native in CockroachDB
            "STRPOS" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_LENGTH is native in CockroachDB
            "ARRAY_LENGTH" => Ok(Expression::Function(Box::new(f))),

            // SIZE -> ARRAY_LENGTH
            "SIZE" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_LENGTH".to_string(),
                f.args,
            )))),

            // CARDINALITY is native in CockroachDB
            "CARDINALITY" => Ok(Expression::Function(Box::new(f))),

            // TO_CHAR is native in CockroachDB
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT -> TO_CHAR
            "DATE_FORMAT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("TO_CHAR".to_string(), f.args),
            ))),

            // strftime -> TO_CHAR
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_CHAR".to_string(),
                    vec![date, format],
                ))))
            }

            // JSONB_EXTRACT_PATH_TEXT is native in CockroachDB
            "JSONB_EXTRACT_PATH_TEXT" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT_PATH_TEXT is native in CockroachDB
            "JSON_EXTRACT_PATH_TEXT" => Ok(Expression::Function(Box::new(f))),

            // GET_JSON_OBJECT -> JSONB_EXTRACT_PATH_TEXT
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSONB_EXTRACT_PATH_TEXT".to_string(), f.args),
            ))),

            // JSON_EXTRACT -> JSONB_EXTRACT_PATH
            "JSON_EXTRACT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("JSONB_EXTRACT_PATH".to_string(), f.args),
            ))),

            // REGEXP_LIKE is native in CockroachDB
            "REGEXP_LIKE" => Ok(Expression::Function(Box::new(f))),

            // RLIKE -> REGEXP_LIKE
            "RLIKE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
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
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // CockroachDB type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
