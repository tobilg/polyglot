//! Athena Dialect
//!
//! AWS Athena-specific transformations based on sqlglot patterns.
//! Athena routes between Hive (DDL) and Trino (DML) engines:
//!
//! - **Hive** (backticks): CREATE EXTERNAL TABLE, CREATE TABLE (no AS SELECT),
//!   ALTER, DROP (except VIEW), DESCRIBE, SHOW
//! - **Trino** (double quotes): CREATE VIEW, CREATE TABLE AS SELECT, DROP VIEW,
//!   SELECT, INSERT, UPDATE, DELETE, MERGE

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, Case, Cast, DataType, Expression, Function, LikeOp, UnaryFunc, VarArgFunc,
};
use crate::generator::{GeneratorConfig, IdentifierQuoteStyle};
use crate::tokens::TokenizerConfig;

/// Athena dialect (based on Trino for DML operations)
pub struct AthenaDialect;

impl DialectImpl for AthenaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Athena
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Athena uses double quotes for identifiers (Trino-style for DML)
        config.identifiers.insert('"', '"');
        // Also supports backticks (Hive-style for DDL)
        config.identifiers.insert('`', '`');
        config.nested_comments = false;
        // Athena/Hive supports backslash escapes in string literals (e.g., \' for escaped quote)
        config.string_escapes.push('\\');
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        // Default config uses Trino style (double quotes)
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Athena),
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    fn generator_config_for_expr(&self, expr: &Expression) -> GeneratorConfig {
        if should_use_hive_engine(expr) {
            // Hive mode: backticks for identifiers
            GeneratorConfig {
                identifier_quote: '`',
                identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
                dialect: Some(DialectType::Athena),
                schema_comment_with_eq: false,
                ..Default::default()
            }
        } else {
            // Trino mode: double quotes for identifiers
            GeneratorConfig {
                identifier_quote: '"',
                identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
                dialect: Some(DialectType::Athena),
                schema_comment_with_eq: false,
                ..Default::default()
            }
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Athena
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in Athena
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // TryCast stays as TryCast (Athena/Trino supports TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in Athena
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILike -> LOWER() LIKE LOWER() (Trino doesn't support ILIKE)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left.clone())));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right.clone())));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier.clone(),
                })))
            }

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

            // EXPLODE -> UNNEST in Athena
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ExplodeOuter -> UNNEST in Athena
            Expression::ExplodeOuter(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

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

impl AthenaDialect {
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

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // RAND -> RANDOM in Athena
            "RAND" => Ok(Expression::Function(Box::new(Function::new(
                "RANDOM".to_string(),
                vec![],
            )))),

            // GROUP_CONCAT -> LISTAGG in Athena (Trino-style)
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Athena
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CHARINDEX -> STRPOS in Athena (with swapped args)
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

            // LOCATE -> STRPOS in Athena (with swapped args)
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // ARRAY_LENGTH -> CARDINALITY in Athena
            "ARRAY_LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("CARDINALITY".to_string(), f.args),
            ))),

            // SIZE -> CARDINALITY in Athena
            "SIZE" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CARDINALITY".to_string(),
                f.args,
            )))),

            // TO_DATE -> CAST to DATE or DATE_PARSE
            "TO_DATE" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Date,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // TO_TIMESTAMP -> CAST or DATE_PARSE
            "TO_TIMESTAMP" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Timestamp {
                            precision: None,
                            timezone: false,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // strftime -> DATE_FORMAT in Athena
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR -> DATE_FORMAT in Athena
            "TO_CHAR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // GET_JSON_OBJECT -> JSON_EXTRACT_SCALAR in Athena
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_SCALAR".to_string(), f.args),
            ))),

            // COLLECT_LIST -> ARRAY_AGG
            "COLLECT_LIST" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("ARRAY_AGG".to_string(), f.args),
            ))),

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

            // ANY_VALUE -> ARBITRARY in Athena (Trino)
            "ANY_VALUE" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "ARBITRARY".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT -> LISTAGG in Athena
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Athena
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Athena type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}

/// Determine if an expression should be generated using Hive engine (backticks)
/// or Trino engine (double quotes).
///
/// Hive is used for:
/// - CREATE EXTERNAL TABLE
/// - CREATE TABLE (without AS SELECT)
/// - CREATE SCHEMA / CREATE DATABASE
/// - ALTER statements
/// - DROP statements (except DROP VIEW)
/// - DESCRIBE / SHOW statements
///
/// Trino is used for everything else (DML, CREATE VIEW, etc.)
fn should_use_hive_engine(expr: &Expression) -> bool {
    match expr {
        // CREATE TABLE: Hive if EXTERNAL or no AS SELECT
        Expression::CreateTable(ct) => {
            // CREATE EXTERNAL TABLE → Hive
            if let Some(ref modifier) = ct.table_modifier {
                if modifier.to_uppercase() == "EXTERNAL" {
                    return true;
                }
            }
            // CREATE TABLE ... AS SELECT → Trino
            // CREATE TABLE (without query) → Hive
            ct.as_select.is_none()
        }

        // CREATE VIEW → Trino
        Expression::CreateView(_) => false,

        // CREATE SCHEMA / DATABASE → Hive
        Expression::CreateSchema(_) => true,
        Expression::CreateDatabase(_) => true,

        // ALTER statements → Hive
        Expression::AlterTable(_) => true,
        Expression::AlterView(_) => true,
        Expression::AlterIndex(_) => true,
        Expression::AlterSequence(_) => true,

        // DROP VIEW → Trino (because CREATE VIEW is Trino)
        Expression::DropView(_) => false,

        // Other DROP statements → Hive
        Expression::DropTable(_) => true,
        Expression::DropSchema(_) => true,
        Expression::DropDatabase(_) => true,
        Expression::DropIndex(_) => true,
        Expression::DropFunction(_) => true,
        Expression::DropProcedure(_) => true,
        Expression::DropSequence(_) => true,

        // DESCRIBE / SHOW → Hive
        Expression::Describe(_) => true,
        Expression::Show(_) => true,

        // Everything else (SELECT, INSERT, UPDATE, DELETE, MERGE, etc.) → Trino
        _ => false,
    }
}
