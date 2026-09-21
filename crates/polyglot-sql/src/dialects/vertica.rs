//! Vertica Dialect
//!
//! Vertica (OpenText Analytics Database) is a columnar MPP analytic database whose
//! SQL surface is largely PostgreSQL-compatible.
//! Reference: https://docs.vertica.com/latest/en/sql-reference/
//! Semantics cross-checked against https://github.com/luisdelatorre012/vertica-sqlglot-dialect
//!
//! Key characteristics:
//! - Double-quote identifiers, case-insensitive (folded to lowercase)
//! - `::` casts, ILIKE, `||` concatenation, LIMIT/OFFSET
//! - MINUS is an alias for EXCEPT
//! - All integer types are 64-bit (INT, SMALLINT, TINYINT are BIGINT)
//! - REAL / FLOAT are DOUBLE PRECISION
//! - LONG VARCHAR / LONG VARBINARY types
//! - NVL, NVL2, DECODE, ZEROIFNULL, LISTAGG, DATEDIFF(unit, a, b), TIMESTAMPADD
//! - GETDATE() / SYSDATE are statement-start timestamps
//! - No QUALIFY, TRY_CAST or semi/anti join syntax
//! - No nested comments

use super::{DialectImpl, DialectType};
#[cfg(feature = "transpile")]
use crate::error::Result;
#[cfg(feature = "transpile")]
use crate::expressions::{
    AggFunc, Case, Expression, Function, Identifier, IntervalUnit, ListAggFunc, Literal, VarArgFunc,
};
#[cfg(feature = "generate")]
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Vertica dialect
pub struct VerticaDialect;

impl DialectImpl for VerticaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Vertica
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        use crate::tokens::TokenType;
        let mut config = TokenizerConfig::default();
        // Vertica uses double quotes for identifiers (PostgreSQL-style)
        config.identifiers.insert('"', '"');
        // Vertica does NOT support nested comments
        config.nested_comments = false;
        // `//` is integer division
        config.double_slash_int_div = true;
        // MINUS is an alias for EXCEPT in Vertica
        config
            .keywords
            .insert("MINUS".to_string(), TokenType::Except);
        config
    }

    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, LimitFetchStyle};
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Vertica),
            single_string_interval: true,
            locking_reads_supported: false,
            limit_fetch_style: LimitFetchStyle::Limit,
            nvl2_supported: true,
            supports_median: true,
            ..Default::default()
        }
    }

    #[cfg(feature = "transpile")]
    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Vertica
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
                inferred_type: None,
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // Vertica has no TRY_CAST; fall back to CAST
            Expression::TryCast(c) => Ok(Expression::Cast(c)),
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                    inferred_type: None,
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
                    inferred_type: None,
                })))
            }

            // RAND -> RANDOM in Vertica
            Expression::Rand(r) => {
                let _ = r.seed;
                Ok(Expression::Random(crate::expressions::Random))
            }

            // DAYOFWEEK_ISO has no shared generator form
            Expression::DayOfWeekIso(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEK_ISO".to_string(),
                vec![f.this],
            )))),

            // DATE_ADD / DATEADD -> TIMESTAMPADD(unit, n, ts); Vertica has no DATEADD
            Expression::DateAdd(f) => Ok(timestamp_add(
                interval_unit_name(&f.unit),
                f.interval,
                f.this,
            )),

            // APPROX_COUNT_DISTINCT -> APPROXIMATE_COUNT_DISTINCT
            Expression::ApproxDistinct(f) | Expression::ApproxCountDistinct(f) => {
                Ok(Expression::Function(Box::new(Function::new(
                    "APPROXIMATE_COUNT_DISTINCT".to_string(),
                    vec![f.this],
                ))))
            }

            // GROUP_CONCAT / STRING_AGG -> LISTAGG
            Expression::GroupConcat(f) => Ok(Expression::ListAgg(Box::new(ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                max_length: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                inferred_type: None,
            }))),
            Expression::StringAgg(f) => Ok(Expression::ListAgg(Box::new(ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                max_length: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                inferred_type: None,
            }))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

#[cfg(feature = "transpile")]
impl VerticaDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL / ISNULL -> COALESCE
            "IFNULL" | "ISNULL" if f.args.len() == 2 => {
                Ok(Expression::Coalesce(Box::new(VarArgFunc {
                    original_name: None,
                    expressions: f.args,
                    inferred_type: None,
                })))
            }

            // SYSDATE is a synonym for GETDATE (statement-start timestamp)
            "SYSDATE" if f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

            // TIMESTAMPDIFF(unit, a, b) is a synonym for DATEDIFF(unit, a, b)
            "TIMESTAMPDIFF" if f.args.len() == 3 => {
                let mut args = f.args;
                upper_unit(&mut args[0]);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    args,
                ))))
            }

            // APPROX_COUNT_DISTINCT -> APPROXIMATE_COUNT_DISTINCT
            "APPROX_COUNT_DISTINCT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("APPROXIMATE_COUNT_DISTINCT".to_string(), f.args),
            ))),

            // DATEADD(unit, n, ts) -> TIMESTAMPADD(unit, n, ts)
            "DATEADD" | "DATE_ADD" if f.args.len() == 3 => {
                let mut args = f.args;
                let ts = args.pop().unwrap();
                let n = args.pop().unwrap();
                let unit = args.pop().unwrap();
                let unit = match unit {
                    Expression::Literal(lit) => match *lit {
                        Literal::String(s) => {
                            Expression::Identifier(Identifier::new(s.to_ascii_uppercase()))
                        }
                        other => Expression::Literal(Box::new(other)),
                    },
                    other => other,
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMPADD".to_string(),
                    vec![unit, n, ts],
                ))))
            }

            // TIMESTAMPADD(unit, n, ts): normalize the unit keyword to upper case
            "TIMESTAMPADD" if f.args.len() == 3 => {
                let mut f = f;
                upper_unit(&mut f.args[0]);
                Ok(Expression::Function(Box::new(f)))
            }

            // CHARINDEX(substr, str[, start]) -> INSTR(str, substr[, start])
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substr = args.remove(0);
                let string = args.remove(0);
                let mut new_args = vec![string, substr];
                new_args.extend(args);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    new_args,
                ))))
            }

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LENGTH".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }
}

#[cfg(feature = "transpile")]
fn interval_unit_name(unit: &IntervalUnit) -> &'static str {
    match unit {
        IntervalUnit::Year => "YEAR",
        IntervalUnit::Quarter => "QUARTER",
        IntervalUnit::Month => "MONTH",
        IntervalUnit::Week => "WEEK",
        IntervalUnit::Day => "DAY",
        IntervalUnit::Hour => "HOUR",
        IntervalUnit::Minute => "MINUTE",
        IntervalUnit::Second => "SECOND",
        IntervalUnit::Millisecond => "MILLISECOND",
        IntervalUnit::Microsecond => "MICROSECOND",
        IntervalUnit::Nanosecond => "NANOSECOND",
    }
}

#[cfg(feature = "transpile")]
fn timestamp_add(unit: &str, amount: Expression, ts: Expression) -> Expression {
    Expression::Function(Box::new(Function::new(
        "TIMESTAMPADD".to_string(),
        vec![Expression::Identifier(Identifier::new(unit)), amount, ts],
    )))
}

/// Upper-case a bare datetime unit keyword (`day` -> `DAY`), leaving expressions alone.
#[cfg(feature = "transpile")]
fn upper_unit(unit: &mut Expression) {
    match unit {
        Expression::Identifier(id) if !id.quoted => id.name = id.name.to_ascii_uppercase(),
        Expression::Column(col) if col.table.is_none() && !col.name.quoted => {
            *unit = Expression::Identifier(Identifier::new(col.name.name.to_ascii_uppercase()));
        }
        _ => {}
    }
}
