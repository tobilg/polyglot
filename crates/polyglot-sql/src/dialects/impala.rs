//! Apache Impala dialect.
//!
//! Standalone `DialectImpl`, like Spark: Hive's shared lexical settings, without
//! Hive's numeric suffixes or Hive expression rewrites. Impala-only spellings
//! (`NDV`, `ILIKE`, `RLIKE`, `DAYS_ADD`) stay on this dialect.

use super::{DialectImpl, DialectType};
#[cfg(feature = "transpile")]
use crate::error::Result;
#[cfg(feature = "transpile")]
use crate::expressions::{
    AggFunc, BinaryOp, Expression, Function, Interval, IntervalUnit, IntervalUnitSpec, Literal,
    Nullif, Paren, VarArgFunc,
};
#[cfg(feature = "generate")]
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Apache Impala dialect.
pub struct ImpalaDialect;

impl DialectImpl for ImpalaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Impala
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Impala shares Hive's identifier and string rules, and DIV.
        // Numeric suffixes are intentionally absent: `SELECT 1 d` is a literal
        // with alias `d`, not a DOUBLE.
        config.identifiers.clear();
        config.identifiers.insert('`', '`');
        config.quotes.insert("\"".to_string(), "\"".to_string());
        config.string_escapes.push('\\');
        config
            .keywords
            .insert("DIV".to_string(), crate::tokens::TokenType::Div);
        config
            .keywords
            .insert("REPAIR".to_string(), crate::tokens::TokenType::Command);
        config
            .keywords
            .insert("MSCK".to_string(), crate::tokens::TokenType::Command);
        // Case-insensitive regex operator. Not a column-name keyword the plan
        // forbids (UPSERT, COMPUTE, SHUFFLE, KUDU, CACHED stay identifiers).
        config
            .keywords
            .insert("IREGEXP".to_string(), crate::tokens::TokenType::IRLike);
        config.identifiers_can_start_with_digit = true;
        config
    }

    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Impala),
            struct_field_sep: ": ",
            alias_post_tablesample: true,
            join_hints: false,
            identifiers_can_start_with_digit: true,
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    #[cfg(feature = "transpile")]
    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            Expression::Function(f) => transform_function(*f),
            other => Ok(other),
        }
    }
}

#[cfg(feature = "transpile")]
fn transform_function(f: Function) -> Result<Expression> {
    let name = f.name.to_ascii_uppercase();
    match name.as_str() {
        // NDV is Impala's spelling of approximate count distinct. One argument
        // maps onto the shared aggregate; a scale argument stays a function call.
        "NDV" if f.args.len() == 1 => {
            let mut args = f.args.into_iter();
            let this = args.next().unwrap();
            Ok(Expression::ApproxDistinct(Box::new(AggFunc {
                this,
                distinct: false,
                filter: None,
                order_by: Vec::new(),
                name: Some("NDV".to_string()),
                ignore_nulls: None,
                having_max: None,
                limit: None,
                inferred_type: None,
            })))
        }
        "ZEROIFNULL" if f.args.len() == 1 => {
            let mut args = f.args.into_iter();
            let this = args.next().unwrap();
            Ok(coalesce(this, number("0")))
        }
        "NULLIFZERO" if f.args.len() == 1 => {
            let mut args = f.args.into_iter();
            let this = args.next().unwrap();
            Ok(nullif(this, number("0")))
        }
        // Two-argument ISNULL is NVL. A single argument stays a predicate.
        "ISNULL" if f.args.len() == 2 => {
            let mut args = f.args.into_iter();
            let this = args.next().unwrap();
            let fallback = args.next().unwrap();
            Ok(coalesce(this, fallback))
        }
        "DATE_SUB" if f.args.len() == 2 => {
            let mut args = f.args.into_iter();
            let left = args.next().unwrap();
            let right = args.next().unwrap();
            // Interval forms stay subtraction. A day count becomes DATE_ADD of
            // a negative product, matching Impala's numeric DATE_SUB spelling.
            if contains_interval(&right) {
                let right = parenthesize_additive(right);
                Ok(Expression::Sub(Box::new(BinaryOp::new(left, right))))
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD",
                    vec![left, negated(right)],
                ))))
            }
        }
        _ => Ok(Expression::Function(Box::new(f))),
    }
}

#[cfg(feature = "transpile")]
fn coalesce(left: Expression, right: Expression) -> Expression {
    Expression::Coalesce(Box::new(VarArgFunc {
        original_name: None,
        expressions: vec![left, right],
        inferred_type: None,
    }))
}

#[cfg(feature = "transpile")]
fn nullif(left: Expression, right: Expression) -> Expression {
    Expression::Nullif(Box::new(Nullif {
        this: Box::new(left),
        expression: Box::new(right),
    }))
}

#[cfg(feature = "transpile")]
fn number(text: &str) -> Expression {
    Expression::Literal(Box::new(Literal::Number(text.to_string())))
}

#[cfg(feature = "transpile")]
fn negated(expr: Expression) -> Expression {
    Expression::Mul(Box::new(BinaryOp::new(expr, number("-1"))))
}

#[cfg(feature = "transpile")]
fn parenthesize_additive(expr: Expression) -> Expression {
    if matches!(expr, Expression::Add(_) | Expression::Sub(_)) {
        Expression::Paren(Box::new(Paren {
            this: expr,
            trailing_comments: Vec::new(),
        }))
    } else {
        expr
    }
}

#[cfg(feature = "transpile")]
fn contains_interval(expr: &Expression) -> bool {
    match expr {
        Expression::Interval(_) => true,
        Expression::Paren(inner) => contains_interval(&inner.this),
        Expression::Neg(inner) => contains_interval(&inner.this),
        Expression::Add(op) | Expression::Sub(op) | Expression::Mul(op) => {
            contains_interval(&op.left) || contains_interval(&op.right)
        }
        _ => false,
    }
}

/// Rewrites Impala function names for another dialect.
///
/// Identity generation does not call this. `NDV` is already an
/// `ApproxDistinct` by the time normalization runs, so Spark and Hive print
/// `APPROX_COUNT_DISTINCT` from the shared generator.
#[cfg(feature = "transpile")]
pub(crate) fn rewrite_outgoing(expr: Expression, target: DialectType) -> Expression {
    let Expression::Function(f) = expr else {
        return expr;
    };
    let spark = matches!(target, DialectType::Spark | DialectType::Databricks);
    if !spark {
        return Expression::Function(f);
    }
    let name = f.name.to_ascii_uppercase();
    match name.as_str() {
        "APPX_MEDIAN" if f.args.len() == 1 => {
            let mut args = f.args.into_iter();
            let this = args.next().unwrap();
            Expression::Function(Box::new(Function::new(
                "PERCENTILE_APPROX",
                vec![this, number("0.5")],
            )))
        }
        "MONTHS_ADD" if f.args.len() == 2 => {
            Expression::Function(Box::new(Function::new("ADD_MONTHS", f.args)))
        }
        "MONTHS_SUB" if f.args.len() == 2 => {
            let mut args = f.args.into_iter();
            let left = args.next().unwrap();
            let right = args.next().unwrap();
            Expression::Function(Box::new(Function::new(
                "ADD_MONTHS",
                vec![left, negated(right)],
            )))
        }
        "DAYS_ADD" if f.args.len() == 2 => {
            Expression::Function(Box::new(Function::new("DATE_ADD", f.args)))
        }
        "DAYS_SUB" if f.args.len() == 2 => {
            let mut args = f.args.into_iter();
            let left = args.next().unwrap();
            let right = args.next().unwrap();
            Expression::Function(Box::new(Function::new(
                "DATE_ADD",
                vec![left, negated(right)],
            )))
        }
        "YEARS_ADD" => interval_add(f.args, IntervalUnit::Year),
        "YEARS_SUB" => interval_sub(f.args, IntervalUnit::Year),
        "WEEKS_ADD" => interval_add(f.args, IntervalUnit::Week),
        "WEEKS_SUB" => interval_sub(f.args, IntervalUnit::Week),
        "HOURS_ADD" => interval_add(f.args, IntervalUnit::Hour),
        "HOURS_SUB" => interval_sub(f.args, IntervalUnit::Hour),
        "MINUTES_ADD" => interval_add(f.args, IntervalUnit::Minute),
        "MINUTES_SUB" => interval_sub(f.args, IntervalUnit::Minute),
        "SECONDS_ADD" => interval_add(f.args, IntervalUnit::Second),
        "SECONDS_SUB" => interval_sub(f.args, IntervalUnit::Second),
        _ => Expression::Function(f),
    }
}

#[cfg(feature = "transpile")]
fn interval_add(args: Vec<Expression>, unit: IntervalUnit) -> Expression {
    if args.len() != 2 {
        return Expression::Function(Box::new(Function::new("INTERVAL_ADD", args)));
    }
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let amount = args.next().unwrap();
    Expression::Add(Box::new(BinaryOp::new(left, interval(amount, unit))))
}

#[cfg(feature = "transpile")]
fn interval_sub(args: Vec<Expression>, unit: IntervalUnit) -> Expression {
    if args.len() != 2 {
        return Expression::Function(Box::new(Function::new("INTERVAL_SUB", args)));
    }
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let amount = args.next().unwrap();
    Expression::Sub(Box::new(BinaryOp::new(left, interval(amount, unit))))
}

#[cfg(feature = "transpile")]
fn interval(amount: Expression, unit: IntervalUnit) -> Expression {
    Expression::Interval(Box::new(Interval {
        this: Some(amount),
        unit: Some(IntervalUnitSpec::Simple {
            unit,
            use_plural: false,
        }),
    }))
}
