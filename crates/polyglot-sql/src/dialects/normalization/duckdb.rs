//! Preserve DuckDB source semantics before target rewrites.

use super::scalar::{expression_numeric_kind, NumericKind};
use super::*;
use crate::expressions::{Annotated, DateTimeField, DateTruncFunc, ParameterStyle, RoundFunc};

fn bigquery_conversion_error(feature: impl Into<String>) -> crate::error::Error {
    crate::error::Error::unsupported(feature, "BigQuery")
}

/// Resolve source semantics before function renames or target casts erase them.
pub(in crate::dialects) fn prepare_bigquery_conversion(
    expr: Expression,
    source: DialectType,
    target: DialectType,
    strict: bool,
) -> Result<Expression> {
    if source != DialectType::DuckDB || target != DialectType::BigQuery {
        return Ok(expr);
    }

    use crate::traversal::ExpressionWalk;
    let mut anonymous = false;
    let mut numbered = false;
    let mut named = false;
    for node in expr.dfs() {
        match node {
            Expression::Placeholder(_) => anonymous = true,
            Expression::Parameter(p) => match p.style {
                ParameterStyle::Question => anonymous = true,
                ParameterStyle::Dollar | ParameterStyle::DoubleDollar => {
                    numbered |= p.index.is_some();
                    named |= p.name.is_some();
                }
                _ => {}
            },
            _ => {}
        }
    }
    if anonymous && (numbered || named) {
        return Err(bigquery_conversion_error(
            "DuckDB queries mixing ? and $ parameters; use only numbered $ parameters or only ? placeholders",
        ));
    }
    if numbered && named {
        return Err(bigquery_conversion_error(
            "DuckDB queries mixing numbered and named $ parameters",
        ));
    }

    crate::traversal::transform_all(expr, &|expr| {
        let (trunc, comments) = match expr {
            Expression::Function(mut f)
                if f.qualified_name.is_empty()
                    && f.source_dialect.is_none()
                    && (f.name.eq_ignore_ascii_case("DATE_TRUNC")
                        || f.name.eq_ignore_ascii_case("DATETRUNC")) =>
            {
                if f.args.len() != 2 {
                    return Err(bigquery_conversion_error(
                        "DuckDB DATE_TRUNC requires exactly two arguments",
                    ));
                }
                let unit = duckdb_trunc_unit(&f.args[0])?;
                let this = f.args.pop().unwrap();
                (DateTruncFunc { this, unit }, f.trailing_comments)
            }
            Expression::DateTrunc(f) | Expression::TimestampTrunc(f) => (*f, Vec::new()),
            other => return Ok(other),
        };
        let result = normalize_bigquery_trunc(trunc, strict)?;
        Ok(if comments.is_empty() {
            result
        } else {
            Expression::Annotated(Box::new(Annotated {
                this: result,
                trailing_comments: comments,
            }))
        })
    })
}

fn duckdb_trunc_unit(expr: &Expression) -> Result<DateTimeField> {
    match expr {
        Expression::Literal(literal) => {
            if let Literal::String(unit) = literal.as_ref() {
                return duckdb_trunc_unit_name(unit);
            }
        }
        Expression::Paren(p) => return duckdb_trunc_unit(&p.this),
        Expression::Annotated(a) => return duckdb_trunc_unit(&a.this),
        _ => {}
    }
    Err(bigquery_conversion_error(
        "DuckDB DATE_TRUNC with a dynamic granularity; BigQuery requires a constant date part",
    ))
}

fn duckdb_trunc_unit_name(unit: &str) -> Result<DateTimeField> {
    use DateTimeField::*;
    Ok(match unit.to_ascii_lowercase().as_str() {
        "year" | "years" | "yr" | "yrs" | "y" => Year,
        "isoyear" => Custom("ISOYEAR".into()),
        "quarter" | "quarters" => Quarter,
        "month" | "months" | "mon" | "mons" => Month,
        "week" | "weeks" | "w" => WeekWithModifier("MONDAY".into()),
        "day" | "days" | "d" | "dayofmonth" => Day,
        "hour" | "hours" | "hr" | "hrs" | "h" => Hour,
        "minute" | "minutes" | "min" | "mins" | "m" => Minute,
        "second" | "seconds" | "sec" | "secs" | "s" => Second,
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" | "msecond" | "mseconds" => {
            Millisecond
        }
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" | "usecond" | "useconds" => {
            Microsecond
        }
        _ => {
            return Err(bigquery_conversion_error(format!(
                "DuckDB DATE_TRUNC granularity {unit:?}"
            )))
        }
    })
}

fn trunc_input_type(expr: &Expression) -> Option<DataType> {
    match expr {
        Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
            Some(c.to.clone())
        }
        Expression::Paren(p) => trunc_input_type(&p.this),
        Expression::Annotated(a) => trunc_input_type(&a.this),
        Expression::Interval(_) => Some(DataType::Interval {
            unit: None,
            to: None,
        }),
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::Date(_) => Some(DataType::Date),
            Literal::Timestamp(_) | Literal::Datetime(_) => Some(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),
            _ => None,
        },
        Expression::CurrentDate(_) => Some(DataType::Date),
        Expression::Function(f) if f.qualified_name.is_empty() && f.args.is_empty() => {
            match f.name.to_ascii_uppercase().as_str() {
                "CURRENT_DATE" | "TODAY" => Some(DataType::Date),
                "CURRENT_TIMESTAMP" | "NOW" | "GET_CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" => {
                    Some(DataType::Timestamp {
                        precision: None,
                        timezone: true,
                    })
                }
                _ => f.inferred_type.clone(),
            }
        }
        Expression::CurrentTimestamp(_) => Some(DataType::Timestamp {
            precision: None,
            timezone: true,
        }),
        Expression::Localtimestamp(_) => Some(DataType::Timestamp {
            precision: None,
            timezone: false,
        }),
        Expression::DateTrunc(f) | Expression::TimestampTrunc(f) => trunc_input_type(&f.this),
        other => other.inferred_type().cloned(),
    }
}

fn normalize_bigquery_trunc(mut trunc: DateTruncFunc, strict: bool) -> Result<Expression> {
    trunc.unit = match trunc.unit {
        DateTimeField::Week => DateTimeField::WeekWithModifier("MONDAY".into()),
        DateTimeField::Custom(unit) => duckdb_trunc_unit_name(&unit)?,
        unit @ (DateTimeField::Year
        | DateTimeField::Quarter
        | DateTimeField::Month
        | DateTimeField::WeekWithModifier(_)
        | DateTimeField::Day
        | DateTimeField::Hour
        | DateTimeField::Minute
        | DateTimeField::Second
        | DateTimeField::Millisecond
        | DateTimeField::Microsecond) => unit,
        unit => {
            return Err(bigquery_conversion_error(format!(
                "DuckDB DATE_TRUNC granularity {unit:?}"
            )))
        }
    };
    match trunc_input_type(&trunc.this) {
        Some(DataType::Date) => {
            // DuckDB returns a timestamp even for DATE inputs. Its timestamp
            // without timezone maps to BigQuery DATETIME, including sub-day units.
            trunc.this = super::operators::cast_expr(
                trunc.this,
                DataType::Timestamp {
                    precision: None,
                    timezone: false,
                },
            );
        }
        Some(DataType::Timestamp { timezone: true, .. }) if strict => {
            return Err(bigquery_conversion_error(
                "DuckDB DATE_TRUNC on TIMESTAMPTZ depends on the session TimeZone",
            ));
        }
        Some(DataType::Timestamp { .. }) => {}
        Some(_) => return Err(bigquery_conversion_error("DuckDB DATE_TRUNC operand type")),
        None if strict => {
            return Err(bigquery_conversion_error(
                "DuckDB DATE_TRUNC with an unresolved operand type; use an explicit DATE or TIMESTAMP cast",
            ));
        }
        None => {}
    }
    Ok(Expression::DateTrunc(Box::new(trunc)))
}

fn combine(left: NumericKind, right: NumericKind) -> NumericKind {
    use NumericKind::*;
    match (left, right) {
        (Unknown, _) | (_, Unknown) => Unknown,
        (Float, _) | (_, Float) => Float,
        (Decimal, _) | (_, Decimal) => Decimal,
        (Integer, Integer) => Integer,
    }
}

fn numeric_kind(expr: &Expression) -> NumericKind {
    match expr {
        Expression::Paren(p) => numeric_kind(&p.this),
        Expression::Neg(n) => numeric_kind(&n.this),
        Expression::Alias(a) => numeric_kind(&a.this),
        Expression::Add(b) | Expression::Sub(b) | Expression::Mul(b) | Expression::Mod(b) => {
            combine(numeric_kind(&b.left), numeric_kind(&b.right))
        }
        // The complete bottom-up source pass has already lowered fractional
        // children to Div. Remaining IntDiv children have integer operands.
        Expression::IntDiv(_) => NumericKind::Integer,
        Expression::Div(_) => NumericKind::Float,
        Expression::Function(f) if f.name.eq_ignore_ascii_case("NULLIF") && f.args.len() == 2 => {
            numeric_kind(&f.args[0])
        }
        Expression::NullIf(f) => numeric_kind(&f.this),
        _ => expression_numeric_kind(expr),
    }
}

/// Classify the rewritten DataFusion expression. Source `/` nodes have already
/// been made floating, and lowered `//` nodes retain their operands' types.
/// Never infer provenance from SQL syntax such as a NULLIF divisor.
fn datafusion_numeric_kind(expr: &Expression) -> NumericKind {
    match expr {
        Expression::Paren(p) => datafusion_numeric_kind(&p.this),
        Expression::Annotated(a) => datafusion_numeric_kind(&a.this),
        Expression::Neg(n) => datafusion_numeric_kind(&n.this),
        Expression::Alias(a) => datafusion_numeric_kind(&a.this),
        Expression::Abs(f) => datafusion_numeric_kind(&f.this),
        Expression::Add(b) | Expression::Sub(b) | Expression::Mul(b) | Expression::Mod(b) => {
            combine(
                datafusion_numeric_kind(&b.left),
                datafusion_numeric_kind(&b.right),
            )
        }
        Expression::Div(b) => {
            let left = datafusion_numeric_kind(&b.left);
            // Div is already target SQL. Its generated NULLIF follows
            // DataFusion's common-type coercion, including a floating divisor.
            let right = match &b.right {
                Expression::Function(f)
                    if f.name.eq_ignore_ascii_case("NULLIF") && f.args.len() == 2 =>
                {
                    combine(
                        datafusion_numeric_kind(&f.args[0]),
                        datafusion_numeric_kind(&f.args[1]),
                    )
                }
                other => datafusion_numeric_kind(other),
            };
            // A source `/` supplies a floating operand even when columns or
            // function return types on the other side are unresolved.
            if left == NumericKind::Float || right == NumericKind::Float {
                NumericKind::Float
            } else {
                combine(left, right)
            }
        }
        Expression::Coalesce(f) => datafusion_branch_kind(f.expressions.iter()),
        Expression::Case(c) => datafusion_branch_kind(
            c.whens
                .iter()
                .map(|(_, result)| result)
                .chain(c.else_.iter()),
        ),
        Expression::IfFunc(f) => {
            datafusion_branch_kind(std::iter::once(&f.true_value).chain(f.false_value.iter()))
        }
        Expression::Function(f) if f.name.eq_ignore_ascii_case("NULLIF") && f.args.len() == 2 => {
            datafusion_nullif_kind(&f.args[0], &f.args[1])
        }
        Expression::NullIf(f) => datafusion_nullif_kind(&f.this, &f.expression),
        // Do not assume functions such as ROUND preserve numeric types across
        // dialects: DuckDB ROUND(integer) is integer, DataFusion's is floating.
        _ => expression_numeric_kind(expr),
    }
}

fn datafusion_branch_kind<'a>(branches: impl Iterator<Item = &'a Expression>) -> NumericKind {
    branches
        .filter(|expr| !matches!(expr, Expression::Null(_)))
        .map(datafusion_numeric_kind)
        .reduce(combine)
        .unwrap_or(NumericKind::Unknown)
}

fn datafusion_nullif_kind(left: &Expression, right: &Expression) -> NumericKind {
    let left_kind = datafusion_numeric_kind(left);
    // DuckDB preserves the first argument's type, but DataFusion coerces both
    // arguments to a common type. Mixed numeric types need an explicit cast.
    if left_kind == datafusion_numeric_kind(right) || matches!(right, Expression::Null(_)) {
        left_kind
    } else {
        NumericKind::Unknown
    }
}

fn as_double(expr: Expression) -> Expression {
    super::operators::cast_expr(
        expr,
        DataType::Double {
            precision: None,
            scale: None,
        },
    )
}

fn unresolved_division(target: DialectType) -> crate::error::Error {
    crate::error::Error::unsupported(
        "DuckDB // with unresolved operand types; use explicit numeric casts",
        target.to_string(),
    )
}

fn prepare_datafusion_division(expr: Expression) -> Result<Expression> {
    // Stop the outer traversal at each outermost IntDiv. Rewrite that complete
    // subtree once, bottom-up, including casts, functions, CASE and subqueries.
    // Replacement nodes are not revisited, so a generated Div cannot be
    // mistaken for a source `/`, even across nested integer divisions.
    crate::traversal::transform_with_dispatch(
        expr,
        &Ok,
        &|expr| !matches!(expr, Expression::IntDiv(_)),
        &|expr, _| {
            crate::traversal::transform_all(expr, &|mut expr| {
                match &mut expr {
                    Expression::Div(b) => {
                        // Integer, decimal and unresolved source operands need
                        // floating arithmetic. Preserve an existing FLOAT operand
                        // rather than widening DuckDB's FLOAT/FLOAT division.
                        if datafusion_numeric_kind(&b.left) != NumericKind::Float
                            && datafusion_numeric_kind(&b.right) != NumericKind::Float
                        {
                            b.left =
                                as_double(std::mem::replace(&mut b.left, Expression::number(0)));
                        }
                    }
                    Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c)
                        if super::scalar::data_type_numeric_kind(&c.to) == NumericKind::Integer
                            && matches!(
                                datafusion_numeric_kind(&c.this),
                                NumericKind::Float | NumericKind::Decimal
                            ) =>
                    {
                        // DuckDB rounds numeric casts to integers; DataFusion
                        // truncates. This also matters after a nested source `/`.
                        c.this = Expression::Round(Box::new(RoundFunc {
                            this: std::mem::replace(&mut c.this, Expression::number(0)),
                            decimals: Some(Expression::number(0)),
                        }));
                    }
                    Expression::IntDiv(_) => {
                        let Expression::IntDiv(mut division) = expr else {
                            unreachable!()
                        };
                        let kind = combine(
                            datafusion_numeric_kind(&division.this),
                            datafusion_numeric_kind(&division.expression),
                        );
                        if kind == NumericKind::Unknown {
                            return Err(unresolved_division(DialectType::DataFusion));
                        }
                        if kind == NumericKind::Decimal {
                            division.this = as_double(division.this);
                        }
                        return Ok(Expression::Div(Box::new(BinaryOp::new(
                            division.this,
                            Expression::Function(Box::new(Function::new(
                                "NULLIF".to_string(),
                                vec![division.expression, Expression::number(0)],
                            ))),
                        ))));
                    }
                    _ => {}
                }
                Ok(expr)
            })
        },
    )
}

pub(in crate::dialects) fn prepare_integer_division(
    expr: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<Expression> {
    if source != DialectType::DuckDB || target == DialectType::DuckDB {
        return Ok(expr);
    }

    if target == DialectType::DataFusion {
        return prepare_datafusion_division(expr);
    }

    // Visit every physical child, including typed function arguments and both
    // operands of nested IntDiv nodes, before lowering the enclosing operator.
    crate::traversal::transform_all(expr, &|expr| {
        let Expression::IntDiv(mut division) = expr else {
            return Ok(expr);
        };
        let left_kind = numeric_kind(&division.this);
        let right_kind = numeric_kind(&division.expression);
        let kind = combine(left_kind, right_kind);

        if kind == NumericKind::Unknown {
            return Err(unresolved_division(target));
        }

        // NULLIF evaluates the divisor once and preserves NULL on zero for
        // literals, casts, computed expressions, and values only known at runtime.
        division.expression = Expression::Function(Box::new(Function::new(
            "NULLIF".to_string(),
            vec![division.expression, Expression::number(0)],
        )));

        if kind != NumericKind::Integer {
            // DuckDB decimal division produces DOUBLE, whereas some targets
            // keep decimal arithmetic (and SQLite may treat DECIMAL as INTEGER).
            // An explicitly floating operand already establishes float division.
            if kind == NumericKind::Decimal {
                division.this = super::operators::cast_expr(
                    division.this,
                    DataType::Double {
                        precision: None,
                        scale: None,
                    },
                );
            }
            return Ok(Expression::Div(Box::new(BinaryOp::new(
                division.this,
                division.expression,
            ))));
        }

        // Only use IntDiv where generation has a native or exact lowering.
        // A generic DIV(...) call is invalid in several other targets.
        if !matches!(
            target,
            DialectType::PostgreSQL
                | DialectType::BigQuery
                | DialectType::ClickHouse
                | DialectType::SQLite
                | DialectType::Vertica
                | DialectType::Hive
                | DialectType::Spark
                | DialectType::Databricks
        ) {
            return Err(crate::error::Error::unsupported(
                "DuckDB integer // conversion",
                target.to_string(),
            ));
        }
        Ok(Expression::IntDiv(division))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigquery_truncation_normalizes_typed_nodes() {
        for timestamp_trunc in [false, true] {
            let trunc = Box::new(DateTruncFunc {
                this: Expression::Cast(Box::new(Cast {
                    this: Expression::string("2024-03-13"),
                    to: DataType::Date,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })),
                unit: DateTimeField::Week,
            });
            let expr = if timestamp_trunc {
                Expression::TimestampTrunc(trunc)
            } else {
                Expression::DateTrunc(trunc)
            };
            let expr =
                prepare_bigquery_conversion(expr, DialectType::DuckDB, DialectType::BigQuery, true)
                    .unwrap();
            assert_eq!(
                Dialect::get(DialectType::BigQuery)
                    .generate_with_source(&expr, DialectType::DuckDB)
                    .unwrap(),
                "DATE_TRUNC(CAST(CAST('2024-03-13' AS DATE) AS DATETIME), WEEK(MONDAY))"
            );
        }
    }
}
