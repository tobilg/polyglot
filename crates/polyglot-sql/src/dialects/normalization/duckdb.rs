//! Preserve DuckDB's type-dependent `//` semantics before target rewrites.

use super::scalar::{expression_numeric_kind, NumericKind};
use super::*;

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

pub(in crate::dialects) fn prepare_integer_division(
    expr: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<Expression> {
    if source != DialectType::DuckDB || target == DialectType::DuckDB {
        return Ok(expr);
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
            return Err(crate::error::Error::unsupported(
                "DuckDB // with unresolved operand types; use explicit numeric casts",
                target.to_string(),
            ));
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
