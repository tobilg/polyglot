//! AST construction shared by Vertica normalization and generation.
//! Keep scope creation here so dialect generation never splices SQL subqueries.

use crate::error::{Error, Result};
use crate::expressions::*;
use crate::traversal::ExpressionWalk;
use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct Names(HashSet<String>);

impl Names {
    pub(crate) fn collect(&mut self, expression: &Expression) {
        for node in expression.dfs() {
            match node {
                Expression::Column(c) => {
                    self.reserve(&c.name);
                    if let Some(table) = &c.table {
                        self.reserve(table);
                    }
                }
                Expression::Identifier(id) => self.reserve(id),
                Expression::Alias(a) => {
                    self.reserve(&a.alias);
                    for id in &a.column_aliases {
                        self.reserve(id);
                    }
                }
                Expression::Table(t) => {
                    self.reserve(&t.name);
                    if let Some(alias) = &t.alias {
                        self.reserve(alias);
                    }
                }
                Expression::Subquery(s) => {
                    if let Some(alias) = &s.alias {
                        self.reserve(alias);
                    }
                    for id in &s.column_aliases {
                        self.reserve(id);
                    }
                }
                _ => {}
            }
        }
    }

    fn reserve(&mut self, id: &Identifier) {
        self.0.insert(id.name.to_ascii_lowercase());
    }

    pub(crate) fn fresh(&mut self, stem: &str) -> String {
        let mut name = stem.to_string();
        let mut suffix = 0;
        while !self.0.insert(name.to_ascii_lowercase()) {
            suffix += 1;
            name = format!("{stem}_{suffix}");
        }
        name
    }
}

pub(crate) fn subquery(this: Expression, alias: Option<String>) -> Expression {
    Expression::Subquery(Box::new(Subquery {
        this,
        alias: alias.map(Identifier::new),
        column_aliases: Vec::new(),
        alias_explicit_as: true,
        alias_keyword: None,
        order_by: None,
        limit: None,
        offset: None,
        distribute_by: None,
        sort_by: None,
        cluster_by: None,
        lateral: false,
        modifiers_inside: true,
        trailing_comments: Vec::new(),
        inferred_type: None,
    }))
}

pub(crate) fn output_name(expression: &Expression) -> Option<&Identifier> {
    match expression {
        Expression::Alias(a) if a.column_aliases.is_empty() => Some(&a.alias),
        Expression::Column(c) => Some(&c.name),
        _ => None,
    }
}

pub(crate) fn projections(mut query: &Expression) -> Option<&[Expression]> {
    loop {
        query = match query {
            Expression::Select(s) => return Some(&s.expressions),
            Expression::Union(s) => &s.left,
            Expression::Intersect(s) => &s.left,
            Expression::Except(s) => &s.left,
            Expression::Subquery(s) => &s.this,
            Expression::Paren(p) => &p.this,
            _ => return None,
        };
    }
}

pub(crate) fn column(table: &str, name: &Identifier) -> Expression {
    let Expression::Column(mut c) = Expression::qualified_column(table, &name.name) else {
        unreachable!()
    };
    c.name = name.clone();
    Expression::Column(c)
}

fn repeatable_index(index: &Expression) -> bool {
    match index {
        Expression::Column(_)
        | Expression::Identifier(_)
        | Expression::Literal(_)
        | Expression::Null(_) => true,
        Expression::Paren(p) => repeatable_index(&p.this),
        Expression::Neg(n) => repeatable_index(&n.this),
        Expression::Cast(c) => repeatable_index(&c.this),
        Expression::Add(b) | Expression::Sub(b) | Expression::Mul(b) | Expression::Mod(b) => {
            repeatable_index(&b.left) && repeatable_index(&b.right)
        }
        _ => false,
    }
}

fn adjusted_index(index: Expression) -> Expression {
    // PostgreSQL subscripts are signed int32. Fold constants so ordinary array
    // access retains the target's native vectorized plan, without a CASE or join.
    if let Expression::Literal(lit) = &index {
        if let Literal::Number(n) = lit.as_ref() {
            if let Ok(n) = n.parse::<i128>() {
                return if (0..i32::MAX as i128).contains(&n) {
                    Expression::number((n + 1) as i64)
                } else {
                    Expression::Null(Null)
                };
            }
        }
    }
    if matches!(index, Expression::Null(_)) {
        return index;
    }
    if let Expression::Neg(n) = &index {
        if matches!(&n.this, Expression::Literal(l) if matches!(l.as_ref(), Literal::Number(v) if v.parse::<u128>().is_ok_and(|v| v > 0)))
        {
            return Expression::Null(Null);
        }
    }
    Expression::Case(Box::new(Case {
        operand: None,
        whens: vec![(
            Expression::Or(Box::new(BinaryOp::new(
                Expression::Lt(Box::new(BinaryOp::new(
                    index.clone(),
                    Expression::number(0),
                ))),
                Expression::Gte(Box::new(BinaryOp::new(
                    index.clone(),
                    Expression::number(i64::from(i32::MAX)),
                ))),
            ))),
            Expression::Null(Null),
        )],
        else_: Some(Expression::Add(Box::new(BinaryOp::new(
            index,
            Expression::number(1),
        )))),
        comments: Vec::new(),
        inferred_type: None,
    }))
}

/// Evaluate a non-repeatable index once, without a correlated subquery (which
/// can evaluate volatile calls once per distinct outer value rather than per row).
fn adjusted_index_once(index: Expression) -> Expression {
    let cast = |this, to| {
        Expression::Cast(Box::new(Cast {
            this,
            to,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    };
    let call =
        |name: &str, args| Expression::Function(Box::new(Function::new(name.to_string(), args)));
    // Clamp before adding one, so int64 extremes cannot overflow. Map both
    // sentinels to zero (out of bounds in a normal one-based target array)
    // before converting to PostgreSQL's int32. All arithmetic stays in int64.
    // Do not use NULLIF: DuckDB expands it to a CASE that repeats its first arg.
    let wide = cast(index, DataType::BigInt { length: None });
    let upper = i64::from(i32::MAX) + 1;
    let bounded = call(
        "LEAST",
        vec![
            call("GREATEST", vec![wide, Expression::number(-1)]),
            Expression::number(i64::from(i32::MAX)),
        ],
    );
    let offset = Expression::Add(Box::new(BinaryOp::new(bounded, Expression::number(1))));
    let adjusted = Expression::Mod(Box::new(BinaryOp::new(offset, Expression::number(upper))));
    cast(
        adjusted,
        DataType::Int {
            length: None,
            integer_spelling: false,
        },
    )
}

/// Lower zero-based access to ordinary subscript AST nodes. The base occurs once;
/// constants fold and dynamic indices preserve NULL/negative/overflow behavior.
pub(crate) fn array_access(mut array: Expression, indices: Vec<Expression>) -> Expression {
    if !matches!(
        array,
        Expression::Column(_)
            | Expression::Identifier(_)
            | Expression::Paren(_)
            | Expression::Subscript(_)
    ) {
        array = Expression::Paren(Box::new(Paren {
            this: array,
            trailing_comments: Vec::new(),
        }));
    }
    for index in indices {
        let index = if repeatable_index(&index) {
            adjusted_index(index)
        } else {
            adjusted_index_once(index)
        };
        array = Expression::Subscript(Box::new(Subscript { this: array, index }));
    }
    array
}

/// Sort outside DISTINCT/set operations so emulated NULL ordering may add CASE keys
/// without changing the result columns or the deduplication operation.
pub(crate) fn wrap_ordered_query(mut query: Expression) -> Result<Expression> {
    let unsupported = || {
        Error::unsupported("Vertica ordered DISTINCT/set operation requires unique named outputs and resolvable sort keys", "vertica")
    };
    let projections = projections(&query).ok_or_else(unsupported)?.to_vec();
    let output_names = projections
        .iter()
        .map(|p| output_name(p).cloned().ok_or_else(unsupported))
        .collect::<Result<Vec<_>>>()?;
    let mut unique = HashSet::new();
    if output_names
        .iter()
        .any(|n| !unique.insert(n.name.to_ascii_lowercase()))
    {
        return Err(unsupported());
    }
    let mut names = Names::default();
    names.collect(&query);
    let table = names.fresh("_vertica_ordered");
    let mut outer = Select::new();
    macro_rules! take_set_modifiers {
        ($set:expr) => {{
            if $set.by_name || $set.corresponding || $set.side.is_some() || $set.kind.is_some() {
                return Err(unsupported());
            }
            outer.with = $set.with.take();
            outer.order_by = $set.order_by.take();
            outer.limit = $set.limit.take().map(|l| Limit {
                this: *l,
                percent: false,
                comments: Vec::new(),
            });
            outer.offset = $set.offset.take().map(|o| Offset {
                this: *o,
                rows: None,
            });
        }};
    }
    match &mut query {
        Expression::Select(s) => {
            if s.into.is_some() || !s.locks.is_empty() || s.vertica.is_some() {
                return Err(unsupported());
            }
            outer.with = s.with.take();
            outer.order_by = s.order_by.take();
            outer.limit = s.limit.take();
            outer.offset = s.offset.take();
            outer.fetch = s.fetch.take();
            outer.top = s.top.take();
        }
        Expression::Union(s) => take_set_modifiers!(s),
        Expression::Intersect(s) => take_set_modifiers!(s),
        Expression::Except(s) => take_set_modifiers!(s),
        Expression::Subquery(s) => {
            outer.order_by = s.order_by.take();
            outer.limit = s.limit.take();
            outer.offset = s.offset.take();
            query = std::mem::replace(&mut s.this, Expression::Null(Null));
        }
        _ => return Err(unsupported()),
    }
    for ordered in &mut outer.order_by.as_mut().ok_or_else(unsupported)?.expressions {
        let index = match &ordered.this {
            Expression::Literal(l) => match l.as_ref() {
                Literal::Number(n) => n
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| i.checked_sub(1))
                    .filter(|i| *i < output_names.len()),
                _ => None,
            },
            Expression::Column(c) if c.table.is_none() => output_names
                .iter()
                .position(|n| n.name.eq_ignore_ascii_case(&c.name.name)),
            _ => None,
        }
        .or_else(|| {
            projections.iter().position(|p| {
                let value = if let Expression::Alias(a) = p {
                    &a.this
                } else {
                    p
                };
                value == &ordered.this
            })
        })
        .ok_or_else(unsupported)?;
        ordered.this = column(&table, &output_names[index]);
    }
    outer.expressions = output_names
        .iter()
        .map(|name| Expression::Alias(Box::new(Alias::new(column(&table, name), name.clone()))))
        .collect();
    outer.from = Some(From {
        expressions: vec![subquery(query, Some(table))],
    });
    Ok(Expression::Select(Box::new(outer)))
}

/// Keep the enclosing subquery's alias/column aliases while moving its ordering
/// into an ordinary SELECT that can emulate Vertica's top-level NULL placement.
pub(crate) fn wrap_subquery_order(mut query: Subquery) -> Result<Subquery> {
    if query.distribute_by.is_some() || query.sort_by.is_some() || query.cluster_by.is_some() {
        return Err(Error::unsupported("ordered subquery modifiers", "vertica"));
    }
    let mut ordered = subquery(
        std::mem::replace(&mut query.this, Expression::Null(Null)),
        None,
    );
    let Expression::Subquery(inner) = &mut ordered else {
        unreachable!()
    };
    inner.order_by = query.order_by.take();
    inner.limit = query.limit.take();
    inner.offset = query.offset.take();
    query.this = wrap_ordered_query(ordered)?;
    Ok(query)
}
