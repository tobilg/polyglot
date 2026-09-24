//! Canonical AST construction and mutation primitives shared by AST consumers.
//!
//! AST transform helpers, the native fluent builder, and serializable builder
//! plans delegate their AST edits here so a clause has exactly one implementation.
//! This module depends only on AST and error types, not the optional public builder
//! API, SQL generation, serde, or a language binding.

// AST-only consumers use a subset of the primitives; builders use the full set.
#![cfg_attr(not(feature = "builder"), allow(dead_code))]

use crate::error::{Error, Result};
use crate::expressions::*;
use crate::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryKind {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    ILike,
    RLike,
    Is,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnaryKind {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetKind {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockKind {
    Update,
    Share,
}

pub(crate) fn binary(kind: BinaryKind, left: Expression, right: Expression) -> Expression {
    let operation = BinaryOp::new(left, right);
    match kind {
        BinaryKind::Eq => Expression::Eq(Box::new(operation)),
        BinaryKind::Neq => Expression::Neq(Box::new(operation)),
        BinaryKind::Lt => Expression::Lt(Box::new(operation)),
        BinaryKind::Lte => Expression::Lte(Box::new(operation)),
        BinaryKind::Gt => Expression::Gt(Box::new(operation)),
        BinaryKind::Gte => Expression::Gte(Box::new(operation)),
        BinaryKind::And => Expression::And(Box::new(operation)),
        BinaryKind::Or => Expression::Or(Box::new(operation)),
        BinaryKind::Xor => Expression::Xor(Box::new(Xor {
            this: Some(Box::new(operation.left)),
            expression: Some(Box::new(operation.right)),
            expressions: Vec::new(),
        })),
        BinaryKind::Add => Expression::Add(Box::new(operation)),
        BinaryKind::Sub => Expression::Sub(Box::new(operation)),
        BinaryKind::Mul => Expression::Mul(Box::new(operation)),
        BinaryKind::Div => Expression::Div(Box::new(operation)),
        BinaryKind::Mod => Expression::Mod(Box::new(operation)),
        BinaryKind::Like => Expression::Like(Box::new(LikeOp {
            left: operation.left,
            right: operation.right,
            escape: None,
            quantifier: None,
            inferred_type: None,
        })),
        BinaryKind::ILike => Expression::ILike(Box::new(LikeOp {
            left: operation.left,
            right: operation.right,
            escape: None,
            quantifier: None,
            inferred_type: None,
        })),
        BinaryKind::RLike => Expression::RegexpLike(Box::new(RegexpFunc {
            options: None,

            this: operation.left,
            pattern: operation.right,
            flags: None,
        })),
        BinaryKind::Is => Expression::Is(Box::new(operation)),
    }
}

pub(crate) fn unary(kind: UnaryKind, expression: Expression) -> Expression {
    match kind {
        UnaryKind::Not => Expression::Not(Box::new(UnaryOp::new(expression))),
        UnaryKind::Neg => Expression::Neg(Box::new(UnaryOp::new(expression))),
        UnaryKind::IsNull => binary(
            BinaryKind::Is,
            expression,
            Expression::Null(crate::expressions::Null),
        ),
        UnaryKind::IsNotNull => Expression::Not(Box::new(UnaryOp::new(binary(
            BinaryKind::Is,
            expression,
            Expression::Null(crate::expressions::Null),
        )))),
    }
}

pub(crate) fn append_select(
    expression: &mut Expression,
    values: Vec<Expression>,
    append: bool,
) -> Result<()> {
    let select = as_select_mut(expression, "select")?;
    if !append {
        select.expressions.clear();
    }
    select.expressions.extend(values);
    Ok(())
}

pub(crate) fn set_from(expression: &mut Expression, values: Vec<Expression>) -> Result<()> {
    let from = Some(From {
        expressions: values,
    });
    match expression {
        Expression::Select(select) => select.from = from,
        Expression::Update(update) => update.from_clause = from,
        _ => return Err(invalid_method("from", expression)),
    }
    Ok(())
}

pub(crate) fn append_update_assignments(
    expression: &mut Expression,
    assignments: Vec<(Identifier, Expression)>,
) -> Result<()> {
    match expression {
        Expression::Update(update) => update.set.extend(assignments),
        _ => return Err(invalid_method("set", expression)),
    }
    Ok(())
}

pub(crate) fn set_insert_columns(
    expression: &mut Expression,
    columns: Vec<Identifier>,
) -> Result<()> {
    match expression {
        Expression::Insert(insert) => insert.columns = columns,
        _ => return Err(invalid_method("columns", expression)),
    }
    Ok(())
}

pub(crate) fn apply_insert_values(
    expression: &mut Expression,
    rows: Vec<Vec<Expression>>,
    append: bool,
) -> Result<()> {
    match expression {
        Expression::Insert(insert) => {
            if !append {
                insert.values.clear();
            }
            insert.values.extend(rows);
            insert.query = None;
        }
        _ => return Err(invalid_method("values", expression)),
    }
    Ok(())
}

pub(crate) fn set_insert_query(expression: &mut Expression, query: Expression) -> Result<()> {
    if !is_query(&query) {
        return Err(Error::invalid_input(
            "insert query requires a query expression",
        ));
    }
    match expression {
        Expression::Insert(insert) => {
            insert.query = Some(query);
            insert.values.clear();
        }
        _ => return Err(invalid_method("query", expression)),
    }
    Ok(())
}

pub(crate) fn append_join(expression: &mut Expression, join: Join) -> Result<()> {
    as_select_mut(expression, "join")?.joins.push(join);
    Ok(())
}

pub(crate) fn apply_where(
    expression: &mut Expression,
    condition: Expression,
    append: bool,
) -> Result<()> {
    let slot = match expression {
        Expression::Select(select) => &mut select.where_clause,
        Expression::Update(update) => &mut update.where_clause,
        Expression::Delete(delete) => &mut delete.where_clause,
        _ => return Err(invalid_method("where", expression)),
    };
    combine_where(slot, condition, append);
    Ok(())
}

pub(crate) fn apply_group_by(
    expression: &mut Expression,
    values: Vec<Expression>,
    append: bool,
) -> Result<()> {
    let select = as_select_mut(expression, "group_by")?;
    if append {
        select
            .group_by
            .get_or_insert_with(empty_group_by)
            .expressions
            .extend(values);
    } else {
        select.group_by = Some(GroupBy {
            expressions: values,
            ..empty_group_by()
        });
    }
    Ok(())
}

pub(crate) fn apply_having(
    expression: &mut Expression,
    condition: Expression,
    append: bool,
) -> Result<()> {
    let select = as_select_mut(expression, "having")?;
    if append {
        select.having = Some(Having {
            this: select
                .having
                .take()
                .map(|old| and(old.this, condition.clone()))
                .unwrap_or(condition),
            comments: Vec::new(),
        });
    } else {
        select.having = Some(Having {
            this: condition,
            comments: Vec::new(),
        });
    }
    Ok(())
}

pub(crate) fn apply_order_by(
    expression: &mut Expression,
    values: Vec<Ordered>,
    append: bool,
) -> Result<()> {
    let slot = match expression {
        Expression::Select(select) => &mut select.order_by,
        Expression::Union(set) => &mut set.order_by,
        Expression::Intersect(set) => &mut set.order_by,
        Expression::Except(set) => &mut set.order_by,
        _ => return Err(invalid_method("order_by", expression)),
    };
    if append {
        slot.get_or_insert_with(empty_order_by)
            .expressions
            .extend(values);
    } else {
        *slot = Some(OrderBy {
            expressions: values,
            ..empty_order_by()
        });
    }
    Ok(())
}

pub(crate) fn apply_sort_by(
    expression: &mut Expression,
    values: Vec<Ordered>,
    append: bool,
) -> Result<()> {
    let select = as_select_mut(expression, "sort_by")?;
    if append {
        select
            .sort_by
            .get_or_insert_with(|| SortBy {
                expressions: Vec::new(),
            })
            .expressions
            .extend(values);
    } else {
        select.sort_by = Some(SortBy {
            expressions: values,
        });
    }
    Ok(())
}

pub(crate) fn apply_qualify(
    expression: &mut Expression,
    condition: Expression,
    append: bool,
) -> Result<()> {
    let select = as_select_mut(expression, "qualify")?;
    select.qualify = Some(Qualify {
        this: if append {
            select
                .qualify
                .take()
                .map(|old| and(old.this, condition.clone()))
                .unwrap_or(condition)
        } else {
            condition
        },
    });
    Ok(())
}

pub(crate) fn apply_limit(expression: &mut Expression, value: Expression) -> Result<()> {
    match expression {
        Expression::Select(select) => {
            select.limit = Some(Limit {
                this: value,
                percent: false,
                comments: Vec::new(),
            })
        }
        Expression::Union(set) => set.limit = Some(Box::new(value)),
        Expression::Intersect(set) => set.limit = Some(Box::new(value)),
        Expression::Except(set) => set.limit = Some(Box::new(value)),
        _ => return Err(invalid_method("limit", expression)),
    }
    Ok(())
}

pub(crate) fn apply_offset(expression: &mut Expression, value: Expression) -> Result<()> {
    match expression {
        Expression::Select(select) => {
            select.offset = Some(Offset {
                this: value,
                rows: None,
            })
        }
        Expression::Union(set) => set.offset = Some(Box::new(value)),
        Expression::Intersect(set) => set.offset = Some(Box::new(value)),
        Expression::Except(set) => set.offset = Some(Box::new(value)),
        _ => return Err(invalid_method("offset", expression)),
    }
    Ok(())
}

pub(crate) fn apply_distinct(expression: &mut Expression, enabled: bool) -> Result<()> {
    as_select_mut(expression, "distinct")?.distinct = enabled;
    Ok(())
}

pub(crate) fn append_lateral_view(
    expression: &mut Expression,
    table_function: Expression,
    table_alias: Option<Identifier>,
    column_aliases: Vec<Identifier>,
    outer: bool,
) -> Result<()> {
    as_select_mut(expression, "lateral_view")?
        .lateral_views
        .push(LateralView {
            this: table_function,
            table_alias,
            column_aliases,
            outer,
        });
    Ok(())
}

pub(crate) fn append_window(
    expression: &mut Expression,
    name: Identifier,
    partition_by: Vec<Expression>,
    order_by: Vec<Ordered>,
) -> Result<()> {
    let select = as_select_mut(expression, "window")?;
    select
        .windows
        .get_or_insert_with(Vec::new)
        .push(NamedWindow {
            name,
            spec: Over {
                window_name: None,
                partition_by,
                order_by,
                frame: None,
                alias: None,
            },
        });
    Ok(())
}

pub(crate) fn append_lock(expression: &mut Expression, kind: LockKind) -> Result<()> {
    as_select_mut(expression, "lock")?.locks.push(Lock {
        ignore_locked: false,
        update: match kind {
            LockKind::Update => Some(Box::new(Expression::Boolean(BooleanLiteral {
                value: true,
            }))),
            LockKind::Share => None,
        },
        expressions: Vec::new(),
        wait: None,
        key: None,
    });
    Ok(())
}

pub(crate) fn append_hint(expression: &mut Expression, text: String) -> Result<()> {
    let select = as_select_mut(expression, "hint")?;
    select
        .hint
        .get_or_insert_with(|| Hint {
            source_dialect: None,

            expressions: Vec::new(),
        })
        .expressions
        .push(HintExpression::Raw(text));
    Ok(())
}

pub(crate) fn create_table_as(
    query: Expression,
    name: TableRef,
    replace: bool,
    temporary: bool,
) -> Result<Expression> {
    if !is_query(&query) {
        return Err(Error::invalid_input("ctas requires a query expression"));
    }
    Ok(Expression::CreateTable(Box::new(CreateTable {
        name,
        on_cluster: None,
        columns: Vec::new(),
        constraints: Vec::new(),
        if_not_exists: false,
        temporary,
        or_replace: replace,
        table_modifier: None,
        source_dialect: None,
        as_select: Some(query),
        as_select_parenthesized: false,
        on_commit: None,
        clone_source: None,
        clone_at_clause: None,
        is_copy: false,
        shallow_clone: false,
        deep_clone: false,
        leading_comments: Vec::new(),
        with_properties: Vec::new(),
        teradata_post_name_options: Vec::new(),
        with_data: None,
        with_statistics: None,
        teradata_indexes: Vec::new(),
        with_cte: None,
        properties: Vec::new(),
        partition_of: None,
        post_table_properties: Vec::new(),
        mysql_table_options: Vec::new(),
        tidb_table_options: Vec::new(),
        inherits: Vec::new(),
        on_property: None,
        copy_grants: false,
        using_template: None,
        rollup: None,
        uuid: None,
        with_partition_columns: Vec::new(),
        with_connection: None,
    })))
}

pub(crate) fn case(operand: Option<Expression>) -> Expression {
    Expression::Case(Box::new(Case {
        operand,
        whens: Vec::new(),
        else_: None,
        comments: Vec::new(),
        inferred_type: None,
    }))
}

pub(crate) fn append_case_when(
    expression: &mut Expression,
    condition: Expression,
    result: Expression,
) -> Result<()> {
    match expression {
        Expression::Case(case) => case.whens.push((condition, result)),
        _ => return Err(invalid_method("when", expression)),
    }
    Ok(())
}

pub(crate) fn set_case_else(expression: &mut Expression, result: Expression) -> Result<()> {
    match expression {
        Expression::Case(case) => case.else_ = Some(result),
        _ => return Err(invalid_method("else_", expression)),
    }
    Ok(())
}

/// Wrap an expression in a derived table with an explicit AS alias.
pub(crate) fn derived_table(this: Expression, alias: Option<Identifier>) -> Expression {
    Expression::Subquery(Box::new(Subquery {
        this,
        alias,
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

/// Qualify a column without losing the output identifier's quoting.
pub(crate) fn qualified_column(table: &str, name: &Identifier) -> Expression {
    let Expression::Column(mut c) = Expression::qualified_column(table, &name.name) else {
        unreachable!()
    };
    c.name = name.clone();
    Expression::Column(c)
}

pub(crate) fn subquery(
    query: Expression,
    alias: Option<Identifier>,
    modifiers_inside: bool,
) -> Result<Expression> {
    if !is_query(&query) {
        return Err(invalid_method("subquery", &query));
    }
    let Expression::Subquery(mut subquery) = derived_table(query, alias) else {
        unreachable!()
    };
    subquery.alias_explicit_as = false;
    subquery.modifiers_inside = modifiers_inside;
    Ok(Expression::Subquery(subquery))
}

pub(crate) fn merge(target: Expression) -> Expression {
    Expression::Merge(Box::new(Merge {
        this: Box::new(target),
        using: Box::new(Expression::Null(crate::expressions::Null)),
        on: None,
        using_cond: None,
        whens: Some(Box::new(Expression::Whens(Box::new(Whens {
            expressions: Vec::new(),
        })))),
        with_: None,
        returning: None,
    }))
}

pub(crate) fn set_merge_using(
    expression: &mut Expression,
    source: Expression,
    on: Expression,
) -> Result<()> {
    let merge = as_merge_mut(expression, "using")?;
    merge.using = Box::new(source);
    merge.on = Some(Box::new(on));
    Ok(())
}

pub(crate) fn append_merge_update(
    expression: &mut Expression,
    assignments: Vec<(Identifier, Expression)>,
    condition: Option<Expression>,
) -> Result<()> {
    let equations = assignments
        .into_iter()
        .map(|(column, value)| {
            binary(
                BinaryKind::Eq,
                Expression::boxed_column(Column {
                    name: column,
                    table: None,
                    join_mark: false,
                    trailing_comments: Vec::new(),
                    span: None,
                    inferred_type: None,
                }),
                value,
            )
        })
        .collect();
    append_merge_when(
        expression,
        true,
        condition,
        Expression::Tuple(Box::new(Tuple {
            expressions: vec![
                Expression::Var(Box::new(Var {
                    this: "UPDATE".to_string(),
                })),
                Expression::Tuple(Box::new(Tuple {
                    expressions: equations,
                })),
            ],
        })),
    )
}

pub(crate) fn append_merge_delete(
    expression: &mut Expression,
    condition: Option<Expression>,
) -> Result<()> {
    append_merge_when(
        expression,
        true,
        condition,
        Expression::Var(Box::new(Var {
            this: "DELETE".to_string(),
        })),
    )
}

pub(crate) fn append_merge_insert(
    expression: &mut Expression,
    columns: Vec<Identifier>,
    values: Vec<Expression>,
    condition: Option<Expression>,
) -> Result<()> {
    let columns = columns
        .into_iter()
        .map(|name| {
            Expression::boxed_column(Column {
                name,
                table: None,
                join_mark: false,
                trailing_comments: Vec::new(),
                span: None,
                inferred_type: None,
            })
        })
        .collect();
    append_merge_when(
        expression,
        false,
        condition,
        Expression::Tuple(Box::new(Tuple {
            expressions: vec![
                Expression::Var(Box::new(Var {
                    this: "INSERT".to_string(),
                })),
                Expression::Tuple(Box::new(Tuple {
                    expressions: columns,
                })),
                Expression::Tuple(Box::new(Tuple {
                    expressions: values,
                })),
            ],
        })),
    )
}

fn append_merge_when(
    expression: &mut Expression,
    matched: bool,
    condition: Option<Expression>,
    action: Expression,
) -> Result<()> {
    let merge = as_merge_mut(expression, "when")?;
    let when = Expression::When(Box::new(When {
        matched: Some(Box::new(Expression::Boolean(BooleanLiteral {
            value: matched,
        }))),
        source: None,
        condition: condition.map(Box::new),
        then: Box::new(action),
    }));
    match merge.whens.as_deref_mut() {
        Some(Expression::Whens(whens)) => whens.expressions.push(when),
        _ => {
            merge.whens = Some(Box::new(Expression::Whens(Box::new(Whens {
                expressions: vec![when],
            }))));
        }
    }
    Ok(())
}

pub(crate) fn set_operation(
    kind: SetKind,
    left: Expression,
    right: Expression,
    distinct: bool,
) -> Result<Expression> {
    if !is_query(&left) || !is_query(&right) {
        return Err(Error::invalid_input(
            "set operations require query expressions",
        ));
    }
    let all = !distinct;
    Ok(match kind {
        SetKind::Union => Expression::Union(Box::new(Union {
            left,
            right,
            all,
            distinct: false,
            with: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            by_name: false,
            side: None,
            kind: None,
            corresponding: false,
            strict: false,
            on_columns: Vec::new(),
        })),
        SetKind::Intersect => Expression::Intersect(Box::new(Intersect {
            left,
            right,
            all,
            distinct: false,
            with: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            by_name: false,
            side: None,
            kind: None,
            corresponding: false,
            strict: false,
            on_columns: Vec::new(),
        })),
        SetKind::Except => Expression::Except(Box::new(Except {
            left,
            right,
            all,
            distinct: false,
            with: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            by_name: false,
            side: None,
            kind: None,
            corresponding: false,
            strict: false,
            on_columns: Vec::new(),
        })),
    })
}

pub(crate) fn is_query(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
    )
}

pub(crate) fn invalid_method(method: &str, expression: &Expression) -> Error {
    Error::invalid_input(format!(
        "{method} is not supported for {}",
        expression.variant_name()
    ))
}

pub(crate) fn ordered(expression: Expression) -> Ordered {
    match expression {
        Expression::Ordered(ordered) => *ordered,
        expression => Ordered::asc(expression),
    }
}

fn as_select_mut<'a>(expression: &'a mut Expression, method: &str) -> Result<&'a mut Select> {
    match expression {
        Expression::Select(select) => Ok(select),
        _ => Err(invalid_method(method, expression)),
    }
}

fn as_merge_mut<'a>(expression: &'a mut Expression, method: &str) -> Result<&'a mut Merge> {
    match expression {
        Expression::Merge(merge) => Ok(merge),
        _ => Err(invalid_method(method, expression)),
    }
}

fn and(left: Expression, right: Expression) -> Expression {
    binary(BinaryKind::And, left, right)
}

fn combine_where(slot: &mut Option<Where>, condition: Expression, append: bool) {
    *slot = Some(Where {
        this: if append {
            slot.take()
                .map(|old| and(old.this, condition.clone()))
                .unwrap_or(condition)
        } else {
            condition
        },
    });
}

fn empty_group_by() -> GroupBy {
    GroupBy {
        expressions: Vec::new(),
        all: None,
        totals: false,
        comments: Vec::new(),
    }
}

fn empty_order_by() -> OrderBy {
    OrderBy {
        expressions: Vec::new(),
        siblings: false,
        comments: Vec::new(),
    }
}
