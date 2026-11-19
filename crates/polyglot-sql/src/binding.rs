//! Private lexical binding helpers shared by validation and analysis.
use crate::dialects::DialectType;
use crate::expressions::{Cast, DataType, DotAccess, Expression};
use crate::optimizer::normalize_identifiers::{get_normalization_strategy, normalize_identifier};
use crate::scope::{build_scope, selected_reference_scope};
use crate::traversal::ExpressionWalk;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) fn lateral_aliases(dialect: DialectType) -> bool {
    matches!(
        dialect,
        DialectType::Snowflake
            | DialectType::DuckDB
            | DialectType::Redshift
            | DialectType::Spark
            | DialectType::Databricks
    )
}

#[derive(Clone, Copy)]
pub(crate) enum AliasClause {
    Where,
    Group,
    Having,
    Qualify,
}

pub(crate) fn clause_aliases(dialect: DialectType, clause: AliasClause) -> bool {
    use AliasClause::*;
    match clause {
        Where => matches!(dialect, DialectType::Snowflake | DialectType::DuckDB),
        Group => matches!(
            dialect,
            DialectType::Snowflake
                | DialectType::DuckDB
                | DialectType::BigQuery
                | DialectType::PostgreSQL
                | DialectType::MySQL
                | DialectType::SQLite
                | DialectType::Redshift
                | DialectType::Spark
                | DialectType::Databricks
        ),
        Having => matches!(
            dialect,
            DialectType::Snowflake
                | DialectType::DuckDB
                | DialectType::BigQuery
                | DialectType::MySQL
                | DialectType::SQLite
                | DialectType::Redshift
                | DialectType::Spark
                | DialectType::Databricks
        ),
        Qualify => matches!(
            dialect,
            DialectType::Snowflake
                | DialectType::DuckDB
                | DialectType::BigQuery
                | DialectType::Databricks
                | DialectType::Redshift
        ),
    }
}

pub(crate) fn schema_identifier(name: &str) -> crate::expressions::Identifier {
    if name.len() >= 2 {
        for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
            if name.starts_with(open) && name.ends_with(close) {
                return crate::expressions::Identifier::quoted(
                    name[open.len_utf8()..name.len() - close.len_utf8()]
                        .replace(&format!("{close}{close}"), &close.to_string()),
                );
            }
        }
    }
    crate::expressions::Identifier::new(name)
}

pub(crate) fn identifier_name(identifier: &crate::expressions::Identifier) -> String {
    identifier.to_type_field_name()
}

/// Render a table path for schema lookup, retaining identifier boundaries and quoting.
pub(crate) fn schema_table_name(table: &crate::expressions::TableRef) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = &table.catalog {
        parts.push(identifier_name(catalog));
    }
    if let Some(schema) = &table.schema {
        parts.push(identifier_name(schema));
    }
    parts.push(identifier_name(&table.name));
    parts.join(".")
}

/// Boolean contexts are inline AST fields as well as standalone nodes. Keep
/// their discovery shared so DML does not silently miss the SELECT checks.
pub(crate) fn predicates(expression: &Expression) -> Vec<(&'static str, &Expression)> {
    let mut result = Vec::new();
    let mut joins = Vec::new();
    match expression {
        Expression::Select(select) => {
            if let Some(e) = &select.prewhere {
                result.push(("PREWHERE", e));
            }
            if let Some(e) = &select.where_clause {
                result.push(("WHERE", &e.this));
            }
            if let Some(e) = &select.having {
                result.push(("HAVING", &e.this));
            }
            if let Some(e) = &select.qualify {
                result.push(("QUALIFY", &e.this));
            }
            joins.extend(&select.joins);
        }
        Expression::Update(update) => {
            if let Some(e) = &update.where_clause {
                result.push(("WHERE", &e.this));
            }
            joins.extend(&update.table_joins);
            joins.extend(&update.from_joins);
        }
        Expression::Delete(delete) => {
            if let Some(e) = &delete.where_clause {
                result.push(("WHERE", &e.this));
            }
        }
        Expression::Merge(merge) => {
            if let Some(e) = &merge.on {
                result.push(("MERGE ON", e));
            }
        }
        Expression::When(when) => {
            if let Some(e) = &when.condition {
                result.push(("WHEN", e));
            }
        }
        Expression::OnConflict(conflict) => {
            if let Some(e) = &conflict.index_predicate {
                result.push(("ON CONFLICT", e));
            }
            if let Some(e) = &conflict.where_ {
                result.push(("ON CONFLICT WHERE", e));
            }
        }
        Expression::Where(e) => result.push(("WHERE", &e.this)),
        Expression::Having(e) => result.push(("HAVING", &e.this)),
        Expression::Qualify(e) => result.push(("QUALIFY", &e.this)),
        _ => {}
    }
    for join in joins {
        if let Some(e) = &join.on {
            result.push(("JOIN ON", e));
        }
        if let Some(e) = &join.match_condition {
            result.push(("JOIN MATCH_CONDITION", e));
        }
    }
    result
}

fn relation_source(mut expression: Expression) -> Expression {
    if let Expression::Alias(alias) = &mut expression {
        if let Expression::Table(table) = &mut alias.this {
            table.alias = Some(alias.alias.clone());
            return alias.this.clone();
        }
    }
    expression
}

/// Pseudo-relations are visible only inside the clause that introduces them.
/// Rewrite them to the real target in the private checking tree, retaining
/// reference locations and avoiding extra unqualified input candidates.
pub(crate) fn bind_dml_pseudoreferences(statement: &mut Expression) {
    fn bind(expression: &mut Expression, names: &[&str], target: &crate::expressions::Identifier) {
        // A nested query may introduce a real source with the same name.
        let visible = if matches!(expression, Expression::Select(_)) {
            let scope = selected_reference_scope(&build_scope(expression));
            Some(
                names
                    .iter()
                    .copied()
                    .filter(|name| {
                        !scope
                            .sources
                            .keys()
                            .any(|source| source.eq_ignore_ascii_case(name))
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let names = visible.as_deref().unwrap_or(names);
        if names.is_empty() {
            return;
        }
        if let Expression::Column(column) = expression {
            if column.table.as_ref().is_some_and(|table| {
                names
                    .iter()
                    .any(|name| table.name.eq_ignore_ascii_case(name))
            }) {
                let span = column.table.as_ref().and_then(|table| table.span);
                column.table = Some(target.clone());
                column.table.as_mut().unwrap().span = span;
            }
        }
        crate::ast_children::for_each_child_mut(expression, |child| bind(child, names, target));
    }
    let (target, names, output) = match statement {
        Expression::Insert(insert) => {
            let target = insert.alias.as_ref().unwrap_or(&insert.table.name);
            if let Some(conflict) = &mut insert.on_conflict {
                bind(conflict, &["excluded"], target);
            }
            (target, &["inserted"][..], &mut insert.output)
        }
        Expression::Update(update) => (
            update.table.alias.as_ref().unwrap_or(&update.table.name),
            &["inserted", "deleted"][..],
            &mut update.output,
        ),
        Expression::Delete(delete) => (
            delete
                .alias
                .as_ref()
                .or(delete.table.alias.as_ref())
                .unwrap_or(&delete.table.name),
            &["deleted"][..],
            &mut delete.output,
        ),
        _ => return,
    };
    if let Some(output) = output {
        for expression in &mut output.columns {
            bind(expression, names, target);
        }
    }
}

/// The value/reference scope of a DML statement, represented with the existing
/// SELECT scope machinery. This is private metadata, never generated as SQL.
pub(crate) fn dml_scope(statement: &Expression) -> Option<crate::expressions::Select> {
    use crate::expressions::{From, Select};
    let mut select = Select::new();
    let mut sources = Vec::new();
    let (target, output) = match statement {
        Expression::Update(update) => {
            select.with = update.with.clone();
            select
                .expressions
                .extend(update.set.iter().map(|(_, value)| value.clone()));
            select.expressions.extend(update.returning.clone());
            select.where_clause = update.where_clause.clone();
            select.order_by = update.order_by.clone();
            sources.extend(
                update
                    .extra_tables
                    .iter()
                    .cloned()
                    .map(|t| Expression::Table(Box::new(t))),
            );
            if let Some(from) = &update.from_clause {
                sources.extend(from.expressions.clone());
            }
            select.joins.extend(update.table_joins.clone());
            select.joins.extend(update.from_joins.clone());
            (Some(update.table.clone()), update.output.as_ref())
        }
        Expression::Delete(delete) => {
            select.with = delete.with.clone();
            select.expressions.extend(delete.returning.clone());
            select.where_clause = delete.where_clause.clone();
            select.order_by = delete.order_by.clone();
            sources.extend(
                delete
                    .using
                    .iter()
                    .cloned()
                    .map(|t| Expression::Table(Box::new(t))),
            );
            let mut target = delete.table.clone();
            if delete.alias.is_some() {
                target.alias = delete.alias.clone();
            }
            (Some(target), delete.output.as_ref())
        }
        Expression::Insert(insert) => {
            select.with = insert.with.clone();
            select.expressions.extend(insert.returning.clone());
            if let Some(conflict) = &insert.on_conflict {
                select.expressions.push(conflict.as_ref().clone());
            }
            let mut target = insert.table.clone();
            if insert.alias.is_some() {
                target.alias = insert.alias.clone();
            }
            (Some(target), insert.output.as_ref())
        }
        Expression::Merge(merge) => {
            if let Some(with) = &merge.with_ {
                if let Expression::With(with) = with.as_ref() {
                    select.with = Some(with.as_ref().clone());
                }
            }
            sources.push(relation_source(merge.this.as_ref().clone()));
            sources.push(relation_source(merge.using.as_ref().clone()));
            if let Some(on) = &merge.on {
                select.expressions.push(on.as_ref().clone());
            }
            if let Some(whens) = &merge.whens {
                select.expressions.push(whens.as_ref().clone());
            }
            if let Some(returning) = &merge.returning {
                select.expressions.push(returning.as_ref().clone());
            }
            (None, None)
        }
        _ => return None,
    };
    if let Some(target) = target {
        // TSQL UPDATE alias FROM physical_table AS alias names the source, not
        // an additional physical table called alias.
        if !sources.iter().any(|source| {
            matches!(source, Expression::Table(table)
            if table.alias.as_ref().is_some_and(|alias| alias.name == target.name.name))
        }) {
            sources.insert(0, Expression::Table(Box::new(target.clone())));
        }
        if let Some(output) = output {
            select.expressions.extend(output.columns.clone());
        }
    }
    if !sources.is_empty() {
        select.from = Some(From {
            expressions: sources,
        });
    }
    Some(select)
}

pub(crate) fn bound_identifier(
    identifier: crate::expressions::Identifier,
    data_type: &DataType,
) -> Expression {
    let bound = Expression::Identifier(identifier);
    if *data_type == DataType::Unknown {
        bound
    } else {
        Expression::Cast(Box::new(Cast {
            this: bound,
            to: data_type.clone(),
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }
}

pub(crate) fn bind_lambdas(expression: Expression, dialect: DialectType) -> Expression {
    if !expression
        .dfs()
        .any(|node| matches!(node, Expression::Lambda(_)))
    {
        return expression;
    }

    struct Bindings {
        parameters: HashMap<String, Option<DataType>>,
        parent: Option<Arc<Bindings>>,
    }
    impl Bindings {
        fn get(&self, name: &str) -> Option<&Option<DataType>> {
            let mut frame = Some(self);
            while let Some(current) = frame {
                if let Some(data_type) = current.parameters.get(name) {
                    return Some(data_type);
                }
                frame = current.parent.as_deref();
            }
            None
        }
    }
    #[derive(Default)]
    struct Sources {
        visible: HashSet<String>,
        outer: Option<Arc<Sources>>,
    }
    enum Task {
        Visit(Expression, Option<Arc<Bindings>>, Arc<Sources>),
        Finish(Expression, usize),
    }

    let strategy = get_normalization_strategy(Some(dialect));
    let mut pending = vec![Task::Visit(expression, None, Arc::default())];
    let mut results = Vec::new();
    while let Some(task) = pending.pop() {
        match task {
            Task::Visit(mut expression, mut bindings, mut sources) => {
                if matches!(
                    expression,
                    Expression::Subquery(_) | Expression::Exists(_) | Expression::Cte(_)
                ) {
                    bindings = None;
                }
                // Match reference validation: derived tables and CTEs cannot
                // capture their containing SELECT, but retain ancestors of an
                // enclosing correlated subquery. Lambda bindings never cross
                // a query boundary.
                if matches!(&expression, Expression::Cte(_))
                    || matches!(&expression, Expression::Subquery(query) if query.alias.is_some())
                {
                    sources = sources.outer.clone().unwrap_or_default();
                }
                if matches!(
                    expression,
                    Expression::Select(_)
                        | Expression::Union(_)
                        | Expression::Intersect(_)
                        | Expression::Except(_)
                ) {
                    bindings = None;
                }
                let input_scope = if matches!(expression, Expression::Select(_)) {
                    Some(build_scope(&expression))
                } else {
                    dml_scope(&expression)
                        .map(|select| build_scope(&Expression::Select(Box::new(select))))
                };
                if let Some(scope) = input_scope {
                    let scope = selected_reference_scope(&scope);
                    let mut visible = sources.visible.clone();
                    visible.extend(scope.sources.keys().map(|name| name.to_lowercase()));
                    sources = Arc::new(Sources {
                        visible,
                        outer: Some(sources),
                    });
                }
                if let Expression::Lambda(lambda) = &expression {
                    let parameters = lambda
                        .parameters
                        .iter()
                        .enumerate()
                        .map(|(index, identifier)| {
                            (
                                normalize_identifier(identifier.clone(), strategy).name,
                                lambda.parameter_types.get(index).cloned().flatten(),
                            )
                        })
                        .collect();
                    bindings = Some(Arc::new(Bindings {
                        parameters,
                        parent: bindings,
                    }));
                }
                if let (Expression::Column(column), Some(bindings)) = (&expression, &bindings) {
                    let root = column.table.as_ref().unwrap_or(&column.name);
                    let name = normalize_identifier(root.clone(), strategy).name;
                    // An explicitly qualified source column remains a capture,
                    // even if a parameter happens to share its table alias.
                    let qualified_source = column.table.is_some()
                        && sources.visible.contains(&root.name.to_lowercase());
                    if !qualified_source {
                        if let Some(data_type) = bindings.get(&name) {
                            let mut bound = bound_identifier(
                                root.clone(),
                                data_type.as_ref().unwrap_or(&DataType::Unknown),
                            );
                            if column.table.is_some() {
                                bound = Expression::Dot(Box::new(DotAccess {
                                    this: bound,
                                    field: column.name.clone(),
                                    inferred_type: None,
                                }));
                            }
                            results.push(bound);
                            continue;
                        }
                    }
                }

                // Use the canonical child visitor, with an explicit work stack
                // so deeply nested lambdas do not add Rust call-stack recursion.
                // WITH stores CTE bodies inline rather than as Expression::Cte
                // nodes, so preserve that boundary using canonical child paths.
                let mut cte_children = Vec::new();
                crate::ast_children::for_each_child(&expression, |path, _| {
                    cte_children
                        .push(path.contains(&crate::ast_children::ChildPathSegment::Field("ctes")));
                });
                let cte_sources =
                    if matches!(expression, Expression::Select(_) | Expression::With(_)) {
                        sources.outer.clone().unwrap_or_default()
                    } else {
                        sources.clone()
                    };
                let mut children = Vec::new();
                crate::ast_children::for_each_child_mut(&mut expression, |child| {
                    children.push(std::mem::replace(
                        child,
                        Expression::Null(crate::expressions::Null),
                    ));
                });
                pending.push(Task::Finish(expression, children.len()));
                pending.extend(children.into_iter().zip(cte_children).rev().map(
                    |(child, is_cte)| {
                        if is_cte {
                            Task::Visit(child, None, cte_sources.clone())
                        } else {
                            Task::Visit(child, bindings.clone(), sources.clone())
                        }
                    },
                ));
            }
            Task::Finish(mut expression, count) => {
                let mut children = results.split_off(results.len() - count).into_iter();
                crate::ast_children::for_each_child_mut(&mut expression, |child| {
                    *child = children.next().expect("lambda binding child");
                });
                results.push(expression);
            }
        }
    }
    results.pop().expect("lambda binding result")
}
