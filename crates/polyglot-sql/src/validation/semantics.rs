//! Scope-local correctness checks, separate from optional query-quality hints.
use super::*;
use crate::expressions::{Identifier, Select};
use crate::traversal::is_aggregate;

fn boundary(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Select(_)
            | Expression::Subquery(_)
            | Expression::Exists(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
    )
}

fn ordinary_aggregate(expression: &Expression) -> bool {
    let mut pending = vec![expression];
    while let Some(node) = pending.pop() {
        if boundary(node) {
            continue;
        }
        if let Expression::WindowFunction(window) = node {
            // The window's function is not an ordinary aggregate, but its
            // arguments/partition/order may contain grouped aggregates.
            pending.extend(window.this.children());
            pending.extend(
                node.children()
                    .into_iter()
                    .filter(|child| !std::ptr::eq(*child, &window.this)),
            );
            continue;
        }
        if is_aggregate(node) {
            return true;
        }
        pending.extend(node.children());
    }
    false
}

fn issue(node: &Expression, code: &str, message: &str) -> ValidationError {
    let span = match node {
        Expression::Column(column) => column.span.or(column.name.span),
        Expression::Identifier(identifier) => identifier.span,
        Expression::Star(star) => star.span,
        _ => walk_in_scope(node, false).find_map(|node| match node {
            Expression::Column(column) => column.span.or(column.name.span),
            _ => None,
        }),
    };
    let mut error = ValidationError::error(message, code);
    if let Some(span) = span {
        error = error
            .with_location(span.line, span.column)
            .with_span(Some(span.start), Some(span.end));
    }
    error
}

fn placement(
    expression: &Expression,
    aggregates: bool,
    windows: bool,
    aliases: &HashMap<String, &Expression>,
    dialect: DialectType,
    errors: &mut Vec<ValidationError>,
) {
    let mut pending = vec![(expression, false, false, false)];
    let mut expanded = HashSet::new();
    while let Some((node, inside_aggregate, inside_window, window_function)) = pending.pop() {
        if boundary(node) {
            continue;
        }
        if let Expression::Column(column) = node {
            if column.table.is_none() {
                let key = crate::set_operation::identifier_key(&column.name, Some(dialect));
                if let Some(alias) = aliases.get(&key) {
                    if expanded.insert((key, inside_aggregate, inside_window)) {
                        pending.push((alias, inside_aggregate, inside_window, false));
                    }
                    continue;
                }
            }
        }
        if let Expression::WindowFunction(window) = node {
            if !windows || inside_window || inside_aggregate {
                errors.push(issue(node, "E232", "Window function is not allowed in this clause or inside another aggregate/window function"));
                continue;
            }
            pending.push((&window.this, false, true, true));
            // Partition/order/frame expressions also belong to this window.
            for child in node.children() {
                if !std::ptr::eq(child, &window.this) {
                    pending.push((child, false, true, false));
                }
            }
            continue;
        }
        let aggregate = is_aggregate(node) && !window_function;
        if aggregate && (!aggregates || inside_aggregate) {
            errors.push(issue(
                node,
                "E231",
                "Aggregate function is not allowed in this clause or inside another aggregate",
            ));
            continue;
        }
        pending.extend(
            node.children()
                .into_iter()
                .rev()
                .map(|child| (child, inside_aggregate || aggregate, inside_window, false)),
        );
    }
}

fn unalias(expression: &Expression) -> &Expression {
    match expression {
        Expression::Alias(alias) => &alias.this,
        _ => expression,
    }
}

fn grouping(
    select: &Select,
    dialect: DialectType,
    schema: Option<&ValidationSchema>,
    aliases: &HashMap<String, &Expression>,
    resolver: &mut Resolver<'_>,
    sources: &HashSet<String>,
    errors: &mut Vec<ValidationError>,
) {
    let aggregate_query = select.group_by.is_some()
        || select.expressions.iter().any(ordinary_aggregate)
        || select
            .having
            .as_ref()
            .is_some_and(|having| ordinary_aggregate(&having.this))
        || select.order_by.as_ref().is_some_and(|order| {
            order
                .expressions
                .iter()
                .any(|e| ordinary_aggregate(&e.this))
        });
    if !aggregate_query {
        return;
    }
    let key =
        |identifier: &Identifier| crate::set_operation::identifier_key(identifier, Some(dialect));
    let mut groups = Vec::new();
    let mut pending: Vec<_> = select
        .group_by
        .as_ref()
        .into_iter()
        .flat_map(|g| &g.expressions)
        .collect();
    while let Some(mut node) = pending.pop() {
        if let Expression::Literal(literal) = node {
            if let crate::expressions::Literal::Number(number) = literal.as_ref() {
                if let Some(projection) = number
                    .parse::<usize>()
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|n| select.expressions.get(n))
                {
                    node = unalias(projection);
                }
            }
        }
        if let Expression::Column(column) = node {
            if column.table.is_none() {
                if let Some(alias) = aliases.get(&key(&column.name)) {
                    node = alias;
                }
            }
        }
        groups.push(node);
        if matches!(
            node,
            Expression::GroupingSets(_)
                | Expression::Cube(_)
                | Expression::Rollup(_)
                | Expression::Tuple(_)
        ) {
            pending.extend(node.children());
        }
    }
    let mut grouped_stars = Vec::new();
    if select
        .group_by
        .as_ref()
        .is_some_and(|g| g.all == Some(true) && g.expressions.is_empty())
    {
        for projection in select.expressions.iter().map(unalias) {
            match projection {
                Expression::Star(star) => {
                    grouped_stars.push((star.table.as_ref().map(&key), Some(star)));
                    groups.extend(
                        star.replace
                            .iter()
                            .flatten()
                            .map(|alias| &alias.this)
                            .filter(|e| crate::scope::implicit_group_key(e)),
                    );
                }
                Expression::Column(column) if column.name.name == "*" => {
                    grouped_stars.push((column.table.as_ref().map(&key), None));
                }
                expr if crate::scope::implicit_group_key(expr) => groups.push(expr),
                _ => {}
            }
        }
    }
    let mut column_key = |column: &Column| {
        let table = column
            .table
            .as_ref()
            .map(&key)
            .or_else(|| {
                resolver
                    .get_table(&crate::binding::identifier_name(&column.name))
                    .map(|name| {
                        crate::set_operation::identifier_key(
                            &crate::binding::schema_identifier(&name),
                            Some(dialect),
                        )
                    })
            })
            .or_else(|| {
                if sources.len() == 1 {
                    sources.iter().next().cloned()
                } else {
                    None
                }
            });
        (table, key(&column.name))
    };
    let grouped_columns: HashSet<_> = groups
        .iter()
        .filter_map(|node| match node {
            Expression::Column(column) => Some(column_key(column)),
            _ => None,
        })
        .collect();
    // Only use declared keys of a single physical source, and only for dialects
    // that recognize that functional dependency. Missing key metadata is not proof.
    let dependent = matches!(dialect, DialectType::PostgreSQL)
        && select.joins.is_empty()
        && select.from.as_ref().is_some_and(|from| {
            if from.expressions.len() != 1 {
                return false;
            }
            let Expression::Table(table) = &from.expressions[0] else {
                return false;
            };
            schema.is_some_and(|schema| {
                schema.tables.iter().any(|entry| {
                    if !entry.name.eq_ignore_ascii_case(&table.name.name) {
                        return false;
                    }
                    let keys: Vec<_> = entry
                        .primary_key
                        .iter()
                        .chain(
                            entry
                                .columns
                                .iter()
                                .filter(|c| c.primary_key)
                                .map(|c| &c.name),
                        )
                        .collect();
                    !keys.is_empty()
                        && keys.iter().all(|name| {
                            grouped_columns
                                .iter()
                                .any(|(_, column)| column == &key(&Identifier::new(*name)))
                        })
                })
            })
        });
    if dependent {
        return;
    }
    let uncertain = matches!(dialect, DialectType::MySQL | DialectType::SQLite)
        || (select.group_by.is_some()
            && schema.is_none()
            && matches!(dialect, DialectType::PostgreSQL));
    let expressions = select
        .expressions
        .iter()
        .map(|e| (e, crate::binding::lateral_aliases(dialect)))
        .chain(select.having.iter().map(|e| {
            (
                &e.this,
                crate::binding::clause_aliases(dialect, crate::binding::AliasClause::Having),
            )
        }))
        .chain(select.qualify.iter().map(|e| {
            (
                &e.this,
                crate::binding::clause_aliases(dialect, crate::binding::AliasClause::Qualify),
            )
        }))
        .chain(
            select
                .order_by
                .iter()
                .flat_map(|order| order.expressions.iter().map(|e| (&e.this, true))),
        );
    for (projection, allow_aliases) in expressions {
        let mut pending = vec![unalias(projection)];
        let mut expanded = HashSet::new();
        while let Some(node) = pending.pop() {
            if boundary(node)
                || is_aggregate(node)
                || matches!(node, Expression::WithinGroup(group) if is_aggregate(&group.this))
                || groups.iter().any(|group| *group == node)
            {
                continue;
            }
            if let Expression::Column(column) = node {
                let binding = column_key(column);
                if grouped_columns.contains(&binding)
                    || grouped_stars.iter().any(|(table, star)| {
                        (table.is_none() || binding.0.is_none() || table == &binding.0)
                            && star.is_none_or(|star| {
                                !star.excludes(&column.name.name, Some(dialect))
                                    && star.replacement(&column.name.name, Some(dialect)).is_none()
                            })
                    })
                {
                    continue;
                }
                if allow_aliases && column.table.is_none() {
                    let name = key(&column.name);
                    if let Some(alias) = aliases.get(&name) {
                        if expanded.insert(name) {
                            pending.push(alias);
                        }
                        continue;
                    }
                }
                let mut finding = issue(
                    node,
                    if uncertain { "W002" } else { "E230" },
                    "Column must be grouped or used in an aggregate function",
                );
                if uncertain {
                    finding.severity = crate::ValidationSeverity::Warning;
                }
                errors.push(finding);
                break;
            }
            if let Expression::WindowFunction(window) = node {
                // A window aggregate consumes grouped rows, not an ordinary
                // aggregate's input rows.
                pending.extend(window.this.children());
                pending.extend(
                    node.children()
                        .into_iter()
                        .filter(|child| !std::ptr::eq(*child, &window.this)),
                );
            } else {
                pending.extend(node.children().into_iter().rev());
            }
        }
    }
}

pub(crate) fn check_semantics(
    stmt: &Expression,
    dialect: DialectType,
    schema: Option<&ValidationSchema>,
) -> Vec<ValidationError> {
    // Lambda-local identifiers are not grouped input columns. Use the same
    // private lexical binding as schema checks and analysis, without changing
    // the caller's AST or treating names captured from a table as locals.
    let bound;
    let stmt = if stmt.dfs().any(|node| matches!(node, Expression::Lambda(_))) {
        bound = crate::binding::bind_lambdas(stmt.clone(), dialect);
        &bound
    } else {
        stmt
    };
    let mut errors = Vec::new();
    let mapping =
        schema.map(|schema| mapping_schema_from_validation_schema_with_dialect(schema, dialect));
    let empty_schema = MappingSchema::with_dialect(dialect);
    for node in stmt.dfs() {
        let Expression::Select(select) = node else {
            continue;
        };
        let scope = selected_validation_scope(&build_scope(node));
        let mut resolver = Resolver::new(&scope, mapping.as_ref().unwrap_or(&empty_schema), true);
        let mut input_names = HashSet::new();
        let mut open = false;
        for source in scope.sources.keys() {
            let columns = resolver.get_source_columns(source).unwrap_or_default();
            open |= columns.is_empty() || columns.iter().any(|column| column == "*");
            input_names.extend(columns.iter().map(|column| {
                crate::set_operation::identifier_key(
                    &crate::binding::schema_identifier(column),
                    Some(dialect),
                )
            }));
        }
        let no_aliases = HashMap::new();
        let mut aliases = HashMap::new();
        let mut duplicate = HashSet::new();
        for projection in &select.expressions {
            placement(
                projection,
                true,
                true,
                if !open && crate::binding::lateral_aliases(dialect) {
                    &aliases
                } else {
                    &no_aliases
                },
                dialect,
                &mut errors,
            );
            if let Expression::Alias(alias) = projection {
                let key = crate::set_operation::identifier_key(&alias.alias, Some(dialect));
                if matches!(&alias.this, Expression::Column(column) if column.table.is_none() && crate::set_operation::identifier_key(&column.name, Some(dialect)) == key)
                {
                    continue;
                }
                if !input_names.contains(&key)
                    && !duplicate.contains(&key)
                    && aliases.insert(key.clone(), &alias.this).is_some()
                {
                    aliases.remove(&key);
                    duplicate.insert(key);
                }
            }
        }
        // Incomplete source metadata cannot prove that a name refers to an
        // output alias rather than an input column for placement diagnostics.
        let placement_aliases = if open { &no_aliases } else { &aliases };
        use crate::binding::{clause_aliases, AliasClause};
        for expression in select
            .prewhere
            .iter()
            .chain(select.where_clause.iter().map(|c| &c.this))
        {
            placement(
                expression,
                false,
                false,
                if clause_aliases(dialect, AliasClause::Where) {
                    placement_aliases
                } else {
                    &no_aliases
                },
                dialect,
                &mut errors,
            );
        }
        for join in &select.joins {
            if let Some(on) = &join.on {
                placement(on, false, false, &no_aliases, dialect, &mut errors);
            }
        }
        if let Some(group) = &select.group_by {
            for e in &group.expressions {
                let e = match e {
                    Expression::Literal(literal) => literal
                        .value_str()
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| i.checked_sub(1))
                        .and_then(|i| select.expressions.get(i))
                        .unwrap_or(e),
                    _ => e,
                };
                placement(
                    e,
                    false,
                    false,
                    if clause_aliases(dialect, AliasClause::Group) {
                        placement_aliases
                    } else {
                        &no_aliases
                    },
                    dialect,
                    &mut errors,
                );
            }
        }
        if let Some(having) = &select.having {
            placement(
                &having.this,
                true,
                false,
                if clause_aliases(dialect, AliasClause::Having) {
                    placement_aliases
                } else {
                    &no_aliases
                },
                dialect,
                &mut errors,
            );
        }
        if let Some(qualify) = &select.qualify {
            placement(
                &qualify.this,
                true,
                true,
                if clause_aliases(dialect, AliasClause::Qualify) {
                    placement_aliases
                } else {
                    &no_aliases
                },
                dialect,
                &mut errors,
            );
        }
        if let Some(order) = &select.order_by {
            for e in &order.expressions {
                placement(&e.this, true, true, placement_aliases, dialect, &mut errors);
            }
        }
        let sources = scope
            .sources
            .keys()
            .map(|name| {
                crate::set_operation::identifier_key(
                    &crate::binding::schema_identifier(name),
                    Some(dialect),
                )
            })
            .collect();
        grouping(
            select,
            dialect,
            schema,
            &aliases,
            &mut resolver,
            &sources,
            &mut errors,
        );
        if let Some(star) = select
            .expressions
            .iter()
            .find(|e| matches!(e, Expression::Star(_)))
        {
            let mut warning = issue(star, "W001", "SELECT * is discouraged; specify columns explicitly for better performance and maintainability");
            warning.severity = crate::ValidationSeverity::Warning;
            errors.push(warning);
        }
        if select.distinct && select.order_by.is_some() {
            errors.push(ValidationError::warning(
                "DISTINCT with ORDER BY: ensure ORDER BY columns are in SELECT list",
                "W003",
            ));
        }
        if select.limit.is_some() && select.order_by.is_none() {
            errors.push(ValidationError::warning(
                "LIMIT without ORDER BY produces non-deterministic results",
                "W004",
            ));
        }
    }
    errors
}
