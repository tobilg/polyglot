//! Clause uses retain original syntax/locations while sharing qualified scope
//! lineage with projections. No predicate is added to output-value lineage.

use super::*;
use crate::ast_children::{for_each_child, ChildPathSegment};
use crate::expressions::{Column, Identifier, Literal};
use crate::lineage::ScopedLineage;
use crate::resolver::Resolver;
use crate::scope::{scope_query, selected_reference_scope};

pub(super) fn collect(
    original: &Scope,
    prepared: &Scope,
    schema: Option<&MappingSchema>,
    dialect: DialectType,
    uncertain: &HashMap<(usize, usize), ReferenceConfidence>,
    guard: Option<ComplexityGuardOptions>,
) -> Vec<ColumnUseFact> {
    let empty = MappingSchema::with_dialect(dialect);
    let schema = schema.unwrap_or(&empty);
    let mut collector = Collector {
        schema,
        dialect,
        facts: Vec::new(),
        uncertain,
        guard,
    };
    collector.scope(original, prepared, "root", &[], &[], false);
    collector.facts
}

#[derive(Clone)]
struct Frame<'a> {
    path: String,
    selected: Scope,
    lineage: &'a ScopedLineage,
    subqueries: Vec<Expression>,
}

struct Collector<'a> {
    schema: &'a MappingSchema,
    dialect: DialectType,
    facts: Vec<ColumnUseFact>,
    uncertain: &'a HashMap<(usize, usize), ReferenceConfidence>,
    guard: Option<ComplexityGuardOptions>,
}

impl Collector<'_> {
    fn scope(
        &mut self,
        original: &Scope,
        prepared: &Scope,
        path: &str,
        ancestors: &[&Frame],
        inherited_ctes: &[Scope],
        filter_branch: bool,
    ) {
        let lineage = ScopedLineage::new(prepared.clone(), inherited_ctes, self.dialect);
        let frame = Frame {
            path: path.to_string(),
            selected: selected_reference_scope(original),
            lineage: &lineage,
            subqueries: original
                .subquery_scopes
                .iter()
                .map(|scope| scope.expression.clone())
                .collect(),
        };
        let query = scope_query(&original.expression);
        self.scan(query, "", &frame, ancestors, filter_branch, true);
        if let Expression::Select(select) = query {
            self.implicit_joins(select, path, &frame, ancestors);
        }

        let mut ctes = prepared.cte_scopes.clone();
        ctes.extend_from_slice(inherited_ctes);
        let outer: Vec<_> = std::iter::once(&frame)
            .chain(ancestors.iter().copied())
            .collect();
        for (kind, children, resolved, correlated) in [
            ("ctes", &original.cte_scopes, &prepared.cte_scopes, false),
            (
                "derived",
                &original.derived_table_scopes,
                &prepared.derived_table_scopes,
                false,
            ),
            (
                "subqueries",
                &original.subquery_scopes,
                &prepared.subquery_scopes,
                true,
            ),
            ("udtfs", &original.udtf_scopes, &prepared.udtf_scopes, true),
            (
                "branches",
                &original.union_scopes,
                &prepared.union_scopes,
                false,
            ),
        ] {
            for (index, child) in children.iter().enumerate() {
                // Qualification normally preserves scope topology. If a rewrite
                // adds/removes scopes, use original scopes rather than pairing
                // unrelated queries and attributing references to the wrong source.
                let qualified = if children.len() == resolved.len() {
                    &resolved[index]
                } else {
                    child
                };
                let is_filter = kind == "branches"
                    && (filter_branch
                        || (index == 1
                            && matches!(query, Expression::Except(_) | Expression::Intersect(_))));
                self.scope(
                    child,
                    qualified,
                    &format!("{path}.{kind}[{index}]"),
                    if correlated { &outer } else { ancestors },
                    &ctes,
                    is_filter,
                );
            }
        }
    }

    fn scan(
        &mut self,
        expression: &Expression,
        expression_path: &str,
        frame: &Frame,
        ancestors: &[&Frame],
        filter_branch: bool,
        root: bool,
    ) {
        if !root && query_boundary(expression) {
            return;
        }
        for_each_child(expression, |segments, child| {
            use ChildPathSegment::{Field, Index};
            let path = append_path(expression_path, segments);
            let context = match segments {
                [Field("where_clause"), ..] if root => Some(ColumnUseContext::Filter),
                [Field("group_by"), Field("expressions"), Index(_)] if root => {
                    Some(ColumnUseContext::Group)
                }
                [Field("having"), ..] if root => Some(ColumnUseContext::Having),
                [Field("qualify"), ..] if root => Some(ColumnUseContext::Qualify),
                [Field("joins"), Index(_), Field("on" | "match_condition")] => {
                    Some(ColumnUseContext::Join)
                }
                [Field("order_by"), Field("expressions"), Index(_), Field("this")] if root => {
                    Some(ColumnUseContext::Order)
                }
                [Field("expressions"), Index(_)]
                    if root && filter_branch && matches!(expression, Expression::Select(_)) =>
                {
                    Some(ColumnUseContext::SetOperationFilter)
                }
                [Field("expression")] if matches!(expression, Expression::Filter(_)) => {
                    Some(ColumnUseContext::Filter)
                }
                [Field("filter")] => Some(ColumnUseContext::Filter),
                [Field("order_by"), Index(_), Field("this")]
                    if matches!(expression, Expression::WithinGroup(_)) =>
                {
                    Some(ColumnUseContext::AggregateOrder)
                }
                [Field("order_by"), Index(_), Field("this")] if !root => {
                    Some(ColumnUseContext::AggregateOrder)
                }
                _ if segments
                    .iter()
                    .any(|segment| matches!(segment, Field("over" | "spec"))) =>
                {
                    if segments.contains(&Field("partition_by")) {
                        Some(ColumnUseContext::WindowPartition)
                    } else if segments.contains(&Field("order_by")) {
                        Some(ColumnUseContext::WindowOrder)
                    } else if segments.contains(&Field("frame")) {
                        Some(ColumnUseContext::WindowFrame)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(context) = context {
                let child = if let Expression::Where(clause) = child {
                    &clause.this
                } else {
                    child
                };
                let mut join_frame;
                let owner =
                    if let (Expression::Select(select), [Field("joins"), Index(index), ..]) =
                        (expression, segments)
                    {
                        join_frame = frame.clone();
                        let later_sources: Vec<_> = select
                            .joins
                            .iter()
                            .skip(index + 1)
                            .filter_map(|join| expression_source_name(&join.this))
                            .collect();
                        join_frame.selected.sources.retain(|name, _| {
                            !later_sources.iter().any(|later| self.same(name, later))
                        });
                        &join_frame
                    } else {
                        frame
                    };
                let references = self.references(child, context, owner, ancestors);
                self.facts.push(ColumnUseFact {
                    context,
                    scope_path: frame.path.clone(),
                    expression_path: path.clone(),
                    expression_sql: self.sql(child),
                    span: expression_span(child),
                    references,
                });
            }
            self.scan(child, &path, frame, ancestors, filter_branch, false);
        });
    }

    fn sql(&self, expression: &Expression) -> String {
        Dialect::get(self.dialect)
            .generate_with_guard(expression, self.guard)
            .unwrap_or_default()
    }

    fn references(
        &self,
        expression: &Expression,
        context: ColumnUseContext,
        frame: &Frame,
        ancestors: &[&Frame],
    ) -> Vec<ColumnUseReferenceFact> {
        if context == ColumnUseContext::SetOperationFilter
            && projection_is_star(unwrap_projection_alias(expression))
        {
            return frame
                .lineage
                .output_names()
                .iter()
                .enumerate()
                .flat_map(|(index, _)| {
                    frame
                        .lineage
                        .output(index)
                        .ok()
                        .map(|node| {
                            self.lineage_references(&node, expression_span(expression), true)
                        })
                        .unwrap_or_default()
                })
                .collect();
        }
        if matches!(context, ColumnUseContext::Order | ColumnUseContext::Group) {
            if let Expression::Literal(literal) = expression {
                if let Literal::Number(value) = literal.as_ref() {
                    if let Some(ordinal) =
                        value.parse::<usize>().ok().and_then(|n| n.checked_sub(1))
                    {
                        return frame
                            .lineage
                            .output(ordinal)
                            .ok()
                            .map(|node| self.lineage_references(&node, None, true))
                            .unwrap_or_default();
                    }
                }
            }
        }
        let mut references = Vec::new();
        let mut pending = vec![expression];
        while let Some(node) = pending.pop() {
            // EXISTS does not consume the subquery's projected values. Its
            // predicates are collected independently in the child scope.
            if matches!(node, Expression::Exists(_)) {
                continue;
            }
            if query_boundary(node) {
                if let Some(index) = frame
                    .subqueries
                    .iter()
                    .position(|query| scope_query(query) == scope_query(node))
                {
                    if let Ok(lineage) = frame.lineage.subquery_output(index) {
                        references.extend(self.lineage_references(&lineage, None, false));
                    }
                }
                continue;
            }
            if let Expression::Column(column) = node {
                if column.name.name != "*" {
                    references.extend(self.column(column, context, frame, ancestors));
                }
            } else {
                pending.extend(node.children().into_iter().rev());
            }
        }
        for usage in &mut references {
            let reference = &mut usage.reference;
            if reference.source_kind == SourceKind::Table && reference.column != "*" {
                if let Some(table) = &reference.table {
                    if let Ok(columns) = self.schema.column_names(table) {
                        if !columns.is_empty()
                            && !columns
                                .iter()
                                .any(|name| name == "*" || self.same(name, &reference.column))
                        {
                            reference.confidence = ReferenceConfidence::Unknown;
                        }
                    }
                }
            }
        }
        references
    }

    fn column(
        &self,
        column: &Column,
        context: ColumnUseContext,
        frame: &Frame,
        ancestors: &[&Frame],
    ) -> Vec<ColumnUseReferenceFact> {
        let span = column.span.or(column.name.span).map(source_span);
        let mut local = Resolver::new(&frame.selected, self.schema, false);
        if let Some(qualifier) = &column.table {
            // A dotted field may be a STRUCT column rather than a table alias.
            // Preserve real outer qualifiers and reuse the core normalizer;
            // with no schema, leave uncertain interpretations unresolved.
            let mut visible = frame.selected.clone();
            for owner in ancestors {
                for (name, source) in &owner.selected.sources {
                    if !visible.sources.keys().any(|key| self.same(key, name)) {
                        visible.sources.insert(name.clone(), source.clone());
                    }
                }
            }
            if !visible
                .sources
                .keys()
                .any(|name| self.same(name, &qualifier.name))
            {
                let mut resolver = Resolver::new(&visible, self.schema, false);
                let mut normalized = Expression::Column(Box::new(column.clone()));
                if crate::optimizer::qualify_columns::normalize_dotted_columns_in_expression(
                    &mut normalized,
                    &visible,
                    &mut resolver,
                )
                .is_ok()
                    && !matches!(normalized, Expression::Column(_))
                {
                    return self.references(&normalized, context, frame, ancestors);
                }
            }
        }
        if column.table.is_none()
            && matches!(
                context,
                ColumnUseContext::Order
                    | ColumnUseContext::Group
                    | ColumnUseContext::Having
                    | ColumnUseContext::Qualify
            )
        {
            let aliases = frame.lineage.output_names();
            let ordinals: Vec<_> = aliases
                .iter()
                .enumerate()
                .filter(|(_, name)| self.same(name, &column.name.name))
                .map(|(index, _)| index)
                .collect();
            let alias_wins = context == ColumnUseContext::Order
                || local.sources_for_column(&column.name.name).is_empty();
            if alias_wins && ordinals.len() > 1 {
                return vec![unresolved(column, ReferenceConfidence::Ambiguous)];
            }
            if alias_wins && ordinals.len() == 1 {
                if let Ok(node) = frame.lineage.output(ordinals[0]) {
                    return self.lineage_references(&node, span, true);
                }
            }
        }

        for owner in std::iter::once(frame).chain(ancestors.iter().copied()) {
            let mut resolver = Resolver::new(&owner.selected, self.schema, false);
            if let Some(qualifier) = &column.table {
                let Some(source) = owner
                    .selected
                    .sources
                    .keys()
                    .find(|name| self.same(name, &qualifier.name))
                else {
                    continue;
                };
                let columns = resolver.get_source_columns(source).unwrap_or_default();
                if !columns.is_empty()
                    && !columns
                        .iter()
                        .any(|name| name == "*" || self.same(name, &column.name.name))
                {
                    return vec![unresolved(column, ReferenceConfidence::Unknown)];
                }
                return self.lineage_references(
                    &owner.lineage.column(source, &column.name.name),
                    span,
                    false,
                );
            }
            let mut definite = resolver.sources_for_column(&column.name.name);
            definite.sort();
            let mut open: Vec<_> = owner
                .selected
                .sources
                .keys()
                .filter(|source| {
                    let columns = resolver.get_source_columns(source).unwrap_or_default();
                    columns.is_empty() || columns.iter().any(|name| name == "*")
                })
                .cloned()
                .collect();
            open.sort();
            // Do not infer a unique owner from a known match plus an open table.
            if definite.len() > 1 {
                if merged_join_column(&owner.selected.expression, &column.name.name) {
                    return definite
                        .iter()
                        .flat_map(|source| {
                            self.lineage_references(
                                &owner.lineage.column(source, &column.name.name),
                                span,
                                true,
                            )
                        })
                        .collect();
                }
                return vec![unresolved(column, ReferenceConfidence::Ambiguous)];
            }
            if definite.len() == 1 && open.iter().all(|source| source == &definite[0]) {
                return self.lineage_references(
                    &owner.lineage.column(&definite[0], &column.name.name),
                    span,
                    true,
                );
            }
            if definite.is_empty() && open.len() == 1 {
                return self.lineage_references(
                    &owner.lineage.column(&open[0], &column.name.name),
                    span,
                    true,
                );
            }
            if !open.is_empty() {
                return vec![unresolved(column, ReferenceConfidence::Unknown)];
            }
        }
        vec![unresolved(column, ReferenceConfidence::Unknown)]
    }

    fn same(&self, left: &str, right: &str) -> bool {
        crate::schema::normalize_name(left, Some(self.dialect), false, true)
            == crate::schema::normalize_name(right, Some(self.dialect), false, true)
    }

    fn lineage_references(
        &self,
        node: &LineageNode,
        span: Option<QuerySourceSpan>,
        unqualified: bool,
    ) -> Vec<ColumnUseReferenceFact> {
        let mut references = use_references(node, span, unqualified);
        // Qualification may infer an owner from partial schema information.
        // Preserve uncertainty from the original scope, including through CTEs,
        // instead of upgrading an inferred upstream reference to resolved.
        if let Some(confidence) = lineage_uncertainty(node, self.uncertain) {
            for usage in &mut references {
                usage.reference.confidence = confidence;
            }
        }
        references
    }

    fn implicit_joins(
        &mut self,
        select: &crate::expressions::Select,
        scope_path: &str,
        frame: &Frame,
        ancestors: &[&Frame],
    ) {
        let mut left = Vec::new();
        if let Some(from) = &select.from {
            left.extend(from.expressions.iter().filter_map(expression_source_name));
        }
        for (index, join) in select.joins.iter().enumerate() {
            let right = expression_source_name(&join.this);
            let natural = matches!(
                join.kind,
                JoinKind::Natural
                    | JoinKind::NaturalLeft
                    | JoinKind::NaturalRight
                    | JoinKind::NaturalFull
            );
            if !join.using.is_empty() || natural {
                let mut resolver = Resolver::new(&frame.selected, self.schema, false);
                let columns = |resolver: &mut Resolver<'_>, source: &str| {
                    resolver.get_source_columns(source).ok().filter(|columns| {
                        !columns.is_empty() && !columns.iter().any(|name| name == "*")
                    })
                };
                let right_columns = right.as_ref().and_then(|name| columns(&mut resolver, name));
                let left_columns: Option<Vec<_>> = left
                    .iter()
                    .map(|name| columns(&mut resolver, name))
                    .collect();
                let mut keys = join.using.clone();
                let complete = right_columns.is_some() && left_columns.is_some();
                if natural && complete {
                    keys = right_columns
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|name| {
                            left_columns
                                .as_ref()
                                .unwrap()
                                .iter()
                                .flatten()
                                .any(|other| self.same(name, other))
                        })
                        .map(Identifier::new)
                        .collect();
                    keys.sort_by(|a, b| a.name.cmp(&b.name));
                    keys.dedup_by(|a, b| self.same(&a.name, &b.name));
                }
                let mut references = Vec::new();
                for key in &keys {
                    for source in left.iter().chain(right.iter()) {
                        let known = columns(&mut resolver, source);
                        if known.as_ref().is_some_and(|names| {
                            !names.iter().any(|name| self.same(name, &key.name))
                        }) {
                            continue;
                        }
                        let Expression::Column(mut column) =
                            Expression::qualified_column(source, &key.name)
                        else {
                            unreachable!()
                        };
                        column.span = key.span;
                        column.name.span = key.span;
                        let mut uses =
                            self.column(&column, ColumnUseContext::Join, frame, ancestors);
                        for usage in &mut uses {
                            usage.reference.unqualified = true;
                        }
                        references.extend(uses);
                    }
                }
                if natural && !complete {
                    let Expression::Column(column) = Expression::column("*") else {
                        unreachable!()
                    };
                    references.push(unresolved(&column, ReferenceConfidence::Unknown));
                }
                self.facts.push(ColumnUseFact {
                    context: ColumnUseContext::Join,
                    scope_path: scope_path.to_string(),
                    expression_path: format!(
                        "joins[{index}].{}",
                        if natural { "natural" } else { "using" }
                    ),
                    expression_sql: if natural {
                        "NATURAL JOIN".into()
                    } else {
                        format!(
                            "USING ({})",
                            keys.iter()
                                .map(|key| self.sql(&Expression::Identifier(key.clone())))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                    span: None,
                    references,
                });
            }
            left.extend(right);
        }
    }
}

fn query_boundary(expression: &Expression) -> bool {
    match expression {
        Expression::JoinedTable(joined) => joined.alias.is_some(),
        Expression::Alias(alias) => {
            matches!(scope_query(&alias.this), Expression::JoinedTable(_))
                || query_boundary(&alias.this)
        }
        Expression::Paren(paren) => query_boundary(&paren.this),
        _ => matches!(
            expression,
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Cte(_)
                | Expression::Subquery(_)
        ),
    }
}

fn append_path(prefix: &str, segments: &[ChildPathSegment]) -> String {
    let mut path = prefix.to_string();
    for segment in segments {
        match segment {
            ChildPathSegment::Field(field) => {
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(field);
            }
            ChildPathSegment::Index(index) => path.push_str(&format!("[{index}]")),
        }
    }
    path
}

fn source_span(span: crate::tokens::Span) -> QuerySourceSpan {
    QuerySourceSpan {
        start: span.start,
        end: span.end,
    }
}

fn expression_span(expression: &Expression) -> Option<QuerySourceSpan> {
    // Compound AST nodes do not universally retain complete source ranges.
    // Do not label a bounding range of column tokens as the whole expression.
    match expression {
        Expression::Column(column) => column.span.or(column.name.span),
        Expression::Identifier(identifier) => identifier.span,
        Expression::Star(star) => star.span,
        _ => None,
    }
    .map(source_span)
}

fn unresolved(column: &Column, confidence: ReferenceConfidence) -> ColumnUseReferenceFact {
    ColumnUseReferenceFact {
        reference: ColumnReferenceFact {
            source_name: column.table.as_ref().map(|table| table.name.clone()),
            source_alias: None,
            source_kind: SourceKind::Unknown,
            table: column.table.as_ref().map(|table| table.name.clone()),
            column: column.name.name.clone(),
            unqualified: column.table.is_none(),
            confidence,
            path_transform_kind: None,
            path_functions: Vec::new(),
            path_window: false,
        },
        span: column.span.or(column.name.span).map(source_span),
    }
}

fn use_references(
    node: &LineageNode,
    span: Option<QuerySourceSpan>,
    unqualified: bool,
) -> Vec<ColumnUseReferenceFact> {
    terminal_references_from_lineage(node, None)
        .into_iter()
        .map(|mut reference| {
            reference.unqualified = unqualified;
            if matches!(
                reference.source_kind,
                SourceKind::Unknown | SourceKind::Cte | SourceKind::DerivedTable
            ) {
                reference.confidence = ReferenceConfidence::Unknown;
            }
            ColumnUseReferenceFact { reference, span }
        })
        .collect()
}

fn merged_join_column(expression: &Expression, column: &str) -> bool {
    matches!(expression, Expression::Select(select) if !select.joins.is_empty() && select.from.as_ref().is_some_and(|from| from.expressions.len() == 1) && select.joins.iter().all(|join| {
        join.using.iter().any(|key| key.name.eq_ignore_ascii_case(column))
            || matches!(join.kind, JoinKind::Natural | JoinKind::NaturalLeft | JoinKind::NaturalRight | JoinKind::NaturalFull)
    }))
}

pub(super) fn collect_uncertain_occurrences(
    scope: &Scope,
    schema: &MappingSchema,
    uncertain: &mut HashMap<(usize, usize), ReferenceConfidence>,
) {
    let selected = selected_reference_scope(scope);
    let mut resolver = Resolver::new(&selected, schema, false);
    let open: Vec<_> = selected
        .sources
        .keys()
        .filter(|source| {
            let columns = resolver.get_source_columns(source).unwrap_or_default();
            columns.is_empty() || columns.iter().any(|name| name == "*")
        })
        .cloned()
        .collect();
    for expression in crate::scope::walk_in_scope(&selected.expression, false) {
        if let Expression::Column(column) = expression {
            if column.table.is_some() {
                continue;
            }
            let matches = resolver.sources_for_column(&column.name.name);
            let ambiguous =
                matches.len() > 1 && !merged_join_column(&selected.expression, &column.name.name);
            let incomplete = open.len() > 1
                || (matches.len() == 1 && open.iter().any(|source| source != &matches[0]));
            if ambiguous || incomplete {
                if let Some(span) = expression_span(expression) {
                    uncertain.insert(
                        (span.start, span.end),
                        if ambiguous {
                            ReferenceConfidence::Ambiguous
                        } else {
                            ReferenceConfidence::Unknown
                        },
                    );
                }
            }
        }
    }
    for child in scope
        .cte_scopes
        .iter()
        .chain(&scope.derived_table_scopes)
        .chain(&scope.subquery_scopes)
        .chain(&scope.udtf_scopes)
        .chain(&scope.union_scopes)
    {
        collect_uncertain_occurrences(child, schema, uncertain);
    }
}
