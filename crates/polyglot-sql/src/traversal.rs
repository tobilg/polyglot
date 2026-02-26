//! Tree traversal utilities for SQL expression ASTs.
//!
//! This module provides read-only traversal, search, and transformation utilities
//! for the [`Expression`] tree produced by the parser. Because Rust's ownership
//! model does not allow parent pointers inside the AST, parent information is
//! tracked externally via [`TreeContext`] (built on demand).
//!
//! # Traversal
//!
//! Two iterator types are provided:
//! - [`DfsIter`] -- depth-first (pre-order) traversal using a stack. Visits a node
//!   before its children. Good for top-down analysis and early termination.
//! - [`BfsIter`] -- breadth-first (level-order) traversal using a queue. Visits all
//!   nodes at depth N before any node at depth N+1. Good for level-aware analysis.
//!
//! Both are available through the [`ExpressionWalk`] trait methods [`dfs`](ExpressionWalk::dfs)
//! and [`bfs`](ExpressionWalk::bfs).
//!
//! # Searching
//!
//! The [`ExpressionWalk`] trait also provides convenience methods for finding expressions:
//! [`find`](ExpressionWalk::find), [`find_all`](ExpressionWalk::find_all),
//! [`contains`](ExpressionWalk::contains), and [`count`](ExpressionWalk::count).
//! Common predicates are available as free functions: [`is_column`], [`is_literal`],
//! [`is_function`], [`is_aggregate`], [`is_window_function`], [`is_subquery`], and
//! [`is_select`].
//!
//! # Transformation
//!
//! The [`transform`] and [`transform_map`] functions perform bottom-up (post-order)
//! tree rewrites, delegating to [`transform_recursive`](crate::dialects::transform_recursive).
//! The [`ExpressionWalk::transform_owned`] method provides the same capability as
//! an owned method on `Expression`.
//!
//! Based on traversal patterns from `sqlglot/expressions.py`.

use crate::expressions::Expression;
use std::collections::{HashMap, VecDeque};

/// Unique identifier for expression nodes during traversal
pub type NodeId = usize;

/// Information about a node's parent relationship
#[derive(Debug, Clone)]
pub struct ParentInfo {
    /// The NodeId of the parent (None for root)
    pub parent_id: Option<NodeId>,
    /// Which argument/field in the parent this node occupies
    pub arg_key: String,
    /// Index if the node is part of a list (e.g., expressions in SELECT)
    pub index: Option<usize>,
}

/// External parent-tracking context for an expression tree.
///
/// Since Rust's ownership model does not allow intrusive parent pointers in the AST,
/// `TreeContext` provides an on-demand side-table that maps each node (identified by
/// a [`NodeId`]) to its [`ParentInfo`] (parent node, field name, and list index).
///
/// Build a context from any expression root with [`TreeContext::build`], then query
/// parent relationships with [`get`](TreeContext::get), ancestry chains with
/// [`ancestors_of`](TreeContext::ancestors_of), or tree depth with
/// [`depth_of`](TreeContext::depth_of).
///
/// This is useful when analysis requires upward navigation (e.g., determining whether
/// a column reference appears inside a WHERE clause or a JOIN condition).
#[derive(Debug, Default)]
pub struct TreeContext {
    /// Map from NodeId to parent information
    nodes: HashMap<NodeId, ParentInfo>,
    /// Counter for generating NodeIds
    next_id: NodeId,
    /// Stack for tracking current path during traversal
    path: Vec<(NodeId, String, Option<usize>)>,
}

impl TreeContext {
    /// Create a new empty tree context
    pub fn new() -> Self {
        Self::default()
    }

    /// Build context from an expression tree
    pub fn build(root: &Expression) -> Self {
        let mut ctx = Self::new();
        ctx.visit_expr(root);
        ctx
    }

    /// Visit an expression and record parent information
    fn visit_expr(&mut self, expr: &Expression) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;

        // Record parent info based on current path
        let parent_info = if let Some((parent_id, arg_key, index)) = self.path.last() {
            ParentInfo {
                parent_id: Some(*parent_id),
                arg_key: arg_key.clone(),
                index: *index,
            }
        } else {
            ParentInfo {
                parent_id: None,
                arg_key: String::new(),
                index: None,
            }
        };
        self.nodes.insert(id, parent_info);

        // Visit children
        for (key, child) in iter_children(expr) {
            self.path.push((id, key.to_string(), None));
            self.visit_expr(child);
            self.path.pop();
        }

        // Visit children in lists
        for (key, children) in iter_children_lists(expr) {
            for (idx, child) in children.iter().enumerate() {
                self.path.push((id, key.to_string(), Some(idx)));
                self.visit_expr(child);
                self.path.pop();
            }
        }

        id
    }

    /// Get parent info for a node
    pub fn get(&self, id: NodeId) -> Option<&ParentInfo> {
        self.nodes.get(&id)
    }

    /// Get the depth of a node (0 for root)
    pub fn depth_of(&self, id: NodeId) -> usize {
        let mut depth = 0;
        let mut current = id;
        while let Some(info) = self.nodes.get(&current) {
            if let Some(parent_id) = info.parent_id {
                depth += 1;
                current = parent_id;
            } else {
                break;
            }
        }
        depth
    }

    /// Get ancestors of a node (parent, grandparent, etc.)
    pub fn ancestors_of(&self, id: NodeId) -> Vec<NodeId> {
        let mut ancestors = Vec::new();
        let mut current = id;
        while let Some(info) = self.nodes.get(&current) {
            if let Some(parent_id) = info.parent_id {
                ancestors.push(parent_id);
                current = parent_id;
            } else {
                break;
            }
        }
        ancestors
    }
}

/// Iterate over single-child fields of an expression
///
/// Returns an iterator of (field_name, &Expression) pairs.
fn iter_children(expr: &Expression) -> Vec<(&'static str, &Expression)> {
    let mut children = Vec::new();

    match expr {
        Expression::Select(s) => {
            if let Some(from) = &s.from {
                for source in &from.expressions {
                    children.push(("from", source));
                }
            }
            for join in &s.joins {
                children.push(("join_this", &join.this));
                if let Some(on) = &join.on {
                    children.push(("join_on", on));
                }
                if let Some(match_condition) = &join.match_condition {
                    children.push(("join_match_condition", match_condition));
                }
                for pivot in &join.pivots {
                    children.push(("join_pivot", pivot));
                }
            }
            for lateral_view in &s.lateral_views {
                children.push(("lateral_view", &lateral_view.this));
            }
            if let Some(prewhere) = &s.prewhere {
                children.push(("prewhere", prewhere));
            }
            if let Some(where_clause) = &s.where_clause {
                children.push(("where", &where_clause.this));
            }
            if let Some(group_by) = &s.group_by {
                for e in &group_by.expressions {
                    children.push(("group_by", e));
                }
            }
            if let Some(having) = &s.having {
                children.push(("having", &having.this));
            }
            if let Some(qualify) = &s.qualify {
                children.push(("qualify", &qualify.this));
            }
            if let Some(order_by) = &s.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            if let Some(distribute_by) = &s.distribute_by {
                for e in &distribute_by.expressions {
                    children.push(("distribute_by", e));
                }
            }
            if let Some(cluster_by) = &s.cluster_by {
                for ordered in &cluster_by.expressions {
                    children.push(("cluster_by", &ordered.this));
                }
            }
            if let Some(sort_by) = &s.sort_by {
                for ordered in &sort_by.expressions {
                    children.push(("sort_by", &ordered.this));
                }
            }
            if let Some(limit) = &s.limit {
                children.push(("limit", &limit.this));
            }
            if let Some(offset) = &s.offset {
                children.push(("offset", &offset.this));
            }
            if let Some(limit_by) = &s.limit_by {
                for e in limit_by {
                    children.push(("limit_by", e));
                }
            }
            if let Some(fetch) = &s.fetch {
                if let Some(count) = &fetch.count {
                    children.push(("fetch", count));
                }
            }
            if let Some(top) = &s.top {
                children.push(("top", &top.this));
            }
            if let Some(with) = &s.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(sample) = &s.sample {
                children.push(("sample_size", &sample.size));
                if let Some(seed) = &sample.seed {
                    children.push(("sample_seed", seed));
                }
                if let Some(offset) = &sample.offset {
                    children.push(("sample_offset", offset));
                }
                if let Some(bucket_numerator) = &sample.bucket_numerator {
                    children.push(("sample_bucket_numerator", bucket_numerator));
                }
                if let Some(bucket_denominator) = &sample.bucket_denominator {
                    children.push(("sample_bucket_denominator", bucket_denominator));
                }
                if let Some(bucket_field) = &sample.bucket_field {
                    children.push(("sample_bucket_field", bucket_field));
                }
            }
            if let Some(connect) = &s.connect {
                if let Some(start) = &connect.start {
                    children.push(("connect_start", start));
                }
                children.push(("connect", &connect.connect));
            }
            if let Some(into) = &s.into {
                children.push(("into", &into.this));
            }
            for lock in &s.locks {
                for e in &lock.expressions {
                    children.push(("lock_expression", e));
                }
                if let Some(wait) = &lock.wait {
                    children.push(("lock_wait", wait));
                }
                if let Some(key) = &lock.key {
                    children.push(("lock_key", key));
                }
                if let Some(update) = &lock.update {
                    children.push(("lock_update", update));
                }
            }
            for e in &s.for_xml {
                children.push(("for_xml", e));
            }
        }
        Expression::With(with) => {
            for cte in &with.ctes {
                children.push(("cte", &cte.this));
            }
            if let Some(search) = &with.search {
                children.push(("search", search));
            }
        }
        Expression::Cte(cte) => {
            children.push(("this", &cte.this));
        }
        Expression::Insert(insert) => {
            if let Some(query) = &insert.query {
                children.push(("query", query));
            }
            if let Some(with) = &insert.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(on_conflict) = &insert.on_conflict {
                children.push(("on_conflict", on_conflict));
            }
            if let Some(replace_where) = &insert.replace_where {
                children.push(("replace_where", replace_where));
            }
            if let Some(source) = &insert.source {
                children.push(("source", source));
            }
            if let Some(function_target) = &insert.function_target {
                children.push(("function_target", function_target));
            }
            if let Some(partition_by) = &insert.partition_by {
                children.push(("partition_by", partition_by));
            }
            if let Some(output) = &insert.output {
                for column in &output.columns {
                    children.push(("output_column", column));
                }
                if let Some(into_table) = &output.into_table {
                    children.push(("output_into_table", into_table));
                }
            }
            for row in &insert.values {
                for value in row {
                    children.push(("value", value));
                }
            }
            for (_, value) in &insert.partition {
                if let Some(value) = value {
                    children.push(("partition_value", value));
                }
            }
            for returning in &insert.returning {
                children.push(("returning", returning));
            }
            for setting in &insert.settings {
                children.push(("setting", setting));
            }
        }
        Expression::Update(update) => {
            if let Some(from_clause) = &update.from_clause {
                for source in &from_clause.expressions {
                    children.push(("from", source));
                }
            }
            for join in &update.table_joins {
                children.push(("table_join_this", &join.this));
                if let Some(on) = &join.on {
                    children.push(("table_join_on", on));
                }
            }
            for join in &update.from_joins {
                children.push(("from_join_this", &join.this));
                if let Some(on) = &join.on {
                    children.push(("from_join_on", on));
                }
            }
            for (_, value) in &update.set {
                children.push(("set_value", value));
            }
            if let Some(where_clause) = &update.where_clause {
                children.push(("where", &where_clause.this));
            }
            if let Some(output) = &update.output {
                for column in &output.columns {
                    children.push(("output_column", column));
                }
                if let Some(into_table) = &output.into_table {
                    children.push(("output_into_table", into_table));
                }
            }
            if let Some(with) = &update.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(limit) = &update.limit {
                children.push(("limit", limit));
            }
            if let Some(order_by) = &update.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            for returning in &update.returning {
                children.push(("returning", returning));
            }
        }
        Expression::Delete(delete) => {
            if let Some(with) = &delete.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(where_clause) = &delete.where_clause {
                children.push(("where", &where_clause.this));
            }
            if let Some(output) = &delete.output {
                for column in &output.columns {
                    children.push(("output_column", column));
                }
                if let Some(into_table) = &output.into_table {
                    children.push(("output_into_table", into_table));
                }
            }
            if let Some(limit) = &delete.limit {
                children.push(("limit", limit));
            }
            if let Some(order_by) = &delete.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            for returning in &delete.returning {
                children.push(("returning", returning));
            }
            for join in &delete.joins {
                children.push(("join_this", &join.this));
                if let Some(on) = &join.on {
                    children.push(("join_on", on));
                }
            }
        }
        Expression::Join(join) => {
            children.push(("this", &join.this));
            if let Some(on) = &join.on {
                children.push(("on", on));
            }
            if let Some(match_condition) = &join.match_condition {
                children.push(("match_condition", match_condition));
            }
            for pivot in &join.pivots {
                children.push(("pivot", pivot));
            }
        }
        Expression::Alias(a) => {
            children.push(("this", &a.this));
        }
        Expression::Cast(c) => {
            children.push(("this", &c.this));
        }
        Expression::Not(u) | Expression::Neg(u) | Expression::BitwiseNot(u) => {
            children.push(("this", &u.this));
        }
        Expression::Paren(p) => {
            children.push(("this", &p.this));
        }
        Expression::IsNull(i) => {
            children.push(("this", &i.this));
        }
        Expression::Exists(e) => {
            children.push(("this", &e.this));
        }
        Expression::Subquery(s) => {
            children.push(("this", &s.this));
        }
        Expression::Where(w) => {
            children.push(("this", &w.this));
        }
        Expression::Having(h) => {
            children.push(("this", &h.this));
        }
        Expression::Qualify(q) => {
            children.push(("this", &q.this));
        }
        Expression::And(op)
        | Expression::Or(op)
        | Expression::Add(op)
        | Expression::Sub(op)
        | Expression::Mul(op)
        | Expression::Div(op)
        | Expression::Mod(op)
        | Expression::Eq(op)
        | Expression::Neq(op)
        | Expression::Lt(op)
        | Expression::Lte(op)
        | Expression::Gt(op)
        | Expression::Gte(op)
        | Expression::BitwiseAnd(op)
        | Expression::BitwiseOr(op)
        | Expression::BitwiseXor(op)
        | Expression::Concat(op) => {
            children.push(("left", &op.left));
            children.push(("right", &op.right));
        }
        Expression::Like(op) | Expression::ILike(op) => {
            children.push(("left", &op.left));
            children.push(("right", &op.right));
        }
        Expression::Between(b) => {
            children.push(("this", &b.this));
            children.push(("low", &b.low));
            children.push(("high", &b.high));
        }
        Expression::In(i) => {
            children.push(("this", &i.this));
        }
        Expression::Case(c) => {
            if let Some(ref operand) = &c.operand {
                children.push(("operand", operand));
            }
        }
        Expression::WindowFunction(wf) => {
            children.push(("this", &wf.this));
        }
        Expression::Union(u) => {
            children.push(("left", &u.left));
            children.push(("right", &u.right));
            if let Some(with) = &u.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(order_by) = &u.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            if let Some(limit) = &u.limit {
                children.push(("limit", limit));
            }
            if let Some(offset) = &u.offset {
                children.push(("offset", offset));
            }
            if let Some(distribute_by) = &u.distribute_by {
                for e in &distribute_by.expressions {
                    children.push(("distribute_by", e));
                }
            }
            if let Some(sort_by) = &u.sort_by {
                for ordered in &sort_by.expressions {
                    children.push(("sort_by", &ordered.this));
                }
            }
            if let Some(cluster_by) = &u.cluster_by {
                for ordered in &cluster_by.expressions {
                    children.push(("cluster_by", &ordered.this));
                }
            }
            for e in &u.on_columns {
                children.push(("on_column", e));
            }
        }
        Expression::Intersect(i) => {
            children.push(("left", &i.left));
            children.push(("right", &i.right));
            if let Some(with) = &i.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(order_by) = &i.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            if let Some(limit) = &i.limit {
                children.push(("limit", limit));
            }
            if let Some(offset) = &i.offset {
                children.push(("offset", offset));
            }
            if let Some(distribute_by) = &i.distribute_by {
                for e in &distribute_by.expressions {
                    children.push(("distribute_by", e));
                }
            }
            if let Some(sort_by) = &i.sort_by {
                for ordered in &sort_by.expressions {
                    children.push(("sort_by", &ordered.this));
                }
            }
            if let Some(cluster_by) = &i.cluster_by {
                for ordered in &cluster_by.expressions {
                    children.push(("cluster_by", &ordered.this));
                }
            }
            for e in &i.on_columns {
                children.push(("on_column", e));
            }
        }
        Expression::Except(e) => {
            children.push(("left", &e.left));
            children.push(("right", &e.right));
            if let Some(with) = &e.with {
                for cte in &with.ctes {
                    children.push(("with_cte", &cte.this));
                }
                if let Some(search) = &with.search {
                    children.push(("with_search", search));
                }
            }
            if let Some(order_by) = &e.order_by {
                for ordered in &order_by.expressions {
                    children.push(("order_by", &ordered.this));
                }
            }
            if let Some(limit) = &e.limit {
                children.push(("limit", limit));
            }
            if let Some(offset) = &e.offset {
                children.push(("offset", offset));
            }
            if let Some(distribute_by) = &e.distribute_by {
                for expr in &distribute_by.expressions {
                    children.push(("distribute_by", expr));
                }
            }
            if let Some(sort_by) = &e.sort_by {
                for ordered in &sort_by.expressions {
                    children.push(("sort_by", &ordered.this));
                }
            }
            if let Some(cluster_by) = &e.cluster_by {
                for ordered in &cluster_by.expressions {
                    children.push(("cluster_by", &ordered.this));
                }
            }
            for expr in &e.on_columns {
                children.push(("on_column", expr));
            }
        }
        Expression::Merge(merge) => {
            children.push(("this", &merge.this));
            children.push(("using", &merge.using));
            if let Some(on) = &merge.on {
                children.push(("on", on));
            }
            if let Some(using_cond) = &merge.using_cond {
                children.push(("using_cond", using_cond));
            }
            if let Some(whens) = &merge.whens {
                children.push(("whens", whens));
            }
            if let Some(with_) = &merge.with_ {
                children.push(("with_", with_));
            }
            if let Some(returning) = &merge.returning {
                children.push(("returning", returning));
            }
        }
        Expression::Ordered(o) => {
            children.push(("this", &o.this));
        }
        Expression::Interval(i) => {
            if let Some(ref this) = i.this {
                children.push(("this", this));
            }
        }
        _ => {}
    }

    children
}

/// Iterate over list-child fields of an expression
///
/// Returns an iterator of (field_name, &[Expression]) pairs.
fn iter_children_lists(expr: &Expression) -> Vec<(&'static str, &[Expression])> {
    let mut lists = Vec::new();

    match expr {
        Expression::Select(s) => lists.push(("expressions", s.expressions.as_slice())),
        Expression::Function(f) => {
            lists.push(("args", f.args.as_slice()));
        }
        Expression::AggregateFunction(f) => {
            lists.push(("args", f.args.as_slice()));
        }
        Expression::From(f) => {
            lists.push(("expressions", f.expressions.as_slice()));
        }
        Expression::GroupBy(g) => {
            lists.push(("expressions", g.expressions.as_slice()));
        }
        // OrderBy.expressions is Vec<Ordered>, not Vec<Expression>
        // We handle Ordered items via iter_children
        Expression::In(i) => {
            lists.push(("expressions", i.expressions.as_slice()));
        }
        Expression::Array(a) => {
            lists.push(("expressions", a.expressions.as_slice()));
        }
        Expression::Tuple(t) => {
            lists.push(("expressions", t.expressions.as_slice()));
        }
        // Values.expressions is Vec<Tuple>, handle specially
        Expression::Coalesce(c) => {
            lists.push(("expressions", c.expressions.as_slice()));
        }
        Expression::Greatest(g) | Expression::Least(g) => {
            lists.push(("expressions", g.expressions.as_slice()));
        }
        _ => {}
    }

    lists
}

/// Pre-order depth-first iterator over an expression tree.
///
/// Visits each node before its children, using a stack-based approach. This means
/// the root is yielded first, followed by the entire left subtree (recursively),
/// then the right subtree. For a binary expression `a + b`, the iteration order
/// is: `Add`, `a`, `b`.
///
/// Created via [`ExpressionWalk::dfs`] or [`DfsIter::new`].
pub struct DfsIter<'a> {
    stack: Vec<&'a Expression>,
}

impl<'a> DfsIter<'a> {
    /// Create a new DFS iterator starting from the given expression
    pub fn new(root: &'a Expression) -> Self {
        Self { stack: vec![root] }
    }
}

impl<'a> Iterator for DfsIter<'a> {
    type Item = &'a Expression;

    fn next(&mut self) -> Option<Self::Item> {
        let expr = self.stack.pop()?;

        // Add children in reverse order so they come out in forward order
        let children: Vec<_> = iter_children(expr).into_iter().map(|(_, e)| e).collect();
        for child in children.into_iter().rev() {
            self.stack.push(child);
        }

        let lists: Vec<_> = iter_children_lists(expr)
            .into_iter()
            .flat_map(|(_, es)| es.iter())
            .collect();
        for child in lists.into_iter().rev() {
            self.stack.push(child);
        }

        Some(expr)
    }
}

/// Level-order breadth-first iterator over an expression tree.
///
/// Visits all nodes at depth N before any node at depth N+1, using a queue-based
/// approach. For a tree `(a + b) = c`, the iteration order is: `Eq` (depth 0),
/// `Add`, `c` (depth 1), `a`, `b` (depth 2).
///
/// Created via [`ExpressionWalk::bfs`] or [`BfsIter::new`].
pub struct BfsIter<'a> {
    queue: VecDeque<&'a Expression>,
}

impl<'a> BfsIter<'a> {
    /// Create a new BFS iterator starting from the given expression
    pub fn new(root: &'a Expression) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back(root);
        Self { queue }
    }
}

impl<'a> Iterator for BfsIter<'a> {
    type Item = &'a Expression;

    fn next(&mut self) -> Option<Self::Item> {
        let expr = self.queue.pop_front()?;

        // Add children to queue
        for (_, child) in iter_children(expr) {
            self.queue.push_back(child);
        }

        for (_, children) in iter_children_lists(expr) {
            for child in children {
                self.queue.push_back(child);
            }
        }

        Some(expr)
    }
}

/// Extension trait that adds traversal and search methods to [`Expression`].
///
/// This trait is implemented for `Expression` and provides a fluent API for
/// iterating, searching, measuring, and transforming expression trees without
/// needing to import the iterator types directly.
pub trait ExpressionWalk {
    /// Returns a depth-first (pre-order) iterator over this expression and all descendants.
    ///
    /// The root node is yielded first, then its children are visited recursively
    /// from left to right.
    fn dfs(&self) -> DfsIter<'_>;

    /// Returns a breadth-first (level-order) iterator over this expression and all descendants.
    ///
    /// All nodes at depth N are yielded before any node at depth N+1.
    fn bfs(&self) -> BfsIter<'_>;

    /// Finds the first expression matching `predicate` in depth-first order.
    ///
    /// Returns `None` if no descendant (including this node) matches.
    fn find<F>(&self, predicate: F) -> Option<&Expression>
    where
        F: Fn(&Expression) -> bool;

    /// Collects all expressions matching `predicate` in depth-first order.
    ///
    /// Returns an empty vector if no descendants match.
    fn find_all<F>(&self, predicate: F) -> Vec<&Expression>
    where
        F: Fn(&Expression) -> bool;

    /// Returns `true` if this node or any descendant matches `predicate`.
    fn contains<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Expression) -> bool;

    /// Counts how many nodes (including this one) match `predicate`.
    fn count<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Expression) -> bool;

    /// Returns direct child expressions of this node.
    ///
    /// Collects all single-child fields and list-child fields into a flat vector
    /// of references. Leaf nodes return an empty vector.
    fn children(&self) -> Vec<&Expression>;

    /// Returns the maximum depth of the expression tree rooted at this node.
    ///
    /// A leaf node has depth 0, a node whose deepest child is a leaf has depth 1, etc.
    fn tree_depth(&self) -> usize;

    /// Transforms this expression tree bottom-up using the given function (owned variant).
    ///
    /// Children are transformed first, then `fun` is called on the resulting node.
    /// Return `Ok(None)` from `fun` to replace a node with `NULL`.
    /// Return `Ok(Some(expr))` to substitute the node with `expr`.
    fn transform_owned<F>(self, fun: F) -> crate::Result<Expression>
    where
        F: Fn(Expression) -> crate::Result<Option<Expression>>,
        Self: Sized;
}

impl ExpressionWalk for Expression {
    fn dfs(&self) -> DfsIter<'_> {
        DfsIter::new(self)
    }

    fn bfs(&self) -> BfsIter<'_> {
        BfsIter::new(self)
    }

    fn find<F>(&self, predicate: F) -> Option<&Expression>
    where
        F: Fn(&Expression) -> bool,
    {
        self.dfs().find(|e| predicate(e))
    }

    fn find_all<F>(&self, predicate: F) -> Vec<&Expression>
    where
        F: Fn(&Expression) -> bool,
    {
        self.dfs().filter(|e| predicate(e)).collect()
    }

    fn contains<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Expression) -> bool,
    {
        self.dfs().any(|e| predicate(e))
    }

    fn count<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Expression) -> bool,
    {
        self.dfs().filter(|e| predicate(e)).count()
    }

    fn children(&self) -> Vec<&Expression> {
        let mut result: Vec<&Expression> = Vec::new();
        for (_, child) in iter_children(self) {
            result.push(child);
        }
        for (_, children_list) in iter_children_lists(self) {
            for child in children_list {
                result.push(child);
            }
        }
        result
    }

    fn tree_depth(&self) -> usize {
        let mut max_depth = 0;

        for (_, child) in iter_children(self) {
            let child_depth = child.tree_depth();
            if child_depth + 1 > max_depth {
                max_depth = child_depth + 1;
            }
        }

        for (_, children) in iter_children_lists(self) {
            for child in children {
                let child_depth = child.tree_depth();
                if child_depth + 1 > max_depth {
                    max_depth = child_depth + 1;
                }
            }
        }

        max_depth
    }

    fn transform_owned<F>(self, fun: F) -> crate::Result<Expression>
    where
        F: Fn(Expression) -> crate::Result<Option<Expression>>,
    {
        transform(self, &fun)
    }
}

/// Transforms an expression tree bottom-up, with optional node removal.
///
/// Recursively transforms all children first, then applies `fun` to the resulting node.
/// If `fun` returns `Ok(None)`, the node is replaced with an `Expression::Null`.
/// If `fun` returns `Ok(Some(expr))`, the node is replaced with `expr`.
///
/// This is the primary transformation entry point when callers need the ability to
/// "delete" nodes by returning `None`.
///
/// # Example
///
/// ```rust,ignore
/// use polyglot_sql::traversal::transform;
///
/// // Remove all Paren wrapper nodes from a tree
/// let result = transform(expr, &|e| match e {
///     Expression::Paren(p) => Ok(Some(p.this)),
///     other => Ok(Some(other)),
/// })?;
/// ```
pub fn transform<F>(expr: Expression, fun: &F) -> crate::Result<Expression>
where
    F: Fn(Expression) -> crate::Result<Option<Expression>>,
{
    crate::dialects::transform_recursive(expr, &|e| match fun(e)? {
        Some(transformed) => Ok(transformed),
        None => Ok(Expression::Null(crate::expressions::Null)),
    })
}

/// Transforms an expression tree bottom-up without node removal.
///
/// Like [`transform`], but `fun` returns an `Expression` directly rather than
/// `Option<Expression>`, so nodes cannot be deleted. This is a convenience wrapper
/// for the common case where every node is mapped to exactly one output node.
///
/// # Example
///
/// ```rust,ignore
/// use polyglot_sql::traversal::transform_map;
///
/// // Uppercase all column names in a tree
/// let result = transform_map(expr, &|e| match e {
///     Expression::Column(mut c) => {
///         c.name.name = c.name.name.to_uppercase();
///         Ok(Expression::Column(c))
///     }
///     other => Ok(other),
/// })?;
/// ```
pub fn transform_map<F>(expr: Expression, fun: &F) -> crate::Result<Expression>
where
    F: Fn(Expression) -> crate::Result<Expression>,
{
    crate::dialects::transform_recursive(expr, fun)
}

// ---------------------------------------------------------------------------
// Common expression predicates
// ---------------------------------------------------------------------------
// These free functions are intended for use with the search methods on
// `ExpressionWalk` (e.g., `expr.find(is_column)`, `expr.contains(is_aggregate)`).

/// Returns `true` if `expr` is a column reference ([`Expression::Column`]).
pub fn is_column(expr: &Expression) -> bool {
    matches!(expr, Expression::Column(_))
}

/// Returns `true` if `expr` is a literal value (number, string, boolean, or NULL).
pub fn is_literal(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_)
    )
}

/// Returns `true` if `expr` is a function call (regular or aggregate).
pub fn is_function(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Function(_) | Expression::AggregateFunction(_)
    )
}

/// Returns `true` if `expr` is a subquery ([`Expression::Subquery`]).
pub fn is_subquery(expr: &Expression) -> bool {
    matches!(expr, Expression::Subquery(_))
}

/// Returns `true` if `expr` is a SELECT statement ([`Expression::Select`]).
pub fn is_select(expr: &Expression) -> bool {
    matches!(expr, Expression::Select(_))
}

/// Returns `true` if `expr` is an aggregate function ([`Expression::AggregateFunction`]).
pub fn is_aggregate(expr: &Expression) -> bool {
    matches!(expr, Expression::AggregateFunction(_))
}

/// Returns `true` if `expr` is a window function ([`Expression::WindowFunction`]).
pub fn is_window_function(expr: &Expression) -> bool {
    matches!(expr, Expression::WindowFunction(_))
}

/// Collects all column references ([`Expression::Column`]) from the expression tree.
///
/// Performs a depth-first search and returns references to every column node found.
pub fn get_columns(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(is_column)
}

/// Collects all table references ([`Expression::Table`]) from the expression tree.
///
/// Performs a depth-first search and returns references to every table node found.
pub fn get_tables(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| matches!(e, Expression::Table(_)))
}

/// Returns `true` if the expression tree contains any aggregate function calls.
pub fn contains_aggregate(expr: &Expression) -> bool {
    expr.contains(is_aggregate)
}

/// Returns `true` if the expression tree contains any window function calls.
pub fn contains_window_function(expr: &Expression) -> bool {
    expr.contains(is_window_function)
}

/// Returns `true` if the expression tree contains any subquery nodes.
pub fn contains_subquery(expr: &Expression) -> bool {
    expr.contains(is_subquery)
}

// ---------------------------------------------------------------------------
// Extended type predicates
// ---------------------------------------------------------------------------

/// Macro for generating simple type-predicate functions.
macro_rules! is_type {
    ($name:ident, $($variant:pat),+ $(,)?) => {
        /// Returns `true` if `expr` matches the expected AST variant(s).
        pub fn $name(expr: &Expression) -> bool {
            matches!(expr, $($variant)|+)
        }
    };
}

// Query
is_type!(is_insert, Expression::Insert(_));
is_type!(is_update, Expression::Update(_));
is_type!(is_delete, Expression::Delete(_));
is_type!(is_union, Expression::Union(_));
is_type!(is_intersect, Expression::Intersect(_));
is_type!(is_except, Expression::Except(_));

// Identifiers & literals
is_type!(is_boolean, Expression::Boolean(_));
is_type!(is_null_literal, Expression::Null(_));
is_type!(is_star, Expression::Star(_));
is_type!(is_identifier, Expression::Identifier(_));
is_type!(is_table, Expression::Table(_));

// Comparison
is_type!(is_eq, Expression::Eq(_));
is_type!(is_neq, Expression::Neq(_));
is_type!(is_lt, Expression::Lt(_));
is_type!(is_lte, Expression::Lte(_));
is_type!(is_gt, Expression::Gt(_));
is_type!(is_gte, Expression::Gte(_));
is_type!(is_like, Expression::Like(_));
is_type!(is_ilike, Expression::ILike(_));

// Arithmetic
is_type!(is_add, Expression::Add(_));
is_type!(is_sub, Expression::Sub(_));
is_type!(is_mul, Expression::Mul(_));
is_type!(is_div, Expression::Div(_));
is_type!(is_mod, Expression::Mod(_));
is_type!(is_concat, Expression::Concat(_));

// Logical
is_type!(is_and, Expression::And(_));
is_type!(is_or, Expression::Or(_));
is_type!(is_not, Expression::Not(_));

// Predicates
is_type!(is_in, Expression::In(_));
is_type!(is_between, Expression::Between(_));
is_type!(is_is_null, Expression::IsNull(_));
is_type!(is_exists, Expression::Exists(_));

// Functions
is_type!(is_count, Expression::Count(_));
is_type!(is_sum, Expression::Sum(_));
is_type!(is_avg, Expression::Avg(_));
is_type!(is_min_func, Expression::Min(_));
is_type!(is_max_func, Expression::Max(_));
is_type!(is_coalesce, Expression::Coalesce(_));
is_type!(is_null_if, Expression::NullIf(_));
is_type!(is_cast, Expression::Cast(_));
is_type!(is_try_cast, Expression::TryCast(_));
is_type!(is_safe_cast, Expression::SafeCast(_));
is_type!(is_case, Expression::Case(_));

// Clauses
is_type!(is_from, Expression::From(_));
is_type!(is_join, Expression::Join(_));
is_type!(is_where, Expression::Where(_));
is_type!(is_group_by, Expression::GroupBy(_));
is_type!(is_having, Expression::Having(_));
is_type!(is_order_by, Expression::OrderBy(_));
is_type!(is_limit, Expression::Limit(_));
is_type!(is_offset, Expression::Offset(_));
is_type!(is_with, Expression::With(_));
is_type!(is_cte, Expression::Cte(_));
is_type!(is_alias, Expression::Alias(_));
is_type!(is_paren, Expression::Paren(_));
is_type!(is_ordered, Expression::Ordered(_));

// DDL
is_type!(is_create_table, Expression::CreateTable(_));
is_type!(is_drop_table, Expression::DropTable(_));
is_type!(is_alter_table, Expression::AlterTable(_));
is_type!(is_create_index, Expression::CreateIndex(_));
is_type!(is_drop_index, Expression::DropIndex(_));
is_type!(is_create_view, Expression::CreateView(_));
is_type!(is_drop_view, Expression::DropView(_));

// ---------------------------------------------------------------------------
// Composite predicates
// ---------------------------------------------------------------------------

/// Returns `true` if `expr` is a query statement (SELECT, INSERT, UPDATE, or DELETE).
pub fn is_query(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Select(_)
            | Expression::Insert(_)
            | Expression::Update(_)
            | Expression::Delete(_)
    )
}

/// Returns `true` if `expr` is a set operation (UNION, INTERSECT, or EXCEPT).
pub fn is_set_operation(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
    )
}

/// Returns `true` if `expr` is a comparison operator.
pub fn is_comparison(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Eq(_)
            | Expression::Neq(_)
            | Expression::Lt(_)
            | Expression::Lte(_)
            | Expression::Gt(_)
            | Expression::Gte(_)
            | Expression::Like(_)
            | Expression::ILike(_)
    )
}

/// Returns `true` if `expr` is an arithmetic operator.
pub fn is_arithmetic(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Add(_)
            | Expression::Sub(_)
            | Expression::Mul(_)
            | Expression::Div(_)
            | Expression::Mod(_)
    )
}

/// Returns `true` if `expr` is a logical operator (AND, OR, NOT).
pub fn is_logical(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::And(_) | Expression::Or(_) | Expression::Not(_)
    )
}

/// Returns `true` if `expr` is a DDL statement.
pub fn is_ddl(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::CreateTable(_)
            | Expression::DropTable(_)
            | Expression::AlterTable(_)
            | Expression::CreateIndex(_)
            | Expression::DropIndex(_)
            | Expression::CreateView(_)
            | Expression::DropView(_)
            | Expression::AlterView(_)
            | Expression::CreateSchema(_)
            | Expression::DropSchema(_)
            | Expression::CreateDatabase(_)
            | Expression::DropDatabase(_)
            | Expression::CreateFunction(_)
            | Expression::DropFunction(_)
            | Expression::CreateProcedure(_)
            | Expression::DropProcedure(_)
            | Expression::CreateSequence(_)
            | Expression::DropSequence(_)
            | Expression::AlterSequence(_)
            | Expression::CreateTrigger(_)
            | Expression::DropTrigger(_)
            | Expression::CreateType(_)
            | Expression::DropType(_)
    )
}

/// Find the parent of `target` within the tree rooted at `root`.
///
/// Uses pointer identity ([`std::ptr::eq`]) — `target` must be a reference
/// obtained from the same tree (e.g., via [`ExpressionWalk::find`] or DFS iteration).
///
/// Returns `None` if `target` is the root itself or is not found in the tree.
pub fn find_parent<'a>(root: &'a Expression, target: &Expression) -> Option<&'a Expression> {
    fn search<'a>(node: &'a Expression, target: *const Expression) -> Option<&'a Expression> {
        for (_, child) in iter_children(node) {
            if std::ptr::eq(child, target) {
                return Some(node);
            }
            if let Some(found) = search(child, target) {
                return Some(found);
            }
        }
        for (_, children_list) in iter_children_lists(node) {
            for child in children_list {
                if std::ptr::eq(child, target) {
                    return Some(node);
                }
                if let Some(found) = search(child, target) {
                    return Some(found);
                }
            }
        }
        None
    }

    search(root, target as *const Expression)
}

/// Find the first ancestor of `target` matching `predicate`, walking from
/// parent toward root.
///
/// Uses pointer identity for target lookup. Returns `None` if no ancestor
/// matches or `target` is not found in the tree.
pub fn find_ancestor<'a, F>(
    root: &'a Expression,
    target: &Expression,
    predicate: F,
) -> Option<&'a Expression>
where
    F: Fn(&Expression) -> bool,
{
    // Build path from root to target
    fn build_path<'a>(
        node: &'a Expression,
        target: *const Expression,
        path: &mut Vec<&'a Expression>,
    ) -> bool {
        if std::ptr::eq(node, target) {
            return true;
        }
        path.push(node);
        for (_, child) in iter_children(node) {
            if build_path(child, target, path) {
                return true;
            }
        }
        for (_, children_list) in iter_children_lists(node) {
            for child in children_list {
                if build_path(child, target, path) {
                    return true;
                }
            }
        }
        path.pop();
        false
    }

    let mut path = Vec::new();
    if !build_path(root, target as *const Expression, &mut path) {
        return None;
    }

    // Walk path in reverse (parent first, then grandparent, etc.)
    for ancestor in path.iter().rev() {
        if predicate(ancestor) {
            return Some(ancestor);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryOp, Column, Identifier, Literal};

    fn make_column(name: &str) -> Expression {
        Expression::Column(Column {
            name: Identifier {
                name: name.to_string(),
                quoted: false,
                trailing_comments: vec![],
                span: None,
            },
            table: None,
            join_mark: false,
            trailing_comments: vec![],
            span: None,
        })
    }

    fn make_literal(value: i64) -> Expression {
        Expression::Literal(Literal::Number(value.to_string()))
    }

    #[test]
    fn test_dfs_simple() {
        let left = make_column("a");
        let right = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left,
            right,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let nodes: Vec<_> = expr.dfs().collect();
        assert_eq!(nodes.len(), 3); // Eq, Column, Literal
        assert!(matches!(nodes[0], Expression::Eq(_)));
        assert!(matches!(nodes[1], Expression::Column(_)));
        assert!(matches!(nodes[2], Expression::Literal(_)));
    }

    #[test]
    fn test_find() {
        let left = make_column("a");
        let right = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left,
            right,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let column = expr.find(is_column);
        assert!(column.is_some());
        assert!(matches!(column.unwrap(), Expression::Column(_)));

        let literal = expr.find(is_literal);
        assert!(literal.is_some());
        assert!(matches!(literal.unwrap(), Expression::Literal(_)));
    }

    #[test]
    fn test_find_all() {
        let col1 = make_column("a");
        let col2 = make_column("b");
        let expr = Expression::And(Box::new(BinaryOp {
            left: col1,
            right: col2,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let columns = expr.find_all(is_column);
        assert_eq!(columns.len(), 2);
    }

    #[test]
    fn test_contains() {
        let col = make_column("a");
        let lit = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left: col,
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        assert!(expr.contains(is_column));
        assert!(expr.contains(is_literal));
        assert!(!expr.contains(is_subquery));
    }

    #[test]
    fn test_count() {
        let col1 = make_column("a");
        let col2 = make_column("b");
        let lit = make_literal(1);

        let inner = Expression::Add(Box::new(BinaryOp {
            left: col2,
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let expr = Expression::Eq(Box::new(BinaryOp {
            left: col1,
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        assert_eq!(expr.count(is_column), 2);
        assert_eq!(expr.count(is_literal), 1);
    }

    #[test]
    fn test_tree_depth() {
        // Single node
        let lit = make_literal(1);
        assert_eq!(lit.tree_depth(), 0);

        // One level
        let col = make_column("a");
        let expr = Expression::Eq(Box::new(BinaryOp {
            left: col,
            right: lit.clone(),
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));
        assert_eq!(expr.tree_depth(), 1);

        // Two levels
        let inner = Expression::Add(Box::new(BinaryOp {
            left: make_column("b"),
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));
        let outer = Expression::Eq(Box::new(BinaryOp {
            left: make_column("a"),
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));
        assert_eq!(outer.tree_depth(), 2);
    }

    #[test]
    fn test_tree_context() {
        let col = make_column("a");
        let lit = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left: col,
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let ctx = TreeContext::build(&expr);

        // Root has no parent
        let root_info = ctx.get(0).unwrap();
        assert!(root_info.parent_id.is_none());

        // Children have root as parent
        let left_info = ctx.get(1).unwrap();
        assert_eq!(left_info.parent_id, Some(0));
        assert_eq!(left_info.arg_key, "left");

        let right_info = ctx.get(2).unwrap();
        assert_eq!(right_info.parent_id, Some(0));
        assert_eq!(right_info.arg_key, "right");
    }

    // -- Step 8: transform / transform_map tests --

    #[test]
    fn test_transform_rename_columns() {
        let ast = crate::parser::Parser::parse_sql("SELECT a, b FROM t").unwrap();
        let expr = ast[0].clone();
        let result = super::transform_map(expr, &|e| {
            if let Expression::Column(ref c) = e {
                if c.name.name == "a" {
                    return Ok(Expression::Column(Column {
                        name: Identifier::new("alpha"),
                        table: c.table.clone(),
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                    }));
                }
            }
            Ok(e)
        })
        .unwrap();
        let sql = crate::generator::Generator::sql(&result).unwrap();
        assert!(sql.contains("alpha"), "Expected 'alpha' in: {}", sql);
        assert!(sql.contains("b"), "Expected 'b' in: {}", sql);
    }

    #[test]
    fn test_transform_noop() {
        let ast = crate::parser::Parser::parse_sql("SELECT 1 + 2").unwrap();
        let expr = ast[0].clone();
        let result = super::transform_map(expr.clone(), &|e| Ok(e)).unwrap();
        let sql1 = crate::generator::Generator::sql(&expr).unwrap();
        let sql2 = crate::generator::Generator::sql(&result).unwrap();
        assert_eq!(sql1, sql2);
    }

    #[test]
    fn test_transform_nested() {
        let ast = crate::parser::Parser::parse_sql("SELECT a + b FROM t").unwrap();
        let expr = ast[0].clone();
        let result = super::transform_map(expr, &|e| {
            if let Expression::Column(ref c) = e {
                return Ok(Expression::Literal(Literal::Number(
                    if c.name.name == "a" { "1" } else { "2" }.to_string(),
                )));
            }
            Ok(e)
        })
        .unwrap();
        let sql = crate::generator::Generator::sql(&result).unwrap();
        assert_eq!(sql, "SELECT 1 + 2 FROM t");
    }

    #[test]
    fn test_transform_error() {
        let ast = crate::parser::Parser::parse_sql("SELECT a FROM t").unwrap();
        let expr = ast[0].clone();
        let result = super::transform_map(expr, &|e| {
            if let Expression::Column(ref c) = e {
                if c.name.name == "a" {
                    return Err(crate::error::Error::parse("test error", 0, 0, 0, 0));
                }
            }
            Ok(e)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_owned_trait() {
        let ast = crate::parser::Parser::parse_sql("SELECT x FROM t").unwrap();
        let expr = ast[0].clone();
        let result = expr.transform_owned(|e| Ok(Some(e))).unwrap();
        let sql = crate::generator::Generator::sql(&result).unwrap();
        assert_eq!(sql, "SELECT x FROM t");
    }

    // -- children() tests --

    #[test]
    fn test_children_leaf() {
        let lit = make_literal(1);
        assert_eq!(lit.children().len(), 0);
    }

    #[test]
    fn test_children_binary_op() {
        let left = make_column("a");
        let right = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left,
            right,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));
        let children = expr.children();
        assert_eq!(children.len(), 2);
        assert!(matches!(children[0], Expression::Column(_)));
        assert!(matches!(children[1], Expression::Literal(_)));
    }

    #[test]
    fn test_children_select() {
        let ast = crate::parser::Parser::parse_sql("SELECT a, b FROM t").unwrap();
        let expr = &ast[0];
        let children = expr.children();
        // Should include select list items (a, b)
        assert!(children.len() >= 2);
    }

    #[test]
    fn test_children_select_includes_from_and_join_sources() {
        let ast = crate::parser::Parser::parse_sql(
            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        let expr = &ast[0];
        let children = expr.children();

        let table_names: Vec<&str> = children
            .iter()
            .filter_map(|e| match e {
                Expression::Table(t) => Some(t.name.name.as_str()),
                _ => None,
            })
            .collect();

        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"orders"));
    }

    #[test]
    fn test_get_tables_includes_insert_query_sources() {
        let ast = crate::parser::Parser::parse_sql(
            "INSERT INTO dst (id) SELECT s.id FROM src s JOIN dim d ON s.id = d.id",
        )
        .unwrap();
        let expr = &ast[0];
        let tables = get_tables(expr);
        let names: Vec<&str> = tables
            .iter()
            .filter_map(|e| match e {
                Expression::Table(t) => Some(t.name.name.as_str()),
                _ => None,
            })
            .collect();

        assert!(names.contains(&"src"));
        assert!(names.contains(&"dim"));
    }

    // -- find_parent() tests --

    #[test]
    fn test_find_parent_binary() {
        let left = make_column("a");
        let right = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left,
            right,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        // Find the column child and get its parent
        let col = expr.find(is_column).unwrap();
        let parent = super::find_parent(&expr, col);
        assert!(parent.is_some());
        assert!(matches!(parent.unwrap(), Expression::Eq(_)));
    }

    #[test]
    fn test_find_parent_root_has_none() {
        let lit = make_literal(1);
        let parent = super::find_parent(&lit, &lit);
        assert!(parent.is_none());
    }

    // -- find_ancestor() tests --

    #[test]
    fn test_find_ancestor_select() {
        let ast = crate::parser::Parser::parse_sql("SELECT a FROM t WHERE a > 1").unwrap();
        let expr = &ast[0];

        // Find a column inside the WHERE clause
        let where_col = expr.dfs().find(|e| {
            if let Expression::Column(c) = e {
                c.name.name == "a"
            } else {
                false
            }
        });
        assert!(where_col.is_some());

        // Find Select ancestor of that column
        let ancestor = super::find_ancestor(expr, where_col.unwrap(), is_select);
        assert!(ancestor.is_some());
        assert!(matches!(ancestor.unwrap(), Expression::Select(_)));
    }

    #[test]
    fn test_find_ancestor_no_match() {
        let left = make_column("a");
        let right = make_literal(1);
        let expr = Expression::Eq(Box::new(BinaryOp {
            left,
            right,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let col = expr.find(is_column).unwrap();
        let ancestor = super::find_ancestor(&expr, col, is_select);
        assert!(ancestor.is_none());
    }

    #[test]
    fn test_ancestors() {
        let col = make_column("a");
        let lit = make_literal(1);
        let inner = Expression::Add(Box::new(BinaryOp {
            left: col,
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));
        let outer = Expression::Eq(Box::new(BinaryOp {
            left: make_column("b"),
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }));

        let ctx = TreeContext::build(&outer);

        // The inner Add's left child (column "a") should have ancestors
        // Node 0: Eq
        // Node 1: Column "b" (left of Eq)
        // Node 2: Add (right of Eq)
        // Node 3: Column "a" (left of Add)
        // Node 4: Literal (right of Add)

        let ancestors = ctx.ancestors_of(3);
        assert_eq!(ancestors, vec![2, 0]); // Add, then Eq
    }
}
