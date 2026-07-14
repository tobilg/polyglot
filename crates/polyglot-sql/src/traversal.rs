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

#![cfg_attr(
    not(any(feature = "ast-tools", feature = "generate", feature = "semantic")),
    allow(dead_code)
)]

use crate::expressions::{Expression, TableRef};
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

        crate::ast_children::for_each_child(expr, |child_path, child| {
            let (key, index) = child_location(child_path);
            self.path.push((id, key, index));
            self.visit_expr(child);
            self.path.pop();
        });

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

fn child_location(path: &[crate::ast_children::ChildPathSegment]) -> (String, Option<usize>) {
    use crate::ast_children::ChildPathSegment;

    let mut key = String::new();
    let mut index = None;
    for (position, segment) in path.iter().enumerate() {
        match segment {
            ChildPathSegment::Field(field) => {
                if !key.is_empty() {
                    key.push('.');
                }
                key.push_str(field);
            }
            ChildPathSegment::Index(value) if position + 1 == path.len() => {
                index = Some(*value);
            }
            ChildPathSegment::Index(value) => {
                key.push('[');
                key.push_str(&value.to_string());
                key.push(']');
            }
        }
    }
    (key, index)
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

        let child_start = self.stack.len();
        crate::ast_children::for_each_child(expr, |_, child| self.stack.push(child));
        self.stack[child_start..].reverse();

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

        crate::ast_children::for_each_child(expr, |_, child| self.queue.push_back(child));

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
    #[cfg(any(
        feature = "transpile",
        feature = "ast-tools",
        feature = "generate",
        feature = "semantic"
    ))]
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
        crate::ast_children::for_each_child(self, |_, child| result.push(child));
        result
    }

    fn tree_depth(&self) -> usize {
        let mut max_depth = 0usize;
        let mut stack = vec![(self, 0usize)];
        while let Some((node, depth)) = stack.pop() {
            max_depth = max_depth.max(depth);
            crate::ast_children::for_each_child(node, |_, child| {
                stack.push((child, depth + 1));
            });
        }
        max_depth
    }

    #[cfg(any(
        feature = "transpile",
        feature = "ast-tools",
        feature = "generate",
        feature = "semantic"
    ))]
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
#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
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
#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
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

/// Returns `true` if `expr` is an aggregate function.
pub fn is_aggregate(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::AggregateFunction(_)
            | Expression::Count(_)
            | Expression::Sum(_)
            | Expression::Avg(_)
            | Expression::Min(_)
            | Expression::Max(_)
            | Expression::GroupConcat(_)
            | Expression::StringAgg(_)
            | Expression::ListAgg(_)
            | Expression::CountIf(_)
            | Expression::SumIf(_)
    )
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
///
/// Note: DML target tables (`Insert.table`, `Update.table`, `Delete.table`) are
/// stored as `TableRef` struct fields, not as `Expression::Table` nodes, so they
/// are not reachable via tree traversal. Use [`get_all_tables`] to include those.
pub fn get_tables(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| matches!(e, Expression::Table(_)))
}

/// Collects **all** referenced tables from the expression tree, including DML
/// target tables that are stored as `TableRef` struct fields and are therefore
/// not reachable through normal tree traversal.
///
/// Returns owned `Expression::Table` values. This is the comprehensive version
/// of [`get_tables`] — use it when you need to discover every table referenced
/// in a statement, including inside CTE bodies containing INSERT/UPDATE/DELETE.
pub fn get_all_tables(expr: &Expression) -> Vec<Expression> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut result = Vec::new();

    // First: collect all Expression::Table nodes found via DFS.
    for node in expr.dfs() {
        if let Expression::Table(t) = node {
            let qname = table_ref_qualified_name(t);
            if seen.insert(qname) {
                result.push(node.clone());
            }
        }

        // Also extract DML target TableRef fields not reachable via iter_children.
        let refs: Vec<&TableRef> = match node {
            Expression::Insert(ins) => vec![&ins.table],
            Expression::Update(upd) => {
                let mut v = vec![&upd.table];
                v.extend(upd.extra_tables.iter());
                v
            }
            Expression::Delete(del) => {
                let mut v = vec![&del.table];
                v.extend(del.using.iter());
                v
            }
            _ => continue,
        };
        for tref in refs {
            if tref.name.name.is_empty() {
                continue;
            }
            let qname = table_ref_qualified_name(tref);
            if seen.insert(qname) {
                result.push(Expression::Table(Box::new(tref.clone())));
            }
        }
    }

    result
}

/// Build a qualified name string from a TableRef for deduplication purposes.
fn table_ref_qualified_name(t: &TableRef) -> String {
    let mut name = String::new();
    if let Some(ref cat) = t.catalog {
        name.push_str(&cat.name);
        name.push('.');
    }
    if let Some(ref schema) = t.schema {
        name.push_str(&schema.name);
        name.push('.');
    }
    name.push_str(&t.name.name);
    name
}

/// Extracts the underlying [`Expression::Table`] from a MERGE field that may
/// be a bare `Table`, an `Alias` wrapping a `Table`, or an `Identifier`.
/// Returns `None` if the expression doesn't contain a recognisable table.
fn unwrap_merge_table(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Table(_) => Some(expr),
        Expression::Alias(alias) => match &alias.this {
            Expression::Table(_) => Some(&alias.this),
            _ => None,
        },
        _ => None,
    }
}

/// Returns the target table of a MERGE statement (the `Merge.this` field),
/// unwrapping any alias wrapper to yield the underlying [`Expression::Table`].
///
/// Returns `None` if `expr` is not a `Merge` or the target isn't a recognisable table.
pub fn get_merge_target(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Merge(m) => unwrap_merge_table(&m.this),
        _ => None,
    }
}

/// Returns the source table of a MERGE statement (the `Merge.using` field),
/// unwrapping any alias wrapper to yield the underlying [`Expression::Table`].
///
/// Returns `None` if `expr` is not a `Merge`, the source isn't a recognisable
/// table (e.g. it's a subquery), or the source is otherwise unresolvable.
pub fn get_merge_source(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Merge(m) => unwrap_merge_table(&m.using),
        _ => None,
    }
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
is_type!(is_merge, Expression::Merge(_));
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

/// Returns `true` if `expr` is a query statement (SELECT, INSERT, UPDATE, DELETE, or MERGE).
pub fn is_query(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Select(_)
            | Expression::Insert(_)
            | Expression::Update(_)
            | Expression::Delete(_)
            | Expression::Merge(_)
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
            | Expression::Undrop(_)
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
            | Expression::CreateSynonym(_)
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
        let mut result = None;
        crate::ast_children::for_each_child(node, |_, child| {
            if result.is_none() {
                if std::ptr::eq(child, target) {
                    result = Some(node);
                } else {
                    result = search(child, target);
                }
            }
        });
        result
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
        let mut found = false;
        crate::ast_children::for_each_child(node, |_, child| {
            if !found {
                found = build_path(child, target, path);
            }
        });
        if found {
            return true;
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
    use crate::expressions::{BinaryOp, Column, Identifier, LikeOp, Literal, TableRef};

    fn make_column(name: &str) -> Expression {
        Expression::boxed_column(Column {
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
            inferred_type: None,
        })
    }

    fn make_literal(value: i64) -> Expression {
        Expression::Literal(Box::new(Literal::Number(value.to_string())))
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
            inferred_type: None,
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
            inferred_type: None,
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
            inferred_type: None,
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
            inferred_type: None,
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
            inferred_type: None,
        }));

        let expr = Expression::Eq(Box::new(BinaryOp {
            left: col1,
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
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
            inferred_type: None,
        }));
        assert_eq!(expr.tree_depth(), 1);

        // Two levels
        let inner = Expression::Add(Box::new(BinaryOp {
            left: make_column("b"),
            right: lit,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));
        let outer = Expression::Eq(Box::new(BinaryOp {
            left: make_column("a"),
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
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
            inferred_type: None,
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
                    return Ok(Expression::boxed_column(Column {
                        name: Identifier::new("alpha"),
                        table: c.table.clone(),
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
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
                return Ok(Expression::Literal(Box::new(Literal::Number(
                    if c.name.name == "a" { "1" } else { "2" }.to_string(),
                ))));
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
            inferred_type: None,
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
    fn test_children_follow_ast_field_order() {
        let ast = crate::parser::Parser::parse_sql("SELECT a, b FROM t").unwrap();
        let children = ast[0].children();

        assert!(matches!(children[0], Expression::Column(column) if column.name.name == "a"));
        assert!(matches!(children[1], Expression::Column(column) if column.name.name == "b"));
        assert!(matches!(children[2], Expression::Table(table) if table.name.name == "t"));
    }

    #[test]
    fn test_traversal_covers_previously_omitted_expression_fields() {
        let like = Expression::Like(Box::new(LikeOp {
            left: make_column("name"),
            right: Expression::Literal(Box::new(Literal::String("x%".to_string()))),
            escape: Some(Expression::Literal(Box::new(Literal::String(
                "!".to_string(),
            )))),
            quantifier: None,
            inferred_type: None,
        }));
        let nodes: Vec<_> = like.dfs().collect();
        assert_eq!(nodes.len(), 4);
        assert!(matches!(
            nodes[3],
            Expression::Literal(literal)
                if matches!(literal.as_ref(), Literal::String(value) if value == "!")
        ));

        let mut table = TableRef::new("events");
        table.hints.push(make_column("table_hint"));
        table.identifier_func = Some(Box::new(Expression::identifier("dynamic_table")));
        let table = Expression::Table(Box::new(table));
        let children = table.children();
        assert_eq!(children.len(), 2);
        assert!(
            matches!(children[0], Expression::Column(column) if column.name.name == "table_hint")
        );
        assert!(
            matches!(children[1], Expression::Identifier(identifier) if identifier.name == "dynamic_table")
        );
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
            inferred_type: None,
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
            inferred_type: None,
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
            inferred_type: None,
        }));
        let outer = Expression::Eq(Box::new(BinaryOp {
            left: make_column("b"),
            right: inner,
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
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

    #[test]
    fn test_get_merge_target_and_source() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::Generic);

        // MERGE with aliased target and source tables
        let sql = "MERGE INTO orders o USING customers c ON o.customer_id = c.id WHEN MATCHED THEN UPDATE SET amount = amount + 100";
        let exprs = dialect.parse(sql).unwrap();
        let expr = &exprs[0];

        assert!(is_merge(expr));
        assert!(is_query(expr));

        let target = get_merge_target(expr).expect("should find target table");
        assert!(matches!(target, Expression::Table(_)));
        if let Expression::Table(t) = target {
            assert_eq!(t.name.name, "orders");
        }

        let source = get_merge_source(expr).expect("should find source table");
        assert!(matches!(source, Expression::Table(_)));
        if let Expression::Table(t) = source {
            assert_eq!(t.name.name, "customers");
        }
    }

    #[test]
    fn test_get_merge_source_subquery_returns_none() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::Generic);

        // MERGE with subquery source — get_merge_source should return None
        let sql = "MERGE INTO orders o USING (SELECT * FROM customers) c ON o.customer_id = c.id WHEN MATCHED THEN DELETE";
        let exprs = dialect.parse(sql).unwrap();
        let expr = &exprs[0];

        assert!(get_merge_target(expr).is_some());
        assert!(get_merge_source(expr).is_none());
    }

    #[test]
    fn test_get_merge_on_non_merge_returns_none() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::Generic);
        let exprs = dialect.parse("SELECT 1").unwrap();
        assert!(get_merge_target(&exprs[0]).is_none());
        assert!(get_merge_source(&exprs[0]).is_none());
    }

    #[test]
    fn test_get_tables_finds_tables_inside_in_subquery() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::Generic);
        let sql = "SELECT id, name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 1000)";
        let exprs = dialect.parse(sql).unwrap();
        let tables = get_tables(&exprs[0]);
        let names: Vec<&str> = tables
            .iter()
            .filter_map(|e| {
                if let Expression::Table(t) = e {
                    Some(t.name.name.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(names.contains(&"customers"), "should find outer table");
        assert!(names.contains(&"orders"), "should find subquery table");
    }

    #[test]
    fn test_get_tables_finds_tables_inside_exists_subquery() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::Generic);
        let sql = "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)";
        let exprs = dialect.parse(sql).unwrap();
        let tables = get_tables(&exprs[0]);
        let names: Vec<&str> = tables
            .iter()
            .filter_map(|e| {
                if let Expression::Table(t) = e {
                    Some(t.name.name.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(names.contains(&"customers"), "should find outer table");
        assert!(
            names.contains(&"orders"),
            "should find EXISTS subquery table"
        );
    }

    #[test]
    fn test_get_tables_finds_tables_in_correlated_subquery() {
        let dialect = crate::Dialect::get(crate::dialects::DialectType::TSQL);
        let sql = "SELECT id, name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 1000)";
        let exprs = dialect.parse(sql).unwrap();
        let tables = get_tables(&exprs[0]);
        let names: Vec<&str> = tables
            .iter()
            .filter_map(|e| {
                if let Expression::Table(t) = e {
                    Some(t.name.name.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(
            names.contains(&"customers"),
            "TSQL: should find outer table"
        );
        assert!(
            names.contains(&"orders"),
            "TSQL: should find subquery table"
        );
    }
}
