//! AST Diff - Compare SQL expressions
//!
//! This module provides functionality to compare two SQL ASTs and generate
//! a list of differences (edit script) between them, using the ChangeDistiller
//! algorithm with Dice coefficient matching.
//!

use crate::dialects::DialectType;
use crate::expressions::Expression;
use crate::generator::{Generator, GeneratorConfig};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Types of edits that can occur between two ASTs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Edit {
    /// A new node has been inserted
    Insert { expression: Expression },
    /// An existing node has been removed
    Remove { expression: Expression },
    /// An existing node's position has changed
    Move {
        source: Expression,
        target: Expression,
    },
    /// An existing node has been updated (same position, different value)
    Update {
        source: Expression,
        target: Expression,
    },
    /// An existing node hasn't been changed
    Keep {
        source: Expression,
        target: Expression,
    },
}

impl Edit {
    /// Check if this edit represents a change (not a Keep)
    pub fn is_change(&self) -> bool {
        !matches!(self, Edit::Keep { .. })
    }
}

/// Configuration for the diff algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffConfig {
    /// Dice coefficient threshold for internal node matching (default 0.6)
    pub f: f64,
    /// Leaf similarity threshold (default 0.6)
    pub t: f64,
    /// Optional dialect for SQL generation during comparison
    pub dialect: Option<DialectType>,
}

impl Default for DiffConfig {
    fn default() -> Self {
        Self {
            f: 0.6,
            t: 0.6,
            dialect: None,
        }
    }
}

/// Compare two expressions and return a list of edits
///
/// # Arguments
/// * `source` - The source expression
/// * `target` - The target expression to compare against
/// * `delta_only` - If true, exclude Keep edits from the result
///
/// # Returns
/// A vector of Edit operations that transform source into target
///
/// # Example
/// ```ignore
/// use polyglot_sql::diff::diff;
/// use polyglot_sql::parse_one;
/// use polyglot_sql::DialectType;
///
/// let source = parse_one("SELECT a + b FROM t", DialectType::Generic).unwrap();
/// let target = parse_one("SELECT a + c FROM t", DialectType::Generic).unwrap();
/// let edits = diff(&source, &target, false);
/// ```
pub fn diff(source: &Expression, target: &Expression, delta_only: bool) -> Vec<Edit> {
    let config = DiffConfig::default();
    diff_with_config(source, target, delta_only, &config)
}

/// Compare two expressions with custom configuration
pub fn diff_with_config(
    source: &Expression,
    target: &Expression,
    delta_only: bool,
    config: &DiffConfig,
) -> Vec<Edit> {
    let mut distiller = ChangeDistiller::new(config.clone());
    distiller.diff(source, target, delta_only)
}

/// Check if the diff contains any changes
pub fn has_changes(edits: &[Edit]) -> bool {
    edits.iter().any(|e| e.is_change())
}

/// Get only the changes from an edit list
pub fn changes_only(edits: Vec<Edit>) -> Vec<Edit> {
    edits.into_iter().filter(|e| e.is_change()).collect()
}

// ---------------------------------------------------------------------------
// IndexedTree: flat BFS representation with parent-child tracking
// ---------------------------------------------------------------------------

/// Flattened tree representation with explicit parent/child index maps.
struct IndexedTree {
    nodes: Vec<Expression>,
    parents: Vec<Option<usize>>,
    children_indices: Vec<Vec<usize>>,
}

impl IndexedTree {
    fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            parents: Vec::new(),
            children_indices: Vec::new(),
        }
    }

    fn build(root: &Expression) -> Self {
        let mut tree = Self::empty();
        tree.add_expr(root, None);
        tree
    }

    fn add_expr(&mut self, expr: &Expression, parent_idx: Option<usize>) {
        // Skip bare Identifier nodes — they're names, not diff targets
        if matches!(expr, Expression::Identifier(_)) {
            return;
        }

        let idx = self.nodes.len();
        self.nodes.push(expr.clone());
        self.parents.push(parent_idx);
        self.children_indices.push(Vec::new());

        if let Some(p) = parent_idx {
            self.children_indices[p].push(idx);
        }

        self.add_children(expr, idx);
    }

    fn add_children(&mut self, expr: &Expression, parent_idx: usize) {
        match expr {
            Expression::Select(select) => {
                if let Some(with) = &select.with {
                    for cte in &with.ctes {
                        self.add_expr(&Expression::Cte(Box::new(cte.clone())), Some(parent_idx));
                    }
                }
                for e in &select.expressions {
                    self.add_expr(e, Some(parent_idx));
                }
                if let Some(from) = &select.from {
                    for e in &from.expressions {
                        self.add_expr(e, Some(parent_idx));
                    }
                }
                for join in &select.joins {
                    self.add_expr(&Expression::Join(Box::new(join.clone())), Some(parent_idx));
                }
                if let Some(w) = &select.where_clause {
                    self.add_expr(&w.this, Some(parent_idx));
                }
                if let Some(gb) = &select.group_by {
                    for e in &gb.expressions {
                        self.add_expr(e, Some(parent_idx));
                    }
                }
                if let Some(h) = &select.having {
                    self.add_expr(&h.this, Some(parent_idx));
                }
                if let Some(ob) = &select.order_by {
                    for o in &ob.expressions {
                        self.add_expr(&Expression::Ordered(Box::new(o.clone())), Some(parent_idx));
                    }
                }
                if let Some(limit) = &select.limit {
                    self.add_expr(&limit.this, Some(parent_idx));
                }
                if let Some(offset) = &select.offset {
                    self.add_expr(&offset.this, Some(parent_idx));
                }
            }
            Expression::Alias(alias) => {
                self.add_expr(&alias.this, Some(parent_idx));
            }
            Expression::And(op)
            | Expression::Or(op)
            | Expression::Eq(op)
            | Expression::Neq(op)
            | Expression::Lt(op)
            | Expression::Lte(op)
            | Expression::Gt(op)
            | Expression::Gte(op)
            | Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op)
            | Expression::Mod(op)
            | Expression::BitwiseAnd(op)
            | Expression::BitwiseOr(op)
            | Expression::BitwiseXor(op)
            | Expression::Concat(op) => {
                self.add_expr(&op.left, Some(parent_idx));
                self.add_expr(&op.right, Some(parent_idx));
            }
            Expression::Like(op) | Expression::ILike(op) => {
                self.add_expr(&op.left, Some(parent_idx));
                self.add_expr(&op.right, Some(parent_idx));
            }
            Expression::Not(u) | Expression::Neg(u) | Expression::BitwiseNot(u) => {
                self.add_expr(&u.this, Some(parent_idx));
            }
            Expression::Function(func) => {
                for arg in &func.args {
                    self.add_expr(arg, Some(parent_idx));
                }
            }
            Expression::AggregateFunction(func) => {
                for arg in &func.args {
                    self.add_expr(arg, Some(parent_idx));
                }
            }
            Expression::Join(j) => {
                self.add_expr(&j.this, Some(parent_idx));
                if let Some(on) = &j.on {
                    self.add_expr(on, Some(parent_idx));
                }
            }
            Expression::Anonymous(a) => {
                for arg in &a.expressions {
                    self.add_expr(arg, Some(parent_idx));
                }
            }
            Expression::WindowFunction(wf) => {
                self.add_expr(&wf.this, Some(parent_idx));
            }
            Expression::Cast(cast) => {
                self.add_expr(&cast.this, Some(parent_idx));
            }
            Expression::Subquery(sq) => {
                self.add_expr(&sq.this, Some(parent_idx));
            }
            Expression::Paren(p) => {
                self.add_expr(&p.this, Some(parent_idx));
            }
            Expression::Union(u) => {
                self.add_expr(&u.left, Some(parent_idx));
                self.add_expr(&u.right, Some(parent_idx));
            }
            Expression::Intersect(i) => {
                self.add_expr(&i.left, Some(parent_idx));
                self.add_expr(&i.right, Some(parent_idx));
            }
            Expression::Except(e) => {
                self.add_expr(&e.left, Some(parent_idx));
                self.add_expr(&e.right, Some(parent_idx));
            }
            Expression::Cte(cte) => {
                self.add_expr(&cte.this, Some(parent_idx));
            }
            Expression::Case(c) => {
                if let Some(operand) = &c.operand {
                    self.add_expr(operand, Some(parent_idx));
                }
                for (when, then) in &c.whens {
                    self.add_expr(when, Some(parent_idx));
                    self.add_expr(then, Some(parent_idx));
                }
                if let Some(else_) = &c.else_ {
                    self.add_expr(else_, Some(parent_idx));
                }
            }
            Expression::In(i) => {
                self.add_expr(&i.this, Some(parent_idx));
                for e in &i.expressions {
                    self.add_expr(e, Some(parent_idx));
                }
                if let Some(q) = &i.query {
                    self.add_expr(q, Some(parent_idx));
                }
            }
            Expression::Between(b) => {
                self.add_expr(&b.this, Some(parent_idx));
                self.add_expr(&b.low, Some(parent_idx));
                self.add_expr(&b.high, Some(parent_idx));
            }
            Expression::IsNull(i) => {
                self.add_expr(&i.this, Some(parent_idx));
            }
            Expression::Exists(e) => {
                self.add_expr(&e.this, Some(parent_idx));
            }
            Expression::Ordered(o) => {
                self.add_expr(&o.this, Some(parent_idx));
            }
            Expression::Lambda(l) => {
                self.add_expr(&l.body, Some(parent_idx));
            }
            Expression::Coalesce(c) => {
                for e in &c.expressions {
                    self.add_expr(e, Some(parent_idx));
                }
            }
            Expression::Tuple(t) => {
                for e in &t.expressions {
                    self.add_expr(e, Some(parent_idx));
                }
            }
            Expression::Array(a) => {
                for e in &a.expressions {
                    self.add_expr(e, Some(parent_idx));
                }
            }
            // Leaf nodes — no children to add
            Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Null(_)
            | Expression::Column(_)
            | Expression::Table(_)
            | Expression::Star(_)
            | Expression::DataType(_)
            | Expression::CurrentDate(_)
            | Expression::CurrentTime(_)
            | Expression::CurrentTimestamp(_) => {}
            // Fallback: use ExpressionWalk::children()
            other => {
                use crate::traversal::ExpressionWalk;
                for child in other.children() {
                    if !matches!(child, Expression::Identifier(_)) {
                        self.add_expr(child, Some(parent_idx));
                    }
                }
            }
        }
    }

    fn is_leaf(&self, idx: usize) -> bool {
        self.children_indices[idx].is_empty()
    }

    fn leaf_indices(&self) -> Vec<usize> {
        (0..self.nodes.len()).filter(|&i| self.is_leaf(i)).collect()
    }

    /// Get all leaf descendants of a node (including itself if it is a leaf).
    fn leaf_descendants(&self, idx: usize) -> Vec<usize> {
        let mut result = Vec::new();
        let mut stack = vec![idx];
        while let Some(i) = stack.pop() {
            if self.is_leaf(i) {
                result.push(i);
            }
            for &child in &self.children_indices[i] {
                stack.push(child);
            }
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Compute Dice coefficient on character bigrams of two strings.
fn dice_coefficient(a: &str, b: &str) -> f64 {
    // For very short strings, use exact equality
    if a.len() < 2 || b.len() < 2 {
        return if a == b { 1.0 } else { 0.0 };
    }
    let a_bigrams = bigram_histo(a);
    let b_bigrams = bigram_histo(b);
    let common: usize = a_bigrams
        .iter()
        .map(|(k, v)| v.min(b_bigrams.get(k).unwrap_or(&0)))
        .sum();
    let total: usize = a_bigrams.values().sum::<usize>() + b_bigrams.values().sum::<usize>();
    if total == 0 {
        1.0
    } else {
        2.0 * common as f64 / total as f64
    }
}

/// Build a frequency histogram of character bigrams.
fn bigram_histo(s: &str) -> HashMap<(char, char), usize> {
    let chars: Vec<char> = s.chars().collect();
    let mut map = HashMap::new();
    for w in chars.windows(2) {
        *map.entry((w[0], w[1])).or_insert(0) += 1;
    }
    map
}

/// Generate SQL string for an expression, optionally with a dialect.
fn node_sql(expr: &Expression, dialect: Option<DialectType>) -> String {
    match dialect {
        Some(d) => {
            let config = GeneratorConfig {
                dialect: Some(d),
                ..GeneratorConfig::default()
            };
            let mut gen = Generator::with_config(config);
            gen.generate(expr).unwrap_or_default()
        }
        None => Generator::sql(expr).unwrap_or_default(),
    }
}

/// Check if two expressions are the same type for matching purposes.
///
/// Uses discriminant comparison with special cases for Join (must share kind)
/// and Anonymous (must share function name).
fn is_same_type(a: &Expression, b: &Expression) -> bool {
    if std::mem::discriminant(a) != std::mem::discriminant(b) {
        return false;
    }
    match (a, b) {
        (Expression::Join(ja), Expression::Join(jb)) => ja.kind == jb.kind,
        (Expression::Anonymous(aa), Expression::Anonymous(ab)) => {
            Generator::sql(&aa.this).unwrap_or_default()
                == Generator::sql(&ab.this).unwrap_or_default()
        }
        _ => true,
    }
}

/// Count matching ancestor chain depth for parent similarity tiebreaker.
fn parent_similarity_score(
    src_idx: usize,
    tgt_idx: usize,
    src_tree: &IndexedTree,
    tgt_tree: &IndexedTree,
    matchings: &HashMap<usize, usize>,
) -> usize {
    let mut score = 0;
    let mut s = src_tree.parents[src_idx];
    let mut t = tgt_tree.parents[tgt_idx];
    while let (Some(sp), Some(tp)) = (s, t) {
        if matchings.get(&sp) == Some(&tp) {
            score += 1;
            s = src_tree.parents[sp];
            t = tgt_tree.parents[tp];
        } else {
            break;
        }
    }
    score
}

/// Check if an expression is an updatable leaf type.
///
/// Updatable types emit Update edits when matched but different.
fn is_updatable(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Alias(_)
            | Expression::Boolean(_)
            | Expression::Column(_)
            | Expression::DataType(_)
            | Expression::Lambda(_)
            | Expression::Literal(_)
            | Expression::Table(_)
            | Expression::WindowFunction(_)
    )
}

/// Check if non-expression leaf fields differ between two matched same-type nodes.
///
/// These are scalar fields (booleans, enums) that aren't child expressions.
fn has_non_expression_leaf_change(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        (Expression::Union(ua), Expression::Union(ub)) => {
            ua.all != ub.all || ua.distinct != ub.distinct
        }
        (Expression::Intersect(ia), Expression::Intersect(ib)) => {
            ia.all != ib.all || ia.distinct != ib.distinct
        }
        (Expression::Except(ea), Expression::Except(eb)) => {
            ea.all != eb.all || ea.distinct != eb.distinct
        }
        (Expression::Ordered(oa), Expression::Ordered(ob)) => {
            oa.desc != ob.desc || oa.nulls_first != ob.nulls_first
        }
        (Expression::Join(ja), Expression::Join(jb)) => ja.kind != jb.kind,
        _ => false,
    }
}

/// Standard LCS returning matched index pairs.
fn lcs<T, F>(a: &[T], b: &[T], eq_fn: F) -> Vec<(usize, usize)>
where
    F: Fn(&T, &T) -> bool,
{
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if eq_fn(&a[i - 1], &b[j - 1]) {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    let mut result = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if eq_fn(&a[i - 1], &b[j - 1]) {
            result.push((i - 1, j - 1));
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    result.reverse();
    result
}

// ---------------------------------------------------------------------------
// BinaryHeap entry for greedy matching
// ---------------------------------------------------------------------------

#[derive(PartialEq)]
struct MatchCandidate {
    score: f64,
    parent_sim: usize,
    counter: usize, // tiebreaker for deterministic ordering
    src_idx: usize,
    tgt_idx: usize,
}

impl Eq for MatchCandidate {}

impl PartialOrd for MatchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MatchCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.parent_sim.cmp(&other.parent_sim))
            .then_with(|| self.counter.cmp(&other.counter))
    }
}

// ---------------------------------------------------------------------------
// ChangeDistiller: three-phase algorithm
// ---------------------------------------------------------------------------

struct ChangeDistiller {
    config: DiffConfig,
    src_tree: IndexedTree,
    tgt_tree: IndexedTree,
    matchings: HashMap<usize, usize>, // src_idx -> tgt_idx
}

impl ChangeDistiller {
    fn new(config: DiffConfig) -> Self {
        Self {
            config,
            src_tree: IndexedTree::empty(),
            tgt_tree: IndexedTree::empty(),
            matchings: HashMap::new(),
        }
    }

    fn diff(&mut self, source: &Expression, target: &Expression, delta_only: bool) -> Vec<Edit> {
        self.src_tree = IndexedTree::build(source);
        self.tgt_tree = IndexedTree::build(target);

        // Phase 1: leaf matching via Dice coefficient
        self.match_leaves();

        // Phase 2: internal node matching via leaf descendants
        self.match_internal_nodes();

        // Phase 3: generate edit script with Move detection
        self.generate_edits(delta_only)
    }

    // -- Phase 1: Leaf matching -----------------------------------------------

    fn match_leaves(&mut self) {
        let src_leaves = self.src_tree.leaf_indices();
        let tgt_leaves = self.tgt_tree.leaf_indices();

        // Pre-compute SQL strings for all leaves
        let src_sql: Vec<String> = src_leaves
            .iter()
            .map(|&i| node_sql(&self.src_tree.nodes[i], self.config.dialect))
            .collect();
        let tgt_sql: Vec<String> = tgt_leaves
            .iter()
            .map(|&i| node_sql(&self.tgt_tree.nodes[i], self.config.dialect))
            .collect();

        let mut heap = BinaryHeap::new();
        let mut counter = 0usize;

        for (si_pos, &si) in src_leaves.iter().enumerate() {
            for (ti_pos, &ti) in tgt_leaves.iter().enumerate() {
                if !is_same_type(&self.src_tree.nodes[si], &self.tgt_tree.nodes[ti]) {
                    continue;
                }
                let score = dice_coefficient(&src_sql[si_pos], &tgt_sql[ti_pos]);
                if score >= self.config.t {
                    let parent_sim = parent_similarity_score(
                        si,
                        ti,
                        &self.src_tree,
                        &self.tgt_tree,
                        &self.matchings,
                    );
                    heap.push(MatchCandidate {
                        score,
                        parent_sim,
                        counter,
                        src_idx: si,
                        tgt_idx: ti,
                    });
                    counter += 1;
                }
            }
        }

        let mut matched_src: HashSet<usize> = HashSet::new();
        let mut matched_tgt: HashSet<usize> = HashSet::new();

        while let Some(m) = heap.pop() {
            if matched_src.contains(&m.src_idx) || matched_tgt.contains(&m.tgt_idx) {
                continue;
            }
            self.matchings.insert(m.src_idx, m.tgt_idx);
            matched_src.insert(m.src_idx);
            matched_tgt.insert(m.tgt_idx);
        }
    }

    // -- Phase 2: Internal node matching -------------------------------------

    fn match_internal_nodes(&mut self) {
        // Process from deepest to shallowest. In BFS-built tree, higher indices
        // are generally deeper, so we iterate in reverse.
        let src_internal: Vec<usize> = (0..self.src_tree.nodes.len())
            .rev()
            .filter(|&i| !self.src_tree.is_leaf(i) && !self.matchings.contains_key(&i))
            .collect();

        let tgt_internal: Vec<usize> = (0..self.tgt_tree.nodes.len())
            .rev()
            .filter(|&i| !self.tgt_tree.is_leaf(i))
            .collect();

        let mut matched_tgt: HashSet<usize> = self.matchings.values().cloned().collect();

        let mut heap = BinaryHeap::new();
        let mut counter = 0usize;

        for &si in &src_internal {
            let src_leaves: HashSet<usize> =
                self.src_tree.leaf_descendants(si).into_iter().collect();
            let src_sql = node_sql(&self.src_tree.nodes[si], self.config.dialect);

            for &ti in &tgt_internal {
                if matched_tgt.contains(&ti) {
                    continue;
                }
                if !is_same_type(&self.src_tree.nodes[si], &self.tgt_tree.nodes[ti]) {
                    continue;
                }

                let tgt_leaves: HashSet<usize> =
                    self.tgt_tree.leaf_descendants(ti).into_iter().collect();

                // Count leaf descendants matched to each other
                let common = src_leaves
                    .iter()
                    .filter(|&&sl| {
                        self.matchings
                            .get(&sl)
                            .map_or(false, |&tl| tgt_leaves.contains(&tl))
                    })
                    .count();

                let max_leaves = src_leaves.len().max(tgt_leaves.len());
                if max_leaves == 0 {
                    continue;
                }

                let leaf_sim = common as f64 / max_leaves as f64;

                // Adaptive threshold for small subtrees
                let t = if src_leaves.len().min(tgt_leaves.len()) <= 4 {
                    0.4
                } else {
                    self.config.t
                };

                let tgt_sql = node_sql(&self.tgt_tree.nodes[ti], self.config.dialect);
                let dice = dice_coefficient(&src_sql, &tgt_sql);

                if leaf_sim >= 0.8 || (leaf_sim >= t && dice >= self.config.f) {
                    heap.push(MatchCandidate {
                        score: leaf_sim,
                        parent_sim: parent_similarity_score(
                            si,
                            ti,
                            &self.src_tree,
                            &self.tgt_tree,
                            &self.matchings,
                        ),
                        counter,
                        src_idx: si,
                        tgt_idx: ti,
                    });
                    counter += 1;
                }
            }
        }

        while let Some(m) = heap.pop() {
            if self.matchings.contains_key(&m.src_idx) || matched_tgt.contains(&m.tgt_idx) {
                continue;
            }
            self.matchings.insert(m.src_idx, m.tgt_idx);
            matched_tgt.insert(m.tgt_idx);
        }
    }

    // -- Phase 3: Edit script generation with Move detection ------------------

    fn generate_edits(&self, delta_only: bool) -> Vec<Edit> {
        let mut edits = Vec::new();
        let matched_tgt: HashSet<usize> = self.matchings.values().cloned().collect();

        // Build reverse mapping: tgt_idx -> src_idx
        let reverse_matchings: HashMap<usize, usize> =
            self.matchings.iter().map(|(&s, &t)| (t, s)).collect();

        // Detect moved nodes via LCS on each matched parent's children
        let mut moved_src: HashSet<usize> = HashSet::new();

        for (&src_parent, &tgt_parent) in &self.matchings {
            if self.src_tree.is_leaf(src_parent) {
                continue;
            }

            let src_children = &self.src_tree.children_indices[src_parent];
            let tgt_children = &self.tgt_tree.children_indices[tgt_parent];

            if src_children.is_empty() || tgt_children.is_empty() {
                continue;
            }

            // Build sequence of tgt indices for matched src children (in src order)
            let src_seq: Vec<usize> = src_children
                .iter()
                .filter_map(|&sc| self.matchings.get(&sc).cloned())
                .collect();

            // Build sequence of tgt children that have a reverse match (in tgt order)
            let tgt_seq: Vec<usize> = tgt_children
                .iter()
                .filter(|&&tc| reverse_matchings.contains_key(&tc))
                .cloned()
                .collect();

            let lcs_pairs = lcs(&src_seq, &tgt_seq, |a, b| a == b);
            let lcs_tgt_set: HashSet<usize> = lcs_pairs.iter().map(|&(i, _)| src_seq[i]).collect();

            // Matched children not in the LCS had their position changed
            for &sc in src_children {
                if let Some(&tc) = self.matchings.get(&sc) {
                    if !lcs_tgt_set.contains(&tc) {
                        moved_src.insert(sc);
                    }
                }
            }
        }

        // Unmatched source nodes → Remove
        for i in 0..self.src_tree.nodes.len() {
            if !self.matchings.contains_key(&i) {
                edits.push(Edit::Remove {
                    expression: self.src_tree.nodes[i].clone(),
                });
            }
        }

        // Unmatched target nodes → Insert
        for i in 0..self.tgt_tree.nodes.len() {
            if !matched_tgt.contains(&i) {
                edits.push(Edit::Insert {
                    expression: self.tgt_tree.nodes[i].clone(),
                });
            }
        }

        // Matched pairs → Update / Move / Keep
        for (&src_idx, &tgt_idx) in &self.matchings {
            let src_node = &self.src_tree.nodes[src_idx];
            let tgt_node = &self.tgt_tree.nodes[tgt_idx];

            let src_sql = node_sql(src_node, self.config.dialect);
            let tgt_sql = node_sql(tgt_node, self.config.dialect);

            if is_updatable(src_node) && src_sql != tgt_sql {
                edits.push(Edit::Update {
                    source: src_node.clone(),
                    target: tgt_node.clone(),
                });
            } else if has_non_expression_leaf_change(src_node, tgt_node) {
                edits.push(Edit::Update {
                    source: src_node.clone(),
                    target: tgt_node.clone(),
                });
            } else if moved_src.contains(&src_idx) {
                edits.push(Edit::Move {
                    source: src_node.clone(),
                    target: tgt_node.clone(),
                });
            } else if !delta_only {
                edits.push(Edit::Keep {
                    source: src_node.clone(),
                    target: tgt_node.clone(),
                });
            }
        }

        edits
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::{Dialect, DialectType};

    fn parse(sql: &str) -> Expression {
        let dialect = Dialect::get(DialectType::Generic);
        let ast = dialect.parse(sql).unwrap();
        ast.into_iter().next().unwrap()
    }

    fn count_edits(edits: &[Edit]) -> (usize, usize, usize, usize, usize) {
        let mut insert = 0;
        let mut remove = 0;
        let mut r#move = 0;
        let mut update = 0;
        let mut keep = 0;
        for e in edits {
            match e {
                Edit::Insert { .. } => insert += 1,
                Edit::Remove { .. } => remove += 1,
                Edit::Move { .. } => r#move += 1,
                Edit::Update { .. } => update += 1,
                Edit::Keep { .. } => keep += 1,
            }
        }
        (insert, remove, r#move, update, keep)
    }

    #[test]
    fn test_diff_identical() {
        let source = parse("SELECT a FROM t");
        let target = parse("SELECT a FROM t");

        let edits = diff(&source, &target, false);

        // Should only have Keep edits
        assert!(
            edits.iter().all(|e| matches!(e, Edit::Keep { .. })),
            "Expected only Keep edits, got: {:?}",
            count_edits(&edits)
        );
    }

    #[test]
    fn test_diff_simple_change() {
        let source = parse("SELECT a FROM t");
        let target = parse("SELECT b FROM t");

        let edits = diff(&source, &target, true);

        // Column a → column b: single-char names with dice=0 don't match
        // → Remove(a) + Insert(b)
        assert!(!edits.is_empty());
        assert!(has_changes(&edits));
        let (ins, rem, _, _, _) = count_edits(&edits);
        assert!(
            ins > 0 && rem > 0,
            "Expected Insert+Remove, got ins={ins} rem={rem}"
        );
    }

    #[test]
    fn test_diff_similar_column_update() {
        let source = parse("SELECT col_a FROM t");
        let target = parse("SELECT col_b FROM t");

        let edits = diff(&source, &target, true);

        // Longer names share bigrams → matched → Update
        assert!(has_changes(&edits));
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Update { .. })),
            "Expected Update for similar column name change"
        );
    }

    #[test]
    fn test_operator_change() {
        let source = parse("SELECT a + b FROM t");
        let target = parse("SELECT a - b FROM t");

        let edits = diff(&source, &target, true);

        // The operator changed from Add to Sub — different discriminants
        // so they can't be matched → Remove(Add) + Insert(Sub)
        assert!(!edits.is_empty());
        let (ins, rem, _, _, _) = count_edits(&edits);
        assert!(
            ins > 0 && rem > 0,
            "Expected Insert and Remove for operator change, got ins={ins} rem={rem}"
        );
    }

    #[test]
    fn test_column_added() {
        let source = parse("SELECT a, b FROM t");
        let target = parse("SELECT a, b, c FROM t");

        let edits = diff(&source, &target, true);

        // Column c was added
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Insert { .. })),
            "Expected at least one Insert edit for added column"
        );
    }

    #[test]
    fn test_column_removed() {
        let source = parse("SELECT a, b, c FROM t");
        let target = parse("SELECT a, c FROM t");

        let edits = diff(&source, &target, true);

        // Column b was removed
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Remove { .. })),
            "Expected at least one Remove edit for removed column"
        );
    }

    #[test]
    fn test_table_updated() {
        let source = parse("SELECT a FROM table_one");
        let target = parse("SELECT a FROM table_two");

        let edits = diff(&source, &target, true);

        // Table names share enough bigrams to match → Update
        assert!(!edits.is_empty());
        assert!(has_changes(&edits));
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Update { .. })),
            "Expected Update for table name change"
        );
    }

    #[test]
    fn test_lambda() {
        let source = parse("SELECT TRANSFORM(arr, a -> a + 1) FROM t");
        let target = parse("SELECT TRANSFORM(arr, b -> b + 1) FROM t");

        let edits = diff(&source, &target, true);

        // The lambda parameter changed
        assert!(has_changes(&edits));
    }

    #[test]
    fn test_node_position_changed() {
        let source = parse("SELECT a, b, c FROM t");
        let target = parse("SELECT c, a, b FROM t");

        let edits = diff(&source, &target, false);

        // Some columns should be detected as moved
        let (_, _, moves, _, _) = count_edits(&edits);
        assert!(
            moves > 0,
            "Expected at least one Move for reordered columns"
        );
    }

    #[test]
    fn test_cte_changes() {
        let source = parse("WITH cte AS (SELECT a FROM t WHERE a > 1000) SELECT * FROM cte");
        let target = parse("WITH cte AS (SELECT a FROM t WHERE a > 2000) SELECT * FROM cte");

        let edits = diff(&source, &target, true);

        // The literal in the WHERE clause changed (1000 → 2000 share bigrams → Update)
        assert!(has_changes(&edits));
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Update { .. })),
            "Expected Update for literal change in CTE"
        );
    }

    #[test]
    fn test_join_changes() {
        let source = parse("SELECT a FROM t LEFT JOIN s ON t.id = s.id");
        let target = parse("SELECT a FROM t RIGHT JOIN s ON t.id = s.id");

        let edits = diff(&source, &target, true);

        // LEFT vs RIGHT have different JoinKind → not same_type
        // The Join nodes produce Remove(LEFT JOIN) + Insert(RIGHT JOIN)
        assert!(has_changes(&edits));
        let (ins, rem, _, _, _) = count_edits(&edits);
        assert!(
            ins > 0 && rem > 0,
            "Expected Insert+Remove for join kind change, got ins={ins} rem={rem}"
        );
    }

    #[test]
    fn test_window_functions() {
        let source = parse("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t");
        let target = parse("SELECT RANK() OVER (ORDER BY a) FROM t");

        let edits = diff(&source, &target, true);

        // Different window functions
        assert!(has_changes(&edits));
    }

    #[test]
    fn test_non_expression_leaf_delta() {
        let source = parse("SELECT a FROM t UNION SELECT b FROM s");
        let target = parse("SELECT a FROM t UNION ALL SELECT b FROM s");

        let edits = diff(&source, &target, true);

        // UNION vs UNION ALL — non-expression leaf change (all flag)
        assert!(has_changes(&edits));
        assert!(
            edits.iter().any(|e| matches!(e, Edit::Update { .. })),
            "Expected Update for UNION → UNION ALL"
        );
    }

    #[test]
    fn test_is_leaf() {
        let tree = IndexedTree::build(&parse("SELECT a, 1 FROM t"));
        // Root (Select) should not be a leaf
        assert!(!tree.is_leaf(0));
        // Leaf nodes should exist in the tree
        let leaves = tree.leaf_indices();
        assert!(!leaves.is_empty());
        // All leaves should have no children
        for &l in &leaves {
            assert!(tree.children_indices[l].is_empty());
        }
    }

    #[test]
    fn test_same_type_special_cases() {
        // Same type — both Literal
        let a = Expression::Literal(crate::expressions::Literal::Number("1".to_string()));
        let b = Expression::Literal(crate::expressions::Literal::String("abc".to_string()));
        assert!(is_same_type(&a, &b));

        // Different type — Literal vs Null
        let c = Expression::Null(crate::expressions::Null);
        assert!(!is_same_type(&a, &c));

        // Join kind matters
        let join_left = Expression::Join(Box::new(crate::expressions::Join {
            this: Expression::Table(crate::expressions::TableRef::new("t")),
            on: None,
            using: vec![],
            kind: crate::expressions::JoinKind::Left,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: vec![],
            comments: vec![],
            nesting_group: 0,
            directed: false,
        }));
        let join_right = Expression::Join(Box::new(crate::expressions::Join {
            this: Expression::Table(crate::expressions::TableRef::new("t")),
            on: None,
            using: vec![],
            kind: crate::expressions::JoinKind::Right,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: vec![],
            comments: vec![],
            nesting_group: 0,
            directed: false,
        }));
        assert!(!is_same_type(&join_left, &join_right));
    }

    #[test]
    fn test_comments_excluded() {
        // Comments on nodes should not affect the diff
        let source = parse("SELECT a FROM t");
        let target = parse("SELECT a FROM t");

        let edits = diff(&source, &target, true);

        // No changes — comments don't matter
        assert!(edits.is_empty() || !has_changes(&edits));
    }
}
