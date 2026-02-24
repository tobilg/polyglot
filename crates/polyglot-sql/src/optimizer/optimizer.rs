//! Optimizer Orchestration Module
//!
//! This module provides the main entry point for SQL optimization,
//! coordinating multiple optimization passes in the correct order.
//!
//! Ported from sqlglot's optimizer/optimizer.py

use crate::dialects::DialectType;
use crate::expressions::Expression;
use crate::schema::Schema;
use crate::traversal::ExpressionWalk;

use super::annotate_types::annotate_types;
use super::canonicalize::canonicalize;
use super::eliminate_ctes::eliminate_ctes;
use super::normalize::normalize;
use super::optimize_joins::optimize_joins;
use super::pushdown_predicates::pushdown_predicates;
use super::pushdown_projections::pushdown_projections;
use super::qualify_columns::qualify_columns;
use super::simplify::simplify;
use super::subquery::{merge_subqueries, unnest_subqueries};

/// Optimizer configuration
pub struct OptimizerConfig<'a> {
    /// Database schema for type inference and column resolution
    pub schema: Option<&'a dyn Schema>,
    /// Default database name
    pub db: Option<String>,
    /// Default catalog name
    pub catalog: Option<String>,
    /// Dialect for dialect-specific optimizations
    pub dialect: Option<DialectType>,
    /// Whether to keep tables isolated (don't merge from multiple tables)
    pub isolate_tables: bool,
    /// Whether to quote identifiers
    pub quote_identifiers: bool,
}

impl<'a> Default for OptimizerConfig<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            db: None,
            catalog: None,
            dialect: None,
            isolate_tables: true,
            quote_identifiers: false,
        }
    }
}

/// Optimization rule type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationRule {
    /// Qualify columns and tables with their full names
    Qualify,
    /// Push projections down to eliminate unused columns early
    PushdownProjections,
    /// Normalize boolean expressions
    Normalize,
    /// Unnest correlated subqueries into joins
    UnnestSubqueries,
    /// Push predicates down to filter data early
    PushdownPredicates,
    /// Optimize join order and remove cross joins
    OptimizeJoins,
    /// Eliminate derived tables by converting to CTEs
    EliminateSubqueries,
    /// Merge subqueries into outer queries
    MergeSubqueries,
    /// Remove unused CTEs
    EliminateCtes,
    /// Annotate expressions with type information
    AnnotateTypes,
    /// Convert expressions to canonical form
    Canonicalize,
    /// Simplify expressions
    Simplify,
}

/// Default optimization rules in order of execution
pub const DEFAULT_RULES: &[OptimizationRule] = &[
    OptimizationRule::Qualify,
    OptimizationRule::PushdownProjections,
    OptimizationRule::Normalize,
    OptimizationRule::UnnestSubqueries,
    OptimizationRule::PushdownPredicates,
    OptimizationRule::OptimizeJoins,
    OptimizationRule::EliminateSubqueries,
    OptimizationRule::MergeSubqueries,
    OptimizationRule::EliminateCtes,
    OptimizationRule::AnnotateTypes,
    OptimizationRule::Canonicalize,
    OptimizationRule::Simplify,
];

const QUICK_RULES: &[OptimizationRule] =
    &[OptimizationRule::Simplify, OptimizationRule::Canonicalize];
const FAST_PATH_MAX_DEPTH: usize = 768;
const FAST_PATH_MAX_CONNECTORS: usize = 10_000;
const FAST_PATH_MAX_CONNECTOR_DEPTH: usize = 1024;
const FAST_PATH_MAX_NODES: usize = 50_000;
const CLONE_HEAVY_RULE_SKIP_NODES: usize = 20_000;

#[derive(Debug, Clone, Copy)]
struct ExpressionComplexity {
    node_count: usize,
    max_depth: usize,
    connector_count: usize,
    max_connector_depth: usize,
}

/// Optimize a SQL expression using the default set of rules.
///
/// This function coordinates multiple optimization passes in the correct order
/// to produce an optimized query plan.
///
/// # Arguments
/// * `expression` - The expression to optimize
/// * `config` - Optimizer configuration
///
/// # Returns
/// The optimized expression
pub fn optimize(expression: Expression, config: &OptimizerConfig<'_>) -> Expression {
    optimize_with_rules(expression, config, DEFAULT_RULES)
}

/// Optimize a SQL expression using a custom set of rules.
///
/// # Arguments
/// * `expression` - The expression to optimize
/// * `config` - Optimizer configuration
/// * `rules` - The optimization rules to apply
///
/// # Returns
/// The optimized expression
pub fn optimize_with_rules(
    mut expression: Expression,
    config: &OptimizerConfig<'_>,
    rules: &[OptimizationRule],
) -> Expression {
    let complexity = analyze_expression_complexity(&expression);
    if rules == DEFAULT_RULES && should_skip_all_optimization(&complexity) {
        return expression;
    }

    let active_rules = if rules == DEFAULT_RULES && should_use_quick_path(&complexity) {
        QUICK_RULES
    } else {
        rules
    };

    for rule in active_rules {
        if complexity.node_count >= CLONE_HEAVY_RULE_SKIP_NODES
            && matches!(
                rule,
                OptimizationRule::Qualify | OptimizationRule::Normalize
            )
        {
            continue;
        }
        expression = apply_rule(expression, *rule, config);
    }
    expression
}

fn should_skip_all_optimization(complexity: &ExpressionComplexity) -> bool {
    complexity.max_depth >= FAST_PATH_MAX_DEPTH
        || complexity.max_connector_depth >= FAST_PATH_MAX_CONNECTOR_DEPTH
}

fn should_use_quick_path(complexity: &ExpressionComplexity) -> bool {
    complexity.connector_count >= FAST_PATH_MAX_CONNECTORS
        || complexity.max_connector_depth >= FAST_PATH_MAX_CONNECTOR_DEPTH
        || complexity.node_count >= FAST_PATH_MAX_NODES
}

fn analyze_expression_complexity(expression: &Expression) -> ExpressionComplexity {
    let mut node_count = 0usize;
    let mut max_depth = 0usize;
    let mut connector_count = 0usize;
    let mut max_connector_depth = 0usize;
    let mut stack: Vec<(&Expression, usize, usize)> = vec![(expression, 0, 0)];

    while let Some((node, depth, connector_depth)) = stack.pop() {
        node_count += 1;
        max_depth = max_depth.max(depth);

        match node {
            Expression::And(op) | Expression::Or(op) => {
                connector_count += 1;
                let next_connector_depth = connector_depth + 1;
                max_connector_depth = max_connector_depth.max(next_connector_depth);
                stack.push((&op.right, depth + 1, next_connector_depth));
                stack.push((&op.left, depth + 1, next_connector_depth));
            }
            Expression::Paren(paren) => {
                stack.push((&paren.this, depth + 1, connector_depth));
            }
            _ => {
                for child in node.children().into_iter().rev() {
                    stack.push((child, depth + 1, 0));
                }
            }
        }
    }

    ExpressionComplexity {
        node_count,
        max_depth,
        connector_count,
        max_connector_depth,
    }
}

/// Apply a single optimization rule
fn apply_rule(
    expression: Expression,
    rule: OptimizationRule,
    config: &OptimizerConfig<'_>,
) -> Expression {
    match rule {
        OptimizationRule::Qualify => {
            // Qualify columns with table references
            if let Some(schema) = config.schema {
                let options = super::qualify_columns::QualifyColumnsOptions {
                    dialect: config.dialect,
                    ..Default::default()
                };
                let original = expression.clone();
                qualify_columns(expression, schema, &options).unwrap_or(original)
            } else {
                // Without schema, skip qualification
                expression
            }
        }
        OptimizationRule::PushdownProjections => {
            pushdown_projections(expression, config.dialect, true)
        }
        OptimizationRule::Normalize => {
            // Use CNF (dnf=false) with default max distance
            let original = expression.clone();
            normalize(expression, false, super::normalize::DEFAULT_MAX_DISTANCE).unwrap_or(original)
        }
        OptimizationRule::UnnestSubqueries => unnest_subqueries(expression),
        OptimizationRule::PushdownPredicates => pushdown_predicates(expression, config.dialect),
        OptimizationRule::OptimizeJoins => optimize_joins(expression),
        OptimizationRule::EliminateSubqueries => eliminate_subqueries_opt(expression),
        OptimizationRule::MergeSubqueries => merge_subqueries(expression, config.isolate_tables),
        OptimizationRule::EliminateCtes => eliminate_ctes(expression),
        OptimizationRule::AnnotateTypes => {
            // annotate_types is used for type inference, not expression transformation
            // For now, just return the expression unchanged
            let _ = annotate_types(&expression, config.schema, config.dialect);
            expression
        }
        OptimizationRule::Canonicalize => canonicalize(expression, config.dialect),
        OptimizationRule::Simplify => simplify(expression, config.dialect),
    }
}

// Re-import from subquery module with different name to avoid conflict
use super::subquery::eliminate_subqueries as eliminate_subqueries_opt;

/// Quick optimization that only applies essential passes.
///
/// This is faster than full optimization but may miss some opportunities.
pub fn quick_optimize(expression: Expression, dialect: Option<DialectType>) -> Expression {
    let config = OptimizerConfig {
        dialect,
        ..Default::default()
    };

    optimize_with_rules(expression, &config, QUICK_RULES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;

    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    #[test]
    fn test_optimize_simple() {
        let expr = parse("SELECT a FROM t");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_optimize_with_where() {
        let expr = parse("SELECT a FROM t WHERE b = 1");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_optimize_with_join() {
        let expr = parse("SELECT t.a FROM t JOIN s ON t.id = s.id");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_quick_optimize() {
        let expr = parse("SELECT 1 + 0 FROM t");
        let result = quick_optimize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_optimize_with_custom_rules() {
        let expr = parse("SELECT a FROM t WHERE NOT NOT b = 1");
        let config = OptimizerConfig::default();
        let rules = &[OptimizationRule::Simplify];
        let result = optimize_with_rules(expr, &config, rules);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_optimizer_config_default() {
        let config = OptimizerConfig::default();
        assert!(config.schema.is_none());
        assert!(config.dialect.is_none());
        assert!(config.isolate_tables);
        assert!(!config.quote_identifiers);
    }

    #[test]
    fn test_default_rules() {
        assert!(!DEFAULT_RULES.is_empty());
        assert!(DEFAULT_RULES.contains(&OptimizationRule::Simplify));
        assert!(DEFAULT_RULES.contains(&OptimizationRule::Canonicalize));
    }

    #[test]
    fn test_optimize_subquery() {
        let expr = parse("SELECT * FROM (SELECT a FROM t) AS sub");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_optimize_cte() {
        let expr = parse("WITH cte AS (SELECT a FROM t) SELECT * FROM cte");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("WITH"));
    }

    #[test]
    fn test_optimize_preserves_semantics() {
        let expr = parse("SELECT a, b FROM t WHERE c > 1 ORDER BY a");
        let config = OptimizerConfig::default();
        let result = optimize(expr, &config);
        let sql = gen(&result);
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_analyze_expression_complexity_deep_connector_chain() {
        let mut expr = Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
            Expression::column("c0"),
            Expression::number(0),
        )));

        for i in 1..1500 {
            let predicate = Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
                Expression::column(format!("c{i}")),
                Expression::number(i as i64),
            )));
            expr = Expression::And(Box::new(crate::expressions::BinaryOp::new(expr, predicate)));
        }

        let complexity = analyze_expression_complexity(&expr);
        assert!(complexity.max_connector_depth >= 1499);
        assert!(complexity.connector_count >= 1499);
    }

    #[test]
    fn test_optimize_handles_deep_connector_chain() {
        let mut expr = Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
            Expression::column("c0"),
            Expression::number(0),
        )));

        for i in 1..2200 {
            let predicate = Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
                Expression::column(format!("c{i}")),
                Expression::number(i as i64),
            )));
            expr = Expression::And(Box::new(crate::expressions::BinaryOp::new(expr, predicate)));
        }

        let config = OptimizerConfig::default();
        let optimized = optimize(expr, &config);
        let sql = gen(&optimized);
        assert!(sql.contains("c2199 = 2199"), "{sql}");
    }
}
