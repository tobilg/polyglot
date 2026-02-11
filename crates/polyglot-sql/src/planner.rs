//! Query Execution Planner
//!
//! This module provides functionality to convert SQL AST into an execution plan
//! represented as a DAG (Directed Acyclic Graph) of steps.
//!
//! Reference: `external-projects/sqlglot/sqlglot/planner.py`

use crate::expressions::{Expression, JoinKind};
use std::collections::{HashMap, HashSet};

/// A query execution plan
#[derive(Debug)]
pub struct Plan {
    /// The root step of the plan DAG
    pub root: Step,
    /// Cached DAG representation
    dag: Option<HashMap<usize, HashSet<usize>>>,
}

impl Plan {
    /// Create a new plan from an expression
    pub fn from_expression(expression: &Expression) -> Option<Self> {
        let root = Step::from_expression(expression, &HashMap::new())?;
        Some(Self { root, dag: None })
    }

    /// Get the DAG representation of the plan
    pub fn dag(&mut self) -> &HashMap<usize, HashSet<usize>> {
        if self.dag.is_none() {
            let mut dag = HashMap::new();
            self.build_dag(&self.root, &mut dag, 0);
            self.dag = Some(dag);
        }
        self.dag.as_ref().unwrap()
    }

    fn build_dag(&self, step: &Step, dag: &mut HashMap<usize, HashSet<usize>>, id: usize) {
        let deps: HashSet<usize> = step.dependencies
            .iter()
            .enumerate()
            .map(|(i, _)| id + i + 1)
            .collect();
        dag.insert(id, deps);

        for (i, dep) in step.dependencies.iter().enumerate() {
            self.build_dag(dep, dag, id + i + 1);
        }
    }

    /// Get all leaf steps (steps with no dependencies)
    pub fn leaves(&self) -> Vec<&Step> {
        let mut leaves = Vec::new();
        self.collect_leaves(&self.root, &mut leaves);
        leaves
    }

    fn collect_leaves<'a>(&'a self, step: &'a Step, leaves: &mut Vec<&'a Step>) {
        if step.dependencies.is_empty() {
            leaves.push(step);
        } else {
            for dep in &step.dependencies {
                self.collect_leaves(dep, leaves);
            }
        }
    }
}

/// A step in the execution plan
#[derive(Debug, Clone)]
pub struct Step {
    /// Name of this step
    pub name: String,
    /// Type of step
    pub kind: StepKind,
    /// Projections to output
    pub projections: Vec<Expression>,
    /// Dependencies (other steps that must complete first)
    pub dependencies: Vec<Step>,
    /// Aggregation expressions (for Aggregate steps)
    pub aggregations: Vec<Expression>,
    /// Group by expressions (for Aggregate steps)
    pub group_by: Vec<Expression>,
    /// Join condition (for Join steps)
    pub condition: Option<Expression>,
    /// Sort expressions (for Sort steps)
    pub order_by: Vec<Expression>,
    /// Limit value (for Scan/other steps)
    pub limit: Option<Expression>,
}

/// Types of execution steps
#[derive(Debug, Clone, PartialEq)]
pub enum StepKind {
    /// Scan a table
    Scan,
    /// Join multiple inputs
    Join(JoinType),
    /// Aggregate rows
    Aggregate,
    /// Sort rows
    Sort,
    /// Set operation (UNION, INTERSECT, EXCEPT)
    SetOperation(SetOperationType),
}

/// Types of joins in execution plans
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Types of set operations
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationType {
    Union,
    UnionAll,
    Intersect,
    Except,
}

impl Step {
    /// Create a new step
    pub fn new(name: impl Into<String>, kind: StepKind) -> Self {
        Self {
            name: name.into(),
            kind,
            projections: Vec::new(),
            dependencies: Vec::new(),
            aggregations: Vec::new(),
            group_by: Vec::new(),
            condition: None,
            order_by: Vec::new(),
            limit: None,
        }
    }

    /// Build a step from an expression
    pub fn from_expression(
        expression: &Expression,
        ctes: &HashMap<String, Step>,
    ) -> Option<Self> {
        match expression {
            Expression::Select(select) => {
                let mut step = Self::from_select(select, ctes)?;

                // Handle ORDER BY
                if let Some(ref order_by) = select.order_by {
                    let sort_step = Step {
                        name: step.name.clone(),
                        kind: StepKind::Sort,
                        projections: Vec::new(),
                        dependencies: vec![step],
                        aggregations: Vec::new(),
                        group_by: Vec::new(),
                        condition: None,
                        order_by: order_by.expressions.iter().map(|o| o.this.clone()).collect(),
                        limit: None,
                    };
                    step = sort_step;
                }

                // Handle LIMIT
                if let Some(ref limit) = select.limit {
                    step.limit = Some(limit.this.clone());
                }

                Some(step)
            }
            Expression::Union(union) => {
                let left = Self::from_expression(&union.left, ctes)?;
                let right = Self::from_expression(&union.right, ctes)?;

                let op_type = if union.all {
                    SetOperationType::UnionAll
                } else {
                    SetOperationType::Union
                };

                Some(Step {
                    name: "UNION".to_string(),
                    kind: StepKind::SetOperation(op_type),
                    projections: Vec::new(),
                    dependencies: vec![left, right],
                    aggregations: Vec::new(),
                    group_by: Vec::new(),
                    condition: None,
                    order_by: Vec::new(),
                    limit: None,
                })
            }
            Expression::Intersect(intersect) => {
                let left = Self::from_expression(&intersect.left, ctes)?;
                let right = Self::from_expression(&intersect.right, ctes)?;

                Some(Step {
                    name: "INTERSECT".to_string(),
                    kind: StepKind::SetOperation(SetOperationType::Intersect),
                    projections: Vec::new(),
                    dependencies: vec![left, right],
                    aggregations: Vec::new(),
                    group_by: Vec::new(),
                    condition: None,
                    order_by: Vec::new(),
                    limit: None,
                })
            }
            Expression::Except(except) => {
                let left = Self::from_expression(&except.left, ctes)?;
                let right = Self::from_expression(&except.right, ctes)?;

                Some(Step {
                    name: "EXCEPT".to_string(),
                    kind: StepKind::SetOperation(SetOperationType::Except),
                    projections: Vec::new(),
                    dependencies: vec![left, right],
                    aggregations: Vec::new(),
                    group_by: Vec::new(),
                    condition: None,
                    order_by: Vec::new(),
                    limit: None,
                })
            }
            _ => None,
        }
    }

    fn from_select(
        select: &crate::expressions::Select,
        ctes: &HashMap<String, Step>,
    ) -> Option<Self> {
        // Process CTEs first
        let mut ctes = ctes.clone();
        if let Some(ref with) = select.with {
            for cte in &with.ctes {
                if let Some(step) = Self::from_expression(&cte.this, &ctes) {
                    ctes.insert(cte.alias.name.clone(), step);
                }
            }
        }

        // Start with the FROM clause
        let mut step = if let Some(ref from) = select.from {
            if let Some(table_expr) = from.expressions.first() {
                Self::from_table_expression(table_expr, &ctes)?
            } else {
                return None;
            }
        } else {
            // SELECT without FROM (e.g., SELECT 1)
            Step::new("", StepKind::Scan)
        };

        // Process JOINs
        for join in &select.joins {
            let right = Self::from_table_expression(&join.this, &ctes)?;

            let join_type = match join.kind {
                JoinKind::Inner => JoinType::Inner,
                JoinKind::Left | JoinKind::NaturalLeft => JoinType::Left,
                JoinKind::Right | JoinKind::NaturalRight => JoinType::Right,
                JoinKind::Full | JoinKind::NaturalFull => JoinType::Full,
                JoinKind::Cross | JoinKind::Natural => JoinType::Cross,
                _ => JoinType::Inner,
            };

            let join_step = Step {
                name: step.name.clone(),
                kind: StepKind::Join(join_type),
                projections: Vec::new(),
                dependencies: vec![step, right],
                aggregations: Vec::new(),
                group_by: Vec::new(),
                condition: join.on.clone(),
                order_by: Vec::new(),
                limit: None,
            };
            step = join_step;
        }

        // Check for aggregations
        let has_aggregations = select.expressions.iter().any(|e| contains_aggregate(e));
        let has_group_by = select.group_by.is_some();

        if has_aggregations || has_group_by {
            // Create aggregate step
            let agg_step = Step {
                name: step.name.clone(),
                kind: StepKind::Aggregate,
                projections: select.expressions.clone(),
                dependencies: vec![step],
                aggregations: extract_aggregations(&select.expressions),
                group_by: select.group_by.as_ref()
                    .map(|g| g.expressions.clone())
                    .unwrap_or_default(),
                condition: None,
                order_by: Vec::new(),
                limit: None,
            };
            step = agg_step;
        } else {
            step.projections = select.expressions.clone();
        }

        Some(step)
    }

    fn from_table_expression(
        expr: &Expression,
        ctes: &HashMap<String, Step>,
    ) -> Option<Self> {
        match expr {
            Expression::Table(table) => {
                // Check if this references a CTE
                if let Some(cte_step) = ctes.get(&table.name.name) {
                    return Some(cte_step.clone());
                }

                // Regular table scan
                Some(Step::new(&table.name.name, StepKind::Scan))
            }
            Expression::Alias(alias) => {
                let mut step = Self::from_table_expression(&alias.this, ctes)?;
                step.name = alias.alias.name.clone();
                Some(step)
            }
            Expression::Subquery(sq) => {
                let step = Self::from_expression(&sq.this, ctes)?;
                Some(step)
            }
            _ => None,
        }
    }

    /// Add a dependency to this step
    pub fn add_dependency(&mut self, dep: Step) {
        self.dependencies.push(dep);
    }
}

/// Check if an expression contains an aggregate function
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        // Specific aggregate function variants
        Expression::Sum(_) | Expression::Count(_) | Expression::Avg(_) |
        Expression::Min(_) | Expression::Max(_) | Expression::ArrayAgg(_) |
        Expression::StringAgg(_) | Expression::ListAgg(_) |
        Expression::Stddev(_) | Expression::StddevPop(_) | Expression::StddevSamp(_) |
        Expression::Variance(_) | Expression::VarPop(_) | Expression::VarSamp(_) |
        Expression::Median(_) | Expression::Mode(_) | Expression::First(_) | Expression::Last(_) |
        Expression::AnyValue(_) | Expression::ApproxDistinct(_) | Expression::ApproxCountDistinct(_) |
        Expression::LogicalAnd(_) | Expression::LogicalOr(_) |
        Expression::AggregateFunction(_) => true,

        Expression::Alias(alias) => contains_aggregate(&alias.this),
        Expression::Add(op) | Expression::Sub(op) |
        Expression::Mul(op) | Expression::Div(op) => {
            contains_aggregate(&op.left) || contains_aggregate(&op.right)
        }
        Expression::Function(func) => {
            // Check for aggregate function names (fallback)
            let name = func.name.to_uppercase();
            matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" |
                     "ARRAY_AGG" | "STRING_AGG" | "GROUP_CONCAT")
        }
        _ => false,
    }
}

/// Extract aggregate expressions from a list
fn extract_aggregations(expressions: &[Expression]) -> Vec<Expression> {
    let mut aggs = Vec::new();
    for expr in expressions {
        collect_aggregations(expr, &mut aggs);
    }
    aggs
}

fn collect_aggregations(expr: &Expression, aggs: &mut Vec<Expression>) {
    match expr {
        // Specific aggregate function variants
        Expression::Sum(_) | Expression::Count(_) | Expression::Avg(_) |
        Expression::Min(_) | Expression::Max(_) | Expression::ArrayAgg(_) |
        Expression::StringAgg(_) | Expression::ListAgg(_) |
        Expression::Stddev(_) | Expression::StddevPop(_) | Expression::StddevSamp(_) |
        Expression::Variance(_) | Expression::VarPop(_) | Expression::VarSamp(_) |
        Expression::Median(_) | Expression::Mode(_) | Expression::First(_) | Expression::Last(_) |
        Expression::AnyValue(_) | Expression::ApproxDistinct(_) | Expression::ApproxCountDistinct(_) |
        Expression::LogicalAnd(_) | Expression::LogicalOr(_) |
        Expression::AggregateFunction(_) => {
            aggs.push(expr.clone());
        }
        Expression::Alias(alias) => {
            collect_aggregations(&alias.this, aggs);
        }
        Expression::Add(op) | Expression::Sub(op) |
        Expression::Mul(op) | Expression::Div(op) => {
            collect_aggregations(&op.left, aggs);
            collect_aggregations(&op.right, aggs);
        }
        Expression::Function(func) => {
            let name = func.name.to_uppercase();
            if matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" |
                        "ARRAY_AGG" | "STRING_AGG" | "GROUP_CONCAT") {
                aggs.push(expr.clone());
            } else {
                for arg in &func.args {
                    collect_aggregations(arg, aggs);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::{Dialect, DialectType};

    fn parse(sql: &str) -> Expression {
        let dialect = Dialect::get(DialectType::Generic);
        let ast = dialect.parse(sql).unwrap();
        ast.into_iter().next().unwrap()
    }

    #[test]
    fn test_simple_scan() {
        let sql = "SELECT a, b FROM t";
        let expr = parse(sql);
        let plan = Plan::from_expression(&expr);

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.root.kind, StepKind::Scan);
        assert_eq!(plan.root.name, "t");
    }

    #[test]
    fn test_join() {
        let sql = "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id";
        let expr = parse(sql);
        let plan = Plan::from_expression(&expr);

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert!(matches!(plan.root.kind, StepKind::Join(_)));
        assert_eq!(plan.root.dependencies.len(), 2);
    }

    #[test]
    fn test_aggregate() {
        let sql = "SELECT x, SUM(y) FROM t GROUP BY x";
        let expr = parse(sql);
        let plan = Plan::from_expression(&expr);

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.root.kind, StepKind::Aggregate);
    }

    #[test]
    fn test_union() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";
        let expr = parse(sql);
        let plan = Plan::from_expression(&expr);

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert!(matches!(plan.root.kind, StepKind::SetOperation(SetOperationType::Union)));
    }

    #[test]
    fn test_contains_aggregate() {
        // Parse a SELECT with an aggregate function and check the expression
        let select_with_agg = parse("SELECT SUM(x) FROM t");
        if let Expression::Select(ref sel) = select_with_agg {
            assert!(!sel.expressions.is_empty());
            assert!(contains_aggregate(&sel.expressions[0]),
                "Expected SUM to be detected as aggregate function");
        } else {
            panic!("Expected SELECT expression");
        }

        // Parse a SELECT with a non-aggregate expression
        let select_without_agg = parse("SELECT x + 1 FROM t");
        if let Expression::Select(ref sel) = select_without_agg {
            assert!(!sel.expressions.is_empty());
            assert!(!contains_aggregate(&sel.expressions[0]),
                "Expected x + 1 to not be an aggregate function");
        } else {
            panic!("Expected SELECT expression");
        }
    }
}
