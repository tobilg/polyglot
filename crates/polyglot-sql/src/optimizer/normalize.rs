//! Boolean Normalization Module
//!
//! This module provides functionality for converting SQL boolean expressions
//! to Conjunctive Normal Form (CNF) or Disjunctive Normal Form (DNF).
//!
//! CNF: (a OR b) AND (c OR d) - useful for predicate pushdown
//! DNF: (a AND b) OR (c AND d) - useful for partition pruning
//!
//! Ported from sqlglot's optimizer/normalize.py

use crate::expressions::{BinaryOp, Expression};
use crate::optimizer::simplify::Simplifier;
use thiserror::Error;

/// Maximum default distance for normalization
pub const DEFAULT_MAX_DISTANCE: i64 = 128;

/// Errors that can occur during normalization
#[derive(Debug, Error, Clone)]
pub enum NormalizeError {
    #[error("Normalization distance {distance} exceeds max {max}")]
    DistanceExceeded { distance: i64, max: i64 },
}

/// Result type for normalization operations
pub type NormalizeResult<T> = Result<T, NormalizeError>;

/// Rewrite SQL AST into Conjunctive Normal Form (CNF) or Disjunctive Normal Form (DNF).
///
/// CNF (default): (x AND y) OR z => (x OR z) AND (y OR z)
/// DNF: (x OR y) AND z => (x AND z) OR (y AND z)
///
/// # Arguments
/// * `expression` - Expression to normalize
/// * `dnf` - If true, convert to DNF; otherwise CNF (default)
/// * `max_distance` - Maximum estimated distance before giving up
///
/// # Returns
/// The normalized expression, or the original if normalization would be too expensive.
pub fn normalize(
    expression: Expression,
    dnf: bool,
    max_distance: i64,
) -> NormalizeResult<Expression> {
    let simplifier = Simplifier::new(None);
    normalize_with_simplifier(expression, dnf, max_distance, &simplifier)
}

/// Normalize with a provided simplifier instance.
fn normalize_with_simplifier(
    expression: Expression,
    dnf: bool,
    max_distance: i64,
    simplifier: &Simplifier,
) -> NormalizeResult<Expression> {
    let mut result = expression.clone();

    // Walk through all connector nodes (AND/OR)
    let connectors = collect_connectors(&expression);

    for node in connectors {
        if normalized(&node, dnf) {
            continue;
        }

        // Estimate the cost of normalization
        let distance = normalization_distance(&node, dnf, max_distance);

        if distance > max_distance {
            // Too expensive, return original
            return Ok(expression);
        }

        // Apply distributive law repeatedly until normalized
        let normalized_node = apply_distributive_law(&node, dnf, max_distance, simplifier)?;

        // In a real implementation, we would replace the node in the AST
        // For now, we just return the normalized version if it's the root
        if is_same_expression(&node, &expression) {
            result = normalized_node;
        }
    }

    Ok(result)
}

/// Check whether a given expression is in a normal form.
///
/// CNF (Conjunctive Normal Form): (A OR B) AND (C OR D)
///   - Conjunction (AND) of disjunctions (OR)
///   - An OR cannot have an AND as a descendant
///
/// DNF (Disjunctive Normal Form): (A AND B) OR (C AND D)
///   - Disjunction (OR) of conjunctions (AND)
///   - An AND cannot have an OR as a descendant
///
/// # Arguments
/// * `expression` - The expression to check
/// * `dnf` - Whether to check for DNF (true) or CNF (false)
///
/// # Returns
/// True if the expression is in the requested normal form.
pub fn normalized(expression: &Expression, dnf: bool) -> bool {
    if dnf {
        // DNF: An AND cannot have OR as a descendant
        !has_and_with_or_descendant(expression)
    } else {
        // CNF: An OR cannot have AND as a descendant
        !has_or_with_and_descendant(expression)
    }
}

/// Check if any OR in the expression has an AND as a descendant (violates CNF)
fn has_or_with_and_descendant(expression: &Expression) -> bool {
    match expression {
        Expression::Or(bin) => {
            // Check if either child is an AND, or if children have the violation
            contains_and(&bin.left)
                || contains_and(&bin.right)
                || has_or_with_and_descendant(&bin.left)
                || has_or_with_and_descendant(&bin.right)
        }
        Expression::And(bin) => {
            has_or_with_and_descendant(&bin.left) || has_or_with_and_descendant(&bin.right)
        }
        Expression::Paren(paren) => has_or_with_and_descendant(&paren.this),
        _ => false,
    }
}

/// Check if any AND in the expression has an OR as a descendant (violates DNF)
fn has_and_with_or_descendant(expression: &Expression) -> bool {
    match expression {
        Expression::And(bin) => {
            // Check if either child is an OR, or if children have the violation
            contains_or(&bin.left)
                || contains_or(&bin.right)
                || has_and_with_or_descendant(&bin.left)
                || has_and_with_or_descendant(&bin.right)
        }
        Expression::Or(bin) => {
            has_and_with_or_descendant(&bin.left) || has_and_with_or_descendant(&bin.right)
        }
        Expression::Paren(paren) => has_and_with_or_descendant(&paren.this),
        _ => false,
    }
}

/// Check if expression contains any AND (at any level)
fn contains_and(expression: &Expression) -> bool {
    match expression {
        Expression::And(_) => true,
        Expression::Or(bin) => contains_and(&bin.left) || contains_and(&bin.right),
        Expression::Paren(paren) => contains_and(&paren.this),
        _ => false,
    }
}

/// Check if expression contains any OR (at any level)
fn contains_or(expression: &Expression) -> bool {
    match expression {
        Expression::Or(_) => true,
        Expression::And(bin) => contains_or(&bin.left) || contains_or(&bin.right),
        Expression::Paren(paren) => contains_or(&paren.this),
        _ => false,
    }
}

/// Calculate the normalization distance for an expression.
///
/// This estimates the cost of converting to normal form.
/// The conversion is exponential in complexity, so this helps decide
/// whether to attempt it.
///
/// # Arguments
/// * `expression` - The expression to analyze
/// * `dnf` - Whether checking distance to DNF (true) or CNF (false)
/// * `max_distance` - Early exit if distance exceeds this
///
/// # Returns
/// The estimated normalization distance.
pub fn normalization_distance(expression: &Expression, dnf: bool, max_distance: i64) -> i64 {
    let connector_count = count_connectors(expression);
    let mut total: i64 = -(connector_count as i64 + 1);

    for length in predicate_lengths(expression, dnf, max_distance, 0) {
        total += length;
        if total > max_distance {
            return total;
        }
    }

    total
}

/// Calculate predicate lengths when expanded to normalized form.
///
/// For example: (A AND B) OR C -> [2, 2] because len(A OR C) = 2, len(B OR C) = 2
///
/// In CNF mode (dnf=false): OR distributes over AND
///   x OR (y AND z) => (x OR y) AND (x OR z)
///
/// In DNF mode (dnf=true): AND distributes over OR
///   x AND (y OR z) => (x AND y) OR (x AND z)
fn predicate_lengths(expression: &Expression, dnf: bool, max_distance: i64, depth: i64) -> Vec<i64> {
    if depth > max_distance {
        return vec![depth];
    }

    let expr = unwrap_paren(expression);

    match expr {
        // In CNF mode, OR is the distributing operator (we're breaking up ORs of ANDs)
        Expression::Or(bin) if !dnf => {
            // For CNF: OR causes multiplication in the distance calculation
            let left_lengths = predicate_lengths(&bin.left, dnf, max_distance, depth + 1);
            let right_lengths = predicate_lengths(&bin.right, dnf, max_distance, depth + 1);

            let mut result = Vec::new();
            for a in &left_lengths {
                for b in &right_lengths {
                    result.push(a + b);
                }
            }
            result
        }
        // In DNF mode, AND is the distributing operator (we're breaking up ANDs of ORs)
        Expression::And(bin) if dnf => {
            // For DNF: AND causes multiplication in the distance calculation
            let left_lengths = predicate_lengths(&bin.left, dnf, max_distance, depth + 1);
            let right_lengths = predicate_lengths(&bin.right, dnf, max_distance, depth + 1);

            let mut result = Vec::new();
            for a in &left_lengths {
                for b in &right_lengths {
                    result.push(a + b);
                }
            }
            result
        }
        // Non-distributing connectors: just collect lengths from both sides
        Expression::And(bin) | Expression::Or(bin) => {
            let mut result = predicate_lengths(&bin.left, dnf, max_distance, depth + 1);
            result.extend(predicate_lengths(&bin.right, dnf, max_distance, depth + 1));
            result
        }
        _ => vec![1], // Leaf predicate
    }
}

/// Apply the distributive law to normalize an expression.
///
/// CNF: x OR (y AND z) => (x OR y) AND (x OR z)
/// DNF: x AND (y OR z) => (x AND y) OR (x AND z)
fn apply_distributive_law(
    expression: &Expression,
    dnf: bool,
    max_distance: i64,
    simplifier: &Simplifier,
) -> NormalizeResult<Expression> {
    if normalized(expression, dnf) {
        return Ok(expression.clone());
    }

    let distance = normalization_distance(expression, dnf, max_distance);
    if distance > max_distance {
        return Err(NormalizeError::DistanceExceeded {
            distance,
            max: max_distance,
        });
    }

    // Apply distributive law based on mode
    let result = if dnf {
        distribute_dnf(expression, simplifier)
    } else {
        distribute_cnf(expression, simplifier)
    };

    // Recursively apply until normalized
    if !normalized(&result, dnf) {
        apply_distributive_law(&result, dnf, max_distance, simplifier)
    } else {
        Ok(result)
    }
}

/// Apply distributive law for CNF conversion.
/// x OR (y AND z) => (x OR y) AND (x OR z)
fn distribute_cnf(expression: &Expression, simplifier: &Simplifier) -> Expression {
    match expression {
        Expression::Or(bin) => {
            let left = distribute_cnf(&bin.left, simplifier);
            let right = distribute_cnf(&bin.right, simplifier);

            // Check if either side is an AND
            if let Expression::And(and_bin) = &right {
                // x OR (y AND z) => (x OR y) AND (x OR z)
                let left_or_y = make_or(left.clone(), and_bin.left.clone());
                let left_or_z = make_or(left, and_bin.right.clone());
                return make_and(left_or_y, left_or_z);
            }

            if let Expression::And(and_bin) = &left {
                // (y AND z) OR x => (y OR x) AND (z OR x)
                let y_or_right = make_or(and_bin.left.clone(), right.clone());
                let z_or_right = make_or(and_bin.right.clone(), right);
                return make_and(y_or_right, z_or_right);
            }

            // No AND found, return simplified OR
            make_or(left, right)
        }
        Expression::And(bin) => {
            // Recurse into AND
            let left = distribute_cnf(&bin.left, simplifier);
            let right = distribute_cnf(&bin.right, simplifier);
            make_and(left, right)
        }
        Expression::Paren(paren) => distribute_cnf(&paren.this, simplifier),
        _ => expression.clone(),
    }
}

/// Apply distributive law for DNF conversion.
/// x AND (y OR z) => (x AND y) OR (x AND z)
fn distribute_dnf(expression: &Expression, simplifier: &Simplifier) -> Expression {
    match expression {
        Expression::And(bin) => {
            let left = distribute_dnf(&bin.left, simplifier);
            let right = distribute_dnf(&bin.right, simplifier);

            // Check if either side is an OR
            if let Expression::Or(or_bin) = &right {
                // x AND (y OR z) => (x AND y) OR (x AND z)
                let left_and_y = make_and(left.clone(), or_bin.left.clone());
                let left_and_z = make_and(left, or_bin.right.clone());
                return make_or(left_and_y, left_and_z);
            }

            if let Expression::Or(or_bin) = &left {
                // (y OR z) AND x => (y AND x) OR (z AND x)
                let y_and_right = make_and(or_bin.left.clone(), right.clone());
                let z_and_right = make_and(or_bin.right.clone(), right);
                return make_or(y_and_right, z_and_right);
            }

            // No OR found, return simplified AND
            make_and(left, right)
        }
        Expression::Or(bin) => {
            // Recurse into OR
            let left = distribute_dnf(&bin.left, simplifier);
            let right = distribute_dnf(&bin.right, simplifier);
            make_or(left, right)
        }
        Expression::Paren(paren) => distribute_dnf(&paren.this, simplifier),
        _ => expression.clone(),
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Collect all connector (AND/OR) expressions from an AST
fn collect_connectors(expression: &Expression) -> Vec<Expression> {
    let mut result = Vec::new();
    collect_connectors_recursive(expression, &mut result);
    result
}

fn collect_connectors_recursive(expression: &Expression, result: &mut Vec<Expression>) {
    match expression {
        Expression::And(bin) => {
            result.push(expression.clone());
            collect_connectors_recursive(&bin.left, result);
            collect_connectors_recursive(&bin.right, result);
        }
        Expression::Or(bin) => {
            result.push(expression.clone());
            collect_connectors_recursive(&bin.left, result);
            collect_connectors_recursive(&bin.right, result);
        }
        Expression::Paren(paren) => {
            collect_connectors_recursive(&paren.this, result);
        }
        _ => {}
    }
}

/// Count the number of connector nodes in an expression
fn count_connectors(expression: &Expression) -> usize {
    match expression {
        Expression::And(bin) | Expression::Or(bin) => {
            1 + count_connectors(&bin.left) + count_connectors(&bin.right)
        }
        Expression::Paren(paren) => count_connectors(&paren.this),
        _ => 0,
    }
}

/// Unwrap parentheses from an expression
fn unwrap_paren(expression: &Expression) -> &Expression {
    match expression {
        Expression::Paren(paren) => unwrap_paren(&paren.this),
        _ => expression,
    }
}

/// Check if two expressions are the same (simple identity check)
fn is_same_expression(a: &Expression, b: &Expression) -> bool {
    // Simple identity check - in a real implementation, this would be more sophisticated
    std::ptr::eq(a as *const _, b as *const _) || format!("{:?}", a) == format!("{:?}", b)
}

/// Create an AND expression
fn make_and(left: Expression, right: Expression) -> Expression {
    Expression::And(Box::new(BinaryOp {
        left,
        right,
        left_comments: vec![],
        operator_comments: vec![],
        trailing_comments: vec![],
    }))
}

/// Create an OR expression
fn make_or(left: Expression, right: Expression) -> Expression {
    Expression::Or(Box::new(BinaryOp {
        left,
        right,
        left_comments: vec![],
        operator_comments: vec![],
        trailing_comments: vec![],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    fn parse_predicate(sql: &str) -> Expression {
        let full = format!("SELECT 1 WHERE {}", sql);
        let stmt = parse(&full);
        if let Expression::Select(select) = stmt {
            if let Some(where_clause) = select.where_clause {
                return where_clause.this;
            }
        }
        panic!("Failed to extract predicate from: {}", sql);
    }

    #[test]
    fn test_normalized_cnf() {
        // (a OR b) AND (c OR d) is in CNF
        let expr = parse_predicate("(a OR b) AND (c OR d)");
        assert!(normalized(&expr, false)); // CNF
    }

    #[test]
    fn test_normalized_dnf() {
        // (a AND b) OR (c AND d) is in DNF
        let expr = parse_predicate("(a AND b) OR (c AND d)");
        assert!(normalized(&expr, true)); // DNF
    }

    #[test]
    fn test_not_normalized_cnf() {
        // (a AND b) OR c is NOT in CNF (has AND under OR)
        let expr = parse_predicate("(a AND b) OR c");
        assert!(!normalized(&expr, false)); // Not CNF
    }

    #[test]
    fn test_not_normalized_dnf() {
        // (a OR b) AND c is NOT in DNF (has OR under AND)
        let expr = parse_predicate("(a OR b) AND c");
        assert!(!normalized(&expr, true)); // Not DNF
    }

    #[test]
    fn test_simple_literal_is_normalized() {
        let expr = parse_predicate("a = 1");
        assert!(normalized(&expr, false)); // CNF
        assert!(normalized(&expr, true)); // DNF
    }

    #[test]
    fn test_normalization_distance_simple() {
        // Simple predicate should have low distance
        let expr = parse_predicate("a = 1");
        let distance = normalization_distance(&expr, false, 128);
        assert!(distance <= 0);
    }

    #[test]
    fn test_normalization_distance_complex() {
        // (a AND b) OR (c AND d) requires expansion
        let expr = parse_predicate("(a AND b) OR (c AND d)");
        let distance = normalization_distance(&expr, false, 128);
        assert!(distance > 0);
    }

    #[test]
    fn test_normalize_to_cnf() {
        // (x AND y) OR z => (x OR z) AND (y OR z)
        let expr = parse_predicate("(x AND y) OR z");
        let result = normalize(expr, false, 128).unwrap();

        // Result should be in CNF
        assert!(normalized(&result, false));
    }

    #[test]
    fn test_normalize_to_dnf() {
        // (x OR y) AND z => (x AND z) OR (y AND z)
        let expr = parse_predicate("(x OR y) AND z");
        let result = normalize(expr, true, 128).unwrap();

        // Result should be in DNF
        assert!(normalized(&result, true));
    }

    #[test]
    fn test_count_connectors() {
        let expr = parse_predicate("a AND b AND c");
        let count = count_connectors(&expr);
        assert_eq!(count, 2); // Two AND connectors
    }

    #[test]
    fn test_predicate_lengths() {
        // Simple case
        let expr = parse_predicate("a = 1");
        let lengths = predicate_lengths(&expr, false, 128, 0);
        assert_eq!(lengths, vec![1]);
    }
}
