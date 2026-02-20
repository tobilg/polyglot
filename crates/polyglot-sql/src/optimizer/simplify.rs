//! Expression Simplification
//!
//! This module provides boolean and expression simplification for SQL AST nodes.
//! It applies various algebraic transformations to simplify expressions:
//! - De Morgan's laws (NOT (A AND B) -> NOT A OR NOT B)
//! - Constant folding (1 + 2 -> 3)
//! - Boolean absorption (A AND (A OR B) -> A)
//! - Complement removal (A AND NOT A -> FALSE)
//! - Connector flattening (A AND (B AND C) -> A AND B AND C)
//!
//! Based on SQLGlot's optimizer/simplify.py

use crate::dialects::DialectType;
use crate::expressions::{
    BinaryOp, BooleanLiteral, Case, ConcatWs, DateTruncFunc, Expression, Literal, Null, Paren,
    UnaryOp,
};

/// Main entry point for expression simplification
pub fn simplify(expression: Expression, dialect: Option<DialectType>) -> Expression {
    let mut simplifier = Simplifier::new(dialect);
    simplifier.simplify(expression)
}

/// Check if expression is always true
pub fn always_true(expr: &Expression) -> bool {
    match expr {
        Expression::Boolean(b) => b.value,
        Expression::Literal(Literal::Number(n)) => {
            // Non-zero numbers are truthy
            if let Ok(num) = n.parse::<f64>() {
                num != 0.0
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Check if expression is a boolean TRUE literal (not just truthy)
pub fn is_boolean_true(expr: &Expression) -> bool {
    matches!(expr, Expression::Boolean(b) if b.value)
}

/// Check if expression is a boolean FALSE literal (not just falsy)
pub fn is_boolean_false(expr: &Expression) -> bool {
    matches!(expr, Expression::Boolean(b) if !b.value)
}

/// Check if expression is always false
pub fn always_false(expr: &Expression) -> bool {
    is_false(expr) || is_null(expr) || is_zero(expr)
}

/// Check if expression is boolean FALSE
pub fn is_false(expr: &Expression) -> bool {
    matches!(expr, Expression::Boolean(b) if !b.value)
}

/// Check if expression is NULL
pub fn is_null(expr: &Expression) -> bool {
    matches!(expr, Expression::Null(_))
}

/// Check if expression is zero
pub fn is_zero(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(Literal::Number(n)) => {
            if let Ok(num) = n.parse::<f64>() {
                num == 0.0
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Check if b is the complement of a (i.e., b = NOT a)
pub fn is_complement(a: &Expression, b: &Expression) -> bool {
    if let Expression::Not(not_op) = b {
        &not_op.this == a
    } else {
        false
    }
}

/// Create a TRUE boolean literal
pub fn bool_true() -> Expression {
    Expression::Boolean(BooleanLiteral { value: true })
}

/// Create a FALSE boolean literal
pub fn bool_false() -> Expression {
    Expression::Boolean(BooleanLiteral { value: false })
}

/// Create a NULL expression
pub fn null() -> Expression {
    Expression::Null(Null)
}

/// Evaluate a boolean comparison between two numbers
pub fn eval_boolean_nums(op: &str, a: f64, b: f64) -> Option<Expression> {
    let result = match op {
        "=" | "==" => a == b,
        "!=" | "<>" => a != b,
        ">" => a > b,
        ">=" => a >= b,
        "<" => a < b,
        "<=" => a <= b,
        _ => return None,
    };
    Some(if result { bool_true() } else { bool_false() })
}

/// Evaluate a boolean comparison between two strings
pub fn eval_boolean_strings(op: &str, a: &str, b: &str) -> Option<Expression> {
    let result = match op {
        "=" | "==" => a == b,
        "!=" | "<>" => a != b,
        ">" => a > b,
        ">=" => a >= b,
        "<" => a < b,
        "<=" => a <= b,
        _ => return None,
    };
    Some(if result { bool_true() } else { bool_false() })
}

/// Expression simplifier
pub struct Simplifier {
    _dialect: Option<DialectType>,
    max_iterations: usize,
}

impl Simplifier {
    /// Create a new simplifier
    pub fn new(dialect: Option<DialectType>) -> Self {
        Self {
            _dialect: dialect,
            max_iterations: 100,
        }
    }

    /// Simplify an expression
    pub fn simplify(&mut self, expression: Expression) -> Expression {
        // Apply simplifications until no more changes (or max iterations)
        let mut current = expression;
        for _ in 0..self.max_iterations {
            let simplified = self.simplify_once(current.clone());
            if expressions_equal(&simplified, &current) {
                return simplified;
            }
            current = simplified;
        }
        current
    }

    /// Apply one round of simplifications
    fn simplify_once(&mut self, expression: Expression) -> Expression {
        match expression {
            // Binary logical operations
            Expression::And(op) => self.simplify_and(*op),
            Expression::Or(op) => self.simplify_or(*op),

            // NOT operation - De Morgan's laws
            Expression::Not(op) => self.simplify_not(*op),

            // Arithmetic operations - constant folding
            Expression::Add(op) => self.simplify_add(*op),
            Expression::Sub(op) => self.simplify_sub(*op),
            Expression::Mul(op) => self.simplify_mul(*op),
            Expression::Div(op) => self.simplify_div(*op),

            // Comparison operations
            Expression::Eq(op) => self.simplify_comparison(*op, "="),
            Expression::Neq(op) => self.simplify_comparison(*op, "!="),
            Expression::Gt(op) => self.simplify_comparison(*op, ">"),
            Expression::Gte(op) => self.simplify_comparison(*op, ">="),
            Expression::Lt(op) => self.simplify_comparison(*op, "<"),
            Expression::Lte(op) => self.simplify_comparison(*op, "<="),

            // Negation
            Expression::Neg(op) => self.simplify_neg(*op),

            // CASE expression
            Expression::Case(case) => self.simplify_case(*case),

            // String concatenation
            Expression::Concat(op) => self.simplify_concat(*op),
            Expression::ConcatWs(concat_ws) => self.simplify_concat_ws(*concat_ws),

            // Parentheses - remove if unnecessary
            Expression::Paren(paren) => self.simplify_paren(*paren),

            // Date truncation
            Expression::DateTrunc(dt) => self.simplify_datetrunc(*dt),
            Expression::TimestampTrunc(dt) => self.simplify_datetrunc(*dt),

            // Recursively simplify children for other expressions
            other => self.simplify_children(other),
        }
    }

    /// Simplify AND operation
    fn simplify_and(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // FALSE AND x -> FALSE
        // x AND FALSE -> FALSE
        if is_boolean_false(&left) || is_boolean_false(&right) {
            return bool_false();
        }

        // 0 AND x -> FALSE (in boolean context)
        // x AND 0 -> FALSE
        if is_zero(&left) || is_zero(&right) {
            return bool_false();
        }

        // NULL AND NULL -> NULL
        // NULL AND TRUE -> NULL
        // TRUE AND NULL -> NULL
        if (is_null(&left) && is_null(&right))
            || (is_null(&left) && is_boolean_true(&right))
            || (is_boolean_true(&left) && is_null(&right))
        {
            return null();
        }

        // TRUE AND x -> x (only when left is actually boolean TRUE)
        if is_boolean_true(&left) {
            return right;
        }

        // x AND TRUE -> x (only when right is actually boolean TRUE)
        if is_boolean_true(&right) {
            return left;
        }

        // A AND NOT A -> FALSE (complement elimination)
        if is_complement(&left, &right) || is_complement(&right, &left) {
            return bool_false();
        }

        // A AND A -> A (idempotent)
        if expressions_equal(&left, &right) {
            return left;
        }

        // Apply absorption rules
        // A AND (A OR B) -> A
        // A AND (NOT A OR B) -> A AND B
        absorb_and_eliminate_and(left, right)
    }

    /// Simplify OR operation
    fn simplify_or(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // TRUE OR x -> TRUE (only when left is actually boolean TRUE)
        if is_boolean_true(&left) {
            return bool_true();
        }

        // x OR TRUE -> TRUE (only when right is actually boolean TRUE)
        if is_boolean_true(&right) {
            return bool_true();
        }

        // NULL OR NULL -> NULL
        // NULL OR FALSE -> NULL
        // FALSE OR NULL -> NULL
        if (is_null(&left) && is_null(&right))
            || (is_null(&left) && is_boolean_false(&right))
            || (is_boolean_false(&left) && is_null(&right))
        {
            return null();
        }

        // FALSE OR x -> x (only when left is actually boolean FALSE)
        if is_boolean_false(&left) {
            return right;
        }

        // x OR FALSE -> x (only when right is actually boolean FALSE)
        if is_boolean_false(&right) {
            return left;
        }

        // A OR A -> A (idempotent)
        if expressions_equal(&left, &right) {
            return left;
        }

        // Apply absorption rules
        // A OR (A AND B) -> A
        // A OR (NOT A AND B) -> A OR B
        absorb_and_eliminate_or(left, right)
    }

    /// Simplify NOT operation (De Morgan's laws)
    fn simplify_not(&mut self, op: UnaryOp) -> Expression {
        // Check for De Morgan's laws BEFORE simplifying inner expression
        // This prevents constant folding from eliminating the comparison operator
        match &op.this {
            // NOT (a = b) -> a != b
            Expression::Eq(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Neq(Box::new(BinaryOp::new(left, right)));
            }
            // NOT (a != b) -> a = b
            Expression::Neq(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Eq(Box::new(BinaryOp::new(left, right)));
            }
            // NOT (a > b) -> a <= b
            Expression::Gt(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Lte(Box::new(BinaryOp::new(left, right)));
            }
            // NOT (a >= b) -> a < b
            Expression::Gte(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Lt(Box::new(BinaryOp::new(left, right)));
            }
            // NOT (a < b) -> a >= b
            Expression::Lt(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Gte(Box::new(BinaryOp::new(left, right)));
            }
            // NOT (a <= b) -> a > b
            Expression::Lte(inner_op) => {
                let left = self.simplify_once(inner_op.left.clone());
                let right = self.simplify_once(inner_op.right.clone());
                return Expression::Gt(Box::new(BinaryOp::new(left, right)));
            }
            _ => {}
        }

        // Now simplify the inner expression for other patterns
        let inner = self.simplify_once(op.this);

        // NOT NULL -> NULL (with TRUE for SQL semantics)
        if is_null(&inner) {
            return null();
        }

        // NOT TRUE -> FALSE (only for boolean TRUE literal)
        if is_boolean_true(&inner) {
            return bool_false();
        }

        // NOT FALSE -> TRUE (only for boolean FALSE literal)
        if is_boolean_false(&inner) {
            return bool_true();
        }

        // NOT NOT x -> x (double negation elimination)
        if let Expression::Not(inner_not) = &inner {
            return inner_not.this.clone();
        }

        Expression::Not(Box::new(UnaryOp { this: inner }))
    }

    /// Simplify addition (constant folding)
    fn simplify_add(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Try constant folding for numbers
        if let (Some(a), Some(b)) = (get_number(&left), get_number(&right)) {
            return Expression::Literal(Literal::Number((a + b).to_string()));
        }

        // x + 0 -> x
        if is_zero(&right) {
            return left;
        }

        // 0 + x -> x
        if is_zero(&left) {
            return right;
        }

        Expression::Add(Box::new(BinaryOp::new(left, right)))
    }

    /// Simplify subtraction (constant folding)
    fn simplify_sub(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Try constant folding for numbers
        if let (Some(a), Some(b)) = (get_number(&left), get_number(&right)) {
            return Expression::Literal(Literal::Number((a - b).to_string()));
        }

        // x - 0 -> x
        if is_zero(&right) {
            return left;
        }

        // x - x -> 0 (only for literals/constants)
        if expressions_equal(&left, &right) {
            if let Expression::Literal(Literal::Number(_)) = &left {
                return Expression::Literal(Literal::Number("0".to_string()));
            }
        }

        Expression::Sub(Box::new(BinaryOp::new(left, right)))
    }

    /// Simplify multiplication (constant folding)
    fn simplify_mul(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Try constant folding for numbers
        if let (Some(a), Some(b)) = (get_number(&left), get_number(&right)) {
            return Expression::Literal(Literal::Number((a * b).to_string()));
        }

        // x * 0 -> 0
        if is_zero(&right) {
            return Expression::Literal(Literal::Number("0".to_string()));
        }

        // 0 * x -> 0
        if is_zero(&left) {
            return Expression::Literal(Literal::Number("0".to_string()));
        }

        // x * 1 -> x
        if is_one(&right) {
            return left;
        }

        // 1 * x -> x
        if is_one(&left) {
            return right;
        }

        Expression::Mul(Box::new(BinaryOp::new(left, right)))
    }

    /// Simplify division (constant folding)
    fn simplify_div(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Try constant folding for numbers (but not integer division)
        if let (Some(a), Some(b)) = (get_number(&left), get_number(&right)) {
            // Only fold if both are floats to avoid integer division issues
            if b != 0.0 && (a.fract() != 0.0 || b.fract() != 0.0) {
                return Expression::Literal(Literal::Number((a / b).to_string()));
            }
        }

        // 0 / x -> 0 (when x != 0)
        if is_zero(&left) && !is_zero(&right) {
            return Expression::Literal(Literal::Number("0".to_string()));
        }

        // x / 1 -> x
        if is_one(&right) {
            return left;
        }

        Expression::Div(Box::new(BinaryOp::new(left, right)))
    }

    /// Simplify negation
    fn simplify_neg(&mut self, op: UnaryOp) -> Expression {
        let inner = self.simplify_once(op.this);

        // -(-x) -> x (double negation)
        if let Expression::Neg(inner_neg) = inner {
            return inner_neg.this;
        }

        // -(number) -> -number
        if let Some(n) = get_number(&inner) {
            return Expression::Literal(Literal::Number((-n).to_string()));
        }

        Expression::Neg(Box::new(UnaryOp { this: inner }))
    }

    /// Simplify comparison operations (constant folding)
    fn simplify_comparison(&mut self, op: BinaryOp, operator: &str) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Try constant folding for numbers
        if let (Some(a), Some(b)) = (get_number(&left), get_number(&right)) {
            if let Some(result) = eval_boolean_nums(operator, a, b) {
                return result;
            }
        }

        // Try constant folding for strings
        if let (Some(a), Some(b)) = (get_string(&left), get_string(&right)) {
            if let Some(result) = eval_boolean_strings(operator, &a, &b) {
                return result;
            }
        }

        // For equality, try to solve simple equations (x + 1 = 3 -> x = 2)
        if operator == "=" {
            if let Some(simplified) = self.simplify_equality(left.clone(), right.clone()) {
                return simplified;
            }
        }

        // Reconstruct the comparison
        let new_op = BinaryOp::new(left, right);

        match operator {
            "=" => Expression::Eq(Box::new(new_op)),
            "!=" | "<>" => Expression::Neq(Box::new(new_op)),
            ">" => Expression::Gt(Box::new(new_op)),
            ">=" => Expression::Gte(Box::new(new_op)),
            "<" => Expression::Lt(Box::new(new_op)),
            "<=" => Expression::Lte(Box::new(new_op)),
            _ => Expression::Eq(Box::new(new_op)),
        }
    }

    /// Simplify CASE expression
    fn simplify_case(&mut self, case: Case) -> Expression {
        let mut new_whens = Vec::new();

        for (cond, then_expr) in case.whens {
            let simplified_cond = self.simplify_once(cond);

            // If condition is always true, return the THEN expression
            if always_true(&simplified_cond) {
                return self.simplify_once(then_expr);
            }

            // If condition is always false, skip this WHEN clause
            if always_false(&simplified_cond) {
                continue;
            }

            new_whens.push((simplified_cond, self.simplify_once(then_expr)));
        }

        // If no WHEN clauses remain, return the ELSE expression (or NULL)
        if new_whens.is_empty() {
            return case
                .else_
                .map(|e| self.simplify_once(e))
                .unwrap_or_else(null);
        }

        Expression::Case(Box::new(Case {
            operand: case.operand.map(|e| self.simplify_once(e)),
            whens: new_whens,
            else_: case.else_.map(|e| self.simplify_once(e)),
            comments: Vec::new(),
        }))
    }

    /// Simplify string concatenation (Concat is || operator)
    ///
    /// Folds adjacent string literals:
    /// - 'a' || 'b' -> 'ab'
    /// - 'a' || 'b' || 'c' -> 'abc'
    /// - '' || x -> x
    /// - x || '' -> x
    fn simplify_concat(&mut self, op: BinaryOp) -> Expression {
        let left = self.simplify_once(op.left);
        let right = self.simplify_once(op.right);

        // Fold two string literals: 'a' || 'b' -> 'ab'
        if let (Some(a), Some(b)) = (get_string(&left), get_string(&right)) {
            return Expression::Literal(Literal::String(format!("{}{}", a, b)));
        }

        // '' || x -> x
        if let Some(s) = get_string(&left) {
            if s.is_empty() {
                return right;
            }
        }

        // x || '' -> x
        if let Some(s) = get_string(&right) {
            if s.is_empty() {
                return left;
            }
        }

        // NULL || x -> NULL, x || NULL -> NULL (SQL string concat semantics)
        if is_null(&left) || is_null(&right) {
            return null();
        }

        Expression::Concat(Box::new(BinaryOp::new(left, right)))
    }

    /// Simplify CONCAT_WS function
    ///
    /// CONCAT_WS(sep, a, b, c) -> concatenates with separator, skipping NULLs
    /// - CONCAT_WS(',', 'a', 'b') -> 'a,b' (when all are literals)
    /// - CONCAT_WS(',', 'a', NULL, 'b') -> 'a,b' (NULLs are skipped)
    /// - CONCAT_WS(NULL, ...) -> NULL
    fn simplify_concat_ws(&mut self, concat_ws: ConcatWs) -> Expression {
        let separator = self.simplify_once(concat_ws.separator);

        // If separator is NULL, result is NULL
        if is_null(&separator) {
            return null();
        }

        let expressions: Vec<Expression> = concat_ws
            .expressions
            .into_iter()
            .map(|e| self.simplify_once(e))
            .filter(|e| !is_null(e)) // Skip NULL values
            .collect();

        // If no expressions remain, return empty string
        if expressions.is_empty() {
            return Expression::Literal(Literal::String(String::new()));
        }

        // Try to fold if all are string literals
        if let Some(sep) = get_string(&separator) {
            let all_strings: Option<Vec<String>> =
                expressions.iter().map(|e| get_string(e)).collect();

            if let Some(strings) = all_strings {
                return Expression::Literal(Literal::String(strings.join(&sep)));
            }
        }

        // Return simplified CONCAT_WS
        Expression::ConcatWs(Box::new(ConcatWs {
            separator,
            expressions,
        }))
    }

    /// Simplify parentheses
    ///
    /// Remove unnecessary parentheses:
    /// - (x) -> x when x is a literal, column, or already parenthesized
    /// - ((x)) -> (x) -> x (recursive simplification)
    fn simplify_paren(&mut self, paren: Paren) -> Expression {
        let inner = self.simplify_once(paren.this);

        // If inner is a literal, column, boolean, null, or already parenthesized,
        // we can remove the parentheses
        match &inner {
            Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Null(_)
            | Expression::Column(_)
            | Expression::Paren(_) => inner,
            // For other expressions, keep the parentheses
            _ => Expression::Paren(Box::new(Paren {
                this: inner,
                trailing_comments: paren.trailing_comments,
            })),
        }
    }

    /// Simplify DATE_TRUNC and TIMESTAMP_TRUNC
    ///
    /// Currently just simplifies children and passes through.
    /// Future: could fold DATE_TRUNC('day', '2024-01-15') -> '2024-01-15'
    fn simplify_datetrunc(&mut self, dt: DateTruncFunc) -> Expression {
        let inner = self.simplify_once(dt.this);

        // For now, just return with simplified inner expression
        // A more advanced implementation would fold constant date/timestamps
        Expression::DateTrunc(Box::new(DateTruncFunc {
            this: inner,
            unit: dt.unit,
        }))
    }

    /// Simplify equality with arithmetic (solve simple equations)
    ///
    /// - x + 1 = 3 -> x = 2
    /// - x - 1 = 3 -> x = 4
    /// - x * 2 = 6 -> x = 3 (only when divisible)
    /// - 1 + x = 3 -> x = 2 (commutative)
    fn simplify_equality(&mut self, left: Expression, right: Expression) -> Option<Expression> {
        // Only works when right side is a constant
        let right_val = get_number(&right)?;

        // Check if left side is arithmetic with one constant
        match left {
            Expression::Add(ref op) => {
                // x + c = r -> x = r - c
                if let Some(c) = get_number(&op.right) {
                    let new_right =
                        Expression::Literal(Literal::Number((right_val - c).to_string()));
                    return Some(Expression::Eq(Box::new(BinaryOp::new(
                        op.left.clone(),
                        new_right,
                    ))));
                }
                // c + x = r -> x = r - c
                if let Some(c) = get_number(&op.left) {
                    let new_right =
                        Expression::Literal(Literal::Number((right_val - c).to_string()));
                    return Some(Expression::Eq(Box::new(BinaryOp::new(
                        op.right.clone(),
                        new_right,
                    ))));
                }
            }
            Expression::Sub(ref op) => {
                // x - c = r -> x = r + c
                if let Some(c) = get_number(&op.right) {
                    let new_right =
                        Expression::Literal(Literal::Number((right_val + c).to_string()));
                    return Some(Expression::Eq(Box::new(BinaryOp::new(
                        op.left.clone(),
                        new_right,
                    ))));
                }
                // c - x = r -> x = c - r
                if let Some(c) = get_number(&op.left) {
                    let new_right =
                        Expression::Literal(Literal::Number((c - right_val).to_string()));
                    return Some(Expression::Eq(Box::new(BinaryOp::new(
                        op.right.clone(),
                        new_right,
                    ))));
                }
            }
            Expression::Mul(ref op) => {
                // x * c = r -> x = r / c (only for non-zero c and when divisible)
                if let Some(c) = get_number(&op.right) {
                    if c != 0.0 && right_val % c == 0.0 {
                        let new_right =
                            Expression::Literal(Literal::Number((right_val / c).to_string()));
                        return Some(Expression::Eq(Box::new(BinaryOp::new(
                            op.left.clone(),
                            new_right,
                        ))));
                    }
                }
                // c * x = r -> x = r / c
                if let Some(c) = get_number(&op.left) {
                    if c != 0.0 && right_val % c == 0.0 {
                        let new_right =
                            Expression::Literal(Literal::Number((right_val / c).to_string()));
                        return Some(Expression::Eq(Box::new(BinaryOp::new(
                            op.right.clone(),
                            new_right,
                        ))));
                    }
                }
            }
            _ => {}
        }

        None
    }

    /// Recursively simplify children of an expression
    fn simplify_children(&mut self, expr: Expression) -> Expression {
        // For expressions we don't have specific simplification rules for,
        // we still want to simplify their children
        match expr {
            Expression::Alias(mut alias) => {
                alias.this = self.simplify_once(alias.this);
                Expression::Alias(alias)
            }
            Expression::Between(mut between) => {
                between.this = self.simplify_once(between.this);
                between.low = self.simplify_once(between.low);
                between.high = self.simplify_once(between.high);
                Expression::Between(between)
            }
            Expression::In(mut in_expr) => {
                in_expr.this = self.simplify_once(in_expr.this);
                in_expr.expressions = in_expr
                    .expressions
                    .into_iter()
                    .map(|e| self.simplify_once(e))
                    .collect();
                Expression::In(in_expr)
            }
            Expression::Function(mut func) => {
                func.args = func
                    .args
                    .into_iter()
                    .map(|e| self.simplify_once(e))
                    .collect();
                Expression::Function(func)
            }
            // For other expressions, return as-is for now
            other => other,
        }
    }
}

/// Check if expression equals 1
fn is_one(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(Literal::Number(n)) => {
            if let Ok(num) = n.parse::<f64>() {
                num == 1.0
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Get numeric value from expression if it's a number literal
fn get_number(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::Literal(Literal::Number(n)) => n.parse().ok(),
        _ => None,
    }
}

/// Get string value from expression if it's a string literal
fn get_string(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Literal(Literal::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Check if two expressions are structurally equal
/// This is a simplified comparison - a full implementation would need deep comparison
fn expressions_equal(a: &Expression, b: &Expression) -> bool {
    // For now, use Debug representation for comparison
    // A proper implementation would do structural comparison
    format!("{:?}", a) == format!("{:?}", b)
}

/// Flatten nested AND expressions into a list of operands
/// e.g., (A AND (B AND C)) -> [A, B, C]
fn flatten_and(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::And(op) => {
            let mut result = flatten_and(&op.left);
            result.extend(flatten_and(&op.right));
            result
        }
        other => vec![other.clone()],
    }
}

/// Flatten nested OR expressions into a list of operands
/// e.g., (A OR (B OR C)) -> [A, B, C]
fn flatten_or(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Or(op) => {
            let mut result = flatten_or(&op.left);
            result.extend(flatten_or(&op.right));
            result
        }
        other => vec![other.clone()],
    }
}

/// Rebuild an AND expression from a list of operands
fn rebuild_and(operands: Vec<Expression>) -> Expression {
    if operands.is_empty() {
        return bool_true(); // Empty AND is TRUE
    }
    let mut result = operands.into_iter();
    let first = result.next().unwrap();
    result.fold(first, |acc, op| {
        Expression::And(Box::new(BinaryOp::new(acc, op)))
    })
}

/// Rebuild an OR expression from a list of operands
fn rebuild_or(operands: Vec<Expression>) -> Expression {
    if operands.is_empty() {
        return bool_false(); // Empty OR is FALSE
    }
    let mut result = operands.into_iter();
    let first = result.next().unwrap();
    result.fold(first, |acc, op| {
        Expression::Or(Box::new(BinaryOp::new(acc, op)))
    })
}

/// Get the inner expression of a NOT, if it is one
fn get_not_inner(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Not(op) => Some(&op.this),
        _ => None,
    }
}

/// Apply Boolean absorption and elimination rules to an AND expression
///
/// Absorption:
///   A AND (A OR B) -> A
///   A AND (NOT A OR B) -> A AND B
///
/// Elimination:
///   (A OR B) AND (A OR NOT B) -> A
pub fn absorb_and_eliminate_and(left: Expression, right: Expression) -> Expression {
    // Flatten both sides
    let left_ops = flatten_and(&left);
    let right_ops = flatten_and(&right);
    let all_ops: Vec<Expression> = left_ops.iter().chain(right_ops.iter()).cloned().collect();

    // Build a set of string representations for quick lookup
    let op_strings: std::collections::HashSet<String> = all_ops.iter().map(gen).collect();

    let mut result_ops: Vec<Expression> = Vec::new();
    let mut absorbed = std::collections::HashSet::new();

    for (i, op) in all_ops.iter().enumerate() {
        let op_str = gen(op);

        // Skip if already absorbed
        if absorbed.contains(&op_str) {
            continue;
        }

        // Check if this is an OR expression (potential absorption target)
        if let Expression::Or(_) = op {
            let or_operands = flatten_or(op);

            // Absorption: A AND (A OR B) -> A
            // Check if any OR operand is already in our AND operands
            let absorbed_by_existing = or_operands.iter().any(|or_op| {
                let or_op_str = gen(or_op);
                // Check if this OR operand exists in other AND operands (not this OR itself)
                all_ops
                    .iter()
                    .enumerate()
                    .any(|(j, other)| i != j && gen(other) == or_op_str)
            });

            if absorbed_by_existing {
                // This OR is absorbed, skip it
                absorbed.insert(op_str);
                continue;
            }

            // Absorption with complement: A AND (NOT A OR B) -> A AND B
            // Check if any OR operand's complement is in our AND operands
            let mut remaining_or_ops: Vec<Expression> = Vec::new();
            let mut had_complement_absorption = false;

            for or_op in or_operands {
                let complement_str = if let Some(inner) = get_not_inner(&or_op) {
                    // or_op is NOT X, complement is X
                    gen(inner)
                } else {
                    // or_op is X, complement is NOT X
                    format!("NOT {}", gen(&or_op))
                };

                // Check if complement exists in our AND operands
                let has_complement = all_ops
                    .iter()
                    .enumerate()
                    .any(|(j, other)| i != j && gen(other) == complement_str)
                    || op_strings.contains(&complement_str);

                if has_complement {
                    // This OR operand's complement exists, so this term becomes TRUE in AND context
                    // NOT A OR B, where A exists, becomes TRUE OR B (when A is true) or B (when A is false)
                    // Actually: A AND (NOT A OR B) -> A AND B, so we drop NOT A from the OR
                    had_complement_absorption = true;
                    // Drop this operand from OR
                } else {
                    remaining_or_ops.push(or_op);
                }
            }

            if had_complement_absorption {
                if remaining_or_ops.is_empty() {
                    // All OR operands were absorbed, the OR becomes TRUE
                    // A AND TRUE -> A, so we just skip adding this
                    absorbed.insert(op_str);
                    continue;
                } else if remaining_or_ops.len() == 1 {
                    // Single remaining operand
                    result_ops.push(remaining_or_ops.into_iter().next().unwrap());
                    absorbed.insert(op_str);
                    continue;
                } else {
                    // Rebuild the OR with remaining operands
                    result_ops.push(rebuild_or(remaining_or_ops));
                    absorbed.insert(op_str);
                    continue;
                }
            }
        }

        result_ops.push(op.clone());
    }

    // Deduplicate
    let mut seen = std::collections::HashSet::new();
    result_ops.retain(|op| seen.insert(gen(op)));

    if result_ops.is_empty() {
        bool_true()
    } else {
        rebuild_and(result_ops)
    }
}

/// Apply Boolean absorption and elimination rules to an OR expression
///
/// Absorption:
///   A OR (A AND B) -> A
///   A OR (NOT A AND B) -> A OR B
///
/// Elimination:
///   (A AND B) OR (A AND NOT B) -> A
pub fn absorb_and_eliminate_or(left: Expression, right: Expression) -> Expression {
    // Flatten both sides
    let left_ops = flatten_or(&left);
    let right_ops = flatten_or(&right);
    let all_ops: Vec<Expression> = left_ops.iter().chain(right_ops.iter()).cloned().collect();

    // Build a set of string representations for quick lookup
    let op_strings: std::collections::HashSet<String> = all_ops.iter().map(gen).collect();

    let mut result_ops: Vec<Expression> = Vec::new();
    let mut absorbed = std::collections::HashSet::new();

    for (i, op) in all_ops.iter().enumerate() {
        let op_str = gen(op);

        // Skip if already absorbed
        if absorbed.contains(&op_str) {
            continue;
        }

        // Check if this is an AND expression (potential absorption target)
        if let Expression::And(_) = op {
            let and_operands = flatten_and(op);

            // Absorption: A OR (A AND B) -> A
            // Check if any AND operand is already in our OR operands
            let absorbed_by_existing = and_operands.iter().any(|and_op| {
                let and_op_str = gen(and_op);
                // Check if this AND operand exists in other OR operands (not this AND itself)
                all_ops
                    .iter()
                    .enumerate()
                    .any(|(j, other)| i != j && gen(other) == and_op_str)
            });

            if absorbed_by_existing {
                // This AND is absorbed, skip it
                absorbed.insert(op_str);
                continue;
            }

            // Absorption with complement: A OR (NOT A AND B) -> A OR B
            // Check if any AND operand's complement is in our OR operands
            let mut remaining_and_ops: Vec<Expression> = Vec::new();
            let mut had_complement_absorption = false;

            for and_op in and_operands {
                let complement_str = if let Some(inner) = get_not_inner(&and_op) {
                    // and_op is NOT X, complement is X
                    gen(inner)
                } else {
                    // and_op is X, complement is NOT X
                    format!("NOT {}", gen(&and_op))
                };

                // Check if complement exists in our OR operands
                let has_complement = all_ops
                    .iter()
                    .enumerate()
                    .any(|(j, other)| i != j && gen(other) == complement_str)
                    || op_strings.contains(&complement_str);

                if has_complement {
                    // This AND operand's complement exists, so this term becomes FALSE in OR context
                    // A OR (NOT A AND B) -> A OR B, so we drop NOT A from the AND
                    had_complement_absorption = true;
                    // Drop this operand from AND
                } else {
                    remaining_and_ops.push(and_op);
                }
            }

            if had_complement_absorption {
                if remaining_and_ops.is_empty() {
                    // All AND operands were absorbed, the AND becomes FALSE
                    // A OR FALSE -> A, so we just skip adding this
                    absorbed.insert(op_str);
                    continue;
                } else if remaining_and_ops.len() == 1 {
                    // Single remaining operand
                    result_ops.push(remaining_and_ops.into_iter().next().unwrap());
                    absorbed.insert(op_str);
                    continue;
                } else {
                    // Rebuild the AND with remaining operands
                    result_ops.push(rebuild_and(remaining_and_ops));
                    absorbed.insert(op_str);
                    continue;
                }
            }
        }

        result_ops.push(op.clone());
    }

    // Deduplicate
    let mut seen = std::collections::HashSet::new();
    result_ops.retain(|op| seen.insert(gen(op)));

    if result_ops.is_empty() {
        bool_false()
    } else {
        rebuild_or(result_ops)
    }
}

/// Generate a simple string representation of an expression for sorting/deduping
pub fn gen(expr: &Expression) -> String {
    match expr {
        Expression::Literal(lit) => match lit {
            Literal::String(s) => format!("'{}'", s),
            Literal::Number(n) => n.clone(),
            _ => format!("{:?}", lit),
        },
        Expression::Boolean(b) => if b.value { "TRUE" } else { "FALSE" }.to_string(),
        Expression::Null(_) => "NULL".to_string(),
        Expression::Column(col) => {
            if let Some(ref table) = col.table {
                format!("{}.{}", table.name, col.name.name)
            } else {
                col.name.name.clone()
            }
        }
        Expression::And(op) => format!("({} AND {})", gen(&op.left), gen(&op.right)),
        Expression::Or(op) => format!("({} OR {})", gen(&op.left), gen(&op.right)),
        Expression::Not(op) => format!("NOT {}", gen(&op.this)),
        Expression::Eq(op) => format!("{} = {}", gen(&op.left), gen(&op.right)),
        Expression::Neq(op) => format!("{} <> {}", gen(&op.left), gen(&op.right)),
        Expression::Gt(op) => format!("{} > {}", gen(&op.left), gen(&op.right)),
        Expression::Gte(op) => format!("{} >= {}", gen(&op.left), gen(&op.right)),
        Expression::Lt(op) => format!("{} < {}", gen(&op.left), gen(&op.right)),
        Expression::Lte(op) => format!("{} <= {}", gen(&op.left), gen(&op.right)),
        Expression::Add(op) => format!("{} + {}", gen(&op.left), gen(&op.right)),
        Expression::Sub(op) => format!("{} - {}", gen(&op.left), gen(&op.right)),
        Expression::Mul(op) => format!("{} * {}", gen(&op.left), gen(&op.right)),
        Expression::Div(op) => format!("{} / {}", gen(&op.left), gen(&op.right)),
        Expression::Function(f) => {
            let args: Vec<String> = f.args.iter().map(|a| gen(a)).collect();
            format!("{}({})", f.name.to_uppercase(), args.join(", "))
        }
        _ => format!("{:?}", expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_int(val: i64) -> Expression {
        Expression::Literal(Literal::Number(val.to_string()))
    }

    fn make_string(val: &str) -> Expression {
        Expression::Literal(Literal::String(val.to_string()))
    }

    fn make_bool(val: bool) -> Expression {
        Expression::Boolean(BooleanLiteral { value: val })
    }

    fn make_column(name: &str) -> Expression {
        use crate::expressions::{Column, Identifier};
        Expression::Column(Column {
            name: Identifier::new(name),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        })
    }

    #[test]
    fn test_always_true_false() {
        assert!(always_true(&make_bool(true)));
        assert!(!always_true(&make_bool(false)));
        assert!(always_true(&make_int(1)));
        assert!(!always_true(&make_int(0)));

        assert!(always_false(&make_bool(false)));
        assert!(!always_false(&make_bool(true)));
        assert!(always_false(&null()));
        assert!(always_false(&make_int(0)));
    }

    #[test]
    fn test_simplify_and_with_true() {
        let mut simplifier = Simplifier::new(None);

        // TRUE AND TRUE -> TRUE
        let expr = Expression::And(Box::new(BinaryOp::new(make_bool(true), make_bool(true))));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));

        // TRUE AND FALSE -> FALSE
        let expr = Expression::And(Box::new(BinaryOp::new(make_bool(true), make_bool(false))));
        let result = simplifier.simplify(expr);
        assert!(always_false(&result));

        // TRUE AND x -> x
        let x = make_int(42);
        let expr = Expression::And(Box::new(BinaryOp::new(make_bool(true), x.clone())));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_simplify_or_with_false() {
        let mut simplifier = Simplifier::new(None);

        // FALSE OR FALSE -> FALSE
        let expr = Expression::Or(Box::new(BinaryOp::new(make_bool(false), make_bool(false))));
        let result = simplifier.simplify(expr);
        assert!(always_false(&result));

        // FALSE OR TRUE -> TRUE
        let expr = Expression::Or(Box::new(BinaryOp::new(make_bool(false), make_bool(true))));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));

        // FALSE OR x -> x
        let x = make_int(42);
        let expr = Expression::Or(Box::new(BinaryOp::new(make_bool(false), x.clone())));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_simplify_not() {
        let mut simplifier = Simplifier::new(None);

        // NOT TRUE -> FALSE
        let expr = Expression::Not(Box::new(UnaryOp::new(make_bool(true))));
        let result = simplifier.simplify(expr);
        assert!(is_false(&result));

        // NOT FALSE -> TRUE
        let expr = Expression::Not(Box::new(UnaryOp::new(make_bool(false))));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));

        // NOT NOT x -> x
        let x = make_int(42);
        let inner_not = Expression::Not(Box::new(UnaryOp::new(x.clone())));
        let expr = Expression::Not(Box::new(UnaryOp::new(inner_not)));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_simplify_demorgan_comparison() {
        let mut simplifier = Simplifier::new(None);

        // NOT (a = b) -> a != b (using columns to avoid constant folding)
        let a = make_column("a");
        let b = make_column("b");
        let eq = Expression::Eq(Box::new(BinaryOp::new(a.clone(), b.clone())));
        let expr = Expression::Not(Box::new(UnaryOp::new(eq)));
        let result = simplifier.simplify(expr);
        assert!(matches!(result, Expression::Neq(_)));

        // NOT (a > b) -> a <= b
        let gt = Expression::Gt(Box::new(BinaryOp::new(a, b)));
        let expr = Expression::Not(Box::new(UnaryOp::new(gt)));
        let result = simplifier.simplify(expr);
        assert!(matches!(result, Expression::Lte(_)));
    }

    #[test]
    fn test_constant_folding_add() {
        let mut simplifier = Simplifier::new(None);

        // 1 + 2 -> 3
        let expr = Expression::Add(Box::new(BinaryOp::new(make_int(1), make_int(2))));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(3.0));

        // x + 0 -> x
        let x = make_int(42);
        let expr = Expression::Add(Box::new(BinaryOp::new(x.clone(), make_int(0))));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_constant_folding_mul() {
        let mut simplifier = Simplifier::new(None);

        // 3 * 4 -> 12
        let expr = Expression::Mul(Box::new(BinaryOp::new(make_int(3), make_int(4))));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(12.0));

        // x * 0 -> 0
        let x = make_int(42);
        let expr = Expression::Mul(Box::new(BinaryOp::new(x, make_int(0))));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(0.0));

        // x * 1 -> x
        let x = make_int(42);
        let expr = Expression::Mul(Box::new(BinaryOp::new(x.clone(), make_int(1))));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_constant_folding_comparison() {
        let mut simplifier = Simplifier::new(None);

        // 1 = 1 -> TRUE
        let expr = Expression::Eq(Box::new(BinaryOp::new(make_int(1), make_int(1))));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));

        // 1 = 2 -> FALSE
        let expr = Expression::Eq(Box::new(BinaryOp::new(make_int(1), make_int(2))));
        let result = simplifier.simplify(expr);
        assert!(is_false(&result));

        // 3 > 2 -> TRUE
        let expr = Expression::Gt(Box::new(BinaryOp::new(make_int(3), make_int(2))));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));

        // 'a' = 'a' -> TRUE
        let expr = Expression::Eq(Box::new(BinaryOp::new(
            make_string("abc"),
            make_string("abc"),
        )));
        let result = simplifier.simplify(expr);
        assert!(always_true(&result));
    }

    #[test]
    fn test_simplify_negation() {
        let mut simplifier = Simplifier::new(None);

        // -(-5) -> 5
        let inner = Expression::Neg(Box::new(UnaryOp::new(make_int(5))));
        let expr = Expression::Neg(Box::new(UnaryOp::new(inner)));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(5.0));

        // -(3) -> -3
        let expr = Expression::Neg(Box::new(UnaryOp::new(make_int(3))));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(-3.0));
    }

    #[test]
    fn test_gen_simple() {
        assert_eq!(gen(&make_int(42)), "42");
        assert_eq!(gen(&make_string("hello")), "'hello'");
        assert_eq!(gen(&make_bool(true)), "TRUE");
        assert_eq!(gen(&make_bool(false)), "FALSE");
        assert_eq!(gen(&null()), "NULL");
    }

    #[test]
    fn test_gen_operations() {
        let add = Expression::Add(Box::new(BinaryOp::new(make_int(1), make_int(2))));
        assert_eq!(gen(&add), "1 + 2");

        let and = Expression::And(Box::new(BinaryOp::new(make_bool(true), make_bool(false))));
        assert_eq!(gen(&and), "(TRUE AND FALSE)");
    }

    #[test]
    fn test_complement_elimination() {
        let mut simplifier = Simplifier::new(None);

        // x AND NOT x -> FALSE
        let x = make_int(42);
        let not_x = Expression::Not(Box::new(UnaryOp::new(x.clone())));
        let expr = Expression::And(Box::new(BinaryOp::new(x, not_x)));
        let result = simplifier.simplify(expr);
        assert!(is_false(&result));
    }

    #[test]
    fn test_idempotent() {
        let mut simplifier = Simplifier::new(None);

        // x AND x -> x
        let x = make_int(42);
        let expr = Expression::And(Box::new(BinaryOp::new(x.clone(), x.clone())));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));

        // x OR x -> x
        let x = make_int(42);
        let expr = Expression::Or(Box::new(BinaryOp::new(x.clone(), x.clone())));
        let result = simplifier.simplify(expr);
        assert_eq!(format!("{:?}", result), format!("{:?}", x));
    }

    #[test]
    fn test_absorption_and() {
        let mut simplifier = Simplifier::new(None);

        // A AND (A OR B) -> A
        let a = make_column("a");
        let b = make_column("b");
        let a_or_b = Expression::Or(Box::new(BinaryOp::new(a.clone(), b.clone())));
        let expr = Expression::And(Box::new(BinaryOp::new(a.clone(), a_or_b)));
        let result = simplifier.simplify(expr);
        // Result should be just A
        assert_eq!(gen(&result), gen(&a));
    }

    #[test]
    fn test_absorption_or() {
        let mut simplifier = Simplifier::new(None);

        // A OR (A AND B) -> A
        let a = make_column("a");
        let b = make_column("b");
        let a_and_b = Expression::And(Box::new(BinaryOp::new(a.clone(), b.clone())));
        let expr = Expression::Or(Box::new(BinaryOp::new(a.clone(), a_and_b)));
        let result = simplifier.simplify(expr);
        // Result should be just A
        assert_eq!(gen(&result), gen(&a));
    }

    #[test]
    fn test_absorption_with_complement_and() {
        let mut simplifier = Simplifier::new(None);

        // A AND (NOT A OR B) -> A AND B
        let a = make_column("a");
        let b = make_column("b");
        let not_a = Expression::Not(Box::new(UnaryOp::new(a.clone())));
        let not_a_or_b = Expression::Or(Box::new(BinaryOp::new(not_a, b.clone())));
        let expr = Expression::And(Box::new(BinaryOp::new(a.clone(), not_a_or_b)));
        let result = simplifier.simplify(expr);
        // Result should be A AND B
        let expected = Expression::And(Box::new(BinaryOp::new(a, b)));
        assert_eq!(gen(&result), gen(&expected));
    }

    #[test]
    fn test_absorption_with_complement_or() {
        let mut simplifier = Simplifier::new(None);

        // A OR (NOT A AND B) -> A OR B
        let a = make_column("a");
        let b = make_column("b");
        let not_a = Expression::Not(Box::new(UnaryOp::new(a.clone())));
        let not_a_and_b = Expression::And(Box::new(BinaryOp::new(not_a, b.clone())));
        let expr = Expression::Or(Box::new(BinaryOp::new(a.clone(), not_a_and_b)));
        let result = simplifier.simplify(expr);
        // Result should be A OR B
        let expected = Expression::Or(Box::new(BinaryOp::new(a, b)));
        assert_eq!(gen(&result), gen(&expected));
    }

    #[test]
    fn test_flatten_and() {
        // (A AND (B AND C)) should flatten to [A, B, C]
        let a = make_column("a");
        let b = make_column("b");
        let c = make_column("c");
        let b_and_c = Expression::And(Box::new(BinaryOp::new(b.clone(), c.clone())));
        let expr = Expression::And(Box::new(BinaryOp::new(a.clone(), b_and_c)));
        let flattened = flatten_and(&expr);
        assert_eq!(flattened.len(), 3);
        assert_eq!(gen(&flattened[0]), "a");
        assert_eq!(gen(&flattened[1]), "b");
        assert_eq!(gen(&flattened[2]), "c");
    }

    #[test]
    fn test_flatten_or() {
        // (A OR (B OR C)) should flatten to [A, B, C]
        let a = make_column("a");
        let b = make_column("b");
        let c = make_column("c");
        let b_or_c = Expression::Or(Box::new(BinaryOp::new(b.clone(), c.clone())));
        let expr = Expression::Or(Box::new(BinaryOp::new(a.clone(), b_or_c)));
        let flattened = flatten_or(&expr);
        assert_eq!(flattened.len(), 3);
        assert_eq!(gen(&flattened[0]), "a");
        assert_eq!(gen(&flattened[1]), "b");
        assert_eq!(gen(&flattened[2]), "c");
    }

    #[test]
    fn test_simplify_concat() {
        let mut simplifier = Simplifier::new(None);

        // 'a' || 'b' -> 'ab'
        let expr = Expression::Concat(Box::new(BinaryOp::new(
            make_string("hello"),
            make_string("world"),
        )));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("helloworld".to_string()));

        // '' || x -> x
        let x = make_string("test");
        let expr = Expression::Concat(Box::new(BinaryOp::new(make_string(""), x.clone())));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("test".to_string()));

        // x || '' -> x
        let expr = Expression::Concat(Box::new(BinaryOp::new(x, make_string(""))));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("test".to_string()));

        // NULL || x -> NULL
        let expr = Expression::Concat(Box::new(BinaryOp::new(null(), make_string("test"))));
        let result = simplifier.simplify(expr);
        assert!(is_null(&result));
    }

    #[test]
    fn test_simplify_concat_ws() {
        let mut simplifier = Simplifier::new(None);

        // CONCAT_WS(',', 'a', 'b', 'c') -> 'a,b,c'
        let expr = Expression::ConcatWs(Box::new(ConcatWs {
            separator: make_string(","),
            expressions: vec![make_string("a"), make_string("b"), make_string("c")],
        }));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("a,b,c".to_string()));

        // CONCAT_WS with NULL separator -> NULL
        let expr = Expression::ConcatWs(Box::new(ConcatWs {
            separator: null(),
            expressions: vec![make_string("a"), make_string("b")],
        }));
        let result = simplifier.simplify(expr);
        assert!(is_null(&result));

        // CONCAT_WS with empty expressions -> ''
        let expr = Expression::ConcatWs(Box::new(ConcatWs {
            separator: make_string(","),
            expressions: vec![],
        }));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("".to_string()));

        // CONCAT_WS skips NULLs
        let expr = Expression::ConcatWs(Box::new(ConcatWs {
            separator: make_string("-"),
            expressions: vec![make_string("a"), null(), make_string("b")],
        }));
        let result = simplifier.simplify(expr);
        assert_eq!(get_string(&result), Some("a-b".to_string()));
    }

    #[test]
    fn test_simplify_paren() {
        let mut simplifier = Simplifier::new(None);

        // (42) -> 42
        let expr = Expression::Paren(Box::new(Paren {
            this: make_int(42),
            trailing_comments: vec![],
        }));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(42.0));

        // (TRUE) -> TRUE
        let expr = Expression::Paren(Box::new(Paren {
            this: make_bool(true),
            trailing_comments: vec![],
        }));
        let result = simplifier.simplify(expr);
        assert!(is_boolean_true(&result));

        // (NULL) -> NULL
        let expr = Expression::Paren(Box::new(Paren {
            this: null(),
            trailing_comments: vec![],
        }));
        let result = simplifier.simplify(expr);
        assert!(is_null(&result));

        // ((x)) -> x
        let inner_paren = Expression::Paren(Box::new(Paren {
            this: make_int(10),
            trailing_comments: vec![],
        }));
        let expr = Expression::Paren(Box::new(Paren {
            this: inner_paren,
            trailing_comments: vec![],
        }));
        let result = simplifier.simplify(expr);
        assert_eq!(get_number(&result), Some(10.0));
    }

    #[test]
    fn test_simplify_equality_solve() {
        let mut simplifier = Simplifier::new(None);

        // x + 1 = 3 -> x = 2
        let x = make_column("x");
        let x_plus_1 = Expression::Add(Box::new(BinaryOp::new(x.clone(), make_int(1))));
        let expr = Expression::Eq(Box::new(BinaryOp::new(x_plus_1, make_int(3))));
        let result = simplifier.simplify(expr);
        // Result should be x = 2
        if let Expression::Eq(op) = &result {
            assert_eq!(gen(&op.left), "x");
            assert_eq!(get_number(&op.right), Some(2.0));
        } else {
            panic!("Expected Eq expression");
        }

        // x - 1 = 3 -> x = 4
        let x_minus_1 = Expression::Sub(Box::new(BinaryOp::new(x.clone(), make_int(1))));
        let expr = Expression::Eq(Box::new(BinaryOp::new(x_minus_1, make_int(3))));
        let result = simplifier.simplify(expr);
        if let Expression::Eq(op) = &result {
            assert_eq!(gen(&op.left), "x");
            assert_eq!(get_number(&op.right), Some(4.0));
        } else {
            panic!("Expected Eq expression");
        }

        // x * 2 = 6 -> x = 3
        let x_times_2 = Expression::Mul(Box::new(BinaryOp::new(x.clone(), make_int(2))));
        let expr = Expression::Eq(Box::new(BinaryOp::new(x_times_2, make_int(6))));
        let result = simplifier.simplify(expr);
        if let Expression::Eq(op) = &result {
            assert_eq!(gen(&op.left), "x");
            assert_eq!(get_number(&op.right), Some(3.0));
        } else {
            panic!("Expected Eq expression");
        }

        // 1 + x = 3 -> x = 2 (commutative)
        let one_plus_x = Expression::Add(Box::new(BinaryOp::new(make_int(1), x.clone())));
        let expr = Expression::Eq(Box::new(BinaryOp::new(one_plus_x, make_int(3))));
        let result = simplifier.simplify(expr);
        if let Expression::Eq(op) = &result {
            assert_eq!(gen(&op.left), "x");
            assert_eq!(get_number(&op.right), Some(2.0));
        } else {
            panic!("Expected Eq expression");
        }
    }

    #[test]
    fn test_simplify_datetrunc() {
        use crate::expressions::DateTimeField;
        let mut simplifier = Simplifier::new(None);

        // DATE_TRUNC('day', x) with a column just passes through with simplified children
        let x = make_column("x");
        let expr = Expression::DateTrunc(Box::new(DateTruncFunc {
            this: x.clone(),
            unit: DateTimeField::Day,
        }));
        let result = simplifier.simplify(expr);
        if let Expression::DateTrunc(dt) = &result {
            assert_eq!(gen(&dt.this), "x");
            assert_eq!(dt.unit, DateTimeField::Day);
        } else {
            panic!("Expected DateTrunc expression");
        }
    }
}
