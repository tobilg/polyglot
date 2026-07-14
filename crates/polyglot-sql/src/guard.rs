//! Shared complexity guards for recursion-heavy SQL operations.

use crate::error::{Error, Result};
use crate::expressions::Expression;
use crate::tokens::{ParserToken, Span, Token, TokenType};
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_TOKENS: usize = 1_000_000;
const DEFAULT_MAX_AST_NODES: usize = 1_000_000;
const DEFAULT_MAX_AST_DEPTH: usize = 512;
const DEFAULT_MAX_PARENTHESES_DEPTH: usize = 512;
const DEFAULT_MAX_FUNCTION_CALL_DEPTH: usize = 64;

fn default_max_input_bytes() -> Option<usize> {
    Some(DEFAULT_MAX_INPUT_BYTES)
}

fn default_max_tokens() -> Option<usize> {
    Some(DEFAULT_MAX_TOKENS)
}

fn default_max_ast_nodes() -> Option<usize> {
    Some(DEFAULT_MAX_AST_NODES)
}

fn default_max_ast_depth() -> Option<usize> {
    Some(DEFAULT_MAX_AST_DEPTH)
}

fn default_max_parenthesis_depth() -> Option<usize> {
    Some(DEFAULT_MAX_PARENTHESES_DEPTH)
}

fn default_max_function_call_depth() -> Option<usize> {
    Some(DEFAULT_MAX_FUNCTION_CALL_DEPTH)
}

/// Guard options for parse/transpile/generate complexity.
///
/// These limits turn excessively deep or large inputs into regular errors
/// instead of relying on process stack exhaustion as the failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComplexityGuardOptions {
    /// Maximum allowed SQL input size in bytes.
    /// `None` disables this check.
    #[serde(default = "default_max_input_bytes")]
    pub max_input_bytes: Option<usize>,
    /// Maximum allowed number of tokens after tokenization.
    /// `None` disables this check.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: Option<usize>,
    /// Maximum allowed AST node count after parsing.
    /// `None` disables this check.
    #[serde(default = "default_max_ast_nodes")]
    pub max_ast_nodes: Option<usize>,
    /// Maximum allowed AST depth after parsing.
    /// `None` disables this check.
    #[serde(default = "default_max_ast_depth")]
    pub max_ast_depth: Option<usize>,
    /// Maximum allowed nested parenthesis depth before parsing.
    /// `None` disables this check.
    #[serde(default = "default_max_parenthesis_depth")]
    pub max_parenthesis_depth: Option<usize>,
    /// Maximum allowed nested function-call depth before parsing.
    /// `None` disables this check.
    #[serde(default = "default_max_function_call_depth")]
    pub max_function_call_depth: Option<usize>,
}

#[derive(Debug, Default)]
pub(crate) struct TokenGuardStats {
    pub token_count: usize,
    parenthesis_depth_spans: Vec<Span>,
    function_depth_spans: Vec<Span>,
    parenthesis_stack: Vec<bool>,
    parenthesis_depth: usize,
    function_depth: usize,
    previous_significant: Option<TokenType>,
}

impl TokenGuardStats {
    pub(crate) fn observe(&mut self, token_type: TokenType, span: Span) {
        self.token_count += 1;
        match token_type {
            TokenType::LParen => {
                self.parenthesis_depth += 1;
                if self.parenthesis_depth_spans.len() < self.parenthesis_depth {
                    self.parenthesis_depth_spans.push(span);
                }

                let is_function_call = self
                    .previous_significant
                    .map(is_function_call_name_token)
                    .unwrap_or(false);
                self.parenthesis_stack.push(is_function_call);
                if is_function_call {
                    self.function_depth += 1;
                    if self.function_depth_spans.len() < self.function_depth {
                        self.function_depth_spans.push(span);
                    }
                }
            }
            TokenType::RParen => {
                self.parenthesis_depth = self.parenthesis_depth.saturating_sub(1);
                if self.parenthesis_stack.pop().unwrap_or(false) {
                    self.function_depth = self.function_depth.saturating_sub(1);
                }
            }
            _ => {}
        }

        if !is_trivia_token(token_type) {
            self.previous_significant = Some(token_type);
        }
    }
}

impl Default for ComplexityGuardOptions {
    fn default() -> Self {
        Self {
            max_input_bytes: default_max_input_bytes(),
            max_tokens: default_max_tokens(),
            max_ast_nodes: default_max_ast_nodes(),
            max_ast_depth: default_max_ast_depth(),
            max_parenthesis_depth: default_max_parenthesis_depth(),
            max_function_call_depth: default_max_function_call_depth(),
        }
    }
}

fn parse_guard_error(code: &str, actual: usize, limit: usize, span: Option<Span>) -> Error {
    let message = format!("{code}: value {actual} exceeds configured limit {limit}");
    if let Some(span) = span {
        Error::parse(message, span.line, span.column, span.start, span.end)
    } else {
        Error::parse(message, 0, 0, 0, 0)
    }
}

fn generate_guard_error(code: &str, actual: usize, limit: usize) -> Error {
    Error::generate(format!(
        "{code}: value {actual} exceeds configured limit {limit}"
    ))
}

/// Enforce raw SQL input limits before tokenization.
pub fn enforce_input(sql: &str, options: &ComplexityGuardOptions) -> Result<()> {
    if let Some(max) = options.max_input_bytes {
        let input_bytes = sql.len();
        if input_bytes > max {
            return Err(parse_guard_error(
                "E_GUARD_INPUT_TOO_LARGE",
                input_bytes,
                max,
                None,
            ));
        }
    }

    Ok(())
}

/// Enforce token and pre-parse nesting limits.
trait GuardToken {
    fn token_type(&self) -> TokenType;
    fn span(&self) -> Span;
}

impl GuardToken for Token {
    fn token_type(&self) -> TokenType {
        self.token_type
    }

    fn span(&self) -> Span {
        self.span
    }
}

impl GuardToken for ParserToken {
    fn token_type(&self) -> TokenType {
        self.token_type
    }

    fn span(&self) -> Span {
        self.span
    }
}

fn enforce_token_slice<T: GuardToken>(
    tokens: &[T],
    options: &ComplexityGuardOptions,
) -> Result<()> {
    if let Some(max) = options.max_tokens {
        let token_count = tokens.len();
        if token_count > max {
            let span = tokens
                .get(max)
                .or_else(|| tokens.last())
                .map(GuardToken::span);
            return Err(parse_guard_error(
                "E_GUARD_TOKEN_BUDGET_EXCEEDED",
                token_count,
                max,
                span,
            ));
        }
    }

    if options.max_parenthesis_depth.is_some() || options.max_function_call_depth.is_some() {
        let mut paren_depth = 0usize;
        let mut function_depth = 0usize;
        let mut paren_stack = Vec::new();
        let mut previous_significant: Option<TokenType> = None;

        for token in tokens {
            match token.token_type() {
                TokenType::LParen => {
                    paren_depth += 1;
                    if let Some(max) = options.max_parenthesis_depth {
                        if paren_depth > max {
                            return Err(parse_guard_error(
                                "E_GUARD_NESTING_DEPTH_EXCEEDED",
                                paren_depth,
                                max,
                                Some(token.span()),
                            ));
                        }
                    }

                    let is_function_call = previous_significant
                        .map(is_function_call_name_token)
                        .unwrap_or(false);
                    paren_stack.push(is_function_call);
                    if is_function_call {
                        function_depth += 1;
                        if let Some(max) = options.max_function_call_depth {
                            if function_depth > max {
                                return Err(parse_guard_error(
                                    "E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED",
                                    function_depth,
                                    max,
                                    Some(token.span()),
                                ));
                            }
                        }
                    }
                }
                TokenType::RParen => {
                    paren_depth = paren_depth.saturating_sub(1);
                    if paren_stack.pop().unwrap_or(false) {
                        function_depth = function_depth.saturating_sub(1);
                    }
                }
                _ => {}
            }

            if !is_trivia_token(token.token_type()) {
                previous_significant = Some(token.token_type());
            }
        }
    }

    Ok(())
}

pub fn enforce_tokens(tokens: &[Token], options: &ComplexityGuardOptions) -> Result<()> {
    enforce_token_slice(tokens, options)
}

pub(crate) fn enforce_parser_tokens(
    tokens: &[ParserToken],
    options: &ComplexityGuardOptions,
) -> Result<()> {
    enforce_token_slice(tokens, options)
}

pub(crate) fn enforce_parser_token_stats(
    tokens: &[ParserToken],
    stats: &TokenGuardStats,
    options: &ComplexityGuardOptions,
) -> Result<()> {
    if let Some(max) = options.max_tokens {
        if stats.token_count > max {
            let span = tokens
                .get(max)
                .or_else(|| tokens.last())
                .map(|token| token.span);
            return Err(parse_guard_error(
                "E_GUARD_TOKEN_BUDGET_EXCEEDED",
                stats.token_count,
                max,
                span,
            ));
        }
    }

    let parenthesis_error = options.max_parenthesis_depth.and_then(|max| {
        stats
            .parenthesis_depth_spans
            .get(max)
            .copied()
            .map(|span| (span, max))
    });
    let function_error = options.max_function_call_depth.and_then(|max| {
        stats
            .function_depth_spans
            .get(max)
            .copied()
            .map(|span| (span, max))
    });

    match (parenthesis_error, function_error) {
        (Some((paren_span, _paren_max)), Some((function_span, function_max)))
            if function_span.start < paren_span.start =>
        {
            Err(parse_guard_error(
                "E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED",
                function_max + 1,
                function_max,
                Some(function_span),
            ))
        }
        (Some((span, max)), _) => Err(parse_guard_error(
            "E_GUARD_NESTING_DEPTH_EXCEEDED",
            max + 1,
            max,
            Some(span),
        )),
        (None, Some((span, max))) => Err(parse_guard_error(
            "E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED",
            max + 1,
            max,
            Some(span),
        )),
        (None, None) => Ok(()),
    }
}

fn is_trivia_token(token_type: TokenType) -> bool {
    matches!(
        token_type,
        TokenType::Space | TokenType::Break | TokenType::LineComment | TokenType::BlockComment
    )
}

fn is_function_call_name_token(token_type: TokenType) -> bool {
    matches!(
        token_type,
        TokenType::Identifier
            | TokenType::Var
            | TokenType::QuotedIdentifier
            | TokenType::CurrentDate
            | TokenType::CurrentDateTime
            | TokenType::CurrentTime
            | TokenType::CurrentTimestamp
            | TokenType::CurrentUser
            | TokenType::If
            | TokenType::Index
            | TokenType::Insert
            | TokenType::Left
            | TokenType::Replace
            | TokenType::Right
            | TokenType::Row
    )
}

#[cfg(test)]
mod token_guard_tests {
    use super::*;
    use crate::tokens::Tokenizer;
    use std::sync::Arc;

    #[test]
    fn collected_parser_stats_match_full_token_guard_pass() {
        let sql: Arc<str> = Arc::from("SELECT outer(inner((value))), other(1, 2) FROM t");
        let tokenizer = Tokenizer::default();
        let public_tokens = tokenizer.tokenize(&sql).unwrap();
        let (parser_tokens, stats) = tokenizer.tokenize_for_parser(&sql).unwrap();
        let options = [
            ComplexityGuardOptions::default(),
            ComplexityGuardOptions {
                max_tokens: Some(5),
                ..Default::default()
            },
            ComplexityGuardOptions {
                max_parenthesis_depth: Some(1),
                ..Default::default()
            },
            ComplexityGuardOptions {
                max_function_call_depth: Some(1),
                ..Default::default()
            },
            ComplexityGuardOptions {
                max_parenthesis_depth: Some(2),
                max_function_call_depth: Some(1),
                ..Default::default()
            },
        ];

        for option in options {
            let full_pass =
                enforce_tokens(&public_tokens, &option).map_err(|error| error.to_string());
            let collected = enforce_parser_token_stats(&parser_tokens, &stats, &option)
                .map_err(|error| error.to_string());
            assert_eq!(collected, full_pass);
        }
    }
}

/// Enforce AST size/depth limits and report parse-oriented errors.
pub fn enforce_ast(expressions: &[Expression], options: &ComplexityGuardOptions) -> Result<()> {
    let Some(max_nodes) = options.max_ast_nodes else {
        return enforce_ast_depth_only(expressions, options);
    };

    let stats = ast_stats(expressions, Some(max_nodes), options.max_ast_depth)?;
    if stats.node_count > max_nodes {
        return Err(parse_guard_error(
            "E_GUARD_AST_BUDGET_EXCEEDED",
            stats.node_count,
            max_nodes,
            None,
        ));
    }

    if let Some(max_depth) = options.max_ast_depth {
        if stats.max_depth > max_depth {
            return Err(parse_guard_error(
                "E_GUARD_AST_DEPTH_EXCEEDED",
                stats.max_depth,
                max_depth,
                None,
            ));
        }
    }

    Ok(())
}

/// Enforce AST size/depth limits and report generation-oriented errors.
pub fn enforce_generate_ast(
    expression: &Expression,
    options: &ComplexityGuardOptions,
) -> Result<()> {
    let stats = ast_stats(
        std::slice::from_ref(expression),
        options.max_ast_nodes,
        options.max_ast_depth,
    )?;

    if let Some(max_nodes) = options.max_ast_nodes {
        if stats.node_count > max_nodes {
            return Err(generate_guard_error(
                "E_GUARD_AST_BUDGET_EXCEEDED",
                stats.node_count,
                max_nodes,
            ));
        }
    }

    if let Some(max_depth) = options.max_ast_depth {
        if stats.max_depth > max_depth {
            return Err(generate_guard_error(
                "E_GUARD_AST_DEPTH_EXCEEDED",
                stats.max_depth,
                max_depth,
            ));
        }
    }

    Ok(())
}

fn enforce_ast_depth_only(
    expressions: &[Expression],
    options: &ComplexityGuardOptions,
) -> Result<()> {
    let Some(max_depth) = options.max_ast_depth else {
        return Ok(());
    };

    let stats = ast_stats(expressions, None, Some(max_depth))?;
    if stats.max_depth > max_depth {
        return Err(parse_guard_error(
            "E_GUARD_AST_DEPTH_EXCEEDED",
            stats.max_depth,
            max_depth,
            None,
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct AstStats {
    node_count: usize,
    max_depth: usize,
}

fn ast_stats(
    expressions: &[Expression],
    max_nodes: Option<usize>,
    max_depth: Option<usize>,
) -> Result<AstStats> {
    let mut stats = AstStats::default();
    let mut stack: Vec<(&Expression, usize)> = expressions.iter().rev().map(|e| (e, 0)).collect();

    while let Some((expr, depth)) = stack.pop() {
        stats.node_count += 1;
        stats.max_depth = stats.max_depth.max(depth);

        if let Some(max) = max_nodes {
            if stats.node_count > max {
                return Ok(stats);
            }
        }

        if let Some(max) = max_depth {
            if stats.max_depth > max {
                return Ok(stats);
            }
        }

        push_ast_children(&mut stack, expr, depth);
    }

    Ok(stats)
}

fn push_ast_children<'a>(
    stack: &mut Vec<(&'a Expression, usize)>,
    expr: &'a Expression,
    depth: usize,
) {
    match expr {
        Expression::And(op) if is_commentless_binary_op(op) => {
            push_connector_child(stack, &op.right, depth, ConnectorKind::And);
            push_connector_child(stack, &op.left, depth, ConnectorKind::And);
        }
        Expression::Or(op) if is_commentless_binary_op(op) => {
            push_connector_child(stack, &op.right, depth, ConnectorKind::Or);
            push_connector_child(stack, &op.left, depth, ConnectorKind::Or);
        }
        _ => {
            let child_start = stack.len();
            crate::ast_children::for_each_child(expr, |_, child| {
                stack.push((child, depth + 1));
            });
            stack[child_start..].reverse();
        }
    }
}

fn push_connector_child<'a>(
    stack: &mut Vec<(&'a Expression, usize)>,
    child: &'a Expression,
    depth: usize,
    kind: ConnectorKind,
) {
    let child_depth = if is_commentless_connector(child, kind) {
        depth
    } else {
        depth + 1
    };
    stack.push((child, child_depth));
}

fn is_commentless_connector(expr: &Expression, kind: ConnectorKind) -> bool {
    match (kind, expr) {
        (ConnectorKind::And, Expression::And(op)) | (ConnectorKind::Or, Expression::Or(op)) => {
            is_commentless_binary_op(op)
        }
        _ => false,
    }
}

fn is_commentless_binary_op(op: &crate::expressions::BinaryOp) -> bool {
    op.left_comments.is_empty()
        && op.operator_comments.is_empty()
        && op.trailing_comments.is_empty()
}

#[derive(Debug, Clone, Copy)]
enum ConnectorKind {
    And,
    Or,
}
