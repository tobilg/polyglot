//! Canonical child traversal for the typed AST.
//!
//! Implementations are generated beside the AST definitions by the private
//! `AstNode` derive. New AST payload fields are traversed automatically when
//! their type contains [`Expression`]. Use `#[ast(skip)]` only for derived
//! metadata that must not participate in syntax traversal or transformation.

#![cfg_attr(
    not(any(
        feature = "transpile",
        feature = "ast-tools",
        feature = "generate",
        feature = "semantic"
    )),
    allow(dead_code)
)]

use crate::expressions::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildPathSegment {
    Field(&'static str),
    Index(usize),
}

pub(crate) trait AstNode {
    fn visit_expressions<'ast, F>(&'ast self, path: &mut Vec<ChildPathSegment>, visitor: &mut F)
    where
        F: FnMut(&[ChildPathSegment], &'ast Expression);

    fn visit_expressions_mut<F>(&mut self, visitor: &mut F)
    where
        F: FnMut(&mut Expression);
}

pub(crate) fn for_each_child<'ast>(
    expression: &'ast Expression,
    mut visitor: impl FnMut(&[ChildPathSegment], &'ast Expression),
) {
    let mut path = Vec::new();
    expression.visit_expressions(&mut path, &mut visitor);
}

pub(crate) fn for_each_child_mut(
    expression: &mut Expression,
    mut visitor: impl FnMut(&mut Expression),
) {
    expression.visit_expressions_mut(&mut visitor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Parser;

    #[test]
    fn immutable_and_mutable_visitors_cover_the_same_slots() {
        let mut expression = Parser::parse_sql(
            "SELECT a, CASE WHEN b > 1 THEN c ELSE d END FROM t WHERE e IN (1, 2)",
        )
        .expect("query should parse")
        .remove(0);

        let mut immutable_paths = Vec::new();
        for_each_child(&expression, |path, _| immutable_paths.push(path.to_vec()));

        let mut mutable_count = 0;
        for_each_child_mut(&mut expression, |_| mutable_count += 1);

        assert_eq!(immutable_paths.len(), mutable_count);
        assert!(!immutable_paths.is_empty());
    }

    #[test]
    fn child_paths_follow_declared_fields_and_list_indices() {
        let expression = Parser::parse_sql("SELECT a, b FROM t")
            .expect("query should parse")
            .remove(0);
        let mut paths = Vec::new();
        for_each_child(&expression, |path, _| paths.push(path.to_vec()));

        assert_eq!(
            paths[0],
            vec![
                ChildPathSegment::Field("expressions"),
                ChildPathSegment::Index(0)
            ]
        );
        assert_eq!(
            paths[1],
            vec![
                ChildPathSegment::Field("expressions"),
                ChildPathSegment::Index(1)
            ]
        );
        assert_eq!(
            paths[2],
            vec![
                ChildPathSegment::Field("from"),
                ChildPathSegment::Field("expressions"),
                ChildPathSegment::Index(0),
            ]
        );
    }
}
