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

use crate::expressions::{DataType, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildPathSegment {
    Field(&'static str),
    Index(usize),
}

pub(crate) trait AstNode {
    /// Visit expression children and embedded syntax types without constructing paths.
    /// Expression descendants are left to the caller; nested type fields are visited here.
    fn visit_syntax_untracked<'ast, F, T>(&'ast self, visitor: &mut F, type_visitor: &mut T)
    where
        F: FnMut(&'ast Expression),
        T: FnMut(&'ast DataType);

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

pub(crate) fn for_each_child_untracked<'ast>(
    expression: &'ast Expression,
    mut visitor: impl FnMut(&'ast Expression),
) {
    expression.visit_syntax_untracked(&mut visitor, &mut |_| {});
}

#[cfg(feature = "transpile")]
pub(crate) fn for_each_child_and_type_untracked<'ast>(
    expression: &'ast Expression,
    mut visitor: impl FnMut(&'ast Expression),
    mut type_visitor: impl FnMut(&'ast DataType),
) {
    expression.visit_syntax_untracked(&mut visitor, &mut type_visitor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Parser;

    #[cfg(all(feature = "transpile", feature = "dialect-hana"))]
    #[test]
    fn syntax_visitor_covers_embedded_types_once_and_skips_inferred_metadata() {
        use crate::expressions::HanaDataType;
        use crate::{Dialect, DialectType};
        let hana = Dialect::get(DialectType::HANA);
        for sql in [
            "ALTER TABLE t ADD (x SMALLDECIMAL ARRAY)",
            "CREATE TABLE t (x SMALLDECIMAL ARRAY)",
            "SELECT CAST(xs AS SMALLDECIMAL ARRAY) FROM t",
        ] {
            let ast = hana.parse(sql).unwrap();
            let mut pending = vec![&ast[0]];
            let mut names = Vec::new();
            let mut arrays = 0;
            while let Some(node) = pending.pop() {
                for_each_child_and_type_untracked(
                    node,
                    |child| pending.push(child),
                    |dt| match dt {
                        DataType::Hana { hana_type } => names.push(hana_type.name.as_str()),
                        DataType::Array { .. } => arrays += 1,
                        _ => {}
                    },
                );
            }
            assert_eq!(names, ["SMALLDECIMAL"], "{sql}");
            assert_eq!(arrays, 1, "{sql}");
        }
        let mut node = hana.parse("CAST(1 AS INT)").unwrap().remove(0);
        node.set_inferred_type(DataType::Hana {
            hana_type: HanaDataType {
                name: "SMALLDECIMAL".into(),
                parameters: vec![],
            },
        });
        let mut names = Vec::new();
        for_each_child_and_type_untracked(
            &node,
            |_| {},
            |dt| {
                if let DataType::Hana { hana_type } = dt {
                    names.push(hana_type.name.as_str());
                }
            },
        );
        assert_eq!(names, ["INT"]);
    }

    #[test]
    fn immutable_and_mutable_visitors_cover_the_same_slots() {
        let mut expression = Parser::parse_sql(
            "SELECT a, CASE WHEN b > 1 THEN c ELSE d END FROM t WHERE e IN (1, 2)",
        )
        .expect("query should parse")
        .remove(0);

        let mut immutable_paths = Vec::new();
        for_each_child(&expression, |path, _| immutable_paths.push(path.to_vec()));

        let mut tracked = Vec::new();
        for_each_child(&expression, |_, child| {
            tracked.push(child as *const Expression)
        });
        let mut untracked = Vec::new();
        for_each_child_untracked(&expression, |child| {
            untracked.push(child as *const Expression)
        });
        assert_eq!(
            tracked, untracked,
            "path-free traversal preserves child identity and order"
        );

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
