//! Immutable views into an owned AST. Python builders create new trees; they
//! never mutate an existing expression. Views retain the root allocation and
//! therefore avoid copying every descendant subtree during a walk.

use polyglot_sql::Expression;
use serde::{Serialize, Serializer};
use std::{ops::Deref, ptr::NonNull, sync::Arc};

#[derive(Debug)]
pub(crate) struct SharedExpression {
    owner: Arc<Expression>,
    node: NonNull<Expression>,
}

impl SharedExpression {
    pub(crate) fn new(expression: Expression) -> Self {
        let owner = Arc::new(expression);
        let node = NonNull::from(owner.as_ref());
        Self { owner, node }
    }

    pub(crate) fn share(&self) -> Self {
        Self {
            owner: Arc::clone(&self.owner),
            node: self.node,
        }
    }

    // The higher-ranked selectors can only return references into the input
    // tree (or static data), never a shorter-lived, unrelated expression.
    pub(crate) fn select(
        &self,
        selector: impl for<'a> FnOnce(&'a Expression) -> Option<&'a Expression>,
    ) -> Option<Self> {
        selector(self).map(|node| Self {
            owner: Arc::clone(&self.owner),
            node: NonNull::from(node),
        })
    }

    pub(crate) fn select_many(
        &self,
        selector: impl for<'a> FnOnce(&'a Expression) -> Vec<&'a Expression>,
    ) -> Vec<Self> {
        selector(self)
            .into_iter()
            .map(|node| Self {
                owner: Arc::clone(&self.owner),
                node: NonNull::from(node),
            })
            .collect()
    }
}

impl Deref for SharedExpression {
    type Target = Expression;
    fn deref(&self) -> &Expression {
        // SAFETY: constructors select nodes in the immutable Arc-owned tree.
        // No mutable access to either the root or a view is exposed, so nodes
        // cannot move. Every view holds an owner, and this borrow cannot outlive
        // the view. The private pointer is never exposed or dereferenced mutably.
        unsafe { self.node.as_ref() }
    }
}

// SAFETY: the same immutable ownership invariant applies across threads.
unsafe impl Send for SharedExpression where Expression: Send + Sync {}
unsafe impl Sync for SharedExpression where Expression: Send + Sync {}

impl PartialEq for SharedExpression {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Serialize for SharedExpression {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.deref().serialize(serializer)
    }
}
