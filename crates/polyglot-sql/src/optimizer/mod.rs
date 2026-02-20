//! SQL Optimizer Module
//!
//! This module contains optimization passes for SQL AST transformation,
//! including type annotation, column qualification, and other semantic analysis.

/// Type annotation and coercion for SQL expressions
pub mod annotate_types;
/// Canonicalization of SQL expressions into standard form
pub mod canonicalize;
/// Elimination of unused Common Table Expressions
pub mod eliminate_ctes;
/// Elimination of unused JOIN operations
pub mod eliminate_joins;
/// Isolation of table references into subqueries
pub mod isolate_table_selects;
/// Boolean expression normalization (CNF/DNF)
pub mod normalize;
/// Identifier case and quoting normalization
pub mod normalize_identifiers;
/// JOIN reordering and cross-join optimization
pub mod optimize_joins;
/// Main optimizer entry point and rule orchestration
pub mod optimizer;
/// Predicate pushdown into subqueries and JOINs
pub mod pushdown_predicates;
/// Projection pushdown to eliminate unused columns
pub mod pushdown_projections;
/// Column qualification and star expansion
pub mod qualify_columns;
/// Table reference qualification with catalog and schema
pub mod qualify_tables;
/// Boolean and algebraic expression simplification
pub mod simplify;
/// Subquery merging and unnesting
pub mod subquery;

/// Type annotation, type coercion classes, and the type annotator engine
pub use annotate_types::{annotate_types, TypeAnnotator, TypeCoercionClass};
/// Canonicalization of SQL expressions
pub use canonicalize::canonicalize;
/// CTE elimination and reference checking
pub use eliminate_ctes::{eliminate_ctes, is_cte_referenced};
/// JOIN elimination for unused joins
pub use eliminate_joins::eliminate_joins;
/// Table select isolation into subqueries
pub use isolate_table_selects::isolate_table_selects;
/// Boolean normalization, distance computation, and related utilities
pub use normalize::{
    normalization_distance, normalize, normalized, NormalizeError, DEFAULT_MAX_DISTANCE,
};
/// Identifier normalization, case sensitivity detection, and strategy types
pub use normalize_identifiers::{
    get_normalization_strategy, is_case_sensitive, normalize_identifier, normalize_identifiers,
    NormalizationStrategy,
};
/// JOIN optimization, reordering, normalization, and reorderability checks
pub use optimize_joins::{is_reorderable, normalize_joins, optimize_joins, reorder_joins};
/// Full optimizer pipeline, rule configuration, and quick optimization
pub use optimizer::{
    optimize, optimize_with_rules, quick_optimize, OptimizationRule, OptimizerConfig, DEFAULT_RULES,
};
/// Predicate pushdown and alias replacement
pub use pushdown_predicates::{pushdown_predicates, replace_aliases};
/// Projection pushdown and default column selection
pub use pushdown_projections::{default_selection, pushdown_projections};
/// Column qualification, validation, output handling, and identifier quoting
pub use qualify_columns::{
    qualify_columns, qualify_outputs, quote_identifiers, validate_qualify_columns,
    QualifyColumnsError, QualifyColumnsOptions,
};
/// Table qualification with catalog and schema defaults
pub use qualify_tables::{qualify_tables, QualifyTablesOptions};
/// Expression simplification, constant evaluation, and truthiness checks
pub use simplify::{always_false, always_true, is_false, is_null, is_zero, simplify, Simplifier};
/// Subquery merging, unnesting, and correlation analysis
pub use subquery::{is_correlated, is_mergeable, merge_subqueries, unnest_subqueries};
