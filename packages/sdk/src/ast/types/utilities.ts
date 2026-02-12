/**
 * Utility Types for AST Manipulation
 *
 * These types provide convenient aliases and helpers for working with the AST.
 */

import type { Expression } from '../../generated/Expression';
import type { ExpressionByKey, ExpressionType } from '../helpers';

// ============================================================================
// Expression Type Extraction
// ============================================================================

/**
 * Extract a specific expression variant by its key name
 *
 * @example
 * ```typescript
 * type SelectExpr = ExpressionByType<'select'>;
 * // { "select": Select }
 * ```
 */
export type ExpressionByType<T extends ExpressionType> = ExpressionByKey<T>;

// Re-export ExpressionType for convenience
export type { ExpressionType };

// ============================================================================
// Query Types
// ============================================================================

/**
 * Union of all DML query expression types
 */
export type QueryExpression =
  | ExpressionByType<'select'>
  | ExpressionByType<'insert'>
  | ExpressionByType<'update'>
  | ExpressionByType<'delete'>;

/**
 * Union of all set operation expression types
 */
export type SetOperationExpression =
  | ExpressionByType<'union'>
  | ExpressionByType<'intersect'>
  | ExpressionByType<'except'>;

// ============================================================================
// DDL Types
// ============================================================================

/**
 * Union of all DDL expression types
 */
export type DDLExpression =
  | ExpressionByType<'create_table'>
  | ExpressionByType<'drop_table'>
  | ExpressionByType<'alter_table'>
  | ExpressionByType<'create_index'>
  | ExpressionByType<'drop_index'>
  | ExpressionByType<'create_view'>
  | ExpressionByType<'drop_view'>
  | ExpressionByType<'alter_view'>
  | ExpressionByType<'alter_index'>
  | ExpressionByType<'truncate'>
  | ExpressionByType<'create_schema'>
  | ExpressionByType<'drop_schema'>
  | ExpressionByType<'create_database'>
  | ExpressionByType<'drop_database'>
  | ExpressionByType<'create_function'>
  | ExpressionByType<'drop_function'>
  | ExpressionByType<'create_procedure'>
  | ExpressionByType<'drop_procedure'>
  | ExpressionByType<'create_sequence'>
  | ExpressionByType<'drop_sequence'>
  | ExpressionByType<'alter_sequence'>
  | ExpressionByType<'create_trigger'>
  | ExpressionByType<'drop_trigger'>
  | ExpressionByType<'create_type'>
  | ExpressionByType<'drop_type'>;

// ============================================================================
// Operator Types
// ============================================================================

/**
 * Union of all binary operator expression types
 */
export type BinaryOperatorExpression =
  | ExpressionByType<'and'>
  | ExpressionByType<'or'>
  | ExpressionByType<'add'>
  | ExpressionByType<'sub'>
  | ExpressionByType<'mul'>
  | ExpressionByType<'div'>
  | ExpressionByType<'mod'>
  | ExpressionByType<'eq'>
  | ExpressionByType<'neq'>
  | ExpressionByType<'lt'>
  | ExpressionByType<'lte'>
  | ExpressionByType<'gt'>
  | ExpressionByType<'gte'>
  | ExpressionByType<'like'>
  | ExpressionByType<'i_like'>
  | ExpressionByType<'concat'>
  | ExpressionByType<'bitwise_and'>
  | ExpressionByType<'bitwise_or'>
  | ExpressionByType<'bitwise_xor'>
  | ExpressionByType<'bitwise_left_shift'>
  | ExpressionByType<'bitwise_right_shift'>;

/**
 * Union of all unary operator expression types
 */
export type UnaryOperatorExpression =
  | ExpressionByType<'not'>
  | ExpressionByType<'neg'>
  | ExpressionByType<'bitwise_not'>;

/**
 * Union of all comparison operator types
 */
export type ComparisonExpression =
  | ExpressionByType<'eq'>
  | ExpressionByType<'neq'>
  | ExpressionByType<'lt'>
  | ExpressionByType<'lte'>
  | ExpressionByType<'gt'>
  | ExpressionByType<'gte'>
  | ExpressionByType<'like'>
  | ExpressionByType<'i_like'>;

/**
 * Union of all arithmetic operator types
 */
export type ArithmeticExpression =
  | ExpressionByType<'add'>
  | ExpressionByType<'sub'>
  | ExpressionByType<'mul'>
  | ExpressionByType<'div'>
  | ExpressionByType<'mod'>;

/**
 * Union of all logical operator types
 */
export type LogicalExpression =
  | ExpressionByType<'and'>
  | ExpressionByType<'or'>
  | ExpressionByType<'not'>;

// ============================================================================
// Literal Types
// ============================================================================

/**
 * Union of all literal expression types
 */
export type LiteralExpression =
  | ExpressionByType<'literal'>
  | ExpressionByType<'boolean'>;

// ============================================================================
// Reference Types
// ============================================================================

/**
 * Union of all reference expression types (identifiers, columns, tables)
 */
export type ReferenceExpression =
  | ExpressionByType<'identifier'>
  | ExpressionByType<'column'>
  | ExpressionByType<'table'>
  | ExpressionByType<'star'>;

// ============================================================================
// Function Types
// ============================================================================

/**
 * Union of all function-related expression types
 */
export type FunctionExpression =
  | ExpressionByType<'function'>
  | ExpressionByType<'aggregate_function'>
  | ExpressionByType<'window_function'>;

/**
 * Union of all aggregate function expression types
 */
export type AggregateExpression =
  | ExpressionByType<'count'>
  | ExpressionByType<'sum'>
  | ExpressionByType<'avg'>
  | ExpressionByType<'min'>
  | ExpressionByType<'max'>
  | ExpressionByType<'group_concat'>
  | ExpressionByType<'string_agg'>
  | ExpressionByType<'list_agg'>
  | ExpressionByType<'array_agg'>
  | ExpressionByType<'count_if'>
  | ExpressionByType<'sum_if'>
  | ExpressionByType<'stddev'>
  | ExpressionByType<'variance'>
  | ExpressionByType<'median'>
  | ExpressionByType<'mode'>
  | ExpressionByType<'first'>
  | ExpressionByType<'last'>
  | ExpressionByType<'any_value'>
  | ExpressionByType<'approx_distinct'>
  | ExpressionByType<'approx_count_distinct'>;

/**
 * Union of all window function expression types
 * Note: Some window functions use null types internally, so we use a simplified union
 */
export type WindowExpression =
  | ExpressionByType<'n_tile'>
  | ExpressionByType<'lead'>
  | ExpressionByType<'lag'>
  | ExpressionByType<'first_value'>
  | ExpressionByType<'last_value'>
  | ExpressionByType<'nth_value'>
  | ExpressionByType<'percentile_cont'>
  | ExpressionByType<'percentile_disc'>;

// ============================================================================
// Clause Types
// ============================================================================

/**
 * Union of all clause expression types
 */
export type ClauseExpression =
  | ExpressionByType<'from'>
  | ExpressionByType<'join'>
  | ExpressionByType<'where'>
  | ExpressionByType<'group_by'>
  | ExpressionByType<'having'>
  | ExpressionByType<'order_by'>
  | ExpressionByType<'limit'>
  | ExpressionByType<'offset'>
  | ExpressionByType<'with'>
  | ExpressionByType<'cte'>;

// ============================================================================
// Predicate Types
// ============================================================================

/**
 * Union of all predicate expression types
 */
export type PredicateExpression =
  | ExpressionByType<'in'>
  | ExpressionByType<'between'>
  | ExpressionByType<'is_null'>
  | ExpressionByType<'exists'>
  | ExpressionByType<'similar_to'>
  | ExpressionByType<'any'>
  | ExpressionByType<'all'>
  | ExpressionByType<'overlaps'>;

// ============================================================================
// Helper Types for Builders
// ============================================================================

/**
 * Input type that can be normalized to an Expression
 * Used by builder functions that accept flexible input
 */
export type ExpressionInput = Expression | string | number | boolean | null;

/**
 * Input type for column references
 */
export type ColumnInput = string | ExpressionByType<'column'>;

/**
 * Input type for table references
 */
export type TableInput = string | ExpressionByType<'table'>;

/**
 * Input type for order specifications
 */
export type OrderInput =
  | string
  | ExpressionByType<'ordered'>
  | { expr: Expression; desc?: boolean; nullsFirst?: boolean };
