/**
 * AST Helpers for Externally Tagged Expression Format
 *
 * With externally tagged enums, Expression variants are represented as
 * single-key objects: { "select": { ... } } instead of { "type": "select", ... }
 *
 * These helpers provide runtime utilities for working with this format.
 */

import type { DataType } from '../generated/DataType';
import type { Expression } from '../generated/Expression';

/**
 * Distributive conditional type to extract all variant key names from Expression union.
 *
 * Each variant is { "key": Data }, so keyof each variant gives us the key name.
 * The distributive conditional distributes over the union members.
 */
export type ExpressionType = Expression extends infer E
  ? E extends Record<infer K, unknown>
    ? K extends string
      ? K
      : never
    : never
  : never;

/**
 * Extract a specific Expression variant by its key name.
 *
 * @example
 * ```typescript
 * type SelectExpr = ExpressionByKey<'select'>;
 * // => { "select": Select }
 * ```
 */
export type ExpressionByKey<K extends ExpressionType> = Extract<
  Expression,
  Record<K, unknown>
>;

/**
 * Extract the inner data type of a specific Expression variant.
 *
 * @example
 * ```typescript
 * type SelectData = ExpressionInner<'select'>;
 * // => Select
 * ```
 */
export type ExpressionInner<K extends ExpressionType> =
  ExpressionByKey<K> extends Record<K, infer V> ? V : never;

/**
 * Get the type tag (variant key) of an Expression at runtime.
 *
 * @example
 * ```typescript
 * const expr = parse("SELECT 1")[0];
 * getExprType(expr) // => "select"
 * ```
 */
export function getExprType(expr: Expression): ExpressionType {
  return Object.keys(expr)[0] as ExpressionType;
}

/**
 * Get the inner data of an Expression at runtime.
 *
 * @example
 * ```typescript
 * const expr = parse("SELECT 1")[0];
 * const selectData = getExprData(expr);
 * // selectData.expressions, selectData.from, etc.
 * ```
 */
export function getExprData(expr: Expression): Record<string, unknown> {
  const key = Object.keys(expr)[0];
  return (expr as Record<string, unknown>)[key] as Record<string, unknown>;
}

/**
 * Check if a runtime value looks like an Expression.
 *
 * Expressions in the externally tagged format are single-key objects
 * where the key is the variant name and the value is the inner data object.
 *
 * Important: The inner value must be a non-null, non-array plain object.
 * Structs like From { expressions: Vec<Expression> } serialize as
 * { "expressions": [...] } — a single-key object with an array value.
 * These must NOT be treated as Expressions, or the transformer will corrupt them.
 */
export function isExpressionValue(value: unknown): value is Expression {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const keys = Object.keys(value);
  if (keys.length !== 1) return false;
  const inner = (value as Record<string, unknown>)[keys[0]];
  // Expression inner data is a non-array object (struct data) OR null (unit struct variants like Null, CurrentDate, RowNumber)
  return inner === null || (typeof inner === 'object' && !Array.isArray(inner));
}

/**
 * Create an Expression from a variant key and inner data.
 *
 * @example
 * ```typescript
 * const expr = makeExpr('literal', { literal_type: 'number', value: '42' });
 * ```
 */
export function makeExpr(type: string, data: unknown): Expression {
  return { [type]: data } as Expression;
}

/**
 * Get the inferred data type from an Expression, if it has been type-annotated.
 *
 * After calling `annotateTypes()`, value-producing expressions (columns, operators,
 * functions, casts, etc.) carry an `inferred_type` field with their resolved SQL type.
 *
 * @example
 * ```typescript
 * const result = annotateTypes("SELECT 1 + 2", Dialect.Generic);
 * if (result.success) {
 *   const addExpr = result.ast![0]; // the SELECT
 *   // Navigate to the "1 + 2" expression and check its type:
 *   const dt = getInferredType(someExpr);
 *   // dt => { data_type: "int", length: null, integer_spelling: false }
 * }
 * ```
 *
 * @returns The inferred DataType, or `undefined` if not annotated or not a value-producing expression.
 */
export function getInferredType(expr: Expression): DataType | undefined {
  const data = getExprData(expr);
  if (data && typeof data === 'object' && 'inferred_type' in data) {
    const it = (data as Record<string, unknown>)['inferred_type'];
    if (it != null) {
      return it as DataType;
    }
  }
  return undefined;
}
