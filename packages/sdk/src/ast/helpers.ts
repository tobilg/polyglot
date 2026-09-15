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
import { EXPRESSION_VARIANT_NAMES } from '../generated/ExpressionVariantNames';

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

type GeneratedExpressionType = (typeof EXPRESSION_VARIANT_NAMES)[number];
type MissingExpressionType = Exclude<ExpressionType, GeneratedExpressionType>;
type UnexpectedExpressionType = Exclude<
  GeneratedExpressionType,
  ExpressionType
>;
type ExactExpressionVariantRegistry = [
  MissingExpressionType | UnexpectedExpressionType,
] extends [never]
  ? readonly ExpressionType[]
  : never;

const expressionVariantNames: ExactExpressionVariantRegistry =
  EXPRESSION_VARIANT_NAMES;
const expressionVariantNameSet = new Set<string>(expressionVariantNames);

export type ExpressionBySingleKey<K extends ExpressionType> = Extract<
  Expression,
  Record<K, unknown>
>;

/**
 * Extract a specific Expression variant by its key name.
 *
 * @example
 * ```typescript
 * type SelectExpr = ExpressionByKey<'select'>;
 * // => { "select": Select }
 * ```
 */
export type ExpressionByKey<K extends ExpressionType> = K extends unknown
  ? ExpressionBySingleKey<K>
  : never;

type IsUnionExpressionType<T, Whole = T> = T extends unknown
  ? [Whole] extends [T]
    ? false
    : true
  : never;

export type SingleExpressionType<T extends ExpressionType> = T &
  (IsUnionExpressionType<T> extends true ? never : unknown);

/**
 * Extract the inner data type of a specific Expression variant.
 *
 * @example
 * ```typescript
 * type SelectData = ExpressionInner<'select'>;
 * // => Select
 * ```
 */
export type ExpressionInner<K extends ExpressionType> = K extends unknown
  ? ExpressionByKey<K> extends Record<K, infer V>
    ? V
    : never
  : never;

/**
 * Extract the payload type from one or more Expression variants.
 *
 * A completely unnarrowed Expression returns unknown to avoid materializing
 * the entire generated payload union. Narrowed expressions retain their exact
 * generated payload type.
 */
export type ExpressionData<E extends Expression> = [Expression] extends [E]
  ? unknown
  : E extends unknown
    ? E[keyof E]
    : never;

/** Extract the variant key from one or more Expression variants. */
export type ExpressionTypeOf<E extends Expression> = E extends unknown
  ? Extract<keyof E, string>
  : never;

/**
 * Get the type tag (variant key) of an Expression at runtime.
 *
 * @example
 * ```typescript
 * const result = parse("SELECT 1");
 * if (result.success) {
 *   getExprType(result.ast[0]); // => "select"
 * }
 * ```
 */
export function getExprType<E extends Expression>(
  expr: E,
): ExpressionTypeOf<E> {
  return Object.keys(expr)[0] as ExpressionTypeOf<E>;
}

/**
 * Get the inner data of an Expression at runtime.
 *
 * @example
 * ```typescript
 * const result = parse("SELECT 1");
 * if (result.success && isExpressionType(result.ast[0], 'select')) {
 *   const selectData = getExprData(result.ast[0]);
 *   // selectData.expressions, selectData.from, etc.
 * }
 * ```
 */
export function getExprData<E extends Expression>(expr: E): ExpressionData<E> {
  const key = Object.keys(expr)[0];
  return (expr as Record<string, unknown>)[key] as ExpressionData<E>;
}

/**
 * Check if a runtime value looks like an Expression.
 *
 * Expressions in the externally tagged format are single-key objects whose
 * key is one of the variants generated from Rust's `Expression` enum.
 * Checking the exact variant registry prevents one-field payload structs such
 * as `{ this: expression }` from being exposed as phantom expression nodes.
 * DataType's own `data_type` discriminator also matches an Expression variant:
 * `{ data_type: 'date' }` is a descriptor, whereas the expression envelope is
 * `{ data_type: { data_type: 'date' } }`.
 */
export function isExpressionValue(value: unknown): value is Expression {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const keys = Object.keys(value);
  if (keys.length !== 1) return false;
  const key = keys[0];
  if (!expressionVariantNameSet.has(key)) return false;
  if (key === 'data_type') {
    const payload = (value as Record<string, unknown>)[key];
    return (
      typeof payload === 'object' && payload !== null && !Array.isArray(payload)
    );
  }
  // Other variants can legitimately carry scalars, e.g. column_position: 'First'.
  return true;
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
    const it = (data as Record<string, unknown>).inferred_type;
    if (it != null) {
      return it as DataType;
    }
  }

  if ('paren' in expr || 'annotated' in expr) {
    const nestedExpression = getExprData(expr).this;
    if (isExpressionValue(nestedExpression)) {
      return getInferredType(nestedExpression);
    }
  }

  return undefined;
}
