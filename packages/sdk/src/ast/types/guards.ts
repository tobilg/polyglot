/**
 * Type Guards for AST Nodes
 *
 * These functions enable runtime type checking and TypeScript type narrowing
 * for externally tagged union types in the AST.
 *
 * With externally tagged enums, each Expression variant is a single-key object:
 *   { "select": { ... } }  instead of  { "type": "select", ... }
 */

import type { Expression } from '../../generated/Expression';
import { type ExpressionType, type ExpressionByKey, getExprType } from '../helpers';

/**
 * Generic type guard factory
 */
function isType<T extends ExpressionType>(
  type: T
): (expr: Expression) => expr is ExpressionByKey<T> {
  return (expr: Expression): expr is ExpressionByKey<T> =>
    type in (expr as Record<string, unknown>);
}

// ============================================================================
// Query Type Guards
// ============================================================================

/** Type guard for SELECT expressions */
export const isSelect = isType('select');

/** Type guard for INSERT expressions */
export const isInsert = isType('insert');

/** Type guard for UPDATE expressions */
export const isUpdate = isType('update');

/** Type guard for DELETE expressions */
export const isDelete = isType('delete');

/** Type guard for UNION expressions */
export const isUnion = isType('union');

/** Type guard for INTERSECT expressions */
export const isIntersect = isType('intersect');

/** Type guard for EXCEPT expressions */
export const isExcept = isType('except');

/** Type guard for subquery expressions */
export const isSubquery = isType('subquery');

// ============================================================================
// Identifier Type Guards
// ============================================================================

/** Type guard for identifier expressions */
export const isIdentifier = isType('identifier');

/** Type guard for column references */
export const isColumn = isType('column');

/** Type guard for table references */
export const isTable = isType('table');

/** Type guard for star (*) expressions */
export const isStar = isType('star');

// ============================================================================
// Literal Type Guards
// ============================================================================

/** Type guard for literal expressions */
export const isLiteral = isType('literal');

/** Type guard for boolean literals */
export const isBoolean = isType('boolean');

/** Type guard for NULL literals */
export const isNullLiteral = isType('null');

// ============================================================================
// Operator Type Guards
// ============================================================================

/** Type guard for AND expressions */
export const isAnd = isType('and');

/** Type guard for OR expressions */
export const isOr = isType('or');

/** Type guard for NOT expressions */
export const isNot = isType('not');

/** Type guard for equality (=) expressions */
export const isEq = isType('eq');

/** Type guard for inequality (<>) expressions */
export const isNeq = isType('neq');

/** Type guard for less than (<) expressions */
export const isLt = isType('lt');

/** Type guard for less than or equal (<=) expressions */
export const isLte = isType('lte');

/** Type guard for greater than (>) expressions */
export const isGt = isType('gt');

/** Type guard for greater than or equal (>=) expressions */
export const isGte = isType('gte');

/** Type guard for LIKE expressions */
export const isLike = isType('like');

/** Type guard for ILIKE expressions */
export const isILike = isType('i_like');

/** Type guard for addition (+) expressions */
export const isAdd = isType('add');

/** Type guard for subtraction (-) expressions */
export const isSub = isType('sub');

/** Type guard for multiplication (*) expressions */
export const isMul = isType('mul');

/** Type guard for division (/) expressions */
export const isDiv = isType('div');

/** Type guard for modulo (%) expressions */
export const isMod = isType('mod');

/** Type guard for concatenation (||) expressions */
export const isConcat = isType('concat');

// ============================================================================
// Predicate Type Guards
// ============================================================================

/** Type guard for IN expressions */
export const isIn = isType('in');

/** Type guard for BETWEEN expressions */
export const isBetween = isType('between');

/** Type guard for IS NULL expressions */
export const isIsNull = isType('is_null');

/** Type guard for EXISTS expressions */
export const isExists = isType('exists');

// ============================================================================
// Function Type Guards
// ============================================================================

/** Type guard for generic function calls */
export const isFunction = isType('function');

/** Type guard for aggregate function calls */
export const isAggregateFunction = isType('aggregate_function');

/** Type guard for window function calls */
export const isWindowFunction = isType('window_function');

/** Type guard for COUNT function */
export const isCount = isType('count');

/** Type guard for SUM function */
export const isSum = isType('sum');

/** Type guard for AVG function */
export const isAvg = isType('avg');

/** Type guard for MIN function */
export const isMin = isType('min');

/** Type guard for MAX function */
export const isMax = isType('max');

/** Type guard for COALESCE function */
export const isCoalesce = isType('coalesce');

/** Type guard for NULLIF function */
export const isNullIf = isType('null_if');

/** Type guard for CAST expressions */
export const isCast = isType('cast');

/** Type guard for TRY_CAST expressions */
export const isTryCast = isType('try_cast');

/** Type guard for SAFE_CAST expressions */
export const isSafeCast = isType('safe_cast');

/** Type guard for CASE expressions */
export const isCase = isType('case');

// ============================================================================
// Clause Type Guards
// ============================================================================

/** Type guard for FROM clause */
export const isFrom = isType('from');

/** Type guard for JOIN clause */
export const isJoin = isType('join');

/** Type guard for WHERE clause */
export const isWhere = isType('where');

/** Type guard for GROUP BY clause */
export const isGroupBy = isType('group_by');

/** Type guard for HAVING clause */
export const isHaving = isType('having');

/** Type guard for ORDER BY clause */
export const isOrderBy = isType('order_by');

/** Type guard for LIMIT clause */
export const isLimit = isType('limit');

/** Type guard for OFFSET clause */
export const isOffset = isType('offset');

/** Type guard for WITH clause */
export const isWith = isType('with');

/** Type guard for CTE expressions */
export const isCte = isType('cte');

// ============================================================================
// Expression Type Guards
// ============================================================================

/** Type guard for alias expressions */
export const isAlias = isType('alias');

/** Type guard for parenthesized expressions */
export const isParen = isType('paren');

/** Type guard for ordered expressions (for ORDER BY) */
export const isOrdered = isType('ordered');

// ============================================================================
// DDL Type Guards
// ============================================================================

/** Type guard for CREATE TABLE expressions */
export const isCreateTable = isType('create_table');

/** Type guard for DROP TABLE expressions */
export const isDropTable = isType('drop_table');

/** Type guard for ALTER TABLE expressions */
export const isAlterTable = isType('alter_table');

/** Type guard for CREATE INDEX expressions */
export const isCreateIndex = isType('create_index');

/** Type guard for DROP INDEX expressions */
export const isDropIndex = isType('drop_index');

/** Type guard for CREATE VIEW expressions */
export const isCreateView = isType('create_view');

/** Type guard for DROP VIEW expressions */
export const isDropView = isType('drop_view');

// ============================================================================
// Composite Type Guards
// ============================================================================

/** Check if expression is a DML query (SELECT, INSERT, UPDATE, DELETE) */
export function isQuery(expr: Expression): boolean {
  const t = getExprType(expr);
  return t === 'select' || t === 'insert' || t === 'update' || t === 'delete';
}

/** Check if expression is a set operation (UNION, INTERSECT, EXCEPT) */
export function isSetOperation(expr: Expression): boolean {
  const t = getExprType(expr);
  return t === 'union' || t === 'intersect' || t === 'except';
}

/** Check if expression is a comparison operator */
export function isComparison(expr: Expression): boolean {
  const t = getExprType(expr);
  return (
    t === 'eq' || t === 'neq' || t === 'lt' || t === 'lte' ||
    t === 'gt' || t === 'gte' || t === 'like' || t === 'i_like'
  );
}

/** Check if expression is an arithmetic operator */
export function isArithmetic(expr: Expression): boolean {
  const t = getExprType(expr);
  return t === 'add' || t === 'sub' || t === 'mul' || t === 'div' || t === 'mod';
}

/** Check if expression is a logical operator */
export function isLogical(expr: Expression): boolean {
  const t = getExprType(expr);
  return t === 'and' || t === 'or' || t === 'not';
}

/** Check if expression is a DDL statement */
export function isDDL(expr: Expression): boolean {
  const t = getExprType(expr);
  return (
    t === 'create_table' || t === 'drop_table' || t === 'alter_table' ||
    t === 'create_index' || t === 'drop_index' ||
    t === 'create_view' || t === 'drop_view' ||
    t === 'create_schema' || t === 'drop_schema' ||
    t === 'create_database' || t === 'drop_database' ||
    t === 'create_function' || t === 'drop_function' ||
    t === 'create_procedure' || t === 'drop_procedure' ||
    t === 'create_sequence' || t === 'drop_sequence' || t === 'alter_sequence' ||
    t === 'create_trigger' || t === 'drop_trigger' ||
    t === 'create_type' || t === 'drop_type'
  );
}
