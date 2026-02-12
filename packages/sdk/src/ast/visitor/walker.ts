/**
 * AST Walker Utilities
 *
 * Functions for traversing and searching the SQL AST.
 *
 * With externally tagged enums, each Expression is { "variant": data }
 * instead of { "type": "variant", ...data }.
 */

import type { Expression } from '../../generated/Expression';
import {
  type ExpressionType,
  getExprData,
  getExprType,
  isExpressionValue,
} from '../helpers';
import type { NodePredicate, VisitorCallback, VisitorConfig } from './types';

// ============================================================================
// Core Walker
// ============================================================================

/**
 * Recursively collect Expression children from any value.
 *
 * Handles:
 * - Expression values (single-key objects with object inner data)
 * - Arrays of Expressions
 * - Nested tuple arrays (like CASE whens)
 * - Non-Expression struct objects (like From, Where, GroupBy) that contain Expressions
 */
function collectExprChildren(
  value: unknown,
  key: string,
  results: Array<{ key: string; value: Expression | Expression[] }>,
): void {
  if (value === null || value === undefined) return;

  if (Array.isArray(value)) {
    if (value.length > 0 && isExpressionValue(value[0])) {
      results.push({ key, value: value as Expression[] });
    } else if (value.length > 0 && Array.isArray(value[0])) {
      // Nested arrays (like whens in CASE)
      for (const item of value) {
        if (Array.isArray(item)) {
          for (const subItem of item) {
            if (isExpressionValue(subItem)) {
              results.push({ key, value: subItem as Expression });
            }
          }
        }
      }
    }
  } else if (isExpressionValue(value)) {
    results.push({ key, value: value as Expression });
  } else if (typeof value === 'object') {
    // Non-Expression struct (e.g., From, Where, GroupBy) — recurse into its fields
    for (const [, subValue] of Object.entries(
      value as Record<string, unknown>,
    )) {
      collectExprChildren(subValue, key, results);
    }
  }
}

/**
 * Get all child expressions from a node.
 *
 * Unwraps the outer Expression envelope, then iterates the inner data
 * looking for Expression children — including those nested inside
 * non-Expression structs like From, Where, GroupBy, etc.
 */
function getChildren(
  node: Expression,
): Array<{ key: string; value: Expression | Expression[] }> {
  const children: Array<{ key: string; value: Expression | Expression[] }> = [];

  // Unwrap the outer envelope to get inner data
  const innerData = getExprData(node);
  if (!innerData || typeof innerData !== 'object') return children;

  for (const [key, value] of Object.entries(innerData)) {
    collectExprChildren(value, key, children);
  }

  return children;
}

/**
 * Walk an AST tree, calling visitor callbacks for each node
 *
 * @example
 * ```typescript
 * walk(ast, {
 *   column: (node) => console.log('Found column:', getExprType(node)),
 *   enter: (node) => console.log('Entering:', getExprType(node)),
 *   leave: (node) => console.log('Leaving:', getExprType(node)),
 * });
 * ```
 */
export function walk(
  node: Expression,
  visitor: VisitorConfig,
  parent: Expression | null = null,
  key: string | null = null,
  index: number | null = null,
): void {
  // Call enter callback
  if (visitor.enter) {
    visitor.enter(node, parent, key, index);
  }

  // Call type-specific callback
  const nodeType = getExprType(node) as keyof VisitorConfig;
  const typeCallback = visitor[nodeType] as VisitorCallback | undefined;
  if (typeCallback) {
    typeCallback(node, parent, key, index);
  }

  // Recursively walk children
  const children = getChildren(node);
  for (const child of children) {
    if (Array.isArray(child.value)) {
      child.value.forEach((childNode, i) => {
        walk(childNode, visitor, node, child.key, i);
      });
    } else {
      walk(child.value, visitor, node, child.key, null);
    }
  }

  // Call leave callback
  if (visitor.leave) {
    visitor.leave(node, parent, key, index);
  }
}

// ============================================================================
// Search Functions
// ============================================================================

/**
 * Find all nodes matching a predicate
 *
 * @example
 * ```typescript
 * const columns = findAll(ast, (node) => getExprType(node) === 'column');
 * ```
 */
export function findAll(
  node: Expression,
  predicate: NodePredicate,
): Expression[] {
  const results: Expression[] = [];

  walk(node, {
    enter: (n, parent) => {
      if (predicate(n, parent)) {
        results.push(n);
      }
    },
  });

  return results;
}

/**
 * Find all nodes of a specific type
 *
 * @example
 * ```typescript
 * const selects = findByType(ast, 'select');
 * const columns = findByType(ast, 'column');
 * ```
 */
export function findByType<T extends ExpressionType>(
  node: Expression,
  type: T,
): Expression[] {
  return findAll(node, (n) => getExprType(n) === type);
}

/**
 * Find the first node matching a predicate
 *
 * @example
 * ```typescript
 * const firstColumn = findFirst(ast, (node) => getExprType(node) === 'column');
 * ```
 */
export function findFirst(
  node: Expression,
  predicate: NodePredicate,
): Expression | undefined {
  let result: Expression | undefined;

  // Use a simple flag since we can't truly stop early
  let found = false;

  walk(node, {
    enter: (n, parent) => {
      if (!found && predicate(n, parent)) {
        result = n;
        found = true;
      }
    },
  });

  return result;
}

/**
 * Check if any node matches a predicate
 *
 * @example
 * ```typescript
 * const hasSubquery = some(ast, (node) => getExprType(node) === 'subquery');
 * ```
 */
export function some(node: Expression, predicate: NodePredicate): boolean {
  return findFirst(node, predicate) !== undefined;
}

/**
 * Check if all nodes of a type match a predicate
 *
 * @example
 * ```typescript
 * const allColumnsQualified = every(
 *   ast,
 *   (node) => getExprType(node) !== 'column' || getExprData(node).table !== null
 * );
 * ```
 */
export function every(node: Expression, predicate: NodePredicate): boolean {
  let result = true;

  walk(node, {
    enter: (n, parent) => {
      if (!predicate(n, parent)) {
        result = false;
      }
    },
  });

  return result;
}

/**
 * Count nodes matching a predicate
 *
 * @example
 * ```typescript
 * const columnCount = countNodes(ast, (node) => getExprType(node) === 'column');
 * ```
 */
export function countNodes(node: Expression, predicate: NodePredicate): number {
  return findAll(node, predicate).length;
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Get all column references in the AST
 *
 * @example
 * ```typescript
 * const columns = getColumns(ast);
 * ```
 */
export function getColumns(node: Expression): Expression[] {
  return findByType(node, 'column');
}

/**
 * Get all table references in the AST
 */
export function getTables(node: Expression): Expression[] {
  return findByType(node, 'table');
}

/**
 * Get all identifiers in the AST
 */
export function getIdentifiers(node: Expression): Expression[] {
  return findByType(node, 'identifier');
}

/**
 * Get all function calls in the AST
 */
export function getFunctions(node: Expression): Expression[] {
  return findByType(node, 'function');
}

/**
 * Get all aggregate function calls in the AST
 */
export function getAggregateFunctions(node: Expression): Expression[] {
  const aggregateTypes = [
    'count',
    'sum',
    'avg',
    'min',
    'max',
    'group_concat',
    'string_agg',
    'list_agg',
    'array_agg',
    'count_if',
    'sum_if',
    'stddev',
    'stddev_pop',
    'stddev_samp',
    'variance',
    'var_pop',
    'var_samp',
    'median',
    'mode',
    'first',
    'last',
    'any_value',
    'approx_distinct',
    'approx_count_distinct',
  ];

  return findAll(node, (n) => aggregateTypes.includes(getExprType(n)));
}

/**
 * Get all window function calls in the AST
 */
export function getWindowFunctions(node: Expression): Expression[] {
  const windowTypes = [
    'row_number',
    'rank',
    'dense_rank',
    'n_tile',
    'lead',
    'lag',
    'first_value',
    'last_value',
    'nth_value',
    'percent_rank',
    'cume_dist',
    'percentile_cont',
    'percentile_disc',
  ];

  return findAll(node, (n) => windowTypes.includes(getExprType(n)));
}

/**
 * Get all subqueries in the AST
 */
export function getSubqueries(node: Expression): Expression[] {
  return findByType(node, 'subquery');
}

/**
 * Get all literals in the AST
 */
export function getLiterals(node: Expression): Expression[] {
  return findByType(node, 'literal');
}

/**
 * Get all column names as strings
 */
export function getColumnNames(node: Expression): string[] {
  return getColumns(node).map((col) => {
    const colData = getExprData(col) as { name: { name: string } };
    return colData.name.name;
  });
}

/**
 * Get all table names as strings
 */
export function getTableNames(node: Expression): string[] {
  return getTables(node).map((tbl) => {
    const tblData = getExprData(tbl) as { name: { name: string } };
    return tblData.name.name;
  });
}

/**
 * Check if the AST contains any aggregate functions
 */
export function hasAggregates(node: Expression): boolean {
  return getAggregateFunctions(node).length > 0;
}

/**
 * Check if the AST contains any window functions
 */
export function hasWindowFunctions(node: Expression): boolean {
  return getWindowFunctions(node).length > 0;
}

/**
 * Check if the AST contains any subqueries
 */
export function hasSubqueries(node: Expression): boolean {
  return some(node, (n) => getExprType(n) === 'subquery');
}

/**
 * Get the depth of the AST
 */
export function getDepth(node: Expression): number {
  let maxDepth = 0;
  let currentDepth = 0;

  walk(node, {
    enter: () => {
      currentDepth++;
      maxDepth = Math.max(maxDepth, currentDepth);
    },
    leave: () => {
      currentDepth--;
    },
  });

  return maxDepth;
}

/**
 * Count the total number of nodes in the AST
 */
export function nodeCount(node: Expression): number {
  return countNodes(node, () => true);
}
