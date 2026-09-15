/**
 * AST Walker Utilities
 *
 * Functions for traversing and searching the SQL AST.
 *
 * With externally tagged enums, each Expression is { "variant": data }
 * instead of { "type": "variant", ...data }.
 */

import {
  ast_get_aggregate_functions,
  ast_get_column_names,
  ast_get_functions,
  ast_get_literals,
  ast_get_subqueries,
  ast_get_table_names,
  ast_get_tables,
  ast_get_window_functions,
  ast_node_count,
} from '../../../wasm/polyglot_sql_wasm.js';
import type { Expression } from '../../generated/Expression';
import {
  type ExpressionBySingleKey,
  type ExpressionType,
  getExprType,
  type SingleExpressionType,
} from '../helpers';
import {
  collectExpressionChildren,
  visitExpressionChildren,
} from './traversal';
import type { NodePredicate, VisitorCallback, VisitorConfig } from './types';

/** Serialize Expression to JSON for WASM functions */
function exprToJson(node: Expression): string {
  return JSON.stringify(node);
}

// ============================================================================
// Core Walker
// ============================================================================

/**
 * Get all child expressions from a node.
 *
 * Unwraps the outer Expression envelope, then iterates the inner data
 * looking for Expression children — including those nested inside
 * non-Expression structs like From, Where, GroupBy, etc.
 */
export function getChildren(
  node: Expression,
): Array<{ key: string; value: Expression | Expression[] }> {
  return collectExpressionChildren(node);
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

  // Recursively walk every expression-bearing payload slot, including those
  // nested inside arrays of ordinary AST structs.
  visitExpressionChildren(node, (child, location) => {
    walk(child, visitor, location.parent, location.key, location.index);
  });

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
export function findByType<const T extends ExpressionType>(
  node: Expression,
  type: SingleExpressionType<T>,
): ExpressionBySingleKey<T>[];
export function findByType(
  node: Expression,
  type: ExpressionType,
): Expression[];
export function findByType(
  node: Expression,
  type: ExpressionType,
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
 *   (node) => !isExpressionType(node, 'column') || getExprData(node).table !== null
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
  const result = JSON.parse(ast_get_tables(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all identifiers in the AST
 */
export function getIdentifiers(node: Expression): Expression[] {
  return findByType(node, 'identifier');
}

/**
 * Get all function calls in the AST (via WASM)
 */
export function getFunctions(node: Expression): Expression[] {
  const result = JSON.parse(ast_get_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all aggregate function calls in the AST (via WASM)
 */
export function getAggregateFunctions(node: Expression): Expression[] {
  const result = JSON.parse(ast_get_aggregate_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all window function calls in the AST (via WASM)
 */
export function getWindowFunctions(node: Expression): Expression[] {
  const result = JSON.parse(ast_get_window_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all subqueries in the AST (via WASM)
 */
export function getSubqueries(node: Expression): Expression[] {
  const result = JSON.parse(ast_get_subqueries(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all literals in the AST (via WASM)
 */
export function getLiterals(node: Expression): Expression[] {
  const result = JSON.parse(ast_get_literals(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all column names as strings (via WASM)
 */
export function getColumnNames(node: Expression): string[] {
  const result = JSON.parse(ast_get_column_names(exprToJson(node)));
  return result.success ? result.result : [];
}

/**
 * Get all table names as strings (via WASM)
 */
export function getTableNames(node: Expression): string[] {
  const result = JSON.parse(ast_get_table_names(exprToJson(node)));
  return result.success ? result.result : [];
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
  return getSubqueries(node).length > 0;
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
 * Count the total number of nodes in the AST (via WASM)
 */
export function nodeCount(node: Expression): number {
  const result = JSON.parse(ast_node_count(exprToJson(node)));
  return result.success ? result.result : 0;
}

/**
 * Find the parent of a target node in the AST.
 *
 * Uses reference equality (`===`) to identify the target, so the target
 * must be a reference obtained from the same AST object graph (e.g. via
 * `findFirst()` or `findAll()`).
 *
 * @returns The parent Expression, or `null` if target is the root or not found.
 */
export function getParent(
  root: Expression,
  target: Expression,
): Expression | null {
  let parentNode: Expression | null = null;

  walk(root, {
    enter: (node, parent) => {
      if (node === target) {
        parentNode = parent;
      }
    },
  });

  return parentNode;
}

/**
 * Find the nearest ancestor of a target node that matches a predicate.
 *
 * Walks the tree from the root, tracking the ancestor stack. When the
 * target is found, searches ancestors from nearest to farthest.
 *
 * Uses reference equality (`===`) to identify the target.
 *
 * @returns The matching ancestor, or `null` if none matches or target not found.
 */
export function findAncestor(
  root: Expression,
  target: Expression,
  predicate: NodePredicate,
): Expression | null {
  const ancestors: Expression[] = [];
  let result: Expression | null = null;

  walk(root, {
    enter: (node) => {
      if (result !== null) return; // already found
      if (node === target) {
        // Search ancestors nearest-to-farthest
        for (let i = ancestors.length - 1; i >= 0; i--) {
          const parent = i > 0 ? ancestors[i - 1] : null;
          if (predicate(ancestors[i], parent)) {
            result = ancestors[i];
            break;
          }
        }
      }
      ancestors.push(node);
    },
    leave: () => {
      ancestors.pop();
    },
  });

  return result;
}

/**
 * Get the depth of a specific node within the AST.
 *
 * The root node has depth 1. Returns 0 if the target is not found.
 *
 * Uses reference equality (`===`) to identify the target.
 */
export function getNodeDepth(root: Expression, target: Expression): number {
  let depth = 0;
  let currentDepth = 0;

  walk(root, {
    enter: (node) => {
      currentDepth++;
      if (node === target) {
        depth = currentDepth;
      }
    },
    leave: () => {
      currentDepth--;
    },
  });

  return depth;
}
