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
import { getWasmSync } from '../../wasm-loader';

/** Serialize Expression to JSON for WASM functions */
function exprToJson(node: Expression): string {
  return JSON.stringify(node);
}

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
export function getChildren(
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
 * Get all function calls in the AST (via WASM)
 */
export function getFunctions(node: Expression): Expression[] {
  const result = JSON.parse(getWasmSync().ast_get_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all aggregate function calls in the AST (via WASM)
 */
export function getAggregateFunctions(node: Expression): Expression[] {
  const result = JSON.parse(getWasmSync().ast_get_aggregate_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all window function calls in the AST (via WASM)
 */
export function getWindowFunctions(node: Expression): Expression[] {
  const result = JSON.parse(getWasmSync().ast_get_window_functions(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all subqueries in the AST (via WASM)
 */
export function getSubqueries(node: Expression): Expression[] {
  const result = JSON.parse(getWasmSync().ast_get_subqueries(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all literals in the AST (via WASM)
 */
export function getLiterals(node: Expression): Expression[] {
  const result = JSON.parse(getWasmSync().ast_get_literals(exprToJson(node)));
  return result.success ? JSON.parse(result.ast) : [];
}

/**
 * Get all column names as strings (via WASM)
 */
export function getColumnNames(node: Expression): string[] {
  const result = JSON.parse(getWasmSync().ast_get_column_names(exprToJson(node)));
  return result.success ? result.result : [];
}

/**
 * Get all table names as strings (via WASM)
 */
export function getTableNames(node: Expression): string[] {
  const result = JSON.parse(getWasmSync().ast_get_table_names(exprToJson(node)));
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
  const result = JSON.parse(getWasmSync().ast_node_count(exprToJson(node)));
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
