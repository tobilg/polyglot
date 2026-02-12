/**
 * AST Transformer Utilities
 *
 * Functions for immutably transforming SQL AST nodes.
 * All transformations return new AST nodes without modifying the originals.
 *
 * With externally tagged enums, each Expression is { "variant": data }
 * instead of { "type": "variant", ...data }.
 */

import type { Expression } from '../../generated/Expression';
import {
  getExprData,
  getExprType,
  isExpressionValue,
  makeExpr,
} from '../helpers';
import type {
  NodePredicate,
  TransformCallback,
  TransformConfig,
} from './types';

// ============================================================================
// Core Transformer
// ============================================================================

/**
 * Recursively transform any value that may contain Expression children.
 *
 * Handles:
 * - Expression values → full transformNode
 * - Arrays of Expressions → map with transformNode
 * - Nested tuple arrays (CASE whens) → map elements
 * - Non-Expression struct objects (From, Where, etc.) → recurse into fields
 * - Primitives and null → pass through unchanged
 */
function transformValue(
  value: unknown,
  config: TransformConfig,
  parent: Expression,
): unknown {
  if (value === null || value === undefined) return value;

  if (Array.isArray(value)) {
    if (value.length > 0 && isExpressionValue(value[0])) {
      return value.map((item, i) =>
        transformNode(item as Expression, config, parent, null, i),
      );
    }
    if (value.length > 0 && Array.isArray(value[0])) {
      return value.map((tuple) => {
        if (Array.isArray(tuple)) {
          return tuple.map((item, i) => {
            if (isExpressionValue(item)) {
              return transformNode(item as Expression, config, parent, null, i);
            }
            return item;
          });
        }
        return tuple;
      });
    }
    return value;
  }

  if (isExpressionValue(value)) {
    return transformNode(value as Expression, config, parent, null, null);
  }

  if (typeof value === 'object') {
    // Non-Expression struct — recurse into its fields
    const obj = value as Record<string, unknown>;
    const newObj = { ...obj };
    let changed = false;
    for (const [k, v] of Object.entries(newObj)) {
      const newV = transformValue(v, config, parent);
      if (newV !== v) {
        newObj[k] = newV;
        changed = true;
      }
    }
    return changed ? newObj : value;
  }

  return value;
}

/**
 * Recursively clone and transform an AST node
 */
function transformNode(
  node: Expression,
  config: TransformConfig,
  parent: Expression | null,
  key: string | null,
  index: number | null,
): Expression {
  // Call enter callback
  let currentNode = node;
  if (config.enter) {
    const result = config.enter(currentNode, parent, key, index);
    if (result === null) {
      // Signal to remove this node - return unchanged and let parent handle it
      return currentNode;
    }
    if (result !== undefined) {
      currentNode = result;
    }
  }

  // Call type-specific callback
  const nodeType = getExprType(currentNode);
  const typeCallback = config[nodeType as keyof TransformConfig] as
    | TransformCallback
    | undefined;
  if (typeCallback) {
    const result = typeCallback(currentNode, parent, key, index);
    if (result === null) {
      return currentNode;
    }
    if (result !== undefined) {
      currentNode = result;
    }
  }

  // Unwrap the envelope to get inner data and type
  const currentType = getExprType(currentNode);
  const innerData = getExprData(currentNode);

  // Unit struct variants (Null, CurrentDate, RowNumber, etc.) have null inner data
  let newNode: Expression;
  if (innerData === null || innerData === undefined) {
    newNode = makeExpr(currentType, innerData);
  } else {
    // Create a shallow copy of the inner data
    const newInnerData = { ...innerData };

    // Transform children within the inner data
    for (const [propKey, value] of Object.entries(newInnerData)) {
      const newValue = transformValue(value, config, currentNode);
      if (newValue !== value) {
        newInnerData[propKey] = newValue;
      }
    }

    // Re-wrap in the envelope
    newNode = makeExpr(currentType, newInnerData);
  }

  // Call leave callback
  if (config.leave) {
    const result = config.leave(newNode, parent, key, index);
    if (result !== undefined && result !== null) {
      return result;
    }
  }

  return newNode;
}

/**
 * Transform an AST tree, returning a new tree with modifications
 *
 * @example
 * ```typescript
 * // Rename all columns named 'old' to 'new'
 * const newAst = transform(ast, {
 *   column: (node) => {
 *     const data = getExprData(node);
 *     if (data.name.name === 'old') {
 *       return makeExpr('column', { ...data, name: { ...data.name, name: 'new' } });
 *     }
 *   }
 * });
 * ```
 */
export function transform(
  node: Expression,
  config: TransformConfig,
): Expression {
  return transformNode(node, config, null, null, null);
}

// ============================================================================
// Replace Functions
// ============================================================================

/**
 * Replace nodes matching a predicate with a new node
 *
 * @example
 * ```typescript
 * // Replace all NULL literals with 0
 * const newAst = replaceNodes(
 *   ast,
 *   (node) => getExprType(node) === 'null',
 *   { literal: { literal_type: 'number', value: '0' } } as Expression
 * );
 * ```
 */
export function replaceNodes(
  node: Expression,
  predicate: NodePredicate,
  replacement: Expression | ((node: Expression) => Expression),
): Expression {
  return transform(node, {
    enter: (n, parent) => {
      if (predicate(n, parent)) {
        return typeof replacement === 'function' ? replacement(n) : replacement;
      }
      return undefined;
    },
  });
}

/**
 * Replace nodes of a specific type
 */
export function replaceByType(
  node: Expression,
  type: string,
  replacement: Expression | ((node: Expression) => Expression),
): Expression {
  return replaceNodes(
    node,
    (n: Expression) => getExprType(n) === type,
    typeof replacement === 'function'
      ? (n: Expression) => (replacement as (node: Expression) => Expression)(n)
      : replacement,
  );
}

// ============================================================================
// Column and Table Renaming
// ============================================================================

/**
 * Rename columns in the AST
 *
 * @example
 * ```typescript
 * const newAst = renameColumns(ast, {
 *   old_name: 'new_name',
 *   another_old: 'another_new'
 * });
 * ```
 */
export function renameColumns(
  node: Expression,
  mapping: Record<string, string>,
): Expression {
  return transform(node, {
    column: (n) => {
      const data = getExprData(n) as {
        name: { name: string; quoted: boolean };
        table: unknown;
      };
      const oldName = data.name.name;
      if (mapping[oldName]) {
        return makeExpr('column', {
          ...data,
          name: { ...data.name, name: mapping[oldName] },
        });
      }
      return undefined;
    },
    identifier: (n) => {
      const data = getExprData(n) as { name: string; quoted: boolean };
      const oldName = data.name;
      if (mapping[oldName]) {
        return makeExpr('identifier', {
          ...data,
          name: mapping[oldName],
        });
      }
      return undefined;
    },
  });
}

/**
 * Rename tables in the AST
 *
 * @example
 * ```typescript
 * const newAst = renameTables(ast, {
 *   old_table: 'new_table',
 *   temp_users: 'users'
 * });
 * ```
 */
export function renameTables(
  node: Expression,
  mapping: Record<string, string>,
): Expression {
  return transform(node, {
    table: (n) => {
      const data = getExprData(n) as {
        name: { name: string; quoted: boolean };
      };
      const oldName = data.name.name;
      if (mapping[oldName]) {
        return makeExpr('table', {
          ...data,
          name: { ...data.name, name: mapping[oldName] },
        });
      }
      return undefined;
    },
  });
}

/**
 * Qualify unqualified column references with a table name
 *
 * @example
 * ```typescript
 * // Add table prefix to all unqualified columns
 * const newAst = qualifyColumns(ast, 'users');
 * // col('id') => col('id', 'users')
 * ```
 */
export function qualifyColumns(
  node: Expression,
  tableName: string,
): Expression {
  return transform(node, {
    column: (n) => {
      const data = getExprData(n) as { table: { name: string } | null };
      if (data.table === null) {
        return makeExpr('column', {
          ...data,
          table: { name: tableName, quoted: false },
        });
      }
      return undefined;
    },
  });
}

// ============================================================================
// WHERE Clause Manipulation
// ============================================================================

/**
 * Add a condition to the WHERE clause of a SELECT
 *
 * @example
 * ```typescript
 * const newAst = addWhere(selectAst, eq('deleted', false), 'and');
 * ```
 */
export function addWhere(
  node: Expression,
  condition: Expression,
  operator: 'and' | 'or' = 'and',
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node) as {
    where_clause: { this: Expression } | null;
  };

  if (selectData.where_clause === null) {
    // No existing WHERE — create raw Where struct (not Expression envelope)
    return makeExpr('select', {
      ...selectData,
      where_clause: { this: condition },
    });
  }

  // Get the existing WHERE condition from the raw Where struct
  const existingCondition = selectData.where_clause.this;
  const combinedCondition = makeExpr(operator, {
    left: existingCondition,
    right: condition,
  });

  return makeExpr('select', {
    ...selectData,
    where_clause: { this: combinedCondition },
  });
}

/**
 * Remove the WHERE clause from a SELECT
 */
export function removeWhere(node: Expression): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node);
  return makeExpr('select', {
    ...selectData,
    where_clause: null,
  });
}

// ============================================================================
// SELECT Clause Manipulation
// ============================================================================

/**
 * Add columns to a SELECT expression
 */
export function addSelectColumns(
  node: Expression,
  ...columns: Expression[]
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node) as { expressions: Expression[] };

  return makeExpr('select', {
    ...selectData,
    expressions: [...selectData.expressions, ...columns],
  });
}

/**
 * Remove columns from a SELECT expression by predicate
 */
export function removeSelectColumns(
  node: Expression,
  predicate: (col: Expression) => boolean,
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node) as { expressions: Expression[] };

  return makeExpr('select', {
    ...selectData,
    expressions: selectData.expressions.filter((col) => !predicate(col)),
  });
}

// ============================================================================
// Limit/Offset Manipulation
// ============================================================================

/**
 * Set or update the LIMIT clause
 */
export function setLimit(
  node: Expression,
  limit: number | Expression,
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node);

  const limitExpr: Expression =
    typeof limit === 'number'
      ? makeExpr('literal', { literal_type: 'number', value: String(limit) })
      : limit;

  // Select.limit is a raw Limit struct, not an Expression envelope
  return makeExpr('select', {
    ...selectData,
    limit: { this: limitExpr },
  });
}

/**
 * Set or update the OFFSET clause
 */
export function setOffset(
  node: Expression,
  offset: number | Expression,
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node);

  const offsetExpr: Expression =
    typeof offset === 'number'
      ? makeExpr('literal', { literal_type: 'number', value: String(offset) })
      : offset;

  // Select.offset is a raw Offset struct, not an Expression envelope
  return makeExpr('select', {
    ...selectData,
    offset: { this: offsetExpr },
  });
}

/**
 * Remove LIMIT and OFFSET clauses
 */
export function removeLimitOffset(node: Expression): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node);
  return makeExpr('select', {
    ...selectData,
    limit: null,
    offset: null,
  });
}

// ============================================================================
// Distinct Manipulation
// ============================================================================

/**
 * Set SELECT DISTINCT
 */
export function setDistinct(
  node: Expression,
  distinct: boolean = true,
): Expression {
  if (getExprType(node) !== 'select') {
    return node;
  }

  const selectData = getExprData(node);
  return makeExpr('select', {
    ...selectData,
    distinct,
  });
}

// ============================================================================
// Deep Clone
// ============================================================================

/**
 * Create a deep clone of an AST node
 */
export function clone(node: Expression): Expression {
  return transform(node, {});
}

// ============================================================================
// Remove Nodes
// ============================================================================

/**
 * Recursively remove matching Expression nodes from any value.
 */
function removeFromValue(
  value: unknown,
  predicate: NodePredicate,
  parent: Expression,
): unknown {
  if (value === null || value === undefined) return value;

  if (Array.isArray(value)) {
    if (value.length > 0 && isExpressionValue(value[0])) {
      return value
        .filter((item) => !predicate(item as Expression, parent))
        .map((item) => remove(item as Expression, predicate));
    }
    return value;
  }

  if (isExpressionValue(value)) {
    return remove(value as Expression, predicate);
  }

  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const newObj = { ...obj };
    let changed = false;
    for (const [k, v] of Object.entries(newObj)) {
      const newV = removeFromValue(v, predicate, parent);
      if (newV !== v) {
        newObj[k] = newV;
        changed = true;
      }
    }
    return changed ? newObj : value;
  }

  return value;
}

/**
 * Remove all nodes matching a predicate
 * Note: This can only remove nodes that are in arrays (like expressions in SELECT)
 * Removing required nodes will leave them unchanged
 */
export function remove(node: Expression, predicate: NodePredicate): Expression {
  const nodeType = getExprType(node);
  const innerData = getExprData(node);

  // Clone inner data with filtering
  const newInnerData = { ...innerData };

  for (const [key, value] of Object.entries(newInnerData)) {
    const newValue = removeFromValue(value, predicate, node);
    if (newValue !== value) {
      newInnerData[key] = newValue;
    }
  }

  return makeExpr(nodeType, newInnerData);
}
