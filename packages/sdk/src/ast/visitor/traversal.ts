/**
 * Canonical recursion over the serialized AST representation.
 *
 * Rust's typed AST stores some expression-bearing values inside ordinary
 * structs. Once serialized, those structs have no expression envelope, so the
 * TypeScript visitor must recurse through both object fields and every array
 * element until it reaches a recognized Expression value.
 */

import type { Expression } from '../../generated/Expression';
import { getExprData, isExpressionValue } from '../helpers';

export interface ExpressionLocation {
  parent: Expression;
  key: string;
  index: number | null;
}

interface ProcessOptions {
  onExpression: (node: Expression, location: ExpressionLocation) => Expression;
  cloneContainers: boolean;
  onExpressionArray?: (
    nodes: Expression[],
    location: ExpressionLocation,
  ) => void;
  removeArrayExpression?: (
    node: Expression,
    location: ExpressionLocation,
  ) => boolean;
}

const removed = Symbol('removed serialized AST value');

function processValue(
  value: unknown,
  location: ExpressionLocation,
  options: ProcessOptions,
  removable: boolean,
): unknown | typeof removed {
  if (value === null || value === undefined) return value;

  if (isExpressionValue(value)) {
    if (
      removable &&
      options.removeArrayExpression?.(value, location) === true
    ) {
      return removed;
    }
    return options.onExpression(value, location);
  }

  if (Array.isArray(value)) {
    if (
      value.length > 0 &&
      value.every(isExpressionValue) &&
      location.index === null &&
      options.onExpressionArray
    ) {
      options.onExpressionArray(value, location);
      return value;
    }

    let changed = options.cloneContainers;
    const processed: unknown[] = [];

    value.forEach((item, index) => {
      const nextLocation = { ...location, index };
      const nextValue = processValue(item, nextLocation, options, true);
      if (nextValue === removed) {
        changed = true;
        return;
      }
      processed.push(nextValue);
      if (nextValue !== item) changed = true;
    });

    return changed ? processed : value;
  }

  if (typeof value === 'object') {
    const object = value as Record<string, unknown>;
    const processed: Record<string, unknown> = {};
    let changed = options.cloneContainers;

    for (const [field, fieldValue] of Object.entries(object)) {
      const nextValue = processValue(fieldValue, location, options, false);
      if (nextValue === removed) {
        // Only direct array elements are removable.
        processed[field] = fieldValue;
      } else {
        processed[field] = nextValue;
        if (nextValue !== fieldValue) changed = true;
      }
    }

    return changed ? processed : value;
  }

  return value;
}

/** Read-only traversal never rebuilds arrays or ordinary payload structs. */
function visitValue(
  value: unknown,
  location: ExpressionLocation,
  visitor: (node: Expression, location: ExpressionLocation) => boolean,
  visitArray?: (nodes: Expression[], location: ExpressionLocation) => void,
): boolean {
  if (value === null || value === undefined) return true;
  if (isExpressionValue(value)) return visitor(value, location);
  if (Array.isArray(value)) {
    if (
      visitArray &&
      location.index === null &&
      value.length > 0 &&
      value.every(isExpressionValue)
    ) {
      visitArray(value, location);
      return true;
    }
    // Match Array.forEach's original length snapshot if a visitor mutates it.
    const length = value.length;
    for (let index = 0; index < length; index++) {
      if (
        !visitValue(value[index], { ...location, index }, visitor, visitArray)
      )
        return false;
    }
  } else if (typeof value === 'object') {
    for (const fieldValue of Object.values(value)) {
      if (!visitValue(fieldValue, location, visitor, visitArray)) return false;
    }
  }
  return true;
}

/** Collect children, preserving the legacy grouping of direct expression arrays. */
export function collectExpressionChildren(
  parent: Expression,
): Array<{ key: string; value: Expression | Expression[] }> {
  const children: Array<{ key: string; value: Expression | Expression[] }> = [];
  const data = getExprData(parent);
  if (!data || typeof data !== 'object') return children;
  for (const [key, value] of Object.entries(data)) {
    visitValue(
      value,
      { parent, key, index: null },
      (node, location) => {
        children.push({ key: location.key, value: node });
        return true;
      },
      (nodes, location) => {
        children.push({ key: location.key, value: nodes });
      },
    );
  }
  return children;
}

/** Stop immediately when a visitor returns false, including inside containers. */
export function visitExpressionChildrenWhile(
  parent: Expression,
  visitor: (node: Expression, location: ExpressionLocation) => boolean,
): boolean {
  const data = getExprData(parent);
  if (!data || typeof data !== 'object') return true;
  for (const [key, value] of Object.entries(data)) {
    if (!visitValue(value, { parent, key, index: null }, visitor)) return false;
  }
  return true;
}

/** Visit every immediate expression child below an expression payload. */
export function visitExpressionChildren(
  parent: Expression,
  visitor: (node: Expression, location: ExpressionLocation) => void,
): void {
  visitExpressionChildrenWhile(parent, (node, location) => {
    visitor(node, location);
    return true;
  });
}

/**
 * Rebuild an expression payload while transforming its expression children.
 * Containers are cloned so `transform(..., {})` is also a true deep clone.
 */
export function mapExpressionChildren(
  parent: Expression,
  transform: (node: Expression, location: ExpressionLocation) => Expression,
  removeArrayExpression?: (
    node: Expression,
    location: ExpressionLocation,
  ) => boolean,
): unknown {
  const innerData: unknown = getExprData(parent);
  // Unit variants have null payloads; nested enums can serialize to strings
  // (e.g. ColumnPosition::First). These leaves have no expression children.
  if (innerData === null || typeof innerData !== 'object') return innerData;
  const processed: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(innerData)) {
    const nextValue = processValue(
      value,
      { parent, key, index: null },
      {
        cloneContainers: true,
        onExpression: transform,
        removeArrayExpression,
      },
      false,
    );
    processed[key] = nextValue === removed ? value : nextValue;
  }

  return processed;
}
