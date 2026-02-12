/**
 * Visitor Pattern Types
 *
 * Type definitions for walking and transforming AST nodes.
 */

import type { Expression } from '../../generated/Expression';
import type { ExpressionType } from '../helpers';

/**
 * Visitor callback function type
 */
export type VisitorCallback<T = void> = (
  node: Expression,
  parent: Expression | null,
  key: string | null,
  index: number | null,
) => T;

/**
 * Visitor configuration with callbacks for specific node types
 */
export type VisitorConfig = {
  [K in ExpressionType]?: VisitorCallback;
} & {
  /** Called before visiting any node */
  enter?: VisitorCallback;
  /** Called after visiting any node and its children */
  leave?: VisitorCallback;
};

/**
 * Transform callback - returns new node or undefined to keep original
 */
export type TransformCallback = (
  node: Expression,
  parent: Expression | null,
  key: string | null,
  index: number | null,
) => Expression | undefined | null;

/**
 * Transform configuration with callbacks for specific node types
 */
export type TransformConfig = {
  [K in ExpressionType]?: TransformCallback;
} & {
  /** Called before transforming any node */
  enter?: TransformCallback;
  /** Called after transforming any node and its children */
  leave?: TransformCallback;
};

/**
 * Predicate function for finding nodes
 */
export type NodePredicate = (
  node: Expression,
  parent: Expression | null,
) => boolean;

/**
 * Path to a node in the AST
 */
export interface NodePath {
  node: Expression;
  parent: Expression | null;
  key: string | null;
  index: number | null;
  ancestors: Expression[];
}

/**
 * Context passed to visitors
 */
export interface VisitorContext {
  /** Stop traversal completely */
  stop(): void;
  /** Skip visiting children of current node */
  skip(): void;
  /** Path to current node */
  path: NodePath;
}
