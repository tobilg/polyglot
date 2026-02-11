/**
 * AST Module
 *
 * This module provides types, type guards, helpers, and visitor utilities
 * for working with SQL AST nodes.
 *
 * Query builders are in `src/builders.ts` (WASM-backed).
 */

// Helpers for externally tagged Expression format
export {
  getExprType,
  getExprData,
  isExpressionValue,
  makeExpr,
  type ExpressionType,
  type ExpressionByKey,
  type ExpressionInner,
} from './helpers';

// Types - export all from types (includes type guards like isSelect, isColumn, etc.)
export * from './types';

// Visitor utilities
export * from './visitor';
