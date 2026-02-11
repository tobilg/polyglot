/**
 * AST Visitor Utilities
 *
 * This module exports walker and transformer functions for traversing
 * and manipulating SQL AST nodes.
 */

// Types
export type {
  VisitorCallback,
  VisitorConfig,
  TransformCallback,
  TransformConfig,
  NodePredicate,
  NodePath,
  VisitorContext,
} from './types';

// Walker functions
export {
  walk,
  findAll,
  findByType,
  findFirst,
  some,
  every,
  countNodes,
  getColumns,
  getTables,
  getIdentifiers,
  getFunctions,
  getAggregateFunctions,
  getWindowFunctions,
  getSubqueries,
  getLiterals,
  getColumnNames,
  getTableNames,
  hasAggregates,
  hasWindowFunctions,
  hasSubqueries,
  getDepth,
  nodeCount,
} from './walker';

// Transformer functions
export {
  transform,
  replaceNodes,
  replaceByType,
  renameColumns,
  renameTables,
  qualifyColumns,
  addWhere,
  removeWhere,
  addSelectColumns,
  removeSelectColumns,
  setLimit,
  setOffset,
  removeLimitOffset,
  setDistinct,
  clone,
  remove,
} from './transformer';
