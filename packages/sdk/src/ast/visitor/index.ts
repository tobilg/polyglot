/**
 * AST Visitor Utilities
 *
 * This module exports walker and transformer functions for traversing
 * and manipulating SQL AST nodes.
 */

// Transformer functions
export {
  addSelectColumns,
  addWhere,
  clone,
  qualifyColumns,
  remove,
  removeLimitOffset,
  removeSelectColumns,
  removeWhere,
  renameColumns,
  renameTables,
  replaceByType,
  replaceNodes,
  setDistinct,
  setLimit,
  setOffset,
  transform,
} from './transformer';
// Types
export type {
  NodePath,
  NodePredicate,
  TransformCallback,
  TransformConfig,
  VisitorCallback,
  VisitorConfig,
  VisitorContext,
} from './types';
// Walker functions
export {
  countNodes,
  every,
  findAll,
  findByType,
  findFirst,
  getAggregateFunctions,
  getColumnNames,
  getColumns,
  getDepth,
  getFunctions,
  getIdentifiers,
  getLiterals,
  getSubqueries,
  getTableNames,
  getTables,
  getWindowFunctions,
  hasAggregates,
  hasSubqueries,
  hasWindowFunctions,
  nodeCount,
  some,
  walk,
} from './walker';
