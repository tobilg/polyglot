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
  qualifyTables,
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
export type { QualifyTablesOptions, RenameTablesOptions } from './transformer';
// Walker functions
export {
  countNodes,
  every,
  findAll,
  findAncestor,
  findByType,
  findFirst,
  getAggregateFunctions,
  getChildren,
  getColumnNames,
  getColumns,
  getDepth,
  getFunctions,
  getIdentifiers,
  getLiterals,
  getNodeDepth,
  getParent,
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
