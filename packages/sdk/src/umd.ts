/**
 * UMD Entry Point for CDN Distribution
 *
 * This file exports all public APIs for use via the global `PolyglotSQL` variable
 * when loaded via a script tag.
 *
 * Usage:
 * ```html
 * <script src="https://unpkg.com/@polyglot-sql/sdk/dist/umd/polyglot.umd.js"></script>
 * <script>
 *   const { transpile, parse, Dialect } = window.PolyglotSQL;
 *   const result = transpile('SELECT 1', Dialect.MySQL, Dialect.PostgreSQL);
 * </script>
 * ```
 */

// Core APIs
export {
  ast,
  Dialect,
  format,
  generate,
  getDialects,
  getVersion,
  init,
  isInitialized,
  Polyglot,
  parse,
  transpile,
} from './index';
export type { ParseResult, TranspileResult } from './index';

// AST: Type guards & Visitors
export {
  findAll,
  getColumns,
  isColumn,
  isFunction,
  isLiteral,
  isSelect,
  renameColumns,
  transform,
  walk,
} from './ast';

// Builders: Expression class & types
export { Expr, type ExprInput } from './builders';

// Builders: Expression helpers
export {
  col,
  lit,
  star,
  sqlNull,
  boolean,
  table,
  sqlExpr,
  condition,
  func,
} from './builders';

// Builders: Query builders
export {
  SelectBuilder,
  InsertBuilder,
  UpdateBuilder,
  DeleteBuilder,
  MergeBuilder,
  CaseBuilder,
  SetOpBuilder,
  select,
  insert,
  insertInto,
  update,
  deleteFrom,
  del,
  mergeInto,
  caseWhen,
  caseOf,
  union,
  unionAll,
  intersect,
  except,
} from './builders';

// Builders: Logical operators
export { and, or, not, cast, alias } from './builders';

// Builders: Convenience functions
export {
  // Aggregate
  count,
  countDistinct,
  sum,
  avg,
  min,
  max,
  // String
  upper,
  lower,
  length,
  trim,
  ltrim,
  rtrim,
  reverse,
  initcap,
  substring,
  replace,
  concatWs,
  // Null handling
  coalesce,
  nullIf,
  ifNull,
  // Math
  abs,
  round,
  floor,
  ceil,
  power,
  sqrt,
  ln,
  exp,
  sign,
  greatest,
  least,
  // Date/time
  currentDate,
  currentTime,
  currentTimestamp,
  extract,
  // Window
  rowNumber,
  rank,
  denseRank,
} from './builders';

// Validation
export { ValidationSeverity, validate, validateWithSchema } from './validation';
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableSchema,
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './validation';

// Default export for convenience
import * as PolyglotSQL from './index';
export default PolyglotSQL;
