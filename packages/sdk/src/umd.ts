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

// AST: Type guards & Visitors
export {
  findAll,
  getColumns,
  getInferredType,
  isColumn,
  isFunction,
  isLiteral,
  isSelect,
  renameColumns,
  transform,
  walk,
} from './ast';
// Builders: Expression class & types
// Builders: Expression helpers
// Builders: Query builders
// Builders: Logical operators
// Builders: Convenience functions
export {
  // Math
  abs,
  alias,
  and,
  avg,
  boolean,
  CaseBuilder,
  caseOf,
  caseWhen,
  cast,
  ceil,
  // Null handling
  coalesce,
  col,
  concatWs,
  condition,
  // Aggregate
  count,
  countDistinct,
  // Date/time
  currentDate,
  currentTime,
  currentTimestamp,
  DeleteBuilder,
  del,
  deleteFrom,
  denseRank,
  Expr,
  type ExprInput,
  except,
  exp,
  extract,
  floor,
  func,
  greatest,
  InsertBuilder,
  ifNull,
  initcap,
  insert,
  insertInto,
  intersect,
  least,
  length,
  lit,
  ln,
  lower,
  ltrim,
  MergeBuilder,
  max,
  mergeInto,
  min,
  not,
  nullIf,
  or,
  power,
  rank,
  replace,
  reverse,
  round,
  // Window
  rowNumber,
  rtrim,
  SelectBuilder,
  SetOpBuilder,
  select,
  sign,
  sqlExpr,
  sqlNull,
  sqrt,
  star,
  substring,
  sum,
  table,
  trim,
  UpdateBuilder,
  union,
  unionAll,
  update,
  // String
  upper,
} from './builders';
export type {
  AnnotateTypesResult,
  FormatOptions,
  ParseResult,
  TranspileOptions,
  TranspileResult,
  UnsupportedLevel,
} from './index';
// Core APIs
export {
  annotateTypes,
  ast,
  Dialect,
  format,
  formatWithOptions,
  generate,
  getDialects,
  getVersion,
  init,
  isInitialized,
  Polyglot,
  parse,
  transpile,
} from './index';
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableSchema,
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './validation';
// Validation
export { ValidationSeverity, validate, validateWithSchema } from './validation';

// Default export for convenience
import * as PolyglotSQL from './index';
export default PolyglotSQL;
