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
 *   const result = transpile('SELECT 1', {
 *     read: Dialect.MySQL,
 *     write: Dialect.PostgreSQL,
 *   });
 * </script>
 * ```
 */

// Core APIs
export {
  init,
  isInitialized,
  transpile,
  parse,
  generate,
  format,
  getDialects,
  getVersion,
  Dialect,
  Polyglot,
} from './index';

// Types
export type {
  TranspileOptions,
  TranspileResult,
  ParseResult,
} from './index';

// Validation APIs
export {
  validate,
  validateWithSchema,
  ValidationSeverity,
} from './validation';

export type {
  ValidationResult,
  ValidationError,
  ValidationOptions,
  Schema,
  TableSchema,
  ColumnSchema,
  SchemaValidationOptions,
} from './validation';

// AST Module
export { ast } from './index';

// Type guards
export {
  isSelect,
  isColumn,
  isLiteral,
  isFunction,
} from './ast';

// Builders
export {
  select,
  insert,
  update,
  deleteFrom,
  col,
  and,
  or,
} from './builders';

// Visitors
export {
  walk,
  findAll,
  getColumns,
  transform,
  renameColumns,
} from './ast';

// Default export for convenience
import * as PolyglotSQL from './index';
export default PolyglotSQL;
