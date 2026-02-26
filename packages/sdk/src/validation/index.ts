/**
 * SQL Validation Module
 *
 * Provides syntax and semantic validation for SQL queries.
 */

import { getWasmSync } from '../wasm-loader';
import type { Expression } from '../generated/Expression';
import { validateSemantics } from './rules';
import type {
  ValidationError,
  ValidationOptions,
  ValidationResult,
} from './types';

export type { ValidationResult, ValidationError, ValidationOptions };
export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableForeignKey,
  TableSchema,
} from './schema-validator';

// Re-export schema validation
export { validateWithSchema } from './schema-validator';
export { ValidationSeverity } from './types';

function decodeWasmPayload<T>(payload: unknown): T {
  if (typeof payload === 'string') {
    return JSON.parse(payload) as T;
  }
  return payload as T;
}

/**
 * Dialect type for validation (matches the main Dialect enum)
 */
export type ValidateDialect =
  | 'generic'
  | 'postgresql'
  | 'mysql'
  | 'bigquery'
  | 'snowflake'
  | 'duckdb'
  | 'sqlite'
  | 'hive'
  | 'spark'
  | 'trino'
  | 'presto'
  | 'redshift'
  | 'tsql'
  | 'oracle'
  | 'clickhouse'
  | 'databricks';

/**
 * Validate SQL syntax and optionally semantics.
 *
 * @param sql - The SQL string to validate
 * @param dialect - The dialect to use for validation
 * @param options - Validation options
 * @returns Validation result with errors and warnings
 *
 * @example
 * ```typescript
 * // Basic syntax validation
 * const result = validate('SELECT * FROM users', 'postgresql');
 * if (!result.valid) {
 *   console.log('Errors:', result.errors);
 * }
 *
 * // With semantic validation
 * const result = validate('SELECT * FROM users', 'postgresql', { semantic: true });
 * // This may also report warnings like "W001: SELECT * is discouraged"
 * ```
 */
export function validate(
  sql: string,
  dialect: ValidateDialect | string = 'generic',
  options: ValidationOptions = {},
): ValidationResult {
  const wasm = getWasmSync();
  // Step 1: Syntax validation via WASM
  const resultJson = options.strictSyntax
    ? wasm.validate_with_options(
        sql,
        dialect,
        JSON.stringify({ strictSyntax: true }),
      )
    : wasm.validate(sql, dialect);
  const syntaxResult: ValidationResult = JSON.parse(resultJson);

  // If syntax validation failed or semantic validation not requested, return
  if (!syntaxResult.valid || !options.semantic) {
    return syntaxResult;
  }

  // Step 2: Parse the SQL to get AST for semantic validation
  const parseResult =
    typeof wasm.parse_value === 'function'
      ? decodeWasmPayload<{ success: boolean; ast?: Expression[] }>(
          wasm.parse_value(sql, dialect),
        )
      : decodeWasmPayload<{ success: boolean; ast?: string }>(
          wasm.parse(sql, dialect),
        );

  if (!parseResult.success || !parseResult.ast) {
    // Parse should succeed if syntax validation passed, but handle edge cases
    return syntaxResult;
  }

  const ast: Expression[] =
    typeof parseResult.ast === 'string'
      ? JSON.parse(parseResult.ast)
      : parseResult.ast;

  // Step 3: Run semantic validation on each statement
  const allErrors: ValidationError[] = [...syntaxResult.errors];

  for (const stmt of ast) {
    const semanticErrors = validateSemantics(stmt, options);
    allErrors.push(...semanticErrors);
  }

  // Determine if still valid (only errors affect validity, not warnings)
  const hasErrors = allErrors.some((e) => e.severity === 'error');

  return {
    valid: !hasErrors,
    errors: allErrors,
  };
}
