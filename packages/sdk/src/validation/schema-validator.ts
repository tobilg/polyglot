/**
 * Schema-Aware SQL Validation
 *
 * Delegates schema-aware validation to the Rust core via WASM bindings.
 */

import { validate_with_schema as wasmValidateWithSchema } from '../../wasm/polyglot_sql_wasm.js';
import type { Schema, SchemaValidationOptions } from './schema';
import type { ValidationError, ValidationResult } from './types';

export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableForeignKey,
  TableSchema,
} from './schema';

type WasmSchemaValidationOptions = {
  check_types: boolean;
  check_references: boolean;
  strict: boolean;
  semantic: boolean;
  strict_syntax: boolean;
};

function parseValidationResult(json: string): ValidationResult {
  try {
    return JSON.parse(json) as ValidationResult;
  } catch {
    return {
      valid: false,
      errors: [
        {
          message: 'Failed to parse validation response',
          severity: 'error',
          code: 'E000',
        } satisfies ValidationError,
      ],
    };
  }
}

/**
 * Validate SQL against a database schema.
 */
export function validateWithSchema(
  sql: string,
  schema: Schema,
  dialect: string = 'generic',
  options: SchemaValidationOptions = {},
): ValidationResult {
  const strict = options.strict ?? schema.strict ?? true;
  const wasmOptions: WasmSchemaValidationOptions = {
    check_types: options.checkTypes ?? false,
    check_references: options.checkReferences ?? false,
    strict,
    semantic: options.semantic ?? false,
    strict_syntax: options.strictSyntax ?? false,
  };

  const resultJson = wasmValidateWithSchema(
    sql,
    JSON.stringify(schema),
    dialect,
    JSON.stringify(wasmOptions),
  );

  return parseValidationResult(resultJson);
}
