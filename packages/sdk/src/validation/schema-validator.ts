/**
 * Schema-Aware SQL Validation
 *
 * Validates SQL queries against a database schema definition.
 */

import { parse as wasmParse } from '../../wasm/polyglot_sql_wasm.js';
import { getExprData, getExprType } from '../ast/helpers';
import { getColumns, walk } from '../ast/visitor/walker';
import type { Expression } from '../generated/Expression';
import { validate } from './index';
import type { Schema, SchemaValidationOptions, TableSchema } from './schema';
import type { ValidationError, ValidationResult } from './types';

export type {
  ColumnSchema,
  Schema,
  SchemaValidationOptions,
  TableSchema,
} from './schema';

/**
 * Validate SQL against a database schema.
 *
 * @param sql - The SQL string to validate
 * @param schema - The database schema definition
 * @param dialect - The dialect to use for parsing
 * @param options - Validation options
 * @returns Validation result with schema-related errors
 *
 * @example
 * ```typescript
 * const schema: Schema = {
 *   tables: [{
 *     name: 'users',
 *     columns: [
 *       { name: 'id', type: 'integer', primaryKey: true },
 *       { name: 'name', type: 'varchar' },
 *       { name: 'email', type: 'varchar' },
 *     ],
 *   }],
 * };
 *
 * const result = validateWithSchema(
 *   'SELECT id, name, unknown_col FROM users',
 *   schema,
 *   'postgresql'
 * );
 * // result.errors will contain an error for unknown_col
 * ```
 */
export function validateWithSchema(
  sql: string,
  schema: Schema,
  dialect: string = 'generic',
  options: SchemaValidationOptions = {},
): ValidationResult {
  const { strict = schema.strict ?? true, semantic = false } = options;

  // First, run basic syntax/semantic validation
  const syntaxResult = validate(sql, dialect, { semantic });
  if (!syntaxResult.valid) {
    return syntaxResult;
  }

  // Parse the SQL to get AST
  const parseResultJson = wasmParse(sql, dialect);
  const parseResult = JSON.parse(parseResultJson);

  if (!parseResult.success || !parseResult.ast) {
    return syntaxResult;
  }

  let ast: Expression[];
  try {
    ast = JSON.parse(parseResult.ast);
  } catch {
    return syntaxResult;
  }

  // Build schema lookup maps
  const schemaMap = buildSchemaMap(schema);
  const allErrors: ValidationError[] = [...syntaxResult.errors];

  // Validate each statement
  for (const stmt of ast) {
    const schemaErrors = validateStatement(stmt, schemaMap, strict);
    allErrors.push(...schemaErrors);
  }

  // Determine validity
  const hasErrors = allErrors.some((e) => e.severity === 'error');

  return {
    valid: !hasErrors,
    errors: allErrors,
  };
}

/**
 * Build lookup maps from schema
 */
function buildSchemaMap(schema: Schema): Map<string, TableSchema> {
  const map = new Map<string, TableSchema>();

  for (const table of schema.tables) {
    // Add by full name (schema.table)
    const fullName = table.schema
      ? `${table.schema.toLowerCase()}.${table.name.toLowerCase()}`
      : table.name.toLowerCase();
    map.set(fullName, table);

    // Also add by simple name
    map.set(table.name.toLowerCase(), table);

    // Add aliases
    if (table.aliases) {
      for (const alias of table.aliases) {
        map.set(alias.toLowerCase(), table);
      }
    }
  }

  return map;
}

/**
 * Validate a single statement against the schema
 */
function validateStatement(
  stmt: Expression,
  schemaMap: Map<string, TableSchema>,
  strict: boolean,
): ValidationError[] {
  const errors: ValidationError[] = [];
  const severity = strict ? 'error' : 'warning';

  // Collect table references and build alias map
  const tableAliases = new Map<string, string>(); // alias -> table name
  const referencedTables = new Set<string>();

  // Find all table references (including aliases from FROM and JOIN clauses)
  collectTableReferences(
    stmt,
    schemaMap,
    tableAliases,
    referencedTables,
    errors,
    severity,
  );

  // Validate column references
  const columns = getColumns(stmt);
  for (const column of columns) {
    const colData = getExprData(column) as {
      name?: { name: string };
      table?: { name: string };
    };

    const colName = colData.name?.name?.toLowerCase();
    if (!colName) continue;

    let tableName = colData.table?.name?.toLowerCase();

    // Resolve alias to actual table name
    if (tableName && tableAliases.has(tableName)) {
      tableName = tableAliases.get(tableName)!.toLowerCase();
    }

    // If table is specified, validate column exists in that table
    if (tableName && schemaMap.has(tableName)) {
      const tableSchema = schemaMap.get(tableName)!;
      const columnExists = tableSchema.columns.some(
        (c) => c.name.toLowerCase() === colName,
      );

      if (!columnExists) {
        errors.push({
          message: `Unknown column '${colName}' in table '${tableName}'`,
          severity,
          code: 'E201',
        });
      }
    } else if (!tableName && referencedTables.size === 1) {
      // Unqualified column with single table reference
      const singleTable = [...referencedTables][0];
      if (schemaMap.has(singleTable)) {
        const tableSchema = schemaMap.get(singleTable)!;
        const columnExists = tableSchema.columns.some(
          (c) => c.name.toLowerCase() === colName,
        );

        if (!columnExists) {
          errors.push({
            message: `Unknown column '${colName}' in table '${singleTable}'`,
            severity,
            code: 'E201',
          });
        }
      }
    } else if (!tableName && referencedTables.size > 1) {
      // Unqualified column with multiple tables - check if it exists in any
      let found = false;
      for (const tblName of referencedTables) {
        if (schemaMap.has(tblName)) {
          const tableSchema = schemaMap.get(tblName)!;
          if (
            tableSchema.columns.some((c) => c.name.toLowerCase() === colName)
          ) {
            found = true;
            break;
          }
        }
      }

      if (!found) {
        errors.push({
          message: `Unknown column '${colName}' (not found in any referenced table)`,
          severity,
          code: 'E201',
        });
      }
    } else if (
      !tableName &&
      referencedTables.size === 0 &&
      schemaMap.size > 0
    ) {
      // No FROM clause - check column against all tables in the schema
      let found = false;
      for (const [, tableSchema] of schemaMap) {
        if (tableSchema.columns.some((c) => c.name.toLowerCase() === colName)) {
          found = true;
          break;
        }
      }

      if (!found) {
        errors.push({
          message: `Unknown column '${colName}'`,
          severity,
          code: 'E201',
        });
      }
    }
  }

  return errors;
}

/**
 * Helper to get the variant key from an externally tagged Expression node
 */
function getNodeType(node: unknown): string | null {
  if (typeof node !== 'object' || node === null) return null;
  const keys = Object.keys(node);
  if (keys.length === 1) return keys[0];
  return null;
}

/**
 * Helper to get the inner data from an externally tagged Expression node
 */
function getNodeData(node: unknown): Record<string, unknown> | null {
  if (typeof node !== 'object' || node === null) return null;
  const keys = Object.keys(node);
  if (keys.length === 1) {
    const val = (node as Record<string, unknown>)[keys[0]];
    if (typeof val === 'object' && val !== null)
      return val as Record<string, unknown>;
  }
  return null;
}

/**
 * Extract table references from FROM clause
 */
function extractTablesFromFrom(fromClause: unknown): Array<{
  name: string;
  alias?: string;
}> {
  const tables: Array<{ name: string; alias?: string }> = [];

  if (!fromClause || typeof fromClause !== 'object') {
    return tables;
  }

  const from = fromClause as { expressions?: unknown[] };
  if (!Array.isArray(from.expressions)) {
    return tables;
  }

  for (const expr of from.expressions) {
    if (typeof expr !== 'object' || expr === null) continue;

    const nodeType = getNodeType(expr);
    const nodeData = getNodeData(expr);

    // Handle table reference directly
    if (nodeType === 'table' && nodeData) {
      const name = (nodeData.name as { name?: string })?.name;
      const alias = (nodeData.alias as { name?: string })?.name;
      if (name) {
        tables.push({ name, alias });
      }
    }
    // Handle aliased table (alias wrapping table)
    else if (nodeType === 'alias' && nodeData) {
      const thisExpr = nodeData.this;
      const innerType = getNodeType(thisExpr);
      const innerData = getNodeData(thisExpr);
      if (innerType === 'table' && innerData) {
        const name = (innerData.name as { name?: string })?.name;
        const alias = (nodeData.alias as { name?: string })?.name;
        if (name) {
          tables.push({ name, alias });
        }
      }
    }
  }

  return tables;
}

/**
 * Extract tables from JOIN clauses
 */
function extractTablesFromJoins(joins: unknown): Array<{
  name: string;
  alias?: string;
}> {
  const tables: Array<{ name: string; alias?: string }> = [];

  if (!Array.isArray(joins)) {
    return tables;
  }

  for (const join of joins) {
    if (typeof join !== 'object' || join === null) continue;

    const joinNode = join as Record<string, unknown>;
    const right = joinNode.this; // In externally tagged format, join's table is in 'this'

    const rightType = getNodeType(right);
    const rightData = getNodeData(right);

    if (rightType === 'table' && rightData) {
      const name = (rightData.name as { name?: string })?.name;
      const alias = (rightData.alias as { name?: string })?.name;
      if (name) {
        tables.push({ name, alias });
      }
    } else if (rightType === 'alias' && rightData) {
      const thisExpr = rightData.this;
      const innerType = getNodeType(thisExpr);
      const innerData = getNodeData(thisExpr);
      if (innerType === 'table' && innerData) {
        const name = (innerData.name as { name?: string })?.name;
        const alias = (rightData.alias as { name?: string })?.name;
        if (name) {
          tables.push({ name, alias });
        }
      }
    }
  }

  return tables;
}

/**
 * Collect table references from the AST
 */
function collectTableReferences(
  stmt: Expression,
  schemaMap: Map<string, TableSchema>,
  tableAliases: Map<string, string>,
  referencedTables: Set<string>,
  errors: ValidationError[],
  severity: 'error' | 'warning',
): void {
  // For SELECT statements, extract from FROM clause directly
  const stmtData = getExprData(stmt) as Record<string, unknown>;

  // Extract from FROM clause
  const fromTables = extractTablesFromFrom(stmtData.from);
  for (const table of fromTables) {
    const tableName = table.name.toLowerCase();
    referencedTables.add(tableName);

    if (table.alias) {
      tableAliases.set(table.alias.toLowerCase(), tableName);
    }

    if (!schemaMap.has(tableName)) {
      errors.push({
        message: `Unknown table '${table.name}'`,
        severity,
        code: 'E200',
      });
    }
  }

  // Extract from JOINs
  const joinTables = extractTablesFromJoins(stmtData.joins);
  for (const table of joinTables) {
    const tableName = table.name.toLowerCase();
    referencedTables.add(tableName);

    if (table.alias) {
      tableAliases.set(table.alias.toLowerCase(), tableName);
    }

    if (!schemaMap.has(tableName)) {
      errors.push({
        message: `Unknown table '${table.name}'`,
        severity,
        code: 'E200',
      });
    }
  }

  // Also use walker to find table references elsewhere (e.g., in subqueries)
  walk(stmt, {
    enter: (node) => {
      if (getExprType(node) === 'table') {
        const tblData = getExprData(node) as {
          name?: { name: string };
          alias?: { name: string };
        };

        const tableName = tblData.name?.name?.toLowerCase();
        if (!tableName) return;

        // Only add if not already found
        if (!referencedTables.has(tableName)) {
          referencedTables.add(tableName);

          if (tblData.alias?.name) {
            tableAliases.set(tblData.alias.name.toLowerCase(), tableName);
          }

          if (!schemaMap.has(tableName)) {
            errors.push({
              message: `Unknown table '${tableName}'`,
              severity,
              code: 'E200',
            });
          }
        }
      }
    },
  });
}
