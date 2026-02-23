/**
 * Validation Tests
 */

import { describe, expect, it } from 'vitest';
import { validate, validateWithSchema } from './index';
import type { Schema } from './schema';

describe('validate', () => {
  describe('syntax validation', () => {
    it('should return valid for correct SQL', () => {
      const result = validate('SELECT 1', 'generic');
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return valid for complex SQL', () => {
      const result = validate(
        'SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10',
        'postgresql',
      );
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect syntax errors', () => {
      const result = validate('SELECT 1 +', 'generic');
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
      expect(result.errors[0].severity).toBe('error');
    });

    it('should detect unbalanced expressions', () => {
      const result = validate('SELECT * FROM users ORDER BY ,', 'generic');
      expect(result.valid).toBe(false);
    });

    it('should detect unbalanced parentheses', () => {
      const result = validate('SELECT (1 + 2', 'generic');
      expect(result.valid).toBe(false);
    });
  });

  describe('semantic validation', () => {
    it('should warn about SELECT * when semantic validation is enabled', () => {
      const result = validate('SELECT * FROM users', 'generic', {
        semantic: true,
      });
      const warning = result.errors.find((e) => e.code === 'W001');
      expect(warning).toBeDefined();
      expect(warning?.severity).toBe('warning');
      expect(warning?.message).toContain('SELECT *');
    });

    it('should not warn about SELECT * when semantic validation is disabled', () => {
      const result = validate('SELECT * FROM users', 'generic', {
        semantic: false,
      });
      expect(result.errors.filter((e) => e.code === 'W001')).toHaveLength(0);
    });

    it('should warn about LIMIT without ORDER BY', () => {
      const result = validate('SELECT id FROM users LIMIT 10', 'generic', {
        semantic: true,
      });
      const warning = result.errors.find((e) => e.code === 'W004');
      expect(warning).toBeDefined();
      expect(warning?.message).toContain('LIMIT without ORDER BY');
    });

    it('should not warn when LIMIT has ORDER BY', () => {
      const result = validate(
        'SELECT id FROM users ORDER BY id LIMIT 10',
        'generic',
        {
          semantic: true,
        },
      );
      const warning = result.errors.find((e) => e.code === 'W004');
      expect(warning).toBeUndefined();
    });

    it('should still be valid with only warnings', () => {
      const result = validate('SELECT * FROM users', 'generic', {
        semantic: true,
      });
      expect(result.valid).toBe(true); // Warnings don't make it invalid
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  describe('dialect validation', () => {
    it('should validate PostgreSQL syntax', () => {
      const result = validate(
        "SELECT * FROM users WHERE name ILIKE '%john%'",
        'postgresql',
      );
      expect(result.valid).toBe(true);
    });

    it('should validate MySQL syntax', () => {
      const result = validate('SELECT IFNULL(a, b) FROM t', 'mysql');
      expect(result.valid).toBe(true);
    });

    it('should validate BigQuery syntax', () => {
      // Test basic BigQuery query (SAFE_CAST is transpiled via TRY_CAST internally)
      const result = validate('SELECT COALESCE(a, b) FROM t', 'bigquery');
      expect(result.valid).toBe(true);
    });
  });
});

describe('validateWithSchema', () => {
  const schema: Schema = {
    tables: [
      {
        name: 'users',
        columns: [
          { name: 'id', type: 'integer', primaryKey: true },
          { name: 'name', type: 'varchar' },
          { name: 'email', type: 'varchar' },
          { name: 'age', type: 'integer' },
        ],
      },
      {
        name: 'orders',
        columns: [
          { name: 'id', type: 'integer', primaryKey: true },
          { name: 'user_id', type: 'integer' },
          { name: 'total', type: 'decimal' },
        ],
      },
    ],
  };

  describe('table validation', () => {
    it('should validate known tables', () => {
      const result = validateWithSchema('SELECT id FROM users', schema);
      expect(result.valid).toBe(true);
    });

    it('should detect unknown tables', () => {
      const result = validateWithSchema('SELECT * FROM nonexistent', schema);
      expect(result.valid).toBe(false);
      const error = result.errors.find((e) => e.code === 'E200');
      expect(error).toBeDefined();
      expect(error?.message).toContain('nonexistent');
    });
  });

  describe('column validation', () => {
    it('should validate known columns', () => {
      const result = validateWithSchema(
        'SELECT id, name, email FROM users',
        schema,
      );
      expect(result.valid).toBe(true);
    });

    it('should detect unknown columns', () => {
      const result = validateWithSchema(
        'SELECT unknown_col FROM users',
        schema,
      );
      expect(result.valid).toBe(false);
      const error = result.errors.find((e) => e.code === 'E201');
      expect(error).toBeDefined();
      expect(error?.message).toContain('unknown_col');
    });

    it('should validate qualified column references', () => {
      const result = validateWithSchema(
        'SELECT users.id, users.name FROM users',
        schema,
      );
      expect(result.valid).toBe(true);
    });
  });

  describe('multiple tables', () => {
    it('should validate joins with multiple tables', () => {
      const result = validateWithSchema(
        'SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id',
        schema,
      );
      expect(result.valid).toBe(true);
    });

    it('should detect unknown columns in joins', () => {
      const result = validateWithSchema(
        'SELECT users.id, orders.unknown FROM users JOIN orders ON users.id = orders.user_id',
        schema,
      );
      expect(result.valid).toBe(false);
    });
  });

  describe('column validation without FROM clause', () => {
    it('should detect unknown columns when no FROM clause is present', () => {
      const result = validateWithSchema('SELECT scooby', schema);
      expect(result.valid).toBe(false);
      const error = result.errors.find((e) => e.code === 'E201');
      expect(error).toBeDefined();
      expect(error?.message).toContain('scooby');
    });

    it('should pass known columns even without FROM clause', () => {
      const result = validateWithSchema('SELECT id', schema);
      expect(result.valid).toBe(true);
    });

    it('should detect unknown columns with dialect specified', () => {
      const clickhouseSchema: Schema = {
        tables: [
          {
            name: 'demo_daily_orders',
            columns: [
              { name: 'date', type: 'Date' },
              { name: 'category', type: 'String' },
              { name: 'transactions', type: 'Int64' },
            ],
          },
        ],
      };
      const result = validateWithSchema(
        'SELECT scooby',
        clickhouseSchema,
        'clickhouse',
      );
      expect(result.valid).toBe(false);
      const error = result.errors.find((e) => e.code === 'E201');
      expect(error).toBeDefined();
      expect(error?.message).toContain('scooby');
    });
  });

  describe('strict mode', () => {
    it('should report errors in strict mode (default)', () => {
      const result = validateWithSchema('SELECT unknown FROM users', schema);
      expect(result.valid).toBe(false);
      expect(result.errors[0].severity).toBe('error');
    });

    it('should report warnings in non-strict mode', () => {
      const result = validateWithSchema(
        'SELECT unknown FROM users',
        schema,
        'generic',
        {
          strict: false,
        },
      );
      expect(result.valid).toBe(true); // Warnings don't affect validity
      expect(result.errors[0].severity).toBe('warning');
    });
  });

  describe('with semantic validation', () => {
    it('should combine schema and semantic validation', () => {
      const result = validateWithSchema(
        'SELECT * FROM users LIMIT 10',
        schema,
        'generic',
        {
          semantic: true,
        },
      );
      // Should have both W001 (SELECT *) and W004 (LIMIT without ORDER BY)
      const selectStarWarning = result.errors.find((e) => e.code === 'W001');
      const limitWarning = result.errors.find((e) => e.code === 'W004');
      expect(selectStarWarning).toBeDefined();
      expect(limitWarning).toBeDefined();
    });
  });

  describe('reference validation', () => {
    it('should validate foreign key metadata when checkReferences is enabled', () => {
      const schemaWithInvalidReference: Schema = {
        tables: [
          {
            name: 'users',
            columns: [{ name: 'id', type: 'integer', primaryKey: true }],
            primaryKey: ['id'],
          },
          {
            name: 'orders',
            columns: [
              {
                name: 'user_id',
                type: 'integer',
                references: { table: 'missing_users', column: 'id' },
              },
            ],
          },
        ],
      };

      const result = validateWithSchema(
        'SELECT 1',
        schemaWithInvalidReference,
        'generic',
        { checkReferences: true },
      );
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.code === 'E220')).toBe(true);
    });

    it('should return warning in non-strict mode for invalid references', () => {
      const schemaWithInvalidReference: Schema = {
        tables: [
          {
            name: 'orders',
            columns: [
              {
                name: 'user_id',
                type: 'integer',
                references: { table: 'missing_users', column: 'id' },
              },
            ],
          },
        ],
      };

      const result = validateWithSchema(
        'SELECT 1',
        schemaWithInvalidReference,
        'generic',
        { checkReferences: true, strict: false },
      );
      expect(result.valid).toBe(true);
      expect(result.errors.some((e) => e.code === 'W222')).toBe(true);
    });

    it('should detect ambiguous unqualified columns across joined tables', () => {
      const result = validateWithSchema(
        'SELECT id FROM users JOIN orders ON users.id = orders.user_id',
        schema,
        'generic',
        { checkReferences: true },
      );
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.code === 'E221')).toBe(true);
    });

    it('should warn on cartesian joins when reference checks are enabled', () => {
      const result = validateWithSchema(
        'SELECT users.id FROM users CROSS JOIN orders',
        schema,
        'generic',
        { checkReferences: true },
      );
      expect(result.valid).toBe(true);
      expect(result.errors.some((e) => e.code === 'W220')).toBe(true);
    });
  });
});
