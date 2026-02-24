import { describe, expect, it } from 'vitest';
import {
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

describe('Polyglot SDK', () => {
  describe('init', () => {
    it('should be initialized (synchronous init on import)', () => {
      expect(isInitialized()).toBe(true);
    });

    it('init() should be a no-op that resolves', async () => {
      await expect(init()).resolves.toBeUndefined();
    });
  });

  describe('getVersion', () => {
    it('should return a version string', () => {
      const version = getVersion();
      expect(typeof version).toBe('string');
      expect(version).toMatch(/^\d+\.\d+\.\d+$/);
    });
  });

  describe('getDialects', () => {
    it('should return an array of dialect names', () => {
      const dialects = getDialects();
      expect(Array.isArray(dialects)).toBe(true);
      expect(dialects.length).toBeGreaterThan(0);
    });

    it('should include common dialects', () => {
      const dialects = getDialects();
      expect(dialects).toContain('generic');
      expect(dialects).toContain('postgresql');
      expect(dialects).toContain('mysql');
    });
  });

  describe('parse', () => {
    it('should parse a simple SELECT statement', () => {
      const result = parse('SELECT 1', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('should parse SELECT with columns and table', () => {
      const result = parse('SELECT a, b FROM users', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('should parse SELECT with WHERE clause', () => {
      const result = parse('SELECT * FROM users WHERE id = 1', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('should handle malformed SQL gracefully', () => {
      const result = parse('SELECT FROM WHERE', Dialect.Generic);
      // The parser may handle some invalid SQL gracefully
      expect(typeof result.success).toBe('boolean');
    });

    it('should use Generic dialect by default', () => {
      const result = parse('SELECT 1');
      expect(result.success).toBe(true);
    });
  });

  describe('generate', () => {
    it('should generate SQL from a parsed AST', () => {
      const parseResult = parse('SELECT a FROM t', Dialect.Generic);
      expect(parseResult.success).toBe(true);

      const generateResult = generate(parseResult.ast, Dialect.Generic);
      expect(generateResult.success).toBe(true);
      expect(generateResult.sql).toBeDefined();
      expect(generateResult.sql!.length).toBeGreaterThan(0);
    });

    it('should roundtrip simple queries', () => {
      const original = 'SELECT a, b FROM users';
      const parseResult = parse(original, Dialect.Generic);
      const generateResult = generate(parseResult.ast, Dialect.Generic);

      expect(generateResult.success).toBe(true);
      expect(generateResult.sql![0].toLowerCase()).toContain('select');
      expect(generateResult.sql![0].toLowerCase()).toContain('from');
    });
  });

  describe('transpile', () => {
    it('should transpile SQL from one dialect to another', () => {
      const result = transpile('SELECT 1', Dialect.Generic, Dialect.PostgreSQL);
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
    });

    it('should transpile same dialect without changes', () => {
      const result = transpile(
        'SELECT a FROM t',
        Dialect.Generic,
        Dialect.Generic,
      );
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
    });

    it('should handle multiple statements', () => {
      const result = transpile(
        'SELECT 1; SELECT 2',
        Dialect.Generic,
        Dialect.Generic,
      );
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
      expect(result.sql!.length).toBe(2);
    });

    it('should transform IFNULL to COALESCE for PostgreSQL', () => {
      const result = transpile(
        'SELECT IFNULL(a, b)',
        Dialect.MySQL,
        Dialect.PostgreSQL,
      );
      expect(result.success).toBe(true);
      expect(result.sql![0]).toContain('COALESCE');
    });

    it('should transform NVL to IFNULL for MySQL', () => {
      const result = transpile(
        'SELECT NVL(a, b)',
        Dialect.Generic,
        Dialect.MySQL,
      );
      expect(result.success).toBe(true);
      expect(result.sql![0]).toContain('IFNULL');
    });
  });

  describe('format', () => {
    it('should format SQL', () => {
      const result = format('SELECT a,b,c FROM t', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
    });

    it('should use Generic dialect by default', () => {
      const result = format('SELECT 1');
      expect(result.success).toBe(true);
    });
  });

  describe('Dialect enum', () => {
    it('should have common dialect values', () => {
      expect(Dialect.Generic).toBe('generic');
      expect(Dialect.PostgreSQL).toBe('postgresql');
      expect(Dialect.MySQL).toBe('mysql');
      expect(Dialect.BigQuery).toBe('bigquery');
      expect(Dialect.Snowflake).toBe('snowflake');
      expect(Dialect.DuckDB).toBe('duckdb');
    });
  });

  describe('Polyglot class', () => {
    it('should create a singleton instance', () => {
      const instance1 = Polyglot.getInstance();
      const instance2 = Polyglot.getInstance();
      expect(instance1).toBe(instance2);
    });

    it('should transpile SQL', () => {
      const polyglot = Polyglot.getInstance();
      const result = polyglot.transpile(
        'SELECT 1',
        Dialect.Generic,
        Dialect.PostgreSQL,
      );
      expect(result.success).toBe(true);
    });

    it('should parse SQL', () => {
      const polyglot = Polyglot.getInstance();
      const result = polyglot.parse('SELECT a FROM t', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('should generate SQL', () => {
      const polyglot = Polyglot.getInstance();
      const parseResult = polyglot.parse('SELECT 1', Dialect.Generic);
      const generateResult = polyglot.generate(
        parseResult.ast,
        Dialect.Generic,
      );
      expect(generateResult.success).toBe(true);
    });

    it('should format SQL', () => {
      const polyglot = Polyglot.getInstance();
      const result = polyglot.format('SELECT a,b FROM t', Dialect.Generic);
      expect(result.success).toBe(true);
    });

    it('should get dialects', () => {
      const polyglot = Polyglot.getInstance();
      const dialects = polyglot.getDialects();
      expect(Array.isArray(dialects)).toBe(true);
      expect(dialects.length).toBeGreaterThan(0);
    });

    it('should get version', () => {
      const polyglot = Polyglot.getInstance();
      const version = polyglot.getVersion();
      expect(typeof version).toBe('string');
    });
  });
});

describe('Edge cases', () => {
  const buildLargeWhereSql = (conditions: number): string => {
    const base = 'SELECT id, name, email, created_at FROM users WHERE ';
    const parts = Array.from(
      { length: conditions },
      (_, i) => `field_${i} = 'value_${i}'`,
    );
    return base + parts.join(' AND ');
  };

  it('should handle empty SQL string', () => {
    const result = parse('', Dialect.Generic);
    // Empty string should either succeed with empty AST or fail gracefully
    expect(typeof result.success).toBe('boolean');
  });

  it('should handle SQL with special characters', () => {
    const result = parse("SELECT 'hello''world'", Dialect.Generic);
    expect(result.success).toBe(true);
  });

  it('should handle SQL with unicode', () => {
    const result = parse("SELECT 'héllo wörld'", Dialect.Generic);
    expect(result.success).toBe(true);
  });

  it('should handle complex nested queries', () => {
    const sql =
      'SELECT * FROM (SELECT a FROM t WHERE a > 1) AS sub WHERE sub.a < 10';
    const result = parse(sql, Dialect.Generic);
    expect(result.success).toBe(true);
  });

  describe('error positions', () => {
    it('should include errorLine and errorColumn on parse errors', () => {
      const result = parse('SELECT 1 + 2)', Dialect.Generic);
      expect(result.success).toBe(false);
      expect(result.errorLine).toBeDefined();
      expect(result.errorColumn).toBeDefined();
      expect(result.errorLine).toBe(1);
      expect(typeof result.errorColumn).toBe('number');
    });

    it('should include errorLine and errorColumn on transpile errors', () => {
      const result = transpile(
        'SELECT 1 + 2)',
        Dialect.Generic,
        Dialect.PostgreSQL,
      );
      expect(result.success).toBe(false);
      expect(result.errorLine).toBeDefined();
      expect(result.errorColumn).toBeDefined();
    });

    it('should not include error positions on success', () => {
      const result = parse('SELECT 1', Dialect.Generic);
      expect(result.success).toBe(true);
      expect(result.errorLine).toBeUndefined();
      expect(result.errorColumn).toBeUndefined();
    });

    it('should not include error positions on successful transpile', () => {
      const result = transpile('SELECT 1', Dialect.Generic, Dialect.PostgreSQL);
      expect(result.success).toBe(true);
      expect(result.errorLine).toBeUndefined();
      expect(result.errorColumn).toBeUndefined();
    });
  });

  describe('WASM trap safeguards', () => {
    it('should not throw from format on very large SQL inputs', () => {
      const sql = buildLargeWhereSql(5000);
      let result: ReturnType<typeof format> | undefined;

      expect(() => {
        result = format(sql, Dialect.PostgreSQL);
      }).not.toThrow();

      expect(result).toBeDefined();
      expect(typeof result!.success).toBe('boolean');
      if (!result!.success) {
        expect(result!.error).toBeDefined();
      }
    });

    it('should not throw from transpile on very large SQL inputs', () => {
      const sql = buildLargeWhereSql(5000);
      let result: ReturnType<typeof transpile> | undefined;

      expect(() => {
        result = transpile(sql, Dialect.PostgreSQL, Dialect.PostgreSQL);
      }).not.toThrow();

      expect(result).toBeDefined();
      expect(typeof result!.success).toBe('boolean');
      if (!result!.success) {
        expect(result!.error).toBeDefined();
      }
    });

    it('should return structured failure from generate when AST is not serializable', () => {
      const circular: Record<string, unknown> = {};
      circular.self = circular;

      const result = generate(circular, Dialect.Generic);
      expect(result.success).toBe(false);
      expect(result.error).toContain('WASM generate failed');
    });
  });
});
