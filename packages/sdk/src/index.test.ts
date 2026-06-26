import { describe, expect, it } from 'vitest';
import {
  analyzeQuery,
  Dialect,
  format,
  formatWithOptions,
  generate,
  generateDataType,
  getDialects,
  getSourceTables,
  getVersion,
  init,
  isInitialized,
  lineage,
  lineageWithSchema,
  openLineageColumnLineage,
  openLineageJobEvent,
  openLineageRunEvent,
  Polyglot,
  parse,
  parseDataType,
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

    it('should parse PostgreSQL PREPARE and EXECUTE statements', () => {
      const prepare = parse(
        'PREPARE leak (int) AS SELECT id FROM sensitive_table WHERE id = $1',
        Dialect.PostgreSQL,
      );
      expect(prepare.success).toBe(true);
      expect((prepare.ast![0] as any).prepare.name.name).toBe('leak');
      expect((prepare.ast![0] as any).prepare.statement.select).toBeDefined();

      const execute = parse('EXECUTE leak(1)', Dialect.PostgreSQL);
      expect(execute.success).toBe(true);
      expect((execute.ast![0] as any).execute.prepared).toBe(true);
      expect((execute.ast![0] as any).execute.arguments).toHaveLength(1);
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

    it('should roundtrip PostgreSQL PREPARE statements', () => {
      const parseResult = parse(
        'PREPARE leak (int) AS SELECT id FROM sensitive_table WHERE id = $1',
        Dialect.PostgreSQL,
      );
      const generateResult = generate(parseResult.ast, Dialect.PostgreSQL);

      expect(generateResult.success).toBe(true);
      expect(generateResult.sql![0]).toContain('PREPARE leak (INT) AS SELECT');
    });
  });

  describe('data types', () => {
    it('should parse a standalone data type', () => {
      const result = parseDataType('DECIMAL(10, 2)', Dialect.DuckDB);

      expect(result.success).toBe(true);
      expect(result.dataType).toEqual({
        data_type: 'decimal',
        precision: 10,
        scale: 2,
      });
    });

    it('should generate a standalone data type for a target dialect', () => {
      const parsed = parseDataType('VARCHAR(255)', Dialect.DuckDB);
      expect(parsed.success).toBe(true);

      const result = generateDataType(parsed.dataType!, Dialect.PostgreSQL);

      expect(result.success).toBe(true);
      expect(result.sql).toBe('VARCHAR(255)');
    });

    it('should reject trailing SQL after a data type', () => {
      const result = parseDataType('DECIMAL(10, 2) SELECT 1', Dialect.DuckDB);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Unexpected token after data type');
    });

    it('should expose data type helpers on the Polyglot instance', () => {
      const polyglot = Polyglot.getInstance();
      const parsed = polyglot.parseDataType('INT[]', Dialect.DuckDB);

      expect(parsed.success).toBe(true);
      expect(parsed.dataType?.data_type).toBe('array');
      expect(polyglot.generateDataType(parsed.dataType!, Dialect.DuckDB).sql).toBe(
        'INT[]',
      );
    });
  });

  describe('lineage helpers', () => {
    const collectNames = (node: { name?: string; downstream?: unknown[] }): string[] => [
      node.name ?? '',
      ...(node.downstream ?? []).flatMap((child) =>
        collectNames(child as { name?: string; downstream?: unknown[] }),
      ),
    ];

    it('should trace schema-less CTE star passthrough to the base table column', () => {
      const result = lineage(
        's',
        'WITH c AS (SELECT * FROM t) SELECT SUM(c.x) AS s FROM c GROUP BY 1',
        Dialect.Generic,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toContain('t.x');
    });

    it('should mark BigQuery UNNEST aliases as virtual lineage sources', () => {
      const result = lineage(
        'week_start',
        "SELECT date_val AS week_start FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-01-31')) AS date_val",
        Dialect.BigQuery,
      );

      expect(result.success).toBe(true);
      expect(result.lineage?.downstream[0]).toMatchObject({
        name: '_0.date_val',
        source_name: '_0',
        source_kind: 'virtual',
        source_alias: 'date_val',
      });
    });

    it('should trace pivot output columns to aggregation inputs', () => {
      const result = lineage(
        'q1',
        "SELECT * FROM (SELECT region, q, amt FROM sales) PIVOT(SUM(amt) FOR q IN ('Q1' AS q1))",
        Dialect.DuckDB,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toContain('sales.amt');
    });

    it('should trace nested set operations inside derived tables', () => {
      const result = lineage(
        'v',
        'SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) UNION ALL SELECT v FROM t3) u',
        Dialect.DuckDB,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toEqual(
        expect.arrayContaining(['t1.v', 't2.v', 't3.v']),
      );
    });

    it('should tolerate partial schemas in lineageWithSchema', () => {
      const result = lineageWithSchema(
        'amount',
        'SELECT order_id, amount FROM t',
        {
          tables: [
            { name: 't', columns: [{ name: 'amount', type: 'INT' }] },
          ],
        },
        Dialect.DuckDB,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toContain('t.amount');
    });

    it('should trace unpivot value columns to input columns', () => {
      const result = lineage(
        'val',
        'SELECT name, val FROM t UNPIVOT(val FOR col IN (a, b, c))',
        Dialect.DuckDB,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toEqual(
        expect.arrayContaining(['t.a', 't.b', 't.c']),
      );
    });

    it('should collect source tables from prepared statement bodies', () => {
      const result = getSourceTables(
        'id',
        'PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1',
        Dialect.PostgreSQL,
      );

      expect(result.success).toBe(true);
      expect(result.tables).toContain('sensitive_table');
    });
  });

  describe('analyzeQuery', () => {
    it('should return compact projection facts', () => {
      const result = analyzeQuery('SELECT a FROM t');

      expect(result.success).toBe(true);
      expect(result.analysis?.shape).toBe('select');
      expect(result.analysis?.projections[0]).toMatchObject({
        name: 'a',
        transformKind: 'direct',
      });
      expect(result.analysis?.projections[0].upstream[0].column).toBe('a');
    });

    it('should expose full compact analysis facts with schema', () => {
      const schema = {
        tables: [
          {
            name: 'orders',
            columns: [
              { name: 'id', type: 'INT', nullable: false },
              { name: 'amount', type: 'DECIMAL(10,2)', nullable: true },
            ],
          },
        ],
      };

      const result = analyzeQuery(
        'SELECT o.id, SUM(o.amount) AS total_amount FROM orders AS o GROUP BY o.id',
        { dialect: Dialect.Generic, schema },
      );

      expect(result.success).toBe(true);
      expect(result.analysis?.baseTables).toMatchObject([
        {
          name: 'orders',
          alias: 'o',
          kind: 'table',
          catalog: null,
          schema: null,
          table: 'orders',
        },
      ]);
      expect(result.analysis?.projections[0].upstream[0]).toMatchObject({
        table: 'orders',
        sourceAlias: 'o',
        column: 'id',
        confidence: 'resolved',
      });
      expect(result.analysis?.projections[1]).toMatchObject({
        transformKind: 'aggregation',
        typeHint: 'DECIMAL(10, 2)',
      });
      expect(result.analysis?.projections[0].nullability).toBe('non_null');
      expect(result.analysis?.projections[1].nullability).toBe('unknown');
    });

    it('should expose structured physical table identity', () => {
      const result = analyzeQuery(
        'SELECT id FROM "my.catalog"."my.schema"."orders.table" AS o',
        Dialect.DuckDB,
      );

      expect(result.success).toBe(true);
      expect(result.analysis?.baseTables[0]).toMatchObject({
        name: 'my.catalog.my.schema.orders.table',
        alias: 'o',
        kind: 'table',
        catalog: 'my.catalog',
        schema: 'my.schema',
        table: 'orders.table',
      });
    });

    it('should expose CTE facts and star projection provenance', () => {
      const schema = {
        tables: [
          {
            name: 'orders',
            columns: [
              { name: 'id', type: 'INT', nullable: false },
              { name: 'amount', type: 'DECIMAL(10,2)', nullable: true },
            ],
          },
        ],
      };

      const result = analyzeQuery(
        'WITH base AS (SELECT id, amount FROM orders) SELECT * FROM base',
        { dialect: Dialect.Generic, schema },
      );

      expect(result.success).toBe(true);
      expect(result.analysis?.cteFacts[0]).toMatchObject({
        name: 'base',
        bodySql: 'SELECT id, amount FROM orders',
        outputColumns: ['id', 'amount'],
      });
      expect(result.analysis?.starProjections[0]).toMatchObject({
        index: 0,
        expandedColumns: ['id', 'amount'],
      });
    });

    it('should resolve PIVOT alias columns and generated outputs', () => {
      const result = analyzeQuery(
        "SELECT region2, p1 FROM (SELECT region, q, amt FROM sales) PIVOT(SUM(amt) FOR q IN ('Q1')) AS p(region2, p1)",
        { dialect: Dialect.DuckDB },
      );

      expect(result.success).toBe(true);
      const region = result.analysis?.projections.find(
        (projection) => projection.name === 'region2',
      );
      expect(region?.upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 'sales', column: 'region' }),
        ]),
      );
      const pivotValue = result.analysis?.projections.find(
        (projection) => projection.name === 'p1',
      );
      expect(pivotValue?.upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 'sales', column: 'amt' }),
        ]),
      );
    });

    it('should resolve nested set operation derived table with schema', () => {
      const result = analyzeQuery(
        'SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) UNION ALL SELECT v FROM t3) u',
        {
          dialect: Dialect.DuckDB,
          schema: {
            tables: [
              { name: 't1', columns: [{ name: 'v', type: 'INT' }] },
              { name: 't2', columns: [{ name: 'v', type: 'INT' }] },
              { name: 't3', columns: [{ name: 'v', type: 'INT' }] },
            ],
          },
        },
      );

      expect(result.success).toBe(true);
      expect(result.analysis?.projections[0].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 't1', column: 'v' }),
          expect.objectContaining({ table: 't2', column: 'v' }),
          expect.objectContaining({ table: 't3', column: 'v' }),
        ]),
      );
    });

    it('should resolve UNNEST output alias with schema', () => {
      const result = analyzeQuery('SELECT i FROM t, UNNEST(t.arr) AS i', {
        dialect: Dialect.DuckDB,
        schema: {
          tables: [
            { name: 't', columns: [{ name: 'arr', type: 'INT' }] },
          ],
        },
      });

      expect(result.success).toBe(true);
      expect(result.analysis?.projections[0].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 't', column: 'arr' }),
        ]),
      );
    });

    it('should tolerate partial schemas in analyzeQuery', () => {
      const result = analyzeQuery('SELECT order_id, amount FROM t', {
        dialect: Dialect.DuckDB,
        schema: {
          tables: [
            { name: 't', columns: [{ name: 'amount', type: 'INT' }] },
          ],
        },
      });

      expect(result.success).toBe(true);
      expect(result.analysis?.projections.map((projection) => projection.name)).toEqual([
        'order_id',
        'amount',
      ]);
      expect(result.analysis?.projections[0].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            column: 'order_id',
            table: 't',
            confidence: 'resolved',
          }),
        ]),
      );
      expect(result.analysis?.projections[1].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ column: 'amount', table: 't' }),
        ]),
      );
    });

    it('should accept a dialect shorthand argument', () => {
      const result = analyzeQuery('SELECT 1', Dialect.DuckDB);

      expect(result.success).toBe(true);
      expect(result.analysis?.shape).toBe('select');
    });

    it('should expose analyzeQuery on the Polyglot instance', () => {
      const polyglot = Polyglot.getInstance();
      const result = polyglot.analyzeQuery('SELECT a FROM t', {
        dialect: Dialect.Generic,
      });

      expect(result.success).toBe(true);
      expect(result.analysis?.relations[0].name).toBe('t');
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

    it('should return guard error when format limits are exceeded', () => {
      const result = formatWithOptions('SELECT 1', Dialect.Generic, {
        maxInputBytes: 7,
      });
      expect(result.success).toBe(false);
      expect(result.error).toContain('E_GUARD_INPUT_TOO_LARGE');
    });

    it('should remain usable after guard failure', () => {
      const guarded = formatWithOptions('SELECT 1', Dialect.Generic, {
        maxInputBytes: 7,
      });
      expect(guarded.success).toBe(false);

      const next = format('SELECT a,b FROM t', Dialect.Generic);
      expect(next.success).toBe(true);
    });
  });

  describe('OpenLineage', () => {
    const options = {
      producer: 'https://github.com/tobilg/polyglot',
      datasetNamespace: 'postgres://warehouse',
      outputDataset: {
        namespace: 'postgres://warehouse',
        name: 'analytics.out',
      },
    };

    it('should produce column lineage facets', () => {
      const result = openLineageColumnLineage('SELECT a FROM t', options);
      expect(result.success).toBe(true);
      expect(result.facet?.fields.a.inputFields[0].field).toBe('a');
      expect(result.outputs?.[0].facets).toHaveProperty('columnLineage');
    });

    it('should produce JobEvent payloads', () => {
      const result = openLineageJobEvent('SELECT a FROM t', {
        ...options,
        jobNamespace: 'polyglot-tests',
        jobName: 'lineage-test',
        eventTime: '2026-05-18T00:00:00Z',
      });
      expect(result.success).toBe(true);
      expect(result.event?.job).toBeDefined();
      expect(result.event?.outputs).toBeDefined();
    });

    it('should produce RunEvent payloads', () => {
      const result = openLineageRunEvent('SELECT a FROM t', {
        ...options,
        jobNamespace: 'polyglot-tests',
        jobName: 'lineage-test',
        eventTime: '2026-05-18T00:00:00Z',
        runId: '3b452093-782c-4ef2-9c0c-aafe2aa6f34d',
        eventType: 'COMPLETE',
      });
      expect(result.success).toBe(true);
      expect(result.event?.eventType).toBe('COMPLETE');
      expect(result.event?.run).toBeDefined();
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

    it('should format SQL with options', () => {
      const polyglot = Polyglot.getInstance();
      const result = polyglot.formatWithOptions('SELECT 1', Dialect.Generic, {
        maxInputBytes: 7,
      });
      expect(result.success).toBe(false);
      expect(result.error).toContain('E_GUARD_INPUT_TOO_LARGE');
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
