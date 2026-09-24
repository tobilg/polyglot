import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import * as wasmModule from '../wasm/polyglot_sql_wasm.js';
import { lit } from './builders';
import * as sdk from './index';
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
  lineageAt,
  lineageWithSchema,
  openLineageColumnLineage,
  openLineageJobEvent,
  openLineageRunEvent,
  outputColumns,
  Polyglot,
  parse,
  parseDataType,
  transpile,
} from './index';

type ContractEntry = {
  status: 'supported' | 'partial' | 'unavailable';
  symbols: string[];
  notes?: string;
};

type CapabilityContract = {
  schemaVersion: number;
  layers: string[];
  capabilities: Array<{
    id: string;
    layers: Record<string, ContractEntry>;
  }>;
};

function hasDottedExport(root: object, symbol: string): boolean {
  let value: unknown = root;
  for (const part of symbol.split('.')) {
    if (
      (typeof value !== 'object' && typeof value !== 'function') ||
      value === null ||
      !(part in value)
    ) {
      return false;
    }
    value = (value as Record<string, unknown>)[part];
  }
  return true;
}

function assertLayerContract(
  contract: CapabilityContract,
  layer: 'wasm' | 'typescript',
  exports: object,
): void {
  const seen = new Set<string>();
  for (const capability of contract.capabilities) {
    expect(
      seen.has(capability.id),
      `duplicate capability ${capability.id}`,
    ).toBe(false);
    seen.add(capability.id);

    const entry = capability.layers[layer];
    expect(entry, `${capability.id} has no ${layer} entry`).toBeDefined();
    expect(['supported', 'partial', 'unavailable']).toContain(entry.status);
    if (entry.status !== 'supported') {
      expect(entry.notes, `${capability.id} requires notes`).toBeTruthy();
    }

    for (const symbol of entry.symbols) {
      const exists = hasDottedExport(exports, symbol);
      if (entry.status === 'unavailable') {
        expect(exists, `${capability.id}: ${symbol} unexpectedly exists`).toBe(
          false,
        );
      } else {
        expect(exists, `${capability.id}: ${symbol} is missing`).toBe(true);
      }
    }
  }
}

describe('Polyglot SDK', () => {
  it('matches the cross-language API capability contract', () => {
    const path = process.env.POLYGLOT_API_CONTRACT;
    if (!path) {
      return;
    }

    const contract = JSON.parse(
      readFileSync(path, 'utf8'),
    ) as CapabilityContract;
    expect(contract.schemaVersion).toBe(1);
    expect(contract.layers).toEqual([
      'rust',
      'python',
      'ffi',
      'go',
      'wasm',
      'typescript',
    ]);
    assertLayerContract(contract, 'typescript', sdk);
    assertLayerContract(contract, 'wasm', wasmModule);
  });

  it('exports the compatibility builders from the named and default APIs', () => {
    expect(sdk.compat.select('x').sql()).toBe('SELECT x');
    expect(sdk.default.compat.select('y').sql()).toBe('SELECT y');
  });

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
      expect(new Set(dialects).size).toBe(dialects.length);
      expect([...dialects].sort()).toEqual([...Object.values(Dialect)].sort());
    });

    it('should include common dialects', () => {
      const dialects = getDialects();
      expect(dialects).toContain('generic');
      expect(dialects).toContain('postgresql');
      expect(dialects).toContain('mysql');
    });
  });

  describe('parse', () => {
    it('preserves identifier and column source spans across WASM', () => {
      const result = parse('SELECT customer_id FROM orders', Dialect.Snowflake);
      const tree = result.ast?.[0];
      if (!tree || !('select' in tree)) throw new Error('Expected SELECT');
      const expression = tree.select.expressions[0];
      if (!expression || !('column' in expression))
        throw new Error('Expected column');
      const expected = { start: 7, end: 18, line: 1, column: 19 };
      expect(expression.column.span).toEqual(expected);
      expect(expression.column.name.span).toEqual(expected);
    });

    it('uses Unicode character offsets for qualified source spans', () => {
      const sql = 'SELECT "é😀", "a"."b" FROM "t"';
      const tree = parse(sql, Dialect.Snowflake).ast?.[0];
      if (!tree || !('select' in tree)) throw new Error('Expected SELECT');
      const expression = tree.select.expressions[1];
      if (!expression || !('column' in expression))
        throw new Error('Expected column');
      const span = expression.column.span;
      if (!span) throw new Error('Expected source span');
      expect(Array.from(sql).slice(span.start, span.end).join('')).toBe(
        '"a"."b"',
      );
    });

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

    it('should parse and generate TiDB DDL extensions', () => {
      const sql =
        'CREATE TABLE posts (id BIGINT AUTO_RANDOM PRIMARY KEY, title VARCHAR(255))';
      const parsed = parse(sql, Dialect.TiDB);

      expect(parsed.success).toBe(true);
      expect(parsed.ast?.[0]).toHaveProperty('create_table');

      const generated = generate(parsed.ast, Dialect.TiDB);
      expect(generated.success).toBe(true);
      expect(generated.sql).toEqual([sql]);
    });

    it('should parse DuckDB EXTRACT date parts and infer their result types', () => {
      const schema = {
        tables: [
          {
            name: 'events',
            columns: [{ name: 'created_at', type: 'TIMESTAMP' }],
          },
        ],
      };

      for (const [datePart, expectedType] of [
        ["'year'", 'BIGINT'],
        ["'month'", 'BIGINT'],
        ["'day'", 'BIGINT'],
        ['YEAR', 'BIGINT'],
        ["'second'", 'BIGINT'],
        ["'epoch'", 'DOUBLE'],
        ['EPOCH', 'DOUBLE'],
        ["'EpOcH'", 'DOUBLE'],
        ["'julian'", 'DOUBLE'],
        ['JULIAN', 'DOUBLE'],
        ["'JuLiAn'", 'DOUBLE'],
      ]) {
        const sql = `SELECT EXTRACT(${datePart} FROM created_at) AS extracted FROM events`;
        const parsed = parse(sql, Dialect.DuckDB);
        expect(parsed.success, sql).toBe(true);

        const analysis = analyzeQuery(sql, {
          dialect: Dialect.DuckDB,
          schema,
        });
        expect(analysis.success, sql).toBe(true);
        expect(analysis.analysis?.projections[0]).toMatchObject({
          name: 'extracted',
          typeHint: expectedType,
        });
      }
    });

    it('should parse DuckDB keyword relation aliases with and without AS', () => {
      for (const template of [
        'WITH ranked_items AS (SELECT 1 AS item_id) SELECT top.item_id FROM ranked_items {as}top',
        'WITH ranked_items AS (SELECT 1 AS item_id), selected_items AS (SELECT 1 AS item_id) SELECT top.item_id FROM selected_items JOIN ranked_items {as}top ON top.item_id = selected_items.item_id',
      ]) {
        const expected = template.replace('{as}', 'AS ');
        for (const asKeyword of ['', 'AS ']) {
          const sql = template.replace('{as}', asKeyword);
          const parsed = parse(sql, Dialect.DuckDB);
          expect(parsed.success, sql).toBe(true);
          expect(parsed.ast, sql).toHaveLength(1);

          const generated = generate(parsed.ast, Dialect.DuckDB);
          expect(generated.success, sql).toBe(true);
          expect(generated.sql, sql).toEqual([expected]);
        }
      }
    });

    it('should accept DuckDB single-quoted projection aliases only after AS', () => {
      for (const [sql, names, expectedSql] of [
        [
          "SELECT 1 AS 'item count'",
          ['item count'],
          'SELECT 1 AS "item count"',
        ],
        [
          "SELECT 1 AS 'owner''s count'",
          ["owner's count"],
          'SELECT 1 AS "owner\'s count"',
        ],
        [
          'SELECT 1 AS \'item "count"\'',
          ['item "count"'],
          'SELECT 1 AS "item ""count"""',
        ],
        [
          "SELECT COUNT(*) FILTER (WHERE state = 'open') AS 'Open', COUNT(*) FILTER (WHERE state = 'closed') AS 'Closed' FROM work_items",
          ['Open', 'Closed'],
          'SELECT COUNT(*) FILTER(WHERE state = \'open\') AS "Open", COUNT(*) FILTER(WHERE state = \'closed\') AS "Closed" FROM work_items',
        ],
      ] as const) {
        const parsed = parse(sql, Dialect.DuckDB);
        expect(parsed.success, sql).toBe(true);
        const aliases = sdk.ast
          .findByType(parsed.ast![0], 'alias')
          .filter(sdk.ast.isAlias);
        expect(
          aliases.map((node) => node.alias.alias.name),
          sql,
        ).toEqual(names);
        for (const alias of aliases) {
          expect(alias.alias.alias.quoted, sql).toBe(true);
        }
        const generated = generate(parsed.ast, Dialect.DuckDB);
        expect(generated.success, sql).toBe(true);
        expect(generated.sql, sql).toEqual([expectedSql]);
        expect(transpile(sql, Dialect.DuckDB, Dialect.DuckDB).sql, sql).toEqual(
          [expectedSql],
        );
        const analysis = analyzeQuery(sql, { dialect: Dialect.DuckDB });
        expect(analysis.success, sql).toBe(true);
        expect(
          analysis.analysis?.projections.map((p) => p.name),
          sql,
        ).toEqual(names);
      }
      expect(parse("SELECT 1 'item count'", Dialect.DuckDB).success).toBe(
        false,
      );
      for (const dialect of [Dialect.Generic, Dialect.PostgreSQL]) {
        expect(parse("SELECT 1 AS 'item count'", dialect).success).toBe(false);
      }
    });

    it('should normalize DuckDB empty string projection aliases to unnamed outputs', () => {
      for (const sql of ["SELECT 1 AS ''", "SELECT 'literal' AS ''"]) {
        const parsed = parse(sql, Dialect.DuckDB);
        expect(parsed.success, sql).toBe(true);
        expect(sdk.ast.findByType(parsed.ast![0], 'alias'), sql).toHaveLength(
          0,
        );
        const generated = generate(parsed.ast, Dialect.DuckDB);
        expect(generated.success, sql).toBe(true);
        expect(generated.sql, sql).toEqual([sql.replace(" AS ''", '')]);
        const analysis = analyzeQuery(sql, { dialect: Dialect.DuckDB });
        expect(analysis.success, sql).toBe(true);
        expect(analysis.analysis?.projections[0].name, sql).toBeNull();
      }
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

    it('should preserve TSQL LEN when generating a parsed AST', () => {
      const sql = 'SELECT LEN(table.col1) - LEN(table.col2) FROM table';
      const parseResult = parse(sql, Dialect.TSQL);
      expect(parseResult.success).toBe(true);

      const generateResult = generate(parseResult.ast, Dialect.TSQL);
      expect(generateResult.success).toBe(true);
      expect(generateResult.sql).toEqual([sql]);
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

    it('should roundtrip generated Snowflake string escapes', () => {
      const quoteLiteral = lit("O'Reilly");
      const backslashLiteral = lit(String.raw`C:\user\n`);

      try {
        const quoteResult = generate(
          [quoteLiteral.toJSON()],
          Dialect.Snowflake,
        );
        expect(quoteResult.success).toBe(true);
        expect(quoteResult.sql).toEqual([String.raw`'O\'Reilly'`]);

        const parsed = parse(
          `SELECT ${quoteResult.sql![0]}`,
          Dialect.Snowflake,
        );
        expect(parsed.success).toBe(true);

        const backslashResult = generate(
          [backslashLiteral.toJSON()],
          Dialect.Snowflake,
        );
        expect(backslashResult.success).toBe(true);
        expect(backslashResult.sql).toEqual([String.raw`'C:\\user\\n'`]);
      } finally {
        quoteLiteral.free();
        backslashLiteral.free();
      }
    });
  });

  describe('data types', () => {
    it.each([
      ['field name', '"field name"'],
      ['a"b', '"a""b"'],
      ['select', '"select"'],
      ['a INT, b', '"a INT, b"'],
      ['a(16)', '"a(16)"'],
    ])('should quote constructed struct field %s without changing its name or field count', (name, identifier) => {
      const dataType: sdk.DataType = {
        data_type: 'struct',
        nested: false,
        fields: [{ name, data_type: { data_type: 'var_char', length: null } }],
      };
      const generated = generateDataType(dataType, Dialect.DuckDB);
      expect(generated).toMatchObject({
        success: true,
        sql: `STRUCT(${identifier} TEXT)`,
      });
      const parsed = parseDataType(generated.sql!, Dialect.DuckDB);
      expect(parsed).toMatchObject({
        success: true,
        dataType: {
          data_type: 'struct',
          fields: [{ name: identifier, data_type: { data_type: 'text' } }],
        },
      });
      expect(generateDataType(parsed.dataType!, Dialect.DuckDB).sql).toBe(
        generated.sql,
      );
    });

    it('should preserve parsed struct field escapes and translate identifier delimiters', () => {
      const parsed = parseDataType(
        'STRUCT("a""b" INT, "field name" INT)',
        Dialect.DuckDB,
      );
      expect(parsed.success).toBe(true);
      expect(generateDataType(parsed.dataType!, Dialect.DuckDB).sql).toBe(
        'STRUCT("a""b" INT, "field name" INT)',
      );
      const spark = generateDataType(parsed.dataType!, Dialect.Spark);
      expect(spark.sql).toBe('STRUCT<`a"b`: INT, `field name`: INT>');
      expect(parseDataType(spark.sql!, Dialect.Spark).dataType).toEqual({
        ...parsed.dataType,
        nested: false,
      });
    });

    it.each([
      ['UTINYINT', 'UINT8', 'uint8', 'UTINYINT'],
      ['USMALLINT', 'UINT16', 'uint16', 'USMALLINT'],
      ['UINTEGER', 'UINT32', 'uint32', 'UINTEGER'],
      ['UBIGINT', 'UINT64', 'uint64', 'UBIGINT'],
      ['UHUGEINT', 'UINT128', 'uint128', 'UINT128'],
    ] as const)('should preserve unsigned %s types across parsing, generation and cloning', (native, alias, tag, output) => {
      const expected: sdk.DataType = { data_type: tag };
      for (const name of [native, alias]) {
        expect(parseDataType(name, Dialect.DuckDB).dataType).toEqual(expected);
        expect(generateDataType(expected, Dialect.DuckDB)).toMatchObject({
          success: true,
          sql: output,
        });
        const nested = parseDataType(
          `STRUCT(x ${name}, xs ${name}[])`,
          Dialect.DuckDB,
        );
        expect(nested.success).toBe(true);
        expect(JSON.stringify(nested.dataType)).toContain(
          `"data_type":"${tag}"`,
        );
        expect(JSON.stringify(nested.dataType)).not.toContain('"custom"');
        const ast = parse(`SELECT CAST(x AS ${name}[]) FROM t`, Dialect.DuckDB)
          .ast![0];
        const cloned = sdk.ast.clone(ast);
        expect(cloned).toEqual(ast);
        expect(generate([cloned], Dialect.DuckDB).success).toBe(true);
      }
    });

    it('should expose DuckDB unsigned coercion and SUM promotion through the shared engine', () => {
      const schema = {
        tables: [
          {
            name: 't',
            columns: [
              { name: 's', type: 'BIGINT' },
              { name: 'u', type: 'UBIGINT' },
              { name: 'h', type: 'UHUGEINT' },
              { name: 'd', type: 'DECIMAL(10,2)' },
              { name: 'b', type: 'BOOLEAN' },
            ],
          },
        ],
      };
      const cases: [string, Partial<sdk.DataType>][] = [
        ['u', { data_type: 'uint64' }],
        ['h', { data_type: 'uint128' }],
        ['s + u', { data_type: 'int128' }],
        ['u + s', { data_type: 'int128' }],
        ['COALESCE(u, s)', { data_type: 'int128' }],
        ['SUM(s)', { data_type: 'int128' }],
        ['SUM(u)', { data_type: 'int128' }],
        ['SUM(b)', { data_type: 'int128' }],
        ['SUM(h)', { data_type: 'double' }],
        ['SUM(d)', { data_type: 'decimal', precision: 38, scale: 2 }],
        ['SUM(DISTINCT u)', { data_type: 'int128' }],
        ['SUM(u) FILTER (WHERE TRUE)', { data_type: 'int128' }],
        ['SUM(u) OVER ()', { data_type: 'int128' }],
      ];
      for (const [projection, expected] of cases) {
        const sql = `SELECT ${projection} AS result FROM t`;
        const result = sdk.annotateTypes(sql, Dialect.DuckDB, schema);
        expect(result.success, sql).toBe(true);
        const alias = sdk.ast.findByType(result.ast![0], 'alias')[0];
        expect(sdk.ast.getInferredType(alias), sql).toMatchObject(expected);
        expect(sdk.ast.clone(result.ast![0])).toEqual(result.ast![0]);
      }
      for (const [projection, typeHint] of [
        ['u', 'UBIGINT'],
        ['h', 'UINT128'],
        ['SUM(s)', 'INT128'],
      ]) {
        const analysis = analyzeQuery(`SELECT ${projection} AS result FROM t`, {
          dialect: Dialect.DuckDB,
          schema,
        });
        expect(analysis.success).toBe(true);
        expect(analysis.analysis?.projections[0].typeHint).toBe(typeHint);
      }
    });

    it.each([
      [Dialect.DuckDB, 'HUGEINT', 'INT128'],
      [Dialect.DuckDB, 'INT128', 'INT128'],
      [Dialect.ClickHouse, 'Int128', 'Int128'],
      [Dialect.StarRocks, 'LARGEINT', 'LARGEINT'],
    ])('should expose a first-class signed 128-bit type for %s %s', (dialect, name, sql) => {
      const expected: sdk.DataType = { data_type: 'int128' };
      const parsed = parseDataType(name, dialect);
      expect(parsed.success).toBe(true);
      expect(parsed.dataType).toEqual(expected);
      expect(generateDataType(expected, dialect)).toMatchObject({
        success: true,
        sql,
      });
    });

    it('should preserve signed 128-bit types in nested positions and cloning', () => {
      for (const name of ['HUGEINT[]', 'STRUCT(x HUGEINT, xs HUGEINT[])']) {
        const parsed = parseDataType(name, Dialect.DuckDB);
        expect(parsed.success).toBe(true);
        expect(JSON.stringify(parsed.dataType)).toContain(
          '"data_type":"int128"',
        );
        expect(JSON.stringify(parsed.dataType)).not.toContain('"custom"');
        const generated = generateDataType(parsed.dataType!, Dialect.DuckDB);
        expect(generated.success).toBe(true);
        expect(parseDataType(generated.sql!, Dialect.DuckDB).dataType).toEqual(
          parsed.dataType,
        );

        const ast = parse(
          `SELECT CAST(value AS ${name}) FROM measurements`,
          Dialect.DuckDB,
        ).ast![0];
        const cloned = sdk.ast.clone(ast);
        expect(cloned).toEqual(ast);
        expect(sdk.ast.findByType(cloned, 'data_type')).toEqual([]);
        expect(generate([cloned], Dialect.DuckDB).success).toBe(true);
      }
    });

    it('should infer signed 128-bit schema columns, casts and numeric widening', () => {
      const schema = {
        tables: [
          {
            name: 'measurements',
            columns: [
              { name: 'wide', type: 'HUGEINT' },
              { name: 'value', type: 'BIGINT' },
            ],
          },
        ],
      };
      for (const projection of [
        'wide',
        'CAST(value AS HUGEINT)',
        'wide + value',
        'COALESCE(value, wide)',
      ]) {
        const result = sdk.annotateTypes(
          `SELECT ${projection} AS widened FROM measurements`,
          Dialect.DuckDB,
          schema,
        );
        expect(result.success).toBe(true);
        const alias = sdk.ast.findByType(result.ast![0], 'alias')[0];
        expect(sdk.ast.getInferredType(alias)).toEqual({ data_type: 'int128' });
        const cloned = sdk.ast.clone(result.ast![0]);
        expect(cloned).toEqual(result.ast![0]);
        expect(generate([cloned], Dialect.DuckDB).success).toBe(true);
      }
    });

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
      expect(
        polyglot.generateDataType(parsed.dataType!, Dialect.DuckDB).sql,
      ).toBe('INT[]');
    });
  });

  describe('lineage helpers', () => {
    const collectNames = (node: {
      name?: string;
      downstream?: unknown[];
    }): string[] => [
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
          tables: [{ name: 't', columns: [{ name: 'amount', type: 'INT' }] }],
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

    it('should trace a zero-based output ordinal across set-operation branches', () => {
      const result = lineageAt(
        1,
        'SELECT a, b FROM t1 UNION ALL SELECT x, y FROM t2',
        Dialect.Generic,
      );

      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toEqual(
        expect.arrayContaining(['t1.b', 't2.y']),
      );
      expect(result.lineage?.downstream.map((node) => node.set_branch)).toEqual(
        [
          { operator: 'union', ordinal: 0, all: true },
          { operator: 'union', ordinal: 1, all: true },
        ],
      );
    });

    it('should expose structured ordinal resolution failures', () => {
      const result = lineageAt(2, 'SELECT a FROM t', Dialect.Generic);

      expect(result.success).toBe(false);
      expect(result.columnResolution).toEqual({
        target: { kind: 'ordinal', ordinal: 2 },
        reason: 'not_found',
      });
    });

    it('should preserve unnamed output slots and unresolved wildcards', () => {
      const result = outputColumns('SELECT 1, t.*, b FROM t', Dialect.Generic);

      expect(result.success).toBe(true);
      expect(result.output).toEqual({
        columns: [
          { kind: 'unnamed', ordinal: 0 },
          {
            kind: 'wildcard',
            qualifier: 't',
            startOrdinal: 1,
          },
          { kind: 'named', name: 'b', ordinal: null },
        ],
        ordinalComplete: false,
      });
    });

    it('should expose Snowflake UNION BY NAME outputs and lineage', () => {
      const sql =
        'SELECT 1 AS left_value UNION ALL BY NAME SELECT 2 AS right_value';
      const output = outputColumns(sql, Dialect.Snowflake);

      expect(output.success).toBe(true);
      expect(output.output).toEqual({
        columns: [
          { kind: 'named', name: 'left_value', ordinal: 0 },
          { kind: 'named', name: 'right_value', ordinal: 1 },
        ],
        ordinalComplete: true,
      });

      const result = lineageAt(1, sql, Dialect.Snowflake);
      expect(result.success).toBe(true);
      expect(collectNames(result.lineage!)).toContain('right_value');
      expect(result.lineage?.downstream.map((node) => node.set_branch)).toEqual(
        [{ operator: 'union', ordinal: 1, all: true }],
      );
    });
  });

  describe('analyzeQuery', () => {
    it.each([
      false,
      true,
    ])('preserves CTE cast types (schema=%s)', (withSchema) => {
      const sql =
        'WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT amount FROM transformed) SELECT amount FROM final';
      const options = {
        dialect: Dialect.Snowflake,
        ...(withSchema
          ? {
              schema: {
                tables: [
                  {
                    name: 'raw_orders',
                    columns: [{ name: 'amount', type: 'VARCHAR' }],
                  },
                ],
              },
            }
          : {}),
      };
      const result = analyzeQuery(sql, options);
      expect(result.success).toBe(true);
      expect(result.analysis?.projections[0]).toMatchObject({
        name: 'amount',
        transformKind: 'direct',
        castType: null,
        typeHint: 'INT',
      });
      const json = JSON.parse(
        wasmModule.analyze_query(sql, JSON.stringify(options)),
      );
      expect(json.analysis.projections).toEqual(result.analysis?.projections);
    });

    it('exposes scoped column uses and Unicode occurrence spans through both WASM transports', () => {
      const sql =
        "SELECT '😀', o.id FROM orders o WHERE o.amount > 0 OR o.amount < -1";
      const result = analyzeQuery(sql, { dialect: 'duckdb' });
      expect(result.success).toBe(true);
      const uses = result.analysis?.columnUses;
      expect(uses).toHaveLength(1);
      const fact = uses?.[0];
      expect(fact).toMatchObject({
        context: 'filter',
        scopePath: 'root',
        expressionPath: 'where_clause.this',
      });
      expect(fact?.references).toHaveLength(2);
      expect(fact?.references[0].span).not.toEqual(fact?.references[1].span);
      for (const reference of fact?.references ?? []) {
        expect(reference).toMatchObject({
          sourceName: 'orders',
          sourceAlias: 'o',
          column: 'amount',
          confidence: 'resolved',
        });
        expect(
          Array.from(sql)
            .slice(reference.span?.start, reference.span?.end)
            .join(''),
        ).toBe('o.amount');
      }
      const json = JSON.parse(
        wasmModule.analyze_query(sql, JSON.stringify({ dialect: 'duckdb' })),
      );
      expect(json.analysis.columnUses).toEqual(uses);
      expect(
        result.analysis?.projections[1].upstream.map((ref) => ref.column),
      ).toEqual(['id']);
      expect(analyzeQuery('SELECT 1').analysis?.columnUses).toEqual([]);
    });

    it('keeps CTE predicates and set-operation filter inputs distinguishable', () => {
      const result = analyzeQuery(
        'WITH base AS (SELECT id, amount FROM orders) SELECT id FROM base WHERE amount > 0 EXCEPT SELECT id FROM blocked',
        { dialect: 'duckdb' },
      );
      expect(result.success).toBe(true);
      const uses = result.analysis?.columnUses ?? [];
      expect(uses.find((fact) => fact.context === 'filter')).toMatchObject({
        scopePath: 'root.branches[0]',
        references: [
          { table: 'orders', column: 'amount', confidence: 'resolved' },
        ],
      });
      expect(
        uses.find((fact) => fact.context === 'set_operation_filter'),
      ).toMatchObject({
        scopePath: 'root.branches[1]',
        references: [{ table: 'blocked', column: 'id' }],
      });
    });

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

    it('should classify DuckDB COUNT_IF, MEDIAN, and FIRST as aggregations', () => {
      const result = analyzeQuery(
        'SELECT COUNT_IF(numeric_value > 0), MEDIAN(numeric_value), FIRST(numeric_value) FROM source_table',
        { dialect: Dialect.DuckDB },
      );

      expect(result.success).toBe(true);
      expect(
        result.analysis?.projections.map(({ transformKind }) => transformKind),
      ).toEqual(['aggregation', 'aggregation', 'aggregation']);
    });

    it('should expose the DuckDB MEDIAN result type with a schema', () => {
      const schema = {
        tables: [
          {
            name: 'values_table',
            columns: [{ name: 'x', type: 'DOUBLE' }],
          },
        ],
      };
      const sql = 'SELECT MEDIAN(x) AS median_x FROM values_table';

      const analysis = analyzeQuery(sql, {
        dialect: Dialect.DuckDB,
        schema,
      });
      expect(analysis.success).toBe(true);
      expect(analysis.analysis?.projections[0]).toMatchObject({
        transformKind: 'aggregation',
        typeHint: 'DOUBLE',
      });

      const annotated = sdk.annotateTypes(sql, Dialect.DuckDB, schema);
      expect(annotated.success).toBe(true);
      const median = sdk.ast.findByType(annotated.ast![0], 'median')[0];
      expect(sdk.ast.getInferredType(median)).toMatchObject({
        data_type: 'double',
      });
    });

    it('should expose DuckDB TRIM node types with and without a schema', () => {
      const schema = {
        tables: [
          {
            name: 'records',
            columns: [
              { name: 'name', type: 'VARCHAR' },
              { name: 'chars', type: 'VARCHAR' },
              { name: 'values_json', type: 'JSON' },
            ],
          },
        ],
      };
      for (const functionSql of [
        'TRIM(name)',
        `TRIM(BOTH '"' FROM values_json->>0)`,
        'TRIM(LEADING chars FROM name)',
        'TRIM(TRAILING chars FROM name)',
        'TRIM(LOWER(TRIM(name)), UPPER(chars))',
        'TRIM(NULL)',
      ]) {
        const sql = `SELECT ${functionSql} AS normalized FROM records`;
        for (const inputSchema of [schema, undefined]) {
          const annotated = sdk.annotateTypes(sql, Dialect.DuckDB, inputSchema);
          expect(annotated.success, sql).toBe(true);
          const trims = sdk.ast.findByType(annotated.ast![0], 'trim');
          expect(trims.length, sql).toBeGreaterThan(0);
          for (const trim of trims) {
            expect(sdk.ast.getInferredType(trim), sql).toMatchObject({
              data_type: 'var_char',
            });
          }
          const projection = sdk.ast.findByType(annotated.ast![0], 'alias')[0];
          expect(sdk.ast.getInferredType(projection), sql).toMatchObject({
            data_type: 'var_char',
          });
          for (const type of ['lower', 'upper', 'column'] as const) {
            for (const child of sdk.ast.findByType(annotated.ast![0], type)) {
              // JSON extraction has independent annotation gaps; here check
              // the directly traversable string arguments and their children.
              if (functionSql.includes('values_json')) continue;
              const inferred = sdk.ast.getInferredType(child);
              if (type === 'column' && !inputSchema) {
                expect(inferred, sql).toBeUndefined();
              } else {
                expect(inferred, sql).toMatchObject({ data_type: 'var_char' });
              }
            }
          }

          const analysis = analyzeQuery(sql, {
            dialect: Dialect.DuckDB,
            schema: inputSchema,
          });
          expect(analysis.success, sql).toBe(true);
          expect(analysis.analysis?.projections[0], sql).toMatchObject({
            name: 'normalized',
            typeHint: 'TEXT',
          });
        }
      }
    });

    it('should infer DuckDB REGEXP_EXTRACT_ALL list and named-capture types', () => {
      const schema = {
        tables: [
          {
            name: 'documents',
            columns: [{ name: 'body', type: 'VARCHAR' }],
          },
        ],
      };
      const varchar = { data_type: 'var_char' };
      const captures = {
        data_type: 'struct',
        fields: [
          { name: 'letter', data_type: varchar },
          { name: 'number', data_type: varchar },
        ],
      };
      for (const [argumentsSql, elementType, typeHint] of [
        ["body, '[0-9]+'", varchar, 'TEXT'],
        ["NULL, '[0-9]+'", varchar, 'TEXT'],
        ["body, '([a-z])([0-9]+)', 2, 'i'", varchar, 'TEXT'],
        [
          "body, '([a-z])([0-9]+)', ['letter', 'number']",
          captures,
          'STRUCT(letter TEXT, number TEXT)',
        ],
        [
          "body, '([a-z])([0-9]+)', (['letter', 'number']), 'i'",
          captures,
          'STRUCT(letter TEXT, number TEXT)',
        ],
      ] as const) {
        const arrayType = { data_type: 'array', element_type: elementType };
        for (const unnest of [false, true]) {
          const functionSql = `REGEXP_EXTRACT_ALL(${argumentsSql})`;
          const sql = `SELECT ${unnest ? `UNNEST(${functionSql})` : functionSql} AS matches FROM documents`;
          for (const inputSchema of [schema, undefined]) {
            const annotated = sdk.annotateTypes(
              sql,
              Dialect.DuckDB,
              inputSchema,
            );
            expect(annotated.success, sql).toBe(true);
            const functions = sdk.ast.findByType(annotated.ast![0], 'function');
            const extraction = functions.find(
              (node) =>
                sdk.ast.isFunction(node) &&
                node.function.name === 'REGEXP_EXTRACT_ALL',
            );
            expect(extraction, sql).toBeDefined();
            expect(sdk.ast.getInferredType(extraction!), sql).toMatchObject(
              arrayType,
            );
            const projection = sdk.ast.findByType(
              annotated.ast![0],
              'alias',
            )[0];
            expect(sdk.ast.getInferredType(projection), sql).toMatchObject(
              unnest ? elementType : arrayType,
            );
            const analysis = analyzeQuery(sql, {
              dialect: Dialect.DuckDB,
              schema: inputSchema,
            });
            expect(analysis.success, sql).toBe(true);
            expect(analysis.analysis?.projections[0], sql).toMatchObject({
              name: 'matches',
              typeHint: unnest ? typeHint : `${typeHint}[]`,
            });
          }
        }
      }
    });

    it('should infer DuckDB MONTHNAME and DAYNAME results as strings', () => {
      for (const type of ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ']) {
        const schema = {
          tables: [
            {
              name: 'events',
              columns: [{ name: 'created_at', type }],
            },
          ],
        };
        for (const functionName of ['MONTHNAME', 'DAYNAME']) {
          const sql = `SELECT ${functionName}(created_at) AS label FROM events`;
          for (const inputSchema of [schema, undefined]) {
            const annotated = sdk.annotateTypes(
              sql,
              Dialect.DuckDB,
              inputSchema,
            );
            expect(annotated.success, sql).toBe(true);
            const projection = sdk.ast.findByType(
              annotated.ast![0],
              'alias',
            )[0];
            expect(projection, sql).toBeDefined();
            const expression = sdk.ast.isAlias(projection)
              ? projection.alias.this
              : projection;
            for (const node of [expression, projection]) {
              expect(sdk.getInferredType(node), sql).toMatchObject({
                data_type: 'var_char',
              });
            }
            const analysis = analyzeQuery(sql, {
              dialect: Dialect.DuckDB,
              schema: inputSchema,
            });
            expect(analysis.success, sql).toBe(true);
            expect(analysis.analysis?.projections[0], sql).toMatchObject({
              name: 'label',
              typeHint: 'TEXT',
            });
            if (inputSchema) {
              expect(analysis.analysis?.projections[0].upstream, sql).toEqual(
                expect.arrayContaining([
                  expect.objectContaining({
                    table: 'events',
                    column: 'created_at',
                  }),
                ]),
              );
            }
          }
        }
      }
    });

    it('should infer DuckDB ARRAY_TO_STRING results as scalar strings', () => {
      const schema = {
        tables: [
          {
            name: 'events',
            columns: [
              { name: 'labels', type: 'VARCHAR[]' },
              { name: 'label', type: 'VARCHAR' },
            ],
          },
        ],
      };
      for (const functionSql of [
        "ARRAY_TO_STRING(labels, ', ')",
        "ARRAY_TO_STRING(ARRAY_AGG(label), ', ')",
        'ARRAY_TO_STRING_COMMA_DEFAULT(labels)',
        'ARRAY_TO_STRING_COMMA_DEFAULT(ARRAY_AGG(label))',
      ]) {
        const sql = `SELECT ${functionSql} AS label_text FROM events`;
        for (const inputSchema of [schema, undefined]) {
          const annotated = sdk.annotateTypes(sql, Dialect.DuckDB, inputSchema);
          expect(annotated.success, sql).toBe(true);
          const projection = sdk.ast.findByType(annotated.ast![0], 'alias')[0];
          expect(projection, sql).toBeDefined();
          expect(sdk.ast.getInferredType(projection), sql).toMatchObject({
            data_type: 'var_char',
          });
          const expression = sdk.ast.isAlias(projection)
            ? projection.alias.this
            : projection;
          expect(sdk.ast.getInferredType(expression), sql).toMatchObject({
            data_type: 'var_char',
          });

          const analysis = analyzeQuery(sql, {
            dialect: Dialect.DuckDB,
            schema: inputSchema,
          });
          expect(analysis.success, sql).toBe(true);
          expect(analysis.analysis?.projections[0], sql).toMatchObject({
            name: 'label_text',
            typeHint: 'TEXT',
          });
        }
      }
    });

    it('should infer DuckDB ARRAY, CASE, and UNNEST result types', () => {
      const schema = {
        tables: [
          {
            name: 'events',
            columns: [
              { name: 'created_at', type: 'TIMESTAMP' },
              { name: 'closed_at', type: 'TIMESTAMP' },
            ],
          },
        ],
      };
      const sql = `SELECT UNNEST(
        CASE
          WHEN closed_at IS NULL THEN ARRAY[created_at]
          ELSE ARRAY[created_at, closed_at]
        END
      ) AS event_at
      FROM events`;

      const analysis = analyzeQuery(sql, {
        dialect: Dialect.DuckDB,
        schema,
      });
      expect(analysis.success).toBe(true);
      expect(analysis.analysis?.projections[0]).toMatchObject({
        name: 'event_at',
        typeHint: 'TIMESTAMP',
      });
      expect(analysis.analysis?.projections[0].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ table: 'events', column: 'created_at' }),
          expect.objectContaining({ table: 'events', column: 'closed_at' }),
        ]),
      );

      const annotated = sdk.annotateTypes(sql, Dialect.DuckDB, schema);
      expect(annotated.success).toBe(true);

      const arrays = sdk.ast.findByType(annotated.ast![0], 'array_func');
      expect(arrays).toHaveLength(2);
      for (const array of arrays) {
        expect(sdk.ast.getInferredType(array)).toMatchObject({
          data_type: 'array',
          element_type: { data_type: 'timestamp' },
        });
      }

      const caseExpression = sdk.ast.findByType(annotated.ast![0], 'case')[0];
      expect(sdk.ast.getInferredType(caseExpression)).toMatchObject({
        data_type: 'array',
        element_type: { data_type: 'timestamp' },
      });

      const unnest = sdk.ast.findByType(annotated.ast![0], 'function')[0];
      expect(sdk.ast.getExprData(unnest).name).toBe('UNNEST');
      expect(sdk.ast.getInferredType(unnest)).toMatchObject({
        data_type: 'timestamp',
      });
    });

    it('should preserve inferred types through transparent wrappers', () => {
      const schema = {
        tables: [
          {
            name: 'flags',
            columns: [{ name: 'flag', type: 'BOOLEAN' }],
          },
        ],
      };
      const sql =
        "SELECT (CASE WHEN flag THEN 'yes' ELSE 'no' END) AS label FROM flags";

      const analysis = analyzeQuery(sql, {
        dialect: Dialect.DuckDB,
        schema,
      });
      expect(analysis.success).toBe(true);
      expect(analysis.analysis?.projections[0]).toMatchObject({
        name: 'label',
        typeHint: 'TEXT',
      });

      const annotated = sdk.annotateTypes(sql, Dialect.DuckDB, schema);
      expect(annotated.success).toBe(true);
      const paren = sdk.ast.findByType(annotated.ast![0], 'paren')[0];
      const caseExpression = sdk.ast.findByType(annotated.ast![0], 'case')[0];
      const column = sdk.ast.findByType(annotated.ast![0], 'column')[0];

      expect(sdk.ast.getInferredType(paren)).toMatchObject({
        data_type: 'var_char',
      });
      expect(sdk.ast.getInferredType(caseExpression)).toMatchObject({
        data_type: 'var_char',
      });
      expect(sdk.ast.getInferredType(column)).toMatchObject({
        data_type: 'boolean',
      });

      const withComment = sdk.annotateTypes(
        "SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END /*tail*/ FROM flags",
        Dialect.DuckDB,
        schema,
      );
      expect(withComment.success).toBe(true);
      const commentWrapper = sdk.ast.findByType(
        withComment.ast![0],
        'annotated',
      )[0];
      expect(sdk.ast.getInferredType(commentWrapper)).toMatchObject({
        data_type: 'var_char',
      });
    });

    it('should classify DuckDB null-preserving arg extrema as aggregations', () => {
      const result = analyzeQuery(
        'SELECT ARG_MAX_NULL(label, score), ARG_MIN_NULL(label, score) FROM source_table',
        { dialect: Dialect.DuckDB },
      );

      expect(result.success).toBe(true);
      expect(
        result.analysis?.projections.map(({ transformKind }) => transformKind),
      ).toEqual(['aggregation', 'aggregation']);
    });

    it('should classify DuckDB product, histogram, and quantile aggregates', () => {
      const result = analyzeQuery(
        'SELECT PRODUCT(x), APPROX_QUANTILE(x, 0.5), HISTOGRAM_EXACT(x, [1, 2]), MAD(x), QUANTILE(x, 0.5), QUANTILE_CONT(x, 0.5), QUANTILE_DISC(x, 0.5), RESERVOIR_QUANTILE(x, 0.5) FROM source_table',
        { dialect: Dialect.DuckDB },
      );

      expect(result.success).toBe(true);
      expect(
        result.analysis?.projections.map(({ transformKind }) => transformKind),
      ).toEqual(Array.from({ length: 8 }, () => 'aggregation'));
    });

    it('should expose set-operation branch roles', () => {
      const union = analyzeQuery('SELECT a FROM x UNION SELECT b FROM y');
      const except = analyzeQuery('SELECT a FROM x EXCEPT SELECT b FROM y');

      expect(union.success).toBe(true);
      expect(
        union.analysis?.setOperations[0].branches.map(({ role }) => role),
      ).toEqual(['value', 'value']);
      expect(except.success).toBe(true);
      expect(
        except.analysis?.setOperations[0].branches.map(({ role }) => role),
      ).toEqual(['value', 'filter']);
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
          tables: [{ name: 't', columns: [{ name: 'arr', type: 'INT' }] }],
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
          tables: [{ name: 't', columns: [{ name: 'amount', type: 'INT' }] }],
        },
      });

      expect(result.success).toBe(true);
      expect(
        result.analysis?.projections.map((projection) => projection.name),
      ).toEqual(['order_id', 'amount']);
      expect(result.analysis?.projections[0].upstream).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            column: 'order_id',
            table: 't',
            confidence: 'unknown',
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
    it('shares configurable parser depth and remains usable after rejection', () => {
      const sql = `SELECT ${'~ '.repeat(12)}1`;
      for (const maxParserDepth of [0, 8]) {
        const result = transpile(sql, Dialect.Generic, Dialect.Generic, {
          complexityGuard: { maxParserDepth },
        });
        expect(result.success).toBe(false);
        expect(result.error).toContain('E_GUARD_PARSER_DEPTH_EXCEEDED');
      }
      for (const maxParserDepth of [undefined, 64, null]) {
        expect(
          transpile(sql, Dialect.Generic, Dialect.Generic, {
            complexityGuard: { maxParserDepth },
          }).success,
        ).toBe(true);
      }
      for (const maxParserDepth of [-1, 1.5, NaN, Infinity, -Infinity]) {
        const result = transpile('SELECT 1', Dialect.Generic, Dialect.Generic, {
          complexityGuard: { maxParserDepth },
        });
        expect(result.success).toBe(false);
        expect(result.error).toContain('Invalid transpile options');
      }
      const longUnary = `SELECT ${'+ '.repeat(2000)}1`;
      for (const complexityGuard of [{}, { maxParserDepth: undefined }]) {
        const result = transpile(longUnary, Dialect.Generic, Dialect.Generic, {
          complexityGuard,
        });
        expect(result.success).toBe(false);
        expect(result.error).toContain('E_GUARD_PARSER_DEPTH_EXCEEDED');
      }
      for (const maxParserDepth of [null, 2048]) {
        expect(
          transpile(longUnary, Dialect.Generic, Dialect.Generic, {
            complexityGuard: { maxParserDepth },
          }).success,
        ).toBe(true);
      }
      for (const prefix of ['', 'SELECT ']) {
        const result = parse(
          `${prefix}${'IF~'.repeat(4000)}I?{`,
          Dialect.Generic,
        );
        expect(result.success).toBe(false);
        expect(result.error).toContain('E_GUARD_PARSER_DEPTH_EXCEEDED');
      }
      expect(
        transpile('SELECT 1', Dialect.Generic, Dialect.Generic).success,
      ).toBe(true);
    });

    it('uses a conservative WASM default without capping explicit overrides', () => {
      const sql = `SELECT ${'+ '.repeat(48)}1`;
      const rejected = transpile(sql, Dialect.Generic, Dialect.Generic);
      expect(rejected.success).toBe(false);
      expect(rejected.error).toContain('configured limit 32');
      expect(
        transpile(sql, Dialect.Generic, Dialect.Generic, {
          complexityGuard: { maxParserDepth: 64 },
        }).sql,
      ).toEqual(['SELECT 1']);
    });

    it('rejects recursive WASM grammar paths before exhausting its stack', () => {
      const cases: [string, Dialect][] = [
        [
          `SELECT ${'CASE WHEN 1 THEN '.repeat(200)}1${' END'.repeat(200)}`,
          Dialect.Generic,
        ],
        [`${'SELECT ('.repeat(200)}1${')'.repeat(200)}`, Dialect.Generic],
        [
          `${'BEGIN TRY '.repeat(200)}SELECT 1${' END TRY BEGIN CATCH SELECT 2 END CATCH'.repeat(200)}`,
          Dialect.TSQL,
        ],
        ...['ARRAY<', 'MAP<INT,', 'STRUCT<x '].map(
          (prefix): [string, Dialect] => [
            `SELECT CAST(x AS ${prefix.repeat(2000)}INT${'>'.repeat(2000)})`,
            Dialect.BigQuery,
          ],
        ),
      ];
      for (const [sql, dialect] of cases) {
        const result = transpile(sql, dialect, dialect);
        expect(result.success).toBe(false);
        expect(result.error).toContain('E_GUARD_PARSER_DEPTH_EXCEEDED');
        expect(result.error).toContain('configured limit 32');
        expect(transpile('SELECT 1', dialect, dialect).success).toBe(true);
      }
    });

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
    it.each([
      Dialect.Snowflake,
      Dialect.DuckDB,
      Dialect.PostgreSQL,
    ])('preserves explicit and omitted null ordering for %s', (dialect) => {
      for (const ordering of [
        'category NULLS LAST, created_at DESC NULLS FIRST',
        'category, created_at DESC',
      ]) {
        const sql = `SELECT id FROM items ORDER BY ${ordering}`;
        for (const result of [
          format(sql, dialect),
          formatWithOptions(sql, dialect, {}),
        ]) {
          expect(result.success).toBe(true);
          expect(result.sql).toHaveLength(1);
          expect(result.sql?.[0].replace(/\s+/g, ' ').trim()).toBe(sql);
          expect(format(result.sql![0], dialect).sql).toEqual(result.sql);
        }
      }
    });

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

describe('Vertica structured syntax and semantics', () => {
  it('retains COPY parser arguments through the public AST API', () => {
    const parsed = parse(
      "COPY t FROM LOCAL '/tmp/data.json' PARSER FJSONPARSER(flatten_maps=TRUE)",
      Dialect.Vertica,
    );
    expect(parsed.success).toBe(true);
    expect(JSON.stringify(parsed.ast)).toContain('"kind":"copy"');
    const output = generate(parsed.ast ?? [], Dialect.Vertica);
    expect(output.success).toBe(true);
    expect(output.sql?.[0]).toContain('FJSONPARSER(flatten_maps = TRUE)');
  });

  it('preserves binary values and rejects unsafe conversions in every mode', () => {
    expect(
      transpile("SELECT B'101100'", Dialect.Vertica, Dialect.DuckDB).sql,
    ).toEqual(["SELECT UNHEX('2c')"]);
    for (const unsupportedLevel of [
      'ignore',
      'warn',
      'raise',
      'immediate',
    ] as const) {
      for (const sql of [
        'SELECT x::!INT FROM t',
        'SELECT LISTAGG(x) FROM t',
        'SELECT id FROM t FOR UPDATE',
      ]) {
        expect(
          transpile(sql, Dialect.Vertica, Dialect.PostgreSQL, {
            unsupportedLevel,
          }).success,
        ).toBe(false);
      }
    }
  });
});
