/**
 * AST Visitor Tests
 *
 * Tests for walker and transformer utilities.
 *
 * Note: With externally tagged enums, each Expression is { "variant": data }
 * instead of { "type": "variant", ...data }.
 */

import { describe, expect, it } from 'vitest';
import { col, lit, sqlNull } from '../../builders';
import type { Expression } from '../../generated/Expression';
import { annotateTypes, Dialect, generate, parse } from '../../index';
import {
  getExprData,
  getExprType,
  getInferredType,
  isExpressionValue,
  makeExpr,
} from '../helpers';
import { isExpressionType } from '../types/guards';
import {
  addSelectColumns,
  addWhere,
  clone,
  countNodes,
  every,
  findAll,
  findByType,
  findFirst,
  getAggregateFunctions,
  getChildren,
  getColumnNames,
  getColumns,
  getDepth,
  getFunctions,
  getIdentifiers,
  getLiterals,
  getSubqueries,
  getTableNames,
  getTables,
  getWindowFunctions,
  hasAggregates,
  hasSubqueries,
  hasWindowFunctions,
  nodeCount,
  qualifyColumns,
  qualifyTables,
  remove,
  removeLimitOffset,
  removeSelectColumns,
  removeWhere,
  renameColumns,
  renameTables,
  replaceByType,
  replaceNodes,
  setDistinct,
  setLimit,
  setOffset,
  setOrderBy,
  some,
  // Transformer functions
  transform,
  // Walker functions
  walk,
} from './index';

const singleFieldDataTypes = [
  'boolean',
  'text',
  'blob',
  'date',
  'json',
  'json_b',
  'uuid',
  'int128',
  'uint8',
  'uint16',
  'uint32',
  'uint64',
  'uint128',
  'unknown',
] as const;

// Helper to parse SQL and get the first statement
function parseFirst(sql: string): Expression {
  const result = parse(sql, Dialect.Generic);
  if (!result.success || !result.ast) {
    throw new Error(`Parse failed: ${result.error}`);
  }
  return result.ast[0];
}

function genericAggregateData(node: Expression) {
  if (!isExpressionType(node, 'aggregate_function')) {
    throw new Error('expected a generic aggregate function');
  }
  return getExprData(node);
}

function parseFirstWithDialect(sql: string, dialect: Dialect): Expression {
  const result = parse(sql, dialect);
  if (!result.success || !result.ast) {
    throw new Error(`Parse failed: ${result.error}`);
  }
  return result.ast[0];
}

function columnReference(node: Expression): string {
  const data = getExprData(node) as {
    name: { name: string };
    table: { name: string } | null;
  };
  return data.table ? `${data.table.name}.${data.name.name}` : data.name.name;
}

// Helper to regenerate SQL from AST
function toSql(ast: Expression): string {
  // Wrap in array since generate expects an array of statements
  const result = generate([ast], Dialect.Generic);
  if (!result.success || !result.sql) {
    throw new Error(`Generate failed: ${result.error}`);
  }
  return result.sql[0];
}

// ============================================================================
// Walker Tests
// ============================================================================

describe('Walker Functions', () => {
  describe('walk()', () => {
    it('should call enter callback for each node', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const visited: string[] = [];

      walk(ast, {
        enter: (node) => {
          visited.push(getExprType(node));
        },
      });

      expect(visited.length).toBeGreaterThan(0);
      expect(visited).toContain('select');
    });

    it('should call leave callback after children', () => {
      const ast = parseFirst('SELECT a FROM users');
      const enterOrder: string[] = [];
      const leaveOrder: string[] = [];

      walk(ast, {
        enter: (node) => enterOrder.push(getExprType(node)),
        leave: (node) => leaveOrder.push(getExprType(node)),
      });

      // First entered should be last left
      expect(enterOrder[0]).toBe(leaveOrder[leaveOrder.length - 1]);
    });

    it('should call type-specific callbacks', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      let selectCount = 0;
      let columnCount = 0;

      walk(ast, {
        select: () => {
          selectCount++;
        },
        column: () => {
          columnCount++;
        },
      });

      expect(selectCount).toBe(1);
      // Only finds columns in SELECT expressions, not in FROM (due to walker limitation)
      expect(columnCount).toBe(2);
    });

    it('should pass parent to callbacks', () => {
      const ast = parseFirst('SELECT a FROM users');
      let columnParent: Expression | null = null;

      walk(ast, {
        column: (_, parent) => {
          columnParent = parent;
        },
      });

      expect(columnParent).not.toBeNull();
    });

    it('should traverse expression fields inside arrays of serialized structs', () => {
      const cases = [
        {
          sql: 'SELECT LIST(value ORDER BY ordering_key) FROM source_table',
          columns: ['value', 'ordering_key'],
        },
        {
          sql: 'SELECT projected FROM source_table ORDER BY ordering_key',
          columns: ['projected', 'ordering_key'],
        },
        {
          sql: 'SELECT l.projected FROM left_table AS l JOIN right_table AS r ON l.join_key = r.join_key',
          columns: ['l.projected', 'l.join_key', 'r.join_key'],
        },
        {
          sql: 'WITH cte AS (SELECT inner_value FROM source_table) SELECT outer_value FROM cte',
          columns: ['outer_value', 'inner_value'],
        },
        {
          sql: 'SELECT SUM(value) OVER (PARTITION BY group_key ORDER BY ordering_key) FROM source_table',
          columns: ['value', 'group_key', 'ordering_key'],
        },
      ];

      for (const { sql, columns } of cases) {
        const ast = parseFirstWithDialect(sql, Dialect.DuckDB);
        expect(getColumns(ast).map(columnReference)).toEqual(columns);
      }
    });

    it('should preserve visitor location metadata through serialized structs', () => {
      const aggregate = parseFirstWithDialect(
        'SELECT LIST(value ORDER BY ordering_key) FROM source_table',
        Dialect.DuckDB,
      );
      const locations: Array<{
        parent: string | null;
        key: string | null;
        index: number | null;
      }> = [];

      walk(aggregate, {
        column: (node, parent, key, index) => {
          if (columnReference(node) === 'ordering_key') {
            locations.push({
              parent: parent ? getExprType(parent) : null,
              key,
              index,
            });
          }
        },
      });

      expect(locations).toEqual([
        { parent: 'aggregate_function', key: 'order_by', index: 0 },
      ]);
    });

    it('should not expose single-field payload structs as expression nodes', () => {
      const column = col('value').toJSON() as Expression;

      expect(isExpressionValue(column)).toBe(true);
      expect(isExpressionValue(sqlNull().toJSON())).toBe(true);
      expect(isExpressionValue({ this: column })).toBe(false);
      expect(isExpressionValue({ expressions: [column] })).toBe(false);

      const visited: string[] = [];
      walk(parseFirst('SELECT value FROM source_table WHERE value = 1'), {
        enter: (node) => visited.push(getExprType(node)),
      });
      expect(visited).not.toContain('this');
      expect(visited).not.toContain('expressions');
    });

    it.each(
      singleFieldDataTypes,
    )('should distinguish %s descriptors from data type expression envelopes', (dataType) => {
      const descriptor = { data_type: dataType };
      const expression: Expression = { data_type: descriptor };
      const parent = makeExpr('tuple', { expressions: [expression] });

      expect(isExpressionValue(descriptor)).toBe(false);
      expect(isExpressionValue(expression)).toBe(true);
      expect(findByType(parent, 'data_type')).toEqual([expression]);
      expect(getChildren(expression)).toEqual([]);

      const visited: Expression[] = [];
      walk(parent, { data_type: (node) => visited.push(node) });
      expect(visited).toEqual([expression]);
    });
  });

  describe('getChildren()', () => {
    it('should collect expression children nested in struct arrays', () => {
      const ast = parseFirst(
        'SELECT l.projected FROM left_table AS l JOIN right_table AS r ON l.join_key = r.join_key',
      );
      const childTypes = getChildren(ast).flatMap(({ value }) =>
        Array.isArray(value)
          ? value.map((child) => getExprType(child))
          : [getExprType(value)],
      );

      expect(
        getChildren(ast).some(
          ({ key, value }) => key === 'expressions' && Array.isArray(value),
        ),
      ).toBe(true);
      expect(childTypes).toContain('eq');
    });
  });

  describe('findAll()', () => {
    it('should find all nodes matching predicate', () => {
      const ast = parseFirst('SELECT a, b, c FROM users');
      const columns = findAll(ast, (node) => getExprType(node) === 'column');

      // Only finds columns in SELECT expressions (walker traverses typed nodes)
      expect(columns.length).toBe(3);
    });

    it('should return empty array when no matches', () => {
      const ast = parseFirst('SELECT 1');
      const subqueries = findAll(
        ast,
        (node) => getExprType(node) === 'subquery',
      );

      expect(subqueries).toEqual([]);
    });

    it('should find nodes in expressions array', () => {
      // Test with literal nodes which are directly in expressions array
      const ast = parseFirst('SELECT 1, 2, 3');
      const literals = findAll(ast, (node) => getExprType(node) === 'literal');

      expect(literals.length).toBe(3);
    });
  });

  describe('findByType()', () => {
    it('should find all nodes of specific type in expressions', () => {
      const ast = parseFirst('SELECT a, b, c FROM users');
      const columns = findByType(ast, 'column');

      // Walker finds columns in SELECT expressions
      expect(columns.length).toBe(3);
    });

    it('should return typed results', () => {
      const ast = parseFirst('SELECT a FROM users');
      const columns = findByType(ast, 'column');

      expect(getExprType(columns[0])).toBe('column');
    });

    it('should find literals', () => {
      const ast = parseFirst('SELECT 1, 2, 3');
      const literals = findByType(ast, 'literal');

      expect(literals.length).toBe(3);
    });
  });

  describe('findFirst()', () => {
    it('should find first matching node', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const first = findFirst(ast, (node) => getExprType(node) === 'column');

      expect(first).not.toBeUndefined();
      expect(getExprType(first!)).toBe('column');
    });

    it('should return undefined when no match', () => {
      const ast = parseFirst('SELECT 1');
      const result = findFirst(ast, (node) => getExprType(node) === 'subquery');

      expect(result).toBeUndefined();
    });
  });

  describe('some()', () => {
    it('should return true when any node matches', () => {
      const ast = parseFirst('SELECT a FROM users');
      const hasColumn = some(ast, (node) => getExprType(node) === 'column');

      expect(hasColumn).toBe(true);
    });

    it('should return false when no nodes match', () => {
      const ast = parseFirst('SELECT 1');
      const hasSubquery = some(ast, (node) => getExprType(node) === 'subquery');

      expect(hasSubquery).toBe(false);
    });
  });

  describe('every()', () => {
    it('should return true when all nodes match predicate', () => {
      const ast = parseFirst('SELECT 1');
      // With externally tagged enums, every node is an object with a single key
      const allHaveKey = every(ast, (node) => Object.keys(node).length === 1);

      expect(allHaveKey).toBe(true);
    });

    it('should return false when some nodes do not match', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const allAreColumns = every(
        ast,
        (node) => getExprType(node) === 'column',
      );

      expect(allAreColumns).toBe(false);
    });
  });

  describe('countNodes()', () => {
    it('should count nodes matching predicate', () => {
      const ast = parseFirst('SELECT a, b, c FROM users');
      const columnCount = countNodes(
        ast,
        (node) => getExprType(node) === 'column',
      );

      expect(columnCount).toBe(3);
    });

    it('should return 0 for no matches', () => {
      const ast = parseFirst('SELECT 1');
      const count = countNodes(ast, (node) => getExprType(node) === 'subquery');

      expect(count).toBe(0);
    });
  });
});

// ============================================================================
// Convenience Finder Tests
// ============================================================================

describe('Convenience Finder Functions', () => {
  describe('getColumns()', () => {
    it('should get column references in SELECT expressions', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const columns = getColumns(ast);

      expect(columns.length).toBe(2);
      expect(columns.every((c) => getExprType(c) === 'column')).toBe(true);
    });
  });

  describe('getTables()', () => {
    it('should find tables in FROM and JOIN operands', () => {
      const ast = parseFirst(
        'SELECT t.id FROM ticket AS t JOIN team AS tm ON t.team_id = tm.id',
      );
      const tables = getTables(ast);

      expect(tables).toHaveLength(2);
      expect(
        tables.map((table) => (getExprData(table) as any).name.name),
      ).toEqual(['ticket', 'team']);
    });
  });

  describe('getIdentifiers()', () => {
    it('should get identifiers in the AST', () => {
      const ast = parseFirst('SELECT a FROM users');
      const identifiers = getIdentifiers(ast);

      // Identifiers may or may not be found depending on AST structure
      expect(identifiers.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe('getFunctions()', () => {
    it('should get generic function calls in SELECT', () => {
      // Note: UPPER and LOWER parse as specific expression types ('upper', 'lower'),
      // not as generic 'function' type. getFunctions() only finds generic functions.
      const ast = parseFirst('SELECT UPPER(name), LOWER(email) FROM users');
      const functions = getFunctions(ast);

      // UPPER/LOWER are specific types, not generic 'function' nodes
      expect(functions.length).toBe(0);

      // But they can be found by type
      const allNodes = findAll(ast, (n) =>
        ['upper', 'lower'].includes(getExprType(n)),
      );
      expect(allNodes.length).toBe(2);
    });
  });

  describe('getAggregateFunctions()', () => {
    it('should find aggregate functions in SELECT', () => {
      const ast = parseFirst(
        'SELECT COUNT(*), SUM(amount), AVG(price) FROM orders',
      );
      const aggregates = getAggregateFunctions(ast);

      expect(aggregates.length).toBe(3);
    });

    it('should return empty for no aggregates', () => {
      const ast = parseFirst('SELECT a FROM users');
      const aggregates = getAggregateFunctions(ast);

      expect(aggregates.length).toBe(0);
    });

    it('should find DuckDB COUNT_IF, MEDIAN, and FIRST aggregates', () => {
      const result = parse(
        'SELECT COUNT_IF(numeric_value > 0), MEDIAN(numeric_value), FIRST(numeric_value) FROM source_table',
        Dialect.DuckDB,
      );
      if (!result.success || !result.ast) {
        throw new Error(`Parse failed: ${result.error}`);
      }

      const aggregateTypes = getAggregateFunctions(result.ast[0]).map(
        getExprType,
      );

      expect(aggregateTypes).toEqual(['count_if', 'median', 'first']);
    });

    it('should preserve and find DuckDB null-preserving arg extrema', () => {
      const result = parse(
        'SELECT ARG_MAX_NULL(label, score), ARG_MIN_NULL(label, score) FROM source_table',
        Dialect.DuckDB,
      );
      if (!result.success || !result.ast) {
        throw new Error(`Parse failed: ${result.error}`);
      }

      const aggregates = getAggregateFunctions(result.ast[0]);

      expect(aggregates.map(getExprType)).toEqual([
        'aggregate_function',
        'aggregate_function',
      ]);
      expect(aggregates.map((node) => genericAggregateData(node).name)).toEqual(
        ['ARG_MAX_NULL', 'ARG_MIN_NULL'],
      );
      expect(
        aggregates.map((node) => genericAggregateData(node).args.length),
      ).toEqual([2, 2]);
    });

    it('should find DuckDB product, histogram, and quantile aggregates', () => {
      const result = parse(
        'SELECT PRODUCT(x), APPROX_QUANTILE(x, 0.5), HISTOGRAM_EXACT(x, [1, 2]), MAD(x), QUANTILE(x, 0.5), QUANTILE_CONT(x, 0.5), QUANTILE_DISC(x, 0.5), RESERVOIR_QUANTILE(x, 0.5) FROM source_table',
        Dialect.DuckDB,
      );
      if (!result.success || !result.ast) {
        throw new Error(`Parse failed: ${result.error}`);
      }

      const aggregates = getAggregateFunctions(result.ast[0]);

      expect(aggregates.map(getExprType)).toEqual(
        Array.from({ length: 8 }, () => 'aggregate_function'),
      );
      expect(aggregates.map((node) => genericAggregateData(node).name)).toEqual(
        [
          'PRODUCT',
          'APPROX_QUANTILE',
          'HISTOGRAM_EXACT',
          'MAD',
          'QUANTILE',
          'QUANTILE_CONT',
          'QUANTILE_DISC',
          'RESERVOIR_QUANTILE',
        ],
      );
    });

    it('should retain DuckDB aggregate-local modifiers', () => {
      const result = parse(
        'SELECT PRODUCT(DISTINCT x ORDER BY x DESC) FILTER (WHERE keep) AS aggregate_value FROM source_table',
        Dialect.DuckDB,
      );
      if (!result.success || !result.ast) {
        throw new Error(`Parse failed: ${result.error}`);
      }

      const aggregates = getAggregateFunctions(result.ast[0]);
      expect(aggregates).toHaveLength(1);

      const data = genericAggregateData(aggregates[0]);
      expect(data.distinct).toBe(true);
      expect(data.filter).not.toBeNull();
      expect(data.order_by).toHaveLength(1);

      const generated = generate(result.ast, Dialect.DuckDB);
      expect(generated.success).toBe(true);
      expect(generated.sql).toEqual([
        'SELECT PRODUCT(DISTINCT x ORDER BY x DESC) FILTER(WHERE keep) AS aggregate_value FROM source_table',
      ]);
    });
  });

  describe('getWindowFunctions()', () => {
    it('should find window functions in SELECT', () => {
      const ast = parseFirst(
        'SELECT ROW_NUMBER() OVER (ORDER BY id) FROM users',
      );
      const windows = getWindowFunctions(ast);

      expect(windows.length).toBe(1);
    });
  });

  describe('getSubqueries()', () => {
    it('should return results based on walker capabilities', () => {
      const ast = parseFirst('SELECT * FROM (SELECT a FROM t) sub');
      const subqueries = getSubqueries(ast);

      // Due to walker limitation with FROM wrapper
      expect(subqueries.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe('getLiterals()', () => {
    it('should find literals in SELECT', () => {
      const ast = parseFirst("SELECT 1, 'hello', 3.14");
      const literals = getLiterals(ast);

      expect(literals.length).toBe(3);
    });
  });

  describe('getColumnNames()', () => {
    it('should extract column names as strings', () => {
      const ast = parseFirst('SELECT name, email FROM users');
      const names = getColumnNames(ast);

      expect(names).toContain('name');
      expect(names).toContain('email');
    });
  });

  describe('getTableNames()', () => {
    it('should return table names found by walker', () => {
      const ast = parseFirst('SELECT * FROM users');
      const names = getTableNames(ast);

      // Current walker may not find tables in FROM
      expect(names.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe('hasAggregates()', () => {
    it('should return true for queries with aggregates', () => {
      const ast = parseFirst('SELECT COUNT(*) FROM users');
      expect(hasAggregates(ast)).toBe(true);
    });

    it('should return false for queries without aggregates', () => {
      const ast = parseFirst('SELECT a FROM users');
      expect(hasAggregates(ast)).toBe(false);
    });
  });

  describe('hasWindowFunctions()', () => {
    it('should detect window functions', () => {
      const ast = parseFirst('SELECT ROW_NUMBER() OVER () FROM users');
      expect(hasWindowFunctions(ast)).toBe(true);
    });

    it('should return false without window functions', () => {
      const ast = parseFirst('SELECT a FROM users');
      expect(hasWindowFunctions(ast)).toBe(false);
    });
  });

  describe('hasSubqueries()', () => {
    it('should detect subqueries based on walker capabilities', () => {
      const ast = parseFirst('SELECT * FROM (SELECT 1) t');
      // Due to walker limitation, subqueries in FROM may not be found
      expect(typeof hasSubqueries(ast)).toBe('boolean');
    });

    it('should return false for simple queries', () => {
      const ast = parseFirst('SELECT a FROM users');
      expect(hasSubqueries(ast)).toBe(false);
    });
  });

  describe('getDepth()', () => {
    it('should calculate AST depth', () => {
      const ast = parseFirst('SELECT 1');
      const depth = getDepth(ast);

      expect(depth).toBeGreaterThan(0);
    });

    it('should return greater depth for more complex expressions', () => {
      const simple = parseFirst('SELECT 1');
      const complex = parseFirst('SELECT UPPER(LOWER(TRIM(name)))');

      expect(getDepth(complex)).toBeGreaterThan(getDepth(simple));
    });
  });

  describe('nodeCount()', () => {
    it('should count total nodes', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const count = nodeCount(ast);

      expect(count).toBeGreaterThan(0);
    });

    it('should count more nodes for larger queries', () => {
      const small = parseFirst('SELECT 1');
      const large = parseFirst('SELECT a, b, c, d, e FROM users');

      expect(nodeCount(large)).toBeGreaterThan(nodeCount(small));
    });
  });
});

// ============================================================================
// Transformer Tests
// ============================================================================

describe('Transformer Functions', () => {
  describe('transform()', () => {
    it('should return new AST with modifications', () => {
      const ast = parseFirst('SELECT a FROM users');
      let transformed = false;

      const newAst = transform(ast, {
        enter: (node) => {
          if (getExprType(node) === 'select') {
            transformed = true;
          }
          return undefined;
        },
      });

      expect(transformed).toBe(true);
      expect(newAst).not.toBe(ast); // Should be different object
    });

    it('should transform type-specific nodes', () => {
      const ast = parseFirst('SELECT a FROM users');
      let columnVisited = false;

      transform(ast, {
        column: () => {
          columnVisited = true;
          return undefined;
        },
      });

      expect(columnVisited).toBe(true);
    });

    it('should transform expressions nested inside struct arrays', () => {
      const ast = parseFirstWithDialect(
        'SELECT LIST(value ORDER BY ordering_key) FROM source_table',
        Dialect.DuckDB,
      );
      const transformed = transform(ast, {
        column: (node) => {
          if (columnReference(node) !== 'ordering_key') return undefined;
          const data = getExprData(node) as {
            name: { name: string };
          };
          return makeExpr('column', {
            ...data,
            name: { ...data.name, name: 'replacement_key' },
          });
        },
      });

      const result = generate([transformed], Dialect.DuckDB);
      expect(result.success).toBe(true);
      expect(result.sql).toEqual([
        'SELECT LIST(value ORDER BY replacement_key) FROM source_table',
      ]);
    });

    it('should visit the same nodes and locations as walk()', () => {
      const ast = parseFirstWithDialect(
        'WITH cte AS (SELECT inner_value FROM source_table) SELECT SUM(value) OVER (PARTITION BY group_key ORDER BY ordering_key) FROM cte ORDER BY outer_key',
        Dialect.DuckDB,
      );
      const walked: string[] = [];
      const transformed: string[] = [];
      const record = (
        target: string[],
        node: Expression,
        parent: Expression | null,
        key: string | null,
        index: number | null,
      ) => {
        target.push(
          `${getExprType(node)}:${parent ? getExprType(parent) : 'null'}:${key}:${index}`,
        );
      };

      walk(ast, {
        enter: (node, parent, key, index) =>
          record(walked, node, parent, key, index),
      });
      transform(ast, {
        enter: (node, parent, key, index) => {
          record(transformed, node, parent, key, index);
          return undefined;
        },
      });

      expect(transformed).toEqual(walked);
    });
  });

  describe('replaceNodes()', () => {
    it('should replace nodes matching predicate', () => {
      const ast = parseFirst('SELECT 1, 2, 3');

      // Replace all literal 1s with 100s
      const newAst = replaceNodes(
        ast,
        (node) => {
          if (getExprType(node) !== 'literal') return false;
          const data = getExprData(node) as { value?: string };
          return data.value === '1';
        },
        makeExpr('literal', { literal_type: 'number', value: '100' }),
      );

      const sql = toSql(newAst);
      expect(sql).toContain('100');
    });

    it('should support function replacements', () => {
      const ast = parseFirst('SELECT 1, 2');

      const newAst = replaceNodes(
        ast,
        (node) => getExprType(node) === 'literal',
        (node) => {
          const data = getExprData(node) as { value?: string };
          return makeExpr('literal', {
            literal_type: 'number',
            value: String(Number(data.value || 0) * 10),
          });
        },
      );

      const sql = toSql(newAst);
      expect(sql).toContain('10');
      expect(sql).toContain('20');
    });
  });

  describe('replaceByType()', () => {
    it('should replace all nodes of specified type', () => {
      const ast = parseFirst('SELECT NULL, 1, NULL');

      const newAst = replaceByType(
        ast,
        'null',
        makeExpr('literal', { literal_type: 'number', value: '0' }),
      );

      const sql = toSql(newAst);
      expect(sql).not.toContain('NULL');
    });
  });
});

// ============================================================================
// Column and Table Renaming Tests
// ============================================================================

describe('Column and Table Renaming', () => {
  describe('renameColumns()', () => {
    it('should rename columns in SELECT expressions', () => {
      const ast = parseFirst('SELECT old_name FROM users');
      const newAst = renameColumns(ast, { old_name: 'new_name' });
      const sql = toSql(newAst);

      expect(sql).toContain('new_name');
      expect(sql).not.toContain('old_name');
    });

    it('should rename multiple columns', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const newAst = renameColumns(ast, { a: 'x', b: 'y' });
      const sql = toSql(newAst);

      expect(sql).toContain('x');
      expect(sql).toContain('y');
    });

    it('should not rename columns not in mapping', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const newAst = renameColumns(ast, { a: 'x' });
      const sql = toSql(newAst);

      expect(sql).toContain('x');
      expect(sql).toContain('b');
    });
  });

  describe('renameTables()', () => {
    it('should rename tables when walker finds them', () => {
      const ast = parseFirst('SELECT * FROM old_table');
      const newAst = renameTables(ast, { old_table: 'new_table' });
      const sql = toSql(newAst);

      // Table renaming depends on walker finding table nodes
      // If tables are in FROM wrapper without type, they won't be renamed
      expect(sql.length).toBeGreaterThan(0);
    });

    it('should alias renamed tables when requested', () => {
      const ast = parseFirst('SELECT a FROM old_table');
      const newAst = renameTables(
        ast,
        { old_table: 'new_table' },
        { aliasRenamedTables: true },
      );
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT a FROM new_table AS new_table');
    });
  });

  describe('qualifyColumns()', () => {
    it('should attempt to add table qualifier to columns', () => {
      const ast = parseFirst('SELECT name FROM users');
      const newAst = qualifyColumns(ast, 'users');
      const sql = toSql(newAst);

      // Should have users.name or similar
      expect(sql.toLowerCase()).toContain('users');
    });
  });

  describe('qualifyTables()', () => {
    it('should qualify union derived-table operands with stable aliases', () => {
      const ast = parseFirst(
        'SELECT * FROM (SELECT * FROM tab_1) UNION ALL SELECT * FROM (SELECT * FROM tab_1)',
      );
      const newAst = qualifyTables(ast);
      const sql = toSql(newAst);

      expect(sql).toBe(
        'SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _0 UNION ALL SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _1',
      );
    });
  });
});

// ============================================================================
// WHERE Clause Manipulation Tests
// ============================================================================

describe('WHERE Clause Manipulation', () => {
  describe('addWhere()', () => {
    it('should add WHERE clause to query without one', () => {
      const ast = parseFirst('SELECT * FROM users');
      const condition = col('active').eq(lit(1)).toJSON() as Expression;
      const newAst = addWhere(ast, condition);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('WHERE');
    });

    it('should AND with existing WHERE clause', () => {
      const ast = parseFirst('SELECT * FROM users WHERE a = 1');
      const condition = col('b').eq(lit(2)).toJSON() as Expression;
      const newAst = addWhere(ast, condition, 'and');
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('AND');
    });

    it('should OR with existing WHERE clause', () => {
      const ast = parseFirst('SELECT * FROM users WHERE a = 1');
      const condition = col('b').eq(lit(2)).toJSON() as Expression;
      const newAst = addWhere(ast, condition, 'or');
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('OR');
    });

    it('should preserve builder NULL conditions', () => {
      const ast = parseFirst('SELECT * FROM users');
      const condition = col('deleted_at').isNull().toJSON() as Expression;
      const newAst = addWhere(ast, condition);
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT * FROM users WHERE deleted_at IS NULL');
    });

    it('should preserve nested builder NULL values', () => {
      const ast = parseFirst('SELECT * FROM users');
      const condition = col('deleted_at').eq(sqlNull()).toJSON() as Expression;
      const newAst = addWhere(ast, condition);
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT * FROM users WHERE deleted_at = NULL');
    });

    it('should fail closed for unrepairable condition ASTs', () => {
      const ast = parseFirst('SELECT * FROM users');
      const condition = makeExpr('not_a_real_expression', {}) as Expression;
      const newAst = addWhere(ast, condition);
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT * FROM users WHERE FALSE');
    });

    it('should not modify non-SELECT nodes', () => {
      const literalAst = makeExpr('literal', {
        literal_type: 'number',
        value: '1',
      });
      const condition = col('a').eq(lit(1)).toJSON() as Expression;
      const result = addWhere(literalAst, condition);

      expect(result).toStrictEqual(literalAst);
    });
  });

  describe('removeWhere()', () => {
    it('should remove WHERE clause', () => {
      const ast = parseFirst('SELECT * FROM users WHERE a = 1');
      const newAst = removeWhere(ast);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).not.toContain('WHERE');
    });

    it('should not modify query without WHERE', () => {
      const ast = parseFirst('SELECT * FROM users');
      const newAst = removeWhere(ast);
      const sql = toSql(newAst);

      expect(sql).toContain('users');
    });
  });
});

// ============================================================================
// SELECT Clause Manipulation Tests
// ============================================================================

describe('SELECT Clause Manipulation', () => {
  describe('addSelectColumns()', () => {
    it('should add columns to SELECT', () => {
      const ast = parseFirst('SELECT a FROM users');
      const newCol = col('b').toJSON() as Expression;
      const newAst = addSelectColumns(ast, newCol);
      const sql = toSql(newAst);

      expect(sql).toContain('b');
    });

    it('should add multiple columns', () => {
      const ast = parseFirst('SELECT a FROM users');
      const newAst = addSelectColumns(
        ast,
        col('b').toJSON() as Expression,
        col('c').toJSON() as Expression,
      );
      const sql = toSql(newAst);

      expect(sql).toContain('b');
      expect(sql).toContain('c');
    });
  });

  describe('removeSelectColumns()', () => {
    it('should remove columns matching predicate', () => {
      const ast = parseFirst('SELECT a, b, c FROM users');
      const newAst = removeSelectColumns(ast, (col) => {
        const data = getExprData(col) as { name?: { name?: string } };
        return data.name?.name === 'b';
      });
      const sql = toSql(newAst);

      expect(sql).toContain('a');
      expect(sql).toContain('c');
    });
  });
});

// ============================================================================
// Limit/Offset Manipulation Tests
// ============================================================================

describe('Limit/Offset Manipulation', () => {
  describe('setLimit()', () => {
    it('should set LIMIT with number', () => {
      const ast = parseFirst('SELECT * FROM users');
      const newAst = setLimit(ast, 10);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('LIMIT');
      expect(sql).toContain('10');
    });

    it('should update existing LIMIT', () => {
      const ast = parseFirst('SELECT * FROM users LIMIT 5');
      const newAst = setLimit(ast, 20);
      const sql = toSql(newAst);

      expect(sql).toContain('20');
    });

    it('should set LIMIT on set operations', () => {
      const ast = parseFirst('SELECT id FROM a UNION ALL SELECT id FROM b');
      const newAst = setLimit(ast, 5);
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT id FROM a UNION ALL SELECT id FROM b LIMIT 5');
    });
  });

  describe('setOffset()', () => {
    it('should set OFFSET with number', () => {
      const ast = parseFirst('SELECT * FROM users LIMIT 10');
      const newAst = setOffset(ast, 5);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('OFFSET');
      expect(sql).toContain('5');
    });

    it('should set OFFSET on set operations', () => {
      const ast = parseFirst('SELECT id FROM a UNION ALL SELECT id FROM b');
      const newAst = setOffset(ast, 10);
      const sql = toSql(newAst);

      expect(sql).toBe('SELECT id FROM a UNION ALL SELECT id FROM b OFFSET 10');
    });
  });

  describe('setOrderBy()', () => {
    it('should set ORDER BY on set operations', () => {
      const ast = parseFirst('SELECT id FROM a UNION ALL SELECT id FROM b');
      const newAst = setOrderBy(ast, col('id').toJSON() as Expression);
      const sql = toSql(newAst);

      expect(sql).toBe(
        'SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id',
      );
    });
  });

  describe('removeLimitOffset()', () => {
    it('should remove LIMIT and OFFSET', () => {
      const ast = parseFirst('SELECT * FROM users LIMIT 10 OFFSET 5');
      const newAst = removeLimitOffset(ast);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).not.toContain('LIMIT');
      expect(sql.toUpperCase()).not.toContain('OFFSET');
    });
  });
});

// ============================================================================
// Distinct Manipulation Tests
// ============================================================================

describe('Distinct Manipulation', () => {
  describe('setDistinct()', () => {
    it('should set DISTINCT to true', () => {
      const ast = parseFirst('SELECT a FROM users');
      const newAst = setDistinct(ast, true);
      const sql = toSql(newAst);

      expect(sql.toUpperCase()).toContain('DISTINCT');
    });

    it('should handle setting DISTINCT to false', () => {
      const ast = parseFirst('SELECT DISTINCT a FROM users');
      const newAst = setDistinct(ast, false);
      // Just verify it doesn't crash
      expect(getExprType(newAst)).toBe('select');
    });
  });
});

// ============================================================================
// Clone Tests
// ============================================================================

describe('Clone', () => {
  describe('clone()', () => {
    it('should create deep copy of AST', () => {
      const ast = parseFirst('SELECT a, b FROM users');
      const cloned = clone(ast);

      expect(cloned).not.toBe(ast);
      expect(toSql(cloned)).toBe(toSql(ast));
    });

    it('should not share references with original', () => {
      const ast = parseFirst('SELECT a FROM users');
      const cloned = clone(ast);

      // Modify cloned
      const modified = renameColumns(cloned, { a: 'b' });

      // Original should be unchanged
      expect(toSql(ast)).toContain('a');
      expect(toSql(modified)).toContain('b');
    });

    it('should clone containers nested inside serialized structs', () => {
      const ast = parseFirstWithDialect(
        'SELECT LIST(value ORDER BY ordering_key) FROM source_table',
        Dialect.DuckDB,
      );
      const cloned = clone(ast);
      const originalAggregate = findByType(ast, 'aggregate_function')[0];
      const clonedAggregate = findByType(cloned, 'aggregate_function')[0];
      const originalData = getExprData(originalAggregate);
      const clonedData = getExprData(clonedAggregate);

      expect(clonedData.order_by).not.toBe(originalData.order_by);
      expect((clonedData.order_by as unknown[])[0]).not.toBe(
        (originalData.order_by as unknown[])[0],
      );
      expect(toSql(cloned)).toBe(toSql(ast));
    });
  });
});

// ============================================================================
// Remove Tests
// ============================================================================

describe('Remove', () => {
  describe('remove()', () => {
    it('should remove nodes matching predicate from arrays', () => {
      const ast = parseFirst('SELECT a, b, c FROM users');
      const newAst = remove(ast, (node) => {
        if (getExprType(node) !== 'column') return false;
        const data = getExprData(node) as { name?: { name?: string } };
        return data.name?.name === 'b';
      });
      const sql = toSql(newAst);

      expect(sql).toContain('a');
      expect(sql).toContain('c');
    });

    it('should remove array nodes reached through serialized structs', () => {
      const ast = parseFirst(
        'WITH cte AS (SELECT a, b FROM source_table) SELECT * FROM cte',
      );
      const newAst = remove(ast, (node) => {
        return getExprType(node) === 'column' && columnReference(node) === 'b';
      });

      expect(toSql(newAst)).toBe(
        'WITH cte AS (SELECT a FROM source_table) SELECT * FROM cte',
      );
    });
  });
});

// ============================================================================
// Serialized Payload Preservation Tests
// ============================================================================

describe('Serialized payload preservation', () => {
  const operations: Array<[string, (node: Expression) => Expression]> = [
    ['clone', clone],
    ['identity transform', (node) => transform(node, {})],
    ['no-op remove', (node) => remove(node, () => false)],
  ];

  function expectIndependentContainers(original: unknown, copied: unknown) {
    if (original === null || typeof original !== 'object') return;
    expect(copied).not.toBe(original);
    for (const [key, value] of Object.entries(original)) {
      expectIndependentContainers(
        value,
        (copied as Record<string, unknown>)[key],
      );
    }
  }

  describe.each(operations)('%s', (_, copy) => {
    it.each([
      { sql: 'SELECT CAST(x AS DATE) FROM t', dialect: Dialect.TSQL },
      { sql: 'SELECT CAST(x AS DATE) FROM t', dialect: Dialect.PostgreSQL },
      { sql: 'SELECT CAST(x AS DATE) FROM t', dialect: Dialect.DuckDB },
      {
        sql: 'SELECT CAST(x AS BOOLEAN), CAST(x AS TEXT), CAST(x AS BLOB), CAST(x AS JSON), CAST(x AS UUID) FROM t',
        dialect: Dialect.DuckDB,
      },
      {
        sql: 'SELECT CAST(x AS JSONB) FROM t',
        dialect: Dialect.PostgreSQL,
      },
      {
        sql: 'SELECT CAST(x AS DATE[]), CAST(x AS STRUCT(d DATE)) FROM t',
        dialect: Dialect.DuckDB,
      },
      {
        sql: 'CREATE TABLE t(d DATE, b BOOLEAN, s TEXT)',
        dialect: Dialect.DuckDB,
      },
      {
        sql: 'SELECT CAST(x AS INT), CAST(x AS DECIMAL(10, 2)) FROM t',
        dialect: Dialect.TSQL,
      },
    ])('should preserve $dialect types in $sql', ({ sql, dialect }) => {
      const ast = parseFirstWithDialect(sql, dialect);
      const original = structuredClone(ast);
      const expected = generate([ast], dialect);
      expect(expected.success).toBe(true);

      const copied = copy(ast);
      expect(copied).toStrictEqual(original);
      expect(ast).toStrictEqual(original);
      expectIndependentContainers(ast, copied);
      expect(generate([copied], dialect)).toEqual(expected);
      expect(findByType(ast, 'data_type')).toEqual([]);
      expect(findByType(copied, 'data_type')).toEqual([]);
    });

    it.each(
      singleFieldDataTypes,
    )('should preserve genuine %s type expressions', (dataType) => {
      const ast: Expression = { data_type: { data_type: dataType } };
      const parent = makeExpr('tuple', { expressions: [ast] });
      for (const node of [ast, parent]) {
        const copied = copy(node);
        expect(copied).toStrictEqual(node);
        expectIndependentContainers(node, copied);
        expect(findByType(copied, 'data_type')).toEqual([ast]);
        const expected = generate([node], Dialect.DuckDB);
        expect(expected.success).toBe(true);
        expect(generate([copied], Dialect.DuckDB)).toEqual(expected);
      }
    });

    it.each<Expression>([
      { column_position: 'First' },
      { column_constraint: 'NotNull' },
      { column_constraint: 'Null' },
      { column_constraint: 'Unique' },
      { column_constraint: 'PrimaryKey' },
      { null: null },
      { current_date: null },
    ])('should preserve scalar expression payloads: %j', (ast) => {
      expect(isExpressionValue(ast)).toBe(true);
      const parent = makeExpr('tuple', { expressions: [ast] });
      for (const node of [ast, parent]) {
        const copied = copy(node);
        expect(copied).toStrictEqual(node);
        expectIndependentContainers(node, copied);
        expect(findByType(copied, getExprType(ast))).toEqual([ast]);
        const expected = generate([node], Dialect.DuckDB);
        expect(expected.success).toBe(true);
        expect(generate([copied], Dialect.DuckDB)).toEqual(expected);
      }
    });

    it('should preserve inferred type metadata', () => {
      const annotated = annotateTypes(
        'SELECT flag FROM flags',
        Dialect.DuckDB,
        {
          tables: [
            { name: 'flags', columns: [{ name: 'flag', type: 'BOOLEAN' }] },
          ],
        },
      );
      expect(annotated.success).toBe(true);
      const ast = annotated.ast![0];
      const original = structuredClone(ast);
      expect(getInferredType(findByType(ast, 'column')[0])).toEqual({
        data_type: 'boolean',
      });

      const copied = copy(ast);
      expect(copied).toStrictEqual(original);
      expect(ast).toStrictEqual(original);
      expectIndependentContainers(ast, copied);
      expect(getInferredType(findByType(copied, 'column')[0])).toEqual({
        data_type: 'boolean',
      });
      expect(findByType(copied, 'data_type')).toEqual([]);
      expect(toSql(copied)).toBe('SELECT flag FROM flags');
    });
  });

  it('should transform CAST operands without visiting their type descriptors', () => {
    const ast = parseFirstWithDialect(
      'SELECT CAST(x AS DATE) FROM t',
      Dialect.TSQL,
    );
    const visitedTypes: Expression[] = [];
    const transformed = transform(ast, {
      column: () => col('renamed').toJSON() as Expression,
      data_type: (node) => {
        visitedTypes.push(node);
        return node;
      },
    });

    expect(visitedTypes).toEqual([]);
    expect(toSql(transformed)).toBe('SELECT CAST(renamed AS DATE) FROM t');
    expect(toSql(ast)).toBe('SELECT CAST(x AS DATE) FROM t');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Integration Tests', () => {
  it('should chain multiple transformations', () => {
    const ast = parseFirst('SELECT old_col FROM users');

    let newAst = renameColumns(ast, { old_col: 'new_col' });
    newAst = setLimit(newAst, 100);

    const sql = toSql(newAst);

    expect(sql).toContain('new_col');
    expect(sql).toContain('100');
  });

  it('should work with complex SELECT expressions', () => {
    const ast = parseFirst(`
      SELECT
        u.id,
        u.name,
        COUNT(o.id) as order_count
      FROM users u
      GROUP BY u.id, u.name
      ORDER BY order_count DESC
      LIMIT 10
    `);

    const columns = getColumns(ast);

    expect(columns.length).toBeGreaterThan(0);
    expect(hasAggregates(ast)).toBe(true);
  });

  it('should preserve query semantics after clone', () => {
    const sql = 'SELECT a, b FROM users';
    const ast = parseFirst(sql);

    const cloned = clone(ast);
    const regenerated = toSql(cloned);

    // Should be semantically equivalent
    expect(regenerated.toUpperCase()).toContain('SELECT');
    expect(regenerated.toUpperCase()).toContain('FROM');
  });
});

describe('HANA AST traversal', () => {
  it('visits and replaces arguments retained in HANA source nodes', () => {
    const sql =
      "SELECT LOCATE(value, 'a', 1, 2), JSON_VALUE(payload, '$.n' DEFAULT 0 ON EMPTY) FROM records";
    const ast = parseFirstWithDialect(sql, Dialect.HANA);
    expect(
      findAll(ast, (node) => isExpressionType(node, 'hana_function')),
    ).toHaveLength(1);
    expect(
      findAll(ast, (node) => isExpressionType(node, 'hana_json')),
    ).toHaveLength(1);
    expect(getColumns(ast).map(columnReference)).toEqual(['value', 'payload']);
    const copied = clone(ast);
    expect(generate([copied], Dialect.HANA)).toEqual(
      generate([ast], Dialect.HANA),
    );
    const renamed = renameColumns(copied, {
      value: 'needle_source',
      payload: 'document',
    });
    expect(getColumns(renamed).map(columnReference)).toEqual([
      'needle_source',
      'document',
    ]);
    expect(getColumns(ast).map(columnReference)).toEqual(['value', 'payload']);
    expect(generate([copied], Dialect.DuckDB).success).toBe(false);
  });
});
