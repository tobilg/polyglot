import { describe, expect, it } from 'vitest';
import type { Expression } from '../../generated/Expression';
import { Dialect, parse } from '../../index';
import baseline from './query-baseline.json';
import {
  findFirst,
  getAggregateFunctions,
  getColumnNames,
  getFunctions,
  getLiterals,
  getSubqueries,
  getTableNames,
  getTables,
  getWindowFunctions,
  nodeCount,
  some,
  walk,
} from './walker';

const queries = {
  getTables,
  getFunctions,
  getAggregateFunctions,
  getWindowFunctions,
  getSubqueries,
  getLiterals,
  getColumnNames,
  getTableNames,
  nodeCount,
};

// Captured from the 5a25f655 SDK, including compatibility shapes, and updated
// for the TableRef.subpartition default added in 0.14.0.
const cases = baseline as Array<{
  label: string;
  sql?: string;
  ast?: unknown;
  expected: Record<string, unknown>;
}>;

describe('AST query compatibility', () => {
  it.each(cases)('matches baseline output for $label', ({
    sql,
    ast,
    expected,
  }) => {
    let tree = ast as Expression;
    if (sql) {
      const parsed = parse(sql, Dialect.PostgreSQL);
      expect(parsed.success).toBe(true);
      tree = parsed.ast![0];
    }
    for (const [name, query] of Object.entries(queries)) {
      expect(query(tree)).toEqual(expected[name]);
    }
  });

  it('returns independent node copies', () => {
    const tree = parse('SELECT 1 FROM t', Dialect.Generic).ast![0];
    const original = JSON.stringify(tree);
    const tables = getTables(tree);
    if ('table' in tables[0]) tables[0].table.name.name = 'changed';
    expect(JSON.stringify(tree)).toBe(original);
    expect(getTableNames(tree)).toEqual(['t']);
  });

  it('does not read descendants after finding a match', () => {
    const root = {
      select: {
        get expressions() {
          throw new Error('descendants visited');
        },
      },
    } as unknown as Expression;
    expect(findFirst(root, () => true)).toBe(root);
    expect(some(root, () => true)).toBe(true);
  });

  it('preserves the array length snapshot when visitors append children', () => {
    const root = parse('SELECT 1', Dialect.Generic).ast![0];
    const other = parse('SELECT 2', Dialect.Generic).ast![0];
    if (!('select' in root) || !('select' in other))
      throw new Error('Expected SELECT');
    const visited: Expression[] = [];
    walk(root, {
      enter(node) {
        visited.push(node);
        if ('literal' in node)
          root.select.expressions.push(other.select.expressions[0]);
      },
    });
    expect(visited).toHaveLength(2);
    expect(root.select.expressions).toHaveLength(2);
  });
});
