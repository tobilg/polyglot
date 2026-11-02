import { describe, expect, it } from 'vitest';
import { Dialect, getSourceTables, lineage } from '../src/index';
import { sourceTablesFromLineage } from '../../playground/src/lib/sql-operations';

describe('playground lineage table reuse', () => {
  it.each([
    'SELECT t.x FROM t',
    'SELECT t.x + s.x AS x FROM t JOIN s ON t.id = s.id',
    'WITH c AS (SELECT x FROM t) SELECT x FROM c',
    'SELECT x FROM t UNION ALL SELECT x FROM s',
    'SELECT 1 AS x',
    'SELECT t.x + s.x AS x FROM "😀" t JOIN "\uE000" s ON t.x = s.x',
  ])('matches a separate table analysis for %s', (sql) => {
    for (const trimSelects of [true, false]) {
      const result = lineage('x', sql, Dialect.PostgreSQL, trimSelects);
      expect(result.success).toBe(true);
      expect(sourceTablesFromLineage(result.lineage!)).toEqual(
        getSourceTables('x', sql, Dialect.PostgreSQL).tables,
      );
    }
  });
});
