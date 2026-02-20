/**
 * Tests for WASM-backed SQL builders.
 *
 * These tests verify that the thin TypeScript wrappers correctly delegate
 * to the Rust builder API via WASM and produce correct SQL output.
 */
import { describe, expect, it } from 'vitest';
import {
  abs,
  alias,
  and,
  avg,
  boolean,
  caseOf,
  caseWhen,
  cast,
  ceil,
  coalesce,
  col,
  condition,
  // Convenience functions
  count,
  currentDate,
  currentTimestamp,
  del,
  deleteFrom,
  denseRank,
  // Expression helpers
  Expr,
  except,
  floor,
  func,
  greatest,
  insert,
  insertInto,
  intersect,
  least,
  length,
  lit,
  lower,
  max,
  mergeInto,
  min,
  not,
  nullIf,
  or,
  rank,
  round,
  rowNumber,
  SelectBuilder,
  // Query builders
  select,
  sqlExpr,
  sqlNull,
  star,
  sum,
  table,
  trim,
  unionAll,
  update,
  upper,
} from './builders';

// ============================================================================
// Expression helpers
// ============================================================================

describe('Expression helpers', () => {
  it('col() creates a column reference', () => {
    expect(col('name').toSql()).toBe('name');
  });

  it('col() supports dotted names', () => {
    expect(col('users.id').toSql()).toBe('users.id');
  });

  it('lit() creates a string literal', () => {
    expect(lit('hello').toSql()).toBe("'hello'");
  });

  it('lit() creates a numeric literal', () => {
    expect(lit(42).toSql()).toBe('42');
  });

  it('lit() creates a float literal', () => {
    expect(lit(3.14).toSql()).toBe('3.14');
  });

  it('lit() creates a boolean literal', () => {
    expect(lit(true).toSql()).toBe('TRUE');
    expect(lit(false).toSql()).toBe('FALSE');
  });

  it('lit() creates a NULL for null', () => {
    expect(lit(null).toSql()).toBe('NULL');
  });

  it('star() creates *', () => {
    expect(star().toSql()).toBe('*');
  });

  it('sqlNull() creates NULL', () => {
    expect(sqlNull().toSql()).toBe('NULL');
  });

  it('boolean() creates TRUE/FALSE', () => {
    expect(boolean(true).toSql()).toBe('TRUE');
    expect(boolean(false).toSql()).toBe('FALSE');
  });

  it('table() creates table reference', () => {
    expect(table('users').toSql()).toBe('users');
  });

  it('table() supports dotted names', () => {
    expect(table('my_schema.users').toSql()).toBe('my_schema.users');
  });

  it('sqlExpr() parses SQL fragment', () => {
    expect(sqlExpr('COALESCE(a, b, 0)').toSql()).toBe('COALESCE(a, b, 0)');
  });

  it('condition() is alias for sqlExpr()', () => {
    expect(condition('age > 18').toSql()).toBe('age > 18');
  });

  it('func() creates function call', () => {
    expect(func('UPPER', col('name')).toSql()).toBe('UPPER(name)');
  });

  it('func() with multiple args', () => {
    expect(func('COALESCE', col('a'), col('b'), lit(0)).toSql()).toBe(
      'COALESCE(a, b, 0)',
    );
  });

  it('not() negates expression', () => {
    expect(not(col('active')).toSql()).toBe('NOT active');
  });

  it('cast() creates CAST expression', () => {
    expect(cast(col('id'), 'VARCHAR').toSql()).toBe('CAST(id AS VARCHAR)');
  });

  it('alias() creates AS expression', () => {
    expect(alias(col('name'), 'n').toSql()).toBe('name AS n');
  });
});

// ============================================================================
// Expr operators
// ============================================================================

describe('Expr operators', () => {
  describe('comparison', () => {
    it('eq', () => {
      expect(col('x').eq(lit(1)).toSql()).toBe('x = 1');
    });
    it('neq', () => {
      expect(col('x').neq(lit(1)).toSql()).toBe('x <> 1');
    });
    it('lt', () => {
      expect(col('x').lt(lit(1)).toSql()).toBe('x < 1');
    });
    it('lte', () => {
      expect(col('x').lte(lit(1)).toSql()).toBe('x <= 1');
    });
    it('gt', () => {
      expect(col('x').gt(lit(1)).toSql()).toBe('x > 1');
    });
    it('gte', () => {
      expect(col('x').gte(lit(1)).toSql()).toBe('x >= 1');
    });
  });

  describe('string convenience (auto-converts to column)', () => {
    it('eq with string', () => {
      expect(col('x').eq('y').toSql()).toBe('x = y');
    });
    it('gt with number', () => {
      expect(col('x').gt(5).toSql()).toBe('x > 5');
    });
  });

  describe('logical', () => {
    it('and', () => {
      expect(col('a').and(col('b')).toSql()).toBe('a AND b');
    });
    it('or', () => {
      expect(col('a').or(col('b')).toSql()).toBe('a OR b');
    });
    it('not', () => {
      expect(col('a').not().toSql()).toBe('NOT a');
    });
    it('xor', () => {
      expect(col('a').xor(col('b')).toSql()).toBe('a XOR b');
    });
  });

  describe('arithmetic', () => {
    it('add', () => {
      expect(col('a').add(col('b')).toSql()).toBe('a + b');
    });
    it('sub', () => {
      expect(col('a').sub(col('b')).toSql()).toBe('a - b');
    });
    it('mul', () => {
      expect(col('a').mul(col('b')).toSql()).toBe('a * b');
    });
    it('div', () => {
      expect(col('a').div(col('b')).toSql()).toBe('a / b');
    });
  });

  describe('pattern matching', () => {
    it('like', () => {
      expect(col('name').like(lit('%test%')).toSql()).toBe(
        "name LIKE '%test%'",
      );
    });
    it('ilike', () => {
      expect(col('name').ilike(lit('%test%')).toSql()).toBe(
        "name ILIKE '%test%'",
      );
    });
  });

  describe('predicates', () => {
    it('isNull', () => {
      expect(col('x').isNull().toSql()).toBe('x IS NULL');
    });
    it('isNotNull', () => {
      expect(col('x').isNotNull().toSql()).toBe('NOT x IS NULL');
    });
    it('between', () => {
      expect(col('age').between(lit(18), lit(65)).toSql()).toBe(
        'age BETWEEN 18 AND 65',
      );
    });
    it('inList', () => {
      expect(col('status').inList(lit('a'), lit('b')).toSql()).toBe(
        "status IN ('a', 'b')",
      );
    });
    it('notIn', () => {
      expect(col('x').notIn(lit(1), lit(2), lit(3)).toSql()).toBe(
        'x NOT IN (1, 2, 3)',
      );
    });
  });

  describe('transform', () => {
    it('alias', () => {
      expect(col('name').alias('n').toSql()).toBe('name AS n');
    });
    it('as (alias for alias)', () => {
      expect(col('name').as('n').toSql()).toBe('name AS n');
    });
    it('cast', () => {
      expect(col('id').cast('VARCHAR').toSql()).toBe('CAST(id AS VARCHAR)');
    });
    it('asc', () => {
      expect(col('name').asc().toSql()).toBe('name ASC');
    });
    it('desc', () => {
      expect(col('age').desc().toSql()).toBe('age DESC');
    });
  });

  it('Expr is reusable (methods clone internally)', () => {
    const c = col('x');
    const a = c.eq(lit(1));
    const b = c.gt(lit(2));
    expect(a.toSql()).toBe('x = 1');
    expect(b.toSql()).toBe('x > 2');
  });

  it('toJSON returns AST object', () => {
    const json = col('x').toJSON();
    expect(json).toBeDefined();
    expect(typeof json).toBe('object');
  });
});

// ============================================================================
// Variadic logical operators
// ============================================================================

describe('Variadic logical operators', () => {
  it('and() with no args returns TRUE', () => {
    expect(and().toSql()).toBe('TRUE');
  });

  it('and() with single arg returns it unchanged', () => {
    const e = col('x');
    expect(and(e)).toBe(e);
  });

  it('and() chains multiple conditions', () => {
    expect(and(col('a'), col('b'), col('c')).toSql()).toBe('a AND b AND c');
  });

  it('or() with no args returns FALSE', () => {
    expect(or().toSql()).toBe('FALSE');
  });

  it('or() with single arg returns it unchanged', () => {
    const e = col('x');
    expect(or(e)).toBe(e);
  });

  it('or() chains multiple conditions', () => {
    expect(or(col('a'), col('b'), col('c')).toSql()).toBe('a OR b OR c');
  });
});

// ============================================================================
// SelectBuilder
// ============================================================================

describe('SelectBuilder', () => {
  it('basic SELECT', () => {
    const sql = select('id', 'name').from('users').toSql();
    expect(sql).toBe('SELECT id, name FROM users');
  });

  it('SELECT *', () => {
    const sql = select('*').from('users').toSql();
    expect(sql).toBe('SELECT * FROM users');
  });

  it('SELECT with Expr columns', () => {
    const sql = select(col('price').mul(col('qty')).alias('total'))
      .from('items')
      .toSql();
    expect(sql).toBe('SELECT price * qty AS total FROM items');
  });

  it('WHERE clause with Expr', () => {
    const sql = select('*')
      .from('users')
      .where(col('status').eq(lit('active')))
      .toSql();
    expect(sql).toBe("SELECT * FROM users WHERE status = 'active'");
  });

  it('WHERE clause with notIn uses canonical NOT IN', () => {
    const sql = select('id')
      .from('users')
      .where(col('status').notIn(lit('deleted'), lit('banned')))
      .toSql();
    expect(sql).toBe(
      "SELECT id FROM users WHERE status NOT IN ('deleted', 'banned')",
    );
  });

  it('WHERE clause with SQL string', () => {
    const sql = select('*')
      .from('users')
      .where("age > 18 AND status = 'active'")
      .toSql();
    expect(sql).toContain("WHERE age > 18 AND status = 'active'");
  });

  it('ORDER BY, LIMIT, OFFSET', () => {
    const sql = select('name')
      .from('users')
      .orderBy(col('name').asc())
      .limit(10)
      .offset(5)
      .toSql();
    expect(sql).toBe(
      'SELECT name FROM users ORDER BY name ASC LIMIT 10 OFFSET 5',
    );
  });

  it('DISTINCT', () => {
    const sql = select('name').from('users').distinct().toSql();
    expect(sql).toBe('SELECT DISTINCT name FROM users');
  });

  it('JOIN', () => {
    const sql = select('u.id')
      .from('users')
      .leftJoin('orders', col('u.id').eq(col('o.user_id')))
      .toSql();
    expect(sql).toContain('LEFT JOIN orders ON u.id = o.user_id');
  });

  it('GROUP BY + HAVING', () => {
    const sql = select('dept', count(star()).alias('cnt'))
      .from('employees')
      .groupBy('dept')
      .having(count(star()).gt(lit(5)))
      .toSql();
    expect(sql).toContain('GROUP BY dept');
    expect(sql).toContain('HAVING COUNT(*) > 5');
  });

  it('dialect-specific output', () => {
    const sql = select('*').from('users').limit(10).toSql('tsql');
    // TSQL uses TOP instead of LIMIT
    expect(sql).toContain('TOP 10');
  });

  it('build() returns AST JSON', () => {
    const ast = select('id').from('users').build();
    expect(ast).toBeDefined();
    expect(typeof ast).toBe('object');
  });

  it('UNION ALL', () => {
    const a = select('id').from('a');
    const b = select('id').from('b');
    const sql = a.unionAll(b).toSql();
    expect(sql).toContain('UNION ALL');
  });

  it('chaining works correctly', () => {
    const b = new SelectBuilder();
    const result = b
      .select('id', 'name')
      .from('users')
      .where(col('active').eq(boolean(true)))
      .orderBy(col('name').asc())
      .limit(100);
    expect(result).toBe(b); // returns same object for chaining
    const sql = b.toSql();
    expect(sql).toContain('SELECT id, name');
    expect(sql).toContain('FROM users');
    expect(sql).toContain('WHERE active = TRUE');
    expect(sql).toContain('ORDER BY name ASC');
    expect(sql).toContain('LIMIT 100');
  });

  it('throws if toSql() is called after builder is consumed', () => {
    const b = select('*').from('users');
    expect(b.toSql()).toBe('SELECT * FROM users');
    expect(() => b.toSql()).toThrow(/already consumed/i);
  });
});

// ============================================================================
// InsertBuilder
// ============================================================================

describe('InsertBuilder', () => {
  it('basic INSERT', () => {
    const sql = insertInto('users')
      .columns('id', 'name')
      .values(lit(1), lit('Alice'))
      .toSql();
    expect(sql).toBe("INSERT INTO users (id, name) VALUES (1, 'Alice')");
  });

  it('insert() is alias for insertInto()', () => {
    const sql = insert('users').columns('id').values(lit(1)).toSql();
    expect(sql).toContain('INSERT INTO users');
  });

  it('INSERT ... SELECT', () => {
    const sql = insertInto('archive')
      .columns('id', 'name')
      .query(select('id', 'name').from('users'))
      .toSql();
    expect(sql).toContain('INSERT INTO archive');
    expect(sql).toContain('SELECT id, name FROM users');
  });
});

// ============================================================================
// UpdateBuilder
// ============================================================================

describe('UpdateBuilder', () => {
  it('basic UPDATE', () => {
    const sql = update('users')
      .set('name', lit('Bob'))
      .where(col('id').eq(lit(1)))
      .toSql();
    expect(sql).toBe("UPDATE users SET name = 'Bob' WHERE id = 1");
  });

  it('multiple SET assignments', () => {
    const sql = update('users')
      .set('name', lit('Bob'))
      .set('age', lit(30))
      .toSql();
    expect(sql).toContain("name = 'Bob'");
    expect(sql).toContain('age = 30');
  });
});

// ============================================================================
// DeleteBuilder
// ============================================================================

describe('DeleteBuilder', () => {
  it('basic DELETE', () => {
    const sql = deleteFrom('users')
      .where(col('id').eq(lit(1)))
      .toSql();
    expect(sql).toBe('DELETE FROM users WHERE id = 1');
  });

  it('del() is alias for deleteFrom()', () => {
    const sql = del('users')
      .where(col('active').eq(boolean(false)))
      .toSql();
    expect(sql).toContain('DELETE FROM users');
  });
});

// ============================================================================
// MergeBuilder
// ============================================================================

describe('MergeBuilder', () => {
  it('basic MERGE', () => {
    const sql = mergeInto('target')
      .using('source', col('target.id').eq(col('source.id')))
      .whenMatchedUpdate({ name: col('source.name') })
      .whenNotMatchedInsert(
        ['id', 'name'],
        [col('source.id'), col('source.name')],
      )
      .toSql();
    expect(sql).toContain('MERGE INTO');
    expect(sql).toContain('USING source ON');
    expect(sql).toContain('WHEN MATCHED');
  });
});

// ============================================================================
// CaseBuilder
// ============================================================================

describe('CaseBuilder', () => {
  it('searched CASE', () => {
    const sql = caseWhen()
      .when(col('x').gt(lit(0)), lit('positive'))
      .else_(lit('non-positive'))
      .toSql();
    expect(sql).toBe("CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END");
  });

  it('simple CASE', () => {
    const sql = caseOf(col('status'))
      .when(lit(1), lit('active'))
      .when(lit(0), lit('inactive'))
      .toSql();
    expect(sql).toBe(
      "CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END",
    );
  });

  it('build() returns Expr', () => {
    const expr = caseWhen()
      .when(col('x').gt(lit(0)), lit('yes'))
      .build();
    expect(expr).toBeInstanceOf(Expr);
    expect(expr.toSql()).toContain('CASE WHEN');
  });
});

// ============================================================================
// SetOpBuilder
// ============================================================================

describe('SetOpBuilder', () => {
  it('UNION ALL with ORDER BY and LIMIT', () => {
    const a = select('id').from('a');
    const b = select('id').from('b');
    const sql = unionAll(a, b).orderBy('id').limit(10).toSql();
    expect(sql).toContain('UNION ALL');
    expect(sql).toContain('ORDER BY');
    expect(sql).toContain('LIMIT 10');
  });

  it('INTERSECT', () => {
    const a = select('id').from('a');
    const b = select('id').from('b');
    const sql = intersect(a, b).toSql();
    expect(sql).toContain('INTERSECT');
  });

  it('EXCEPT', () => {
    const a = select('id').from('a');
    const b = select('id').from('b');
    const sql = except(a, b).toSql();
    expect(sql).toContain('EXCEPT');
  });
});

// ============================================================================
// Convenience function wrappers
// ============================================================================

describe('Convenience functions', () => {
  it('count()', () => {
    expect(count().toSql()).toBe('COUNT(*)');
  });
  it('count(expr)', () => {
    expect(count(col('id')).toSql()).toBe('COUNT(id)');
  });
  it('sum()', () => {
    expect(sum(col('amount')).toSql()).toBe('SUM(amount)');
  });
  it('avg()', () => {
    expect(avg(col('score')).toSql()).toBe('AVG(score)');
  });
  it('min()', () => {
    expect(min(col('price')).toSql()).toBe('MIN(price)');
  });
  it('max()', () => {
    expect(max(col('price')).toSql()).toBe('MAX(price)');
  });

  it('upper()', () => {
    expect(upper(col('name')).toSql()).toBe('UPPER(name)');
  });
  it('lower()', () => {
    expect(lower(col('name')).toSql()).toBe('LOWER(name)');
  });
  it('length()', () => {
    expect(length(col('name')).toSql()).toBe('LENGTH(name)');
  });
  it('trim()', () => {
    expect(trim(col('name')).toSql()).toBe('TRIM(name)');
  });

  it('coalesce()', () => {
    expect(coalesce(col('a'), col('b'), lit(0)).toSql()).toBe(
      'COALESCE(a, b, 0)',
    );
  });
  it('nullIf()', () => {
    expect(nullIf(col('x'), lit(0)).toSql()).toBe('NULLIF(x, 0)');
  });

  it('abs()', () => {
    expect(abs(col('x')).toSql()).toBe('ABS(x)');
  });
  it('round()', () => {
    expect(round(col('x')).toSql()).toBe('ROUND(x)');
  });
  it('floor()', () => {
    expect(floor(col('x')).toSql()).toBe('FLOOR(x)');
  });
  it('ceil()', () => {
    expect(ceil(col('x')).toSql()).toBe('CEIL(x)');
  });

  it('greatest()', () => {
    expect(greatest(col('a'), col('b')).toSql()).toBe('GREATEST(a, b)');
  });
  it('least()', () => {
    expect(least(col('a'), col('b')).toSql()).toBe('LEAST(a, b)');
  });

  it('currentDate()', () => {
    expect(currentDate().toSql()).toBe('CURRENT_DATE()');
  });
  it('currentTimestamp()', () => {
    expect(currentTimestamp().toSql()).toBe('CURRENT_TIMESTAMP()');
  });

  it('rowNumber()', () => {
    expect(rowNumber().toSql()).toBe('ROW_NUMBER()');
  });
  it('rank()', () => {
    expect(rank().toSql()).toBe('RANK()');
  });
  it('denseRank()', () => {
    expect(denseRank().toSql()).toBe('DENSE_RANK()');
  });
});

// ============================================================================
// End-to-end query building
// ============================================================================

describe('End-to-end queries', () => {
  it('complex SELECT with joins, where, group by, having, order, limit', () => {
    const sql = select(
      col('u.name'),
      count(star()).alias('order_count'),
      sum(col('o.total')).alias('total_spent'),
    )
      .from('users')
      .leftJoin('orders', col('u.id').eq(col('o.user_id')))
      .where(col('u.active').eq(boolean(true)))
      .groupBy('u.name')
      .having(count(star()).gt(lit(0)))
      .orderBy(col('total_spent').desc())
      .limit(50)
      .toSql('postgresql');

    expect(sql).toContain(
      'SELECT u.name, COUNT(*) AS order_count, SUM(o.total) AS total_spent',
    );
    expect(sql).toContain('FROM users');
    expect(sql).toContain('LEFT JOIN orders ON u.id = o.user_id');
    expect(sql).toContain('WHERE u.active = TRUE');
    expect(sql).toContain('GROUP BY u.name');
    expect(sql).toContain('HAVING COUNT(*) > 0');
    expect(sql).toContain('ORDER BY total_spent DESC');
    expect(sql).toContain('LIMIT 50');
  });

  it('INSERT with multiple rows', () => {
    const b = insertInto('users').columns('id', 'name');
    b.values(lit(1), lit('Alice'));
    b.values(lit(2), lit('Bob'));
    const sql = b.toSql();
    expect(sql).toContain("VALUES (1, 'Alice'), (2, 'Bob')");
  });

  it('UPDATE with FROM (PostgreSQL style)', () => {
    const sql = update('t1')
      .set('name', col('t2.name'))
      .from('t2')
      .where(col('t1.id').eq(col('t2.id')))
      .toSql('postgresql');
    expect(sql).toContain('UPDATE t1');
    expect(sql).toContain('SET name = t2.name');
    expect(sql).toContain('FROM t2');
    expect(sql).toContain('WHERE t1.id = t2.id');
  });
});
