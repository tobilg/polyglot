import { createRequire } from 'node:module';
import { describe, it, expect, beforeAll } from 'vitest';

const require = createRequire(import.meta.url);

describe('CJS build', () => {
  let p: any;

  beforeAll(async () => {
    p = require('../dist/index.cjs');
  });

  // ==========================================================================
  // Initialization
  // ==========================================================================

  describe('init', () => {
    it('isInitialized returns false before init', () => {
      expect(p.isInitialized()).toBe(false);
    });

    it('init() succeeds', async () => {
      await p.init();
      expect(p.isInitialized()).toBe(true);
    });

    it('init() is idempotent', async () => {
      await p.init();
      await p.init();
      expect(p.isInitialized()).toBe(true);
    });
  });

  // ==========================================================================
  // Core functions
  // ==========================================================================

  describe('getVersion', () => {
    it('returns a semver string', () => {
      expect(p.getVersion()).toMatch(/^\d+\.\d+\.\d+$/);
    });
  });

  describe('getDialects', () => {
    it('returns an array of dialect names', () => {
      const dialects = p.getDialects();
      expect(Array.isArray(dialects)).toBe(true);
      expect(dialects.length).toBeGreaterThan(0);
    });

    it('includes common dialects', () => {
      const dialects = p.getDialects();
      expect(dialects).toContain('generic');
      expect(dialects).toContain('postgresql');
      expect(dialects).toContain('mysql');
      expect(dialects).toContain('bigquery');
      expect(dialects).toContain('snowflake');
    });
  });

  // ==========================================================================
  // Parse
  // ==========================================================================

  describe('parse', () => {
    it('parses a simple SELECT', () => {
      const result = p.parse('SELECT 1', 'generic');
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('parses SELECT with columns and table', () => {
      const result = p.parse('SELECT a, b FROM users', 'generic');
      expect(result.success).toBe(true);
      expect(result.ast).toBeDefined();
    });

    it('parses SELECT with WHERE clause', () => {
      const result = p.parse('SELECT * FROM users WHERE id = 1', 'generic');
      expect(result.success).toBe(true);
    });

    it('defaults to generic dialect', () => {
      const result = p.parse('SELECT 1');
      expect(result.success).toBe(true);
    });

    it('handles malformed SQL gracefully', () => {
      const result = p.parse('SELECT FROM WHERE', 'generic');
      expect(typeof result.success).toBe('boolean');
    });

    it('includes error positions on failure', () => {
      const result = p.parse('SELECT 1 + 2)', 'generic');
      expect(result.success).toBe(false);
      expect(result.errorLine).toBe(1);
      expect(typeof result.errorColumn).toBe('number');
    });
  });

  // ==========================================================================
  // Generate
  // ==========================================================================

  describe('generate', () => {
    it('generates SQL from a parsed AST', () => {
      const parsed = p.parse('SELECT a FROM t', 'generic');
      const result = p.generate(parsed.ast, 'generic');
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
      expect(result.sql.length).toBeGreaterThan(0);
    });

    it('roundtrips simple queries', () => {
      const parsed = p.parse('SELECT a, b FROM users', 'generic');
      const result = p.generate(parsed.ast, 'generic');
      expect(result.success).toBe(true);
      expect(result.sql[0].toLowerCase()).toContain('select');
      expect(result.sql[0].toLowerCase()).toContain('from');
    });
  });

  // ==========================================================================
  // Transpile
  // ==========================================================================

  describe('transpile', () => {
    it('transpiles between dialects', () => {
      const result = p.transpile('SELECT 1', 'generic', 'postgresql');
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
    });

    it('transpiles same dialect without changes', () => {
      const result = p.transpile('SELECT a FROM t', 'generic', 'generic');
      expect(result.success).toBe(true);
    });

    it('handles multiple statements', () => {
      const result = p.transpile('SELECT 1; SELECT 2', 'generic', 'generic');
      expect(result.success).toBe(true);
      expect(result.sql.length).toBe(2);
    });

    it('transforms IFNULL to COALESCE for PostgreSQL', () => {
      const result = p.transpile('SELECT IFNULL(a, b)', 'mysql', 'postgresql');
      expect(result.success).toBe(true);
      expect(result.sql[0]).toContain('COALESCE');
    });

    it('transforms NVL to IFNULL for MySQL', () => {
      const result = p.transpile('SELECT NVL(a, b)', 'generic', 'mysql');
      expect(result.success).toBe(true);
      expect(result.sql[0]).toContain('IFNULL');
    });

    it('includes error positions on failure', () => {
      const result = p.transpile('SELECT 1 + 2)', 'generic', 'postgresql');
      expect(result.success).toBe(false);
      expect(result.errorLine).toBeDefined();
      expect(result.errorColumn).toBeDefined();
    });
  });

  // ==========================================================================
  // Format
  // ==========================================================================

  describe('format', () => {
    it('formats SQL', () => {
      const result = p.format('SELECT a,b,c FROM t', 'generic');
      expect(result.success).toBe(true);
      expect(result.sql).toBeDefined();
    });

    it('defaults to generic dialect', () => {
      const result = p.format('SELECT 1');
      expect(result.success).toBe(true);
    });

    it('formatWithOptions returns guard error when limits exceeded', () => {
      const result = p.formatWithOptions('SELECT 1', 'generic', {
        maxInputBytes: 7,
      });
      expect(result.success).toBe(false);
      expect(result.error).toContain('E_GUARD_INPUT_TOO_LARGE');
    });

    it('remains usable after guard failure', () => {
      p.formatWithOptions('SELECT 1', 'generic', { maxInputBytes: 7 });
      const result = p.format('SELECT a, b FROM t', 'generic');
      expect(result.success).toBe(true);
    });
  });

  // ==========================================================================
  // Tokenize
  // ==========================================================================

  describe('tokenize', () => {
    it('tokenizes SQL into a token stream', () => {
      const result = p.tokenize('SELECT a, b FROM t', 'generic');
      expect(result.success).toBe(true);
      expect(Array.isArray(result.tokens)).toBe(true);
      expect(result.tokens.length).toBeGreaterThan(0);
    });

    it('tokens have expected shape', () => {
      const result = p.tokenize('SELECT 1', 'generic');
      expect(result.success).toBe(true);
      const token = result.tokens[0];
      expect(token.token_type).toBeDefined();
      expect(token.text).toBeDefined();
      expect(token.span).toBeDefined();
      expect(typeof token.span.start).toBe('number');
      expect(typeof token.span.line).toBe('number');
      expect(Array.isArray(token.comments)).toBe(true);
      expect(Array.isArray(token.trailing_comments)).toBe(true);
    });
  });

  // ==========================================================================
  // Validation
  // ==========================================================================

  describe('validate', () => {
    it('validates correct SQL', () => {
      const result = p.validate('SELECT * FROM users', 'postgresql');
      expect(result.valid).toBe(true);
    });

    it('reports errors for invalid SQL', () => {
      const result = p.validate('SELEKT * FROM users', 'postgresql');
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  // ==========================================================================
  // Lineage
  // ==========================================================================

  describe('lineage', () => {
    it('traces column lineage', () => {
      const result = p.lineage('total', 'SELECT o.total FROM orders o');
      expect(result.success).toBe(true);
      expect(result.lineage).toBeDefined();
    });

    it('lineageWithSchema resolves ambiguous unqualified columns', () => {
      const schema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'INT' },
              { name: 'name', type: 'TEXT' },
            ],
          },
          {
            name: 'orders',
            columns: [
              { name: 'order_id', type: 'INT' },
              { name: 'user_id', type: 'INT' },
            ],
          },
        ],
      };
      const result = p.lineageWithSchema(
        'id',
        'SELECT id FROM users u JOIN orders o ON u.id = o.user_id',
        schema,
      );
      expect(result.success).toBe(true);
      expect(JSON.stringify(result.lineage)).toContain('u.id');
    });

    it('getSourceTables returns source tables', () => {
      const result = p.getSourceTables('total', 'SELECT o.total FROM orders o');
      expect(result.success).toBe(true);
      expect(Array.isArray(result.tables)).toBe(true);
    });
  });

  // ==========================================================================
  // Diff
  // ==========================================================================

  describe('diff', () => {
    it('diffs two SQL statements', () => {
      const result = p.diff(
        'SELECT a, b FROM t WHERE x > 1',
        'SELECT a, c FROM t WHERE x > 2',
      );
      expect(result.success).toBe(true);
      expect(Array.isArray(result.edits)).toBe(true);
    });

    it('hasChanges detects differences', () => {
      const result = p.diff('SELECT a FROM t', 'SELECT b FROM t');
      expect(p.hasChanges(result.edits)).toBe(true);
    });

    it('changesOnly filters to non-keep edits', () => {
      const result = p.diff('SELECT a FROM t', 'SELECT b FROM t');
      const changes = p.changesOnly(result.edits);
      for (const edit of changes) {
        expect(edit.type).not.toBe('keep');
      }
    });
  });

  // ==========================================================================
  // Plan
  // ==========================================================================

  describe('plan', () => {
    it('builds a query plan', () => {
      const result = p.plan(
        'SELECT dept, SUM(salary) FROM employees GROUP BY dept',
      );
      expect(result.success).toBe(true);
      expect(result.plan).toBeDefined();
      expect(result.plan.root).toBeDefined();
    });
  });

  // ==========================================================================
  // Expression helpers
  // ==========================================================================

  describe('expression helpers', () => {
    it('col() creates a column reference', () => {
      expect(p.col('name').toSql()).toBe('name');
    });

    it('col() supports dotted names', () => {
      expect(p.col('users.id').toSql()).toBe('users.id');
    });

    it('lit() creates a string literal', () => {
      expect(p.lit('hello').toSql()).toBe("'hello'");
    });

    it('lit() creates a numeric literal', () => {
      expect(p.lit(42).toSql()).toBe('42');
    });

    it('lit() creates a boolean literal', () => {
      expect(p.lit(true).toSql()).toBe('TRUE');
      expect(p.lit(false).toSql()).toBe('FALSE');
    });

    it('lit() creates NULL for null', () => {
      expect(p.lit(null).toSql()).toBe('NULL');
    });

    it('star() creates *', () => {
      expect(p.star().toSql()).toBe('*');
    });

    it('sqlNull() creates NULL', () => {
      expect(p.sqlNull().toSql()).toBe('NULL');
    });

    it('boolean() creates TRUE/FALSE', () => {
      expect(p.boolean(true).toSql()).toBe('TRUE');
      expect(p.boolean(false).toSql()).toBe('FALSE');
    });

    it('table() creates table reference', () => {
      expect(p.table('users').toSql()).toBe('users');
    });

    it('sqlExpr() parses SQL fragment', () => {
      expect(p.sqlExpr('COALESCE(a, b, 0)').toSql()).toBe('COALESCE(a, b, 0)');
    });

    it('func() creates function call', () => {
      expect(p.func('UPPER', p.col('name')).toSql()).toBe('UPPER(name)');
    });

    it('not() negates expression', () => {
      expect(p.not(p.col('active')).toSql()).toBe('NOT active');
    });

    it('cast() creates CAST', () => {
      expect(p.cast(p.col('id'), 'VARCHAR').toSql()).toBe(
        'CAST(id AS VARCHAR)',
      );
    });

    it('alias() creates AS', () => {
      expect(p.alias(p.col('name'), 'n').toSql()).toBe('name AS n');
    });
  });

  // ==========================================================================
  // Expr operators
  // ==========================================================================

  describe('Expr operators', () => {
    it('comparison operators', () => {
      expect(p.col('x').eq(p.lit(1)).toSql()).toBe('x = 1');
      expect(p.col('x').neq(p.lit(1)).toSql()).toBe('x <> 1');
      expect(p.col('x').lt(p.lit(1)).toSql()).toBe('x < 1');
      expect(p.col('x').lte(p.lit(1)).toSql()).toBe('x <= 1');
      expect(p.col('x').gt(p.lit(1)).toSql()).toBe('x > 1');
      expect(p.col('x').gte(p.lit(1)).toSql()).toBe('x >= 1');
    });

    it('logical operators', () => {
      expect(p.col('a').and(p.col('b')).toSql()).toBe('a AND b');
      expect(p.col('a').or(p.col('b')).toSql()).toBe('a OR b');
      expect(p.col('a').not().toSql()).toBe('NOT a');
      expect(p.col('a').xor(p.col('b')).toSql()).toBe('a XOR b');
    });

    it('arithmetic operators', () => {
      expect(p.col('a').add(p.col('b')).toSql()).toBe('a + b');
      expect(p.col('a').sub(p.col('b')).toSql()).toBe('a - b');
      expect(p.col('a').mul(p.col('b')).toSql()).toBe('a * b');
      expect(p.col('a').div(p.col('b')).toSql()).toBe('a / b');
    });

    it('pattern matching', () => {
      expect(p.col('name').like(p.lit('%test%')).toSql()).toBe(
        "name LIKE '%test%'",
      );
      expect(p.col('name').ilike(p.lit('%test%')).toSql()).toBe(
        "name ILIKE '%test%'",
      );
    });

    it('predicates', () => {
      expect(p.col('x').isNull().toSql()).toBe('x IS NULL');
      expect(p.col('x').isNotNull().toSql()).toBe('NOT x IS NULL');
      expect(p.col('age').between(p.lit(18), p.lit(65)).toSql()).toBe(
        'age BETWEEN 18 AND 65',
      );
      expect(p.col('status').inList(p.lit('a'), p.lit('b')).toSql()).toBe(
        "status IN ('a', 'b')",
      );
      expect(p.col('x').notIn(p.lit(1), p.lit(2)).toSql()).toBe(
        'x NOT IN (1, 2)',
      );
    });

    it('transform methods', () => {
      expect(p.col('name').alias('n').toSql()).toBe('name AS n');
      expect(p.col('name').as('n').toSql()).toBe('name AS n');
      expect(p.col('id').cast('VARCHAR').toSql()).toBe('CAST(id AS VARCHAR)');
      expect(p.col('name').asc().toSql()).toBe('name ASC');
      expect(p.col('age').desc().toSql()).toBe('age DESC');
    });

    it('Expr is reusable (methods clone internally)', () => {
      const c = p.col('x');
      const a = c.eq(p.lit(1));
      const b = c.gt(p.lit(2));
      expect(a.toSql()).toBe('x = 1');
      expect(b.toSql()).toBe('x > 2');
    });

    it('toJSON returns AST object', () => {
      const json = p.col('x').toJSON();
      expect(json).toBeDefined();
      expect(typeof json).toBe('object');
    });
  });

  // ==========================================================================
  // Variadic logical operators
  // ==========================================================================

  describe('variadic logical operators', () => {
    it('and() with no args returns TRUE', () => {
      expect(p.and().toSql()).toBe('TRUE');
    });

    it('and() chains multiple conditions', () => {
      expect(p.and(p.col('a'), p.col('b'), p.col('c')).toSql()).toBe(
        'a AND b AND c',
      );
    });

    it('or() with no args returns FALSE', () => {
      expect(p.or().toSql()).toBe('FALSE');
    });

    it('or() chains multiple conditions', () => {
      expect(p.or(p.col('a'), p.col('b'), p.col('c')).toSql()).toBe(
        'a OR b OR c',
      );
    });
  });

  // ==========================================================================
  // Convenience functions
  // ==========================================================================

  describe('convenience functions', () => {
    it('count()', () => {
      expect(p.count().toSql()).toBe('COUNT(*)');
    });

    it('count(expr)', () => {
      expect(p.count(p.col('id')).toSql()).toBe('COUNT(id)');
    });

    it('countDistinct()', () => {
      expect(p.countDistinct(p.col('id')).toSql()).toBe('COUNT(DISTINCT id)');
    });

    it('sum()', () => {
      expect(p.sum(p.col('amount')).toSql()).toBe('SUM(amount)');
    });

    it('avg()', () => {
      expect(p.avg(p.col('score')).toSql()).toBe('AVG(score)');
    });

    it('min()', () => {
      expect(p.min(p.col('price')).toSql()).toBe('MIN(price)');
    });

    it('max()', () => {
      expect(p.max(p.col('price')).toSql()).toBe('MAX(price)');
    });

    it('upper()', () => {
      expect(p.upper(p.col('name')).toSql()).toBe('UPPER(name)');
    });

    it('lower()', () => {
      expect(p.lower(p.col('name')).toSql()).toBe('LOWER(name)');
    });

    it('length()', () => {
      expect(p.length(p.col('name')).toSql()).toBe('LENGTH(name)');
    });

    it('trim()', () => {
      expect(p.trim(p.col('name')).toSql()).toBe('TRIM(name)');
    });

    it('coalesce()', () => {
      expect(p.coalesce(p.col('a'), p.col('b'), p.lit(0)).toSql()).toBe(
        'COALESCE(a, b, 0)',
      );
    });

    it('nullIf()', () => {
      expect(p.nullIf(p.col('x'), p.lit(0)).toSql()).toBe('NULLIF(x, 0)');
    });

    it('abs()', () => {
      expect(p.abs(p.col('x')).toSql()).toBe('ABS(x)');
    });

    it('round()', () => {
      expect(p.round(p.col('x')).toSql()).toBe('ROUND(x)');
    });

    it('floor()', () => {
      expect(p.floor(p.col('x')).toSql()).toBe('FLOOR(x)');
    });

    it('ceil()', () => {
      expect(p.ceil(p.col('x')).toSql()).toBe('CEIL(x)');
    });

    it('greatest()', () => {
      expect(p.greatest(p.col('a'), p.col('b')).toSql()).toBe('GREATEST(a, b)');
    });

    it('least()', () => {
      expect(p.least(p.col('a'), p.col('b')).toSql()).toBe('LEAST(a, b)');
    });

    it('currentDate()', () => {
      expect(p.currentDate().toSql()).toBe('CURRENT_DATE()');
    });

    it('currentTimestamp()', () => {
      expect(p.currentTimestamp().toSql()).toBe('CURRENT_TIMESTAMP()');
    });

    it('rowNumber()', () => {
      expect(p.rowNumber().toSql()).toBe('ROW_NUMBER()');
    });

    it('rank()', () => {
      expect(p.rank().toSql()).toBe('RANK()');
    });

    it('denseRank()', () => {
      expect(p.denseRank().toSql()).toBe('DENSE_RANK()');
    });
  });

  // ==========================================================================
  // CASE expressions
  // ==========================================================================

  describe('CaseBuilder', () => {
    it('searched CASE', () => {
      const sql = p
        .caseWhen()
        .when(p.col('x').gt(p.lit(0)), p.lit('positive'))
        .else_(p.lit('non-positive'))
        .toSql();
      expect(sql).toBe(
        "CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END",
      );
    });

    it('simple CASE', () => {
      const sql = p
        .caseOf(p.col('status'))
        .when(p.lit(1), p.lit('active'))
        .when(p.lit(0), p.lit('inactive'))
        .toSql();
      expect(sql).toBe(
        "CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END",
      );
    });
  });

  // ==========================================================================
  // SelectBuilder
  // ==========================================================================

  describe('SelectBuilder', () => {
    it('basic SELECT', () => {
      const sql = p.select('id', 'name').from('users').toSql();
      expect(sql).toBe('SELECT id, name FROM users');
    });

    it('SELECT *', () => {
      const sql = p.select('*').from('users').toSql();
      expect(sql).toBe('SELECT * FROM users');
    });

    it('SELECT with Expr columns', () => {
      const sql = p
        .select(p.col('price').mul(p.col('qty')).alias('total'))
        .from('items')
        .toSql();
      expect(sql).toBe('SELECT price * qty AS total FROM items');
    });

    it('WHERE clause with Expr', () => {
      const sql = p
        .select('*')
        .from('users')
        .where(p.col('status').eq(p.lit('active')))
        .toSql();
      expect(sql).toBe("SELECT * FROM users WHERE status = 'active'");
    });

    it('WHERE clause with SQL string', () => {
      const sql = p
        .select('*')
        .from('users')
        .where("age > 18 AND status = 'active'")
        .toSql();
      expect(sql).toContain("WHERE age > 18 AND status = 'active'");
    });

    it('ORDER BY, LIMIT, OFFSET', () => {
      const sql = p
        .select('name')
        .from('users')
        .orderBy(p.col('name').asc())
        .limit(10)
        .offset(5)
        .toSql();
      expect(sql).toBe(
        'SELECT name FROM users ORDER BY name ASC LIMIT 10 OFFSET 5',
      );
    });

    it('DISTINCT', () => {
      const sql = p.select('name').from('users').distinct().toSql();
      expect(sql).toBe('SELECT DISTINCT name FROM users');
    });

    it('JOIN', () => {
      const sql = p
        .select('u.id')
        .from('users')
        .leftJoin('orders', p.col('u.id').eq(p.col('o.user_id')))
        .toSql();
      expect(sql).toContain('LEFT JOIN orders ON u.id = o.user_id');
    });

    it('GROUP BY + HAVING', () => {
      const sql = p
        .select('dept', p.count(p.star()).alias('cnt'))
        .from('employees')
        .groupBy('dept')
        .having(p.count(p.star()).gt(p.lit(5)))
        .toSql();
      expect(sql).toContain('GROUP BY dept');
      expect(sql).toContain('HAVING COUNT(*) > 5');
    });

    it('dialect-specific output (TSQL TOP)', () => {
      const sql = p.select('*').from('users').limit(10).toSql('tsql');
      expect(sql).toContain('TOP 10');
    });

    it('build() returns AST JSON', () => {
      const ast = p.select('id').from('users').build();
      expect(ast).toBeDefined();
      expect(typeof ast).toBe('object');
    });

    it('UNION ALL via builder', () => {
      const a = p.select('id').from('a');
      const b = p.select('id').from('b');
      const sql = a.unionAll(b).toSql();
      expect(sql).toContain('UNION ALL');
    });
  });

  // ==========================================================================
  // InsertBuilder
  // ==========================================================================

  describe('InsertBuilder', () => {
    it('basic INSERT', () => {
      const sql = p
        .insertInto('users')
        .columns('id', 'name')
        .values(p.lit(1), p.lit('Alice'))
        .toSql();
      expect(sql).toBe("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    });

    it('insert() is alias for insertInto()', () => {
      const sql = p.insert('users').columns('id').values(p.lit(1)).toSql();
      expect(sql).toContain('INSERT INTO users');
    });

    it('INSERT ... SELECT', () => {
      const sql = p
        .insertInto('archive')
        .columns('id', 'name')
        .query(p.select('id', 'name').from('users'))
        .toSql();
      expect(sql).toContain('INSERT INTO archive');
      expect(sql).toContain('SELECT id, name FROM users');
    });
  });

  // ==========================================================================
  // UpdateBuilder
  // ==========================================================================

  describe('UpdateBuilder', () => {
    it('basic UPDATE', () => {
      const sql = p
        .update('users')
        .set('name', p.lit('Bob'))
        .where(p.col('id').eq(p.lit(1)))
        .toSql();
      expect(sql).toBe("UPDATE users SET name = 'Bob' WHERE id = 1");
    });

    it('multiple SET assignments', () => {
      const sql = p
        .update('users')
        .set('name', p.lit('Bob'))
        .set('age', p.lit(30))
        .toSql();
      expect(sql).toContain("name = 'Bob'");
      expect(sql).toContain('age = 30');
    });
  });

  // ==========================================================================
  // DeleteBuilder
  // ==========================================================================

  describe('DeleteBuilder', () => {
    it('basic DELETE', () => {
      const sql = p
        .deleteFrom('users')
        .where(p.col('id').eq(p.lit(1)))
        .toSql();
      expect(sql).toBe('DELETE FROM users WHERE id = 1');
    });

    it('del() is alias for deleteFrom()', () => {
      const sql = p
        .del('users')
        .where(p.col('active').eq(p.boolean(false)))
        .toSql();
      expect(sql).toContain('DELETE FROM users');
    });
  });

  // ==========================================================================
  // MergeBuilder
  // ==========================================================================

  describe('MergeBuilder', () => {
    it('basic MERGE', () => {
      const sql = p
        .mergeInto('target')
        .using('source', p.col('target.id').eq(p.col('source.id')))
        .whenMatchedUpdate({ name: p.col('source.name') })
        .whenNotMatchedInsert(
          ['id', 'name'],
          [p.col('source.id'), p.col('source.name')],
        )
        .toSql();
      expect(sql).toContain('MERGE INTO');
      expect(sql).toContain('USING source ON');
      expect(sql).toContain('WHEN MATCHED');
    });
  });

  // ==========================================================================
  // SetOpBuilder
  // ==========================================================================

  describe('SetOpBuilder', () => {
    it('UNION ALL with ORDER BY and LIMIT', () => {
      const a = p.select('id').from('a');
      const b = p.select('id').from('b');
      const sql = p.unionAll(a, b).orderBy('id').limit(10).toSql();
      expect(sql).toContain('UNION ALL');
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('LIMIT 10');
    });

    it('INTERSECT', () => {
      const a = p.select('id').from('a');
      const b = p.select('id').from('b');
      const sql = p.intersect(a, b).toSql();
      expect(sql).toContain('INTERSECT');
    });

    it('EXCEPT', () => {
      const a = p.select('id').from('a');
      const b = p.select('id').from('b');
      const sql = p.except(a, b).toSql();
      expect(sql).toContain('EXCEPT');
    });
  });

  // ==========================================================================
  // Polyglot class
  // ==========================================================================

  describe('Polyglot class', () => {
    it('creates a singleton instance', () => {
      const a = p.Polyglot.getInstance();
      const b = p.Polyglot.getInstance();
      expect(a).toBe(b);
    });

    it('transpile works', () => {
      const result = p.Polyglot.getInstance().transpile(
        'SELECT 1',
        'generic',
        'postgresql',
      );
      expect(result.success).toBe(true);
    });

    it('parse works', () => {
      const result = p.Polyglot.getInstance().parse(
        'SELECT a FROM t',
        'generic',
      );
      expect(result.success).toBe(true);
    });

    it('format works', () => {
      const result = p.Polyglot.getInstance().format(
        'SELECT a,b FROM t',
        'generic',
      );
      expect(result.success).toBe(true);
    });

    it('getDialects works', () => {
      const dialects = p.Polyglot.getInstance().getDialects();
      expect(Array.isArray(dialects)).toBe(true);
    });

    it('getVersion works', () => {
      expect(typeof p.Polyglot.getInstance().getVersion()).toBe('string');
    });
  });

  // ==========================================================================
  // Dialect enum
  // ==========================================================================

  describe('Dialect enum', () => {
    it('has common dialect values', () => {
      expect(p.Dialect.Generic).toBe('generic');
      expect(p.Dialect.PostgreSQL).toBe('postgresql');
      expect(p.Dialect.MySQL).toBe('mysql');
      expect(p.Dialect.BigQuery).toBe('bigquery');
      expect(p.Dialect.Snowflake).toBe('snowflake');
      expect(p.Dialect.DuckDB).toBe('duckdb');
      expect(p.Dialect.TSQL).toBe('tsql');
      expect(p.Dialect.ClickHouse).toBe('clickhouse');
    });
  });

  // ==========================================================================
  // End-to-end
  // ==========================================================================

  describe('end-to-end queries', () => {
    it('complex SELECT with joins, group by, having, order, limit', () => {
      const sql = p
        .select(
          p.col('u.name'),
          p.count(p.star()).alias('order_count'),
          p.sum(p.col('o.total')).alias('total_spent'),
        )
        .from('users')
        .leftJoin('orders', p.col('u.id').eq(p.col('o.user_id')))
        .where(p.col('u.active').eq(p.boolean(true)))
        .groupBy('u.name')
        .having(p.count(p.star()).gt(p.lit(0)))
        .orderBy(p.col('total_spent').desc())
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
      const b = p.insertInto('users').columns('id', 'name');
      b.values(p.lit(1), p.lit('Alice'));
      b.values(p.lit(2), p.lit('Bob'));
      const sql = b.toSql();
      expect(sql).toContain("VALUES (1, 'Alice'), (2, 'Bob')");
    });

    it('UPDATE with FROM (PostgreSQL style)', () => {
      const sql = p
        .update('t1')
        .set('name', p.col('t2.name'))
        .from('t2')
        .where(p.col('t1.id').eq(p.col('t2.id')))
        .toSql('postgresql');
      expect(sql).toContain('UPDATE t1');
      expect(sql).toContain('SET name = t2.name');
      expect(sql).toContain('FROM t2');
      expect(sql).toContain('WHERE t1.id = t2.id');
    });
  });

  // ==========================================================================
  // Default export
  // ==========================================================================

  describe('default export', () => {
    it('default export has all core functions', () => {
      const def = p.default;
      expect(typeof def.init).toBe('function');
      expect(typeof def.isInitialized).toBe('function');
      expect(typeof def.transpile).toBe('function');
      expect(typeof def.parse).toBe('function');
      expect(typeof def.generate).toBe('function');
      expect(typeof def.format).toBe('function');
      expect(typeof def.getDialects).toBe('function');
      expect(typeof def.getVersion).toBe('function');
      expect(typeof def.tokenize).toBe('function');
      expect(typeof def.lineage).toBe('function');
      expect(typeof def.getSourceTables).toBe('function');
      expect(typeof def.diff).toBe('function');
      expect(typeof def.plan).toBe('function');
    });
  });
});
