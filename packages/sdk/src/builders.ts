/**
 * WASM-backed SQL Builders
 *
 * Thin TypeScript wrappers around Rust builder types exposed via WASM.
 * Each class holds a WASM handle and delegates operations to Rust,
 * adding fluent chaining, string convenience, and variadic args.
 */

import type {
  WasmAssignmentArray as WasmAssignmentArrayType,
  WasmCaseBuilder as WasmCaseBuilderType,
  WasmDeleteBuilder as WasmDeleteBuilderType,
  WasmExprArray as WasmExprArrayType,
  WasmExpr as WasmExprType,
  WasmInsertBuilder as WasmInsertBuilderType,
  WasmMergeBuilder as WasmMergeBuilderType,
  WasmSelectBuilder as WasmSelectBuilderType,
  WasmSetOpBuilder as WasmSetOpBuilderType,
  WasmUpdateBuilder as WasmUpdateBuilderType,
  WasmWindowDefBuilder as WasmWindowDefBuilderType,
} from '../wasm/polyglot_sql_wasm.js';

import { getWasmSync } from './wasm-loader';

// ============================================================================
// Types
// ============================================================================

/** Anything that can be used as an expression: Expr, string (→ column), number/boolean/null (→ literal) */
export type ExprInput = Expr | string | number | boolean | null;

// ============================================================================
// Internal helpers
// ============================================================================

/** Convert an ExprInput to a WasmExpr handle. */
function toWasm(value: ExprInput): WasmExprType {
  const wasm = getWasmSync();
  if (value instanceof Expr) return value._w;
  if (typeof value === 'string') return wasm.wasm_col(value);
  if (typeof value === 'number' || typeof value === 'boolean' || value === null)
    return wasm.wasm_lit(value);
  return wasm.wasm_null();
}

/** Convert an array of ExprInput to a WasmExprArray. */
function toWasmArray(items: ExprInput[]): WasmExprArrayType {
  const wasm = getWasmSync();
  const arr = new wasm.WasmExprArray();
  for (const item of items) {
    if (item instanceof Expr) {
      arr.push(item._w);
    } else if (typeof item === 'string') {
      arr.push_col(item);
    } else if (typeof item === 'number') {
      if (Number.isInteger(item)) {
        arr.push_int(item);
      } else {
        arr.push_float(item);
      }
    } else if (typeof item === 'boolean' || item === null) {
      arr.push(wasm.wasm_lit(item));
    } else {
      arr.push(wasm.wasm_null());
    }
  }
  return arr;
}

// ============================================================================
// Expr class — wraps WasmExpr, adds method chaining
// ============================================================================

/**
 * A SQL expression. Wraps a WASM handle and provides operator methods.
 *
 * All methods return new Expr instances (original is reusable).
 *
 * @example
 * ```typescript
 * const cond = col('age').gte(lit(18)).and(col('status').neq(lit('banned')));
 * ```
 */
export class Expr {
  /** @internal WASM handle — do not use directly */
  readonly _w: WasmExprType;

  /** @internal */
  constructor(w: WasmExprType) {
    this._w = w;
  }

  // -- Comparison --

  eq(other: ExprInput): Expr {
    return new Expr(this._w.eq(toWasm(other)));
  }
  neq(other: ExprInput): Expr {
    return new Expr(this._w.neq(toWasm(other)));
  }
  lt(other: ExprInput): Expr {
    return new Expr(this._w.lt(toWasm(other)));
  }
  lte(other: ExprInput): Expr {
    return new Expr(this._w.lte(toWasm(other)));
  }
  gt(other: ExprInput): Expr {
    return new Expr(this._w.gt(toWasm(other)));
  }
  gte(other: ExprInput): Expr {
    return new Expr(this._w.gte(toWasm(other)));
  }

  // -- Logical --

  and(other: ExprInput): Expr {
    return new Expr(this._w.and(toWasm(other)));
  }
  or(other: ExprInput): Expr {
    return new Expr(this._w.or(toWasm(other)));
  }
  not(): Expr {
    return new Expr(this._w.not());
  }
  xor(other: ExprInput): Expr {
    return new Expr(this._w.xor(toWasm(other)));
  }

  // -- Arithmetic --

  add(other: ExprInput): Expr {
    return new Expr(this._w.add(toWasm(other)));
  }
  sub(other: ExprInput): Expr {
    return new Expr(this._w.sub(toWasm(other)));
  }
  mul(other: ExprInput): Expr {
    return new Expr(this._w.mul(toWasm(other)));
  }
  div(other: ExprInput): Expr {
    return new Expr(this._w.div(toWasm(other)));
  }

  // -- Pattern matching --

  like(pattern: ExprInput): Expr {
    return new Expr(this._w.like(toWasm(pattern)));
  }
  ilike(pattern: ExprInput): Expr {
    return new Expr(this._w.ilike(toWasm(pattern)));
  }
  rlike(pattern: ExprInput): Expr {
    return new Expr(this._w.rlike(toWasm(pattern)));
  }

  // -- Predicates --

  isNull(): Expr {
    return new Expr(this._w.is_null());
  }
  isNotNull(): Expr {
    return new Expr(this._w.is_not_null());
  }
  between(low: ExprInput, high: ExprInput): Expr {
    return new Expr(this._w.between(toWasm(low), toWasm(high)));
  }
  inList(...values: ExprInput[]): Expr {
    return new Expr(this._w.in_list(toWasmArray(values)));
  }
  notIn(...values: ExprInput[]): Expr {
    return new Expr(this._w.not_in(toWasmArray(values)));
  }

  // -- Transform --

  alias(name: string): Expr {
    return new Expr(this._w.alias(name));
  }
  as(name: string): Expr {
    return this.alias(name);
  }
  cast(to: string): Expr {
    return new Expr(this._w.cast(to));
  }
  asc(): Expr {
    return new Expr(this._w.asc());
  }
  desc(): Expr {
    return new Expr(this._w.desc());
  }

  // -- Output --

  /** Generate SQL string (generic dialect). */
  toSql(): string {
    return this._w.to_sql();
  }
  /** Return the expression AST as a plain JS object. */
  toJSON(): any {
    return this._w.to_json();
  }
  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

// ============================================================================
// WindowDefBuilder
// ============================================================================

/**
 * Builder for named WINDOW clause definitions.
 *
 * Used with `SelectBuilder.window()` to define reusable window specifications.
 *
 * @example
 * ```typescript
 * const w = new WindowDefBuilder()
 *   .partitionBy('dept')
 *   .orderBy(col('salary').desc());
 * const sql = select('id').from('t').window('w', w).toSql();
 * ```
 */
export class WindowDefBuilder {
  /** @internal WASM handle */
  _w: WasmWindowDefBuilderType;

  constructor() {
    this._w = new (getWasmSync() as { WasmWindowDefBuilder: new () => WasmWindowDefBuilderType })
      .WasmWindowDefBuilder();
  }

  /** Set the PARTITION BY expressions. */
  partitionBy(...exprs: ExprInput[]): this {
    this._w.partition_by(toWasmArray(exprs));
    return this;
  }

  /** Set the ORDER BY expressions. */
  orderBy(...exprs: ExprInput[]): this {
    this._w.order_by(toWasmArray(exprs));
    return this;
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

// ============================================================================
// Expression helper functions
// ============================================================================

/** Create a column reference. Supports dotted names like `'users.id'`. */
export function col(name: string): Expr {
  return new Expr(getWasmSync().wasm_col(name));
}

/** Create a literal value (string, number, boolean, or null). */
export function lit(value: string | number | boolean | null): Expr {
  return new Expr(getWasmSync().wasm_lit(value));
}

/** Create a star (*) expression. */
export function star(): Expr {
  return new Expr(getWasmSync().wasm_star());
}

/** Create a SQL NULL expression. */
export function sqlNull(): Expr {
  return new Expr(getWasmSync().wasm_null());
}

/** Create a SQL boolean literal. */
export function boolean(value: boolean): Expr {
  return new Expr(getWasmSync().wasm_boolean(value));
}

/** Create a table reference. Supports dotted names like `'schema.table'`. */
export function table(name: string): Expr {
  return new Expr(getWasmSync().wasm_table(name));
}

/** Parse a raw SQL fragment into an expression. */
export function sqlExpr(sql: string): Expr {
  return new Expr(getWasmSync().wasm_sql_expr(sql));
}

/** Alias for `sqlExpr()` — parse a SQL condition string. */
export function condition(sql: string): Expr {
  return sqlExpr(sql);
}

/** Create a generic function call expression. */
export function func(name: string, ...args: ExprInput[]): Expr {
  return new Expr(getWasmSync().wasm_func(name, toWasmArray(args)));
}

/** Create a NOT expression. */
export function not(expr: ExprInput): Expr {
  return new Expr(getWasmSync().wasm_not(toWasm(expr)));
}

/** Create a CAST(expr AS type) expression. */
export function cast(expr: ExprInput, to: string): Expr {
  return new Expr(getWasmSync().wasm_cast(toWasm(expr), to));
}

/** Create an expr AS name alias expression. */
export function alias(expr: ExprInput, name: string): Expr {
  return new Expr(getWasmSync().wasm_alias(toWasm(expr), name));
}

/**
 * Wrap a SelectBuilder as a named subquery for use in FROM or JOIN clauses.
 * Note: this consumes the SelectBuilder.
 *
 * @example
 * ```typescript
 * const inner = select('id', 'name').from('users');
 * const sql = select('sub.id').from(subquery(inner, 'sub')).toSql();
 * ```
 */
export function subquery(query: SelectBuilder, alias: string): Expr {
  return new Expr(getWasmSync().wasm_subquery(query._w, alias));
}

// Variadic logical operators

/** Chain multiple conditions with AND. */
export function and(...conditions: ExprInput[]): Expr {
  if (conditions.length === 0) return boolean(true);
  if (conditions.length === 1)
    return conditions[0] instanceof Expr
      ? conditions[0]
      : new Expr(toWasm(conditions[0]));
  const wasm = getWasmSync();
  let result = toWasm(conditions[0]);
  for (let i = 1; i < conditions.length; i++) {
    result = wasm.wasm_and(result, toWasm(conditions[i]));
  }
  return new Expr(result);
}

/** Chain multiple conditions with OR. */
export function or(...conditions: ExprInput[]): Expr {
  if (conditions.length === 0) return boolean(false);
  if (conditions.length === 1)
    return conditions[0] instanceof Expr
      ? conditions[0]
      : new Expr(toWasm(conditions[0]));
  const wasm = getWasmSync();
  let result = toWasm(conditions[0]);
  for (let i = 1; i < conditions.length; i++) {
    result = wasm.wasm_or(result, toWasm(conditions[i]));
  }
  return new Expr(result);
}

// ============================================================================
// Convenience function wrappers (thin TS over func())
// ============================================================================

// -- Aggregate functions --

export function count(expr?: ExprInput): Expr {
  return expr !== undefined ? func('COUNT', expr) : func('COUNT', star());
}
export function countDistinct(expr: ExprInput): Expr {
  return new Expr(getWasmSync().wasm_count_distinct(toWasm(expr)));
}
export function sum(expr: ExprInput): Expr {
  return func('SUM', expr);
}
export function avg(expr: ExprInput): Expr {
  return func('AVG', expr);
}
export function min(expr: ExprInput): Expr {
  return func('MIN', expr);
}
export function max(expr: ExprInput): Expr {
  return func('MAX', expr);
}

// -- String functions --

export function upper(expr: ExprInput): Expr {
  return func('UPPER', expr);
}
export function lower(expr: ExprInput): Expr {
  return func('LOWER', expr);
}
export function length(expr: ExprInput): Expr {
  return func('LENGTH', expr);
}
export function trim(expr: ExprInput): Expr {
  return func('TRIM', expr);
}
export function ltrim(expr: ExprInput): Expr {
  return func('LTRIM', expr);
}
export function rtrim(expr: ExprInput): Expr {
  return func('RTRIM', expr);
}
export function reverse(expr: ExprInput): Expr {
  return func('REVERSE', expr);
}
export function initcap(expr: ExprInput): Expr {
  return func('INITCAP', expr);
}
export function substring(
  expr: ExprInput,
  start: ExprInput,
  length?: ExprInput,
): Expr {
  return length !== undefined
    ? func('SUBSTRING', expr, start, length)
    : func('SUBSTRING', expr, start);
}
export function replace(expr: ExprInput, from: ExprInput, to: ExprInput): Expr {
  return func('REPLACE', expr, from, to);
}
export function concatWs(separator: ExprInput, ...exprs: ExprInput[]): Expr {
  return func('CONCAT_WS', separator, ...exprs);
}

// -- Null handling --

export function coalesce(...exprs: ExprInput[]): Expr {
  return func('COALESCE', ...exprs);
}
export function nullIf(expr1: ExprInput, expr2: ExprInput): Expr {
  return func('NULLIF', expr1, expr2);
}
export function ifNull(expr: ExprInput, fallback: ExprInput): Expr {
  return func('IFNULL', expr, fallback);
}

// -- Math functions --

export function abs(expr: ExprInput): Expr {
  return func('ABS', expr);
}
export function round(expr: ExprInput, decimals?: ExprInput): Expr {
  return decimals !== undefined
    ? func('ROUND', expr, decimals)
    : func('ROUND', expr);
}
export function floor(expr: ExprInput): Expr {
  return func('FLOOR', expr);
}
export function ceil(expr: ExprInput): Expr {
  return func('CEIL', expr);
}
export function power(base: ExprInput, exp: ExprInput): Expr {
  return func('POWER', base, exp);
}
export function sqrt(expr: ExprInput): Expr {
  return func('SQRT', expr);
}
export function ln(expr: ExprInput): Expr {
  return func('LN', expr);
}
export function exp(expr: ExprInput): Expr {
  return func('EXP', expr);
}
export function sign(expr: ExprInput): Expr {
  return func('SIGN', expr);
}
export function greatest(...exprs: ExprInput[]): Expr {
  return func('GREATEST', ...exprs);
}
export function least(...exprs: ExprInput[]): Expr {
  return func('LEAST', ...exprs);
}

// -- Date/time functions --

export function currentDate(): Expr {
  return func('CURRENT_DATE');
}
export function currentTime(): Expr {
  return func('CURRENT_TIME');
}
export function currentTimestamp(): Expr {
  return func('CURRENT_TIMESTAMP');
}
export function extract(unit: string, from: ExprInput): Expr {
  return new Expr(getWasmSync().wasm_extract(unit, toWasm(from)));
}

// -- Window functions --

export function rowNumber(): Expr {
  return func('ROW_NUMBER');
}
export function rank(): Expr {
  return func('RANK');
}
export function denseRank(): Expr {
  return func('DENSE_RANK');
}

// ============================================================================
// SelectBuilder
// ============================================================================

/**
 * Fluent builder for SELECT queries.
 *
 * @example
 * ```typescript
 * const sql = select('id', 'name')
 *   .from('users')
 *   .where(col('status').eq(lit('active')))
 *   .orderBy(col('name').asc())
 *   .limit(10)
 *   .toSql('postgresql');
 * ```
 */
export class SelectBuilder {
  /** @internal */
  _w: WasmSelectBuilderType;

  constructor() {
    this._w = new (getWasmSync() as { WasmSelectBuilder: new () => WasmSelectBuilderType })
      .WasmSelectBuilder();
  }

  /** Add columns to the SELECT list. Accepts Expr, strings (→ col), or '*'. */
  select(...columns: (ExprInput | '*')[]): this {
    for (const c of columns) {
      if (c === '*') this._w.select_star();
      else if (c instanceof Expr) this._w.select_expr(c._w);
      else if (typeof c === 'string') this._w.select_col(c);
      else this._w.select_expr(toWasm(c));
    }
    return this;
  }

  /** Set the FROM clause. */
  from(tableOrExpr: string | Expr): this {
    if (typeof tableOrExpr === 'string') this._w.from(tableOrExpr);
    else this._w.from_expr(tableOrExpr._w);
    return this;
  }

  /** Add an INNER JOIN. */
  join(tableName: string, on: ExprInput): this {
    this._w.join(tableName, toWasm(on));
    return this;
  }

  /** Add a LEFT JOIN. */
  leftJoin(tableName: string, on: ExprInput): this {
    this._w.left_join(tableName, toWasm(on));
    return this;
  }

  /** Add a RIGHT JOIN. */
  rightJoin(tableName: string, on: ExprInput): this {
    this._w.right_join(tableName, toWasm(on));
    return this;
  }

  /** Add a CROSS JOIN. */
  crossJoin(tableName: string): this {
    this._w.cross_join(tableName);
    return this;
  }

  /** Set the WHERE clause. Accepts an Expr or a raw SQL string. */
  where(condition: ExprInput | string): this {
    if (typeof condition === 'string') this._w.where_sql(condition);
    else if (condition instanceof Expr) this._w.where_expr(condition._w);
    else this._w.where_expr(toWasm(condition));
    return this;
  }

  /** Set the GROUP BY clause. */
  groupBy(...cols: ExprInput[]): this {
    this._w.group_by_cols(toWasmArray(cols));
    return this;
  }

  /** Set the HAVING clause. */
  having(condition: ExprInput): this {
    this._w.having(toWasm(condition));
    return this;
  }

  /** Set the ORDER BY clause. */
  orderBy(...exprs: ExprInput[]): this {
    this._w.order_by_exprs(toWasmArray(exprs));
    return this;
  }

  /** Set the LIMIT. */
  limit(n: number): this {
    this._w.limit(n);
    return this;
  }

  /** Set the OFFSET. */
  offset(n: number): this {
    this._w.offset(n);
    return this;
  }

  /** Enable DISTINCT. */
  distinct(): this {
    this._w.distinct();
    return this;
  }

  /** Set the QUALIFY clause (Snowflake/BigQuery/DuckDB). */
  qualify(condition: ExprInput): this {
    this._w.qualify(toWasm(condition));
    return this;
  }

  /** Set the SORT BY clause (Hive/Spark — sorts within each partition). */
  sortBy(...exprs: ExprInput[]): this {
    this._w.sort_by_exprs(toWasmArray(exprs));
    return this;
  }

  /** Add a named WINDOW clause definition. */
  window(name: string, def: WindowDefBuilder): this {
    this._w.window(name, def._w);
    return this;
  }

  /** Add a LATERAL VIEW clause (Hive/Spark UDTF expansion). */
  lateral(funcExpr: Expr, tableAlias: string, colAliases: string[]): this {
    this._w.lateral_view(funcExpr._w, tableAlias, colAliases);
    return this;
  }

  /** Add a query hint (e.g. Oracle hint expressions). */
  hint(text: string): this {
    this._w.hint(text);
    return this;
  }

  /**
   * Convert to CREATE TABLE AS SELECT and return the AST.
   * This is a terminal operation that consumes the builder.
   */
  ctas(tableName: string): any {
    return this._w.ctas(tableName);
  }

  /**
   * Convert to CREATE TABLE AS SELECT and return generated SQL.
   * This is a terminal operation that consumes the builder.
   */
  ctasSql(tableName: string, dialect: string = 'generic'): string {
    return this._w.ctas_sql(tableName, dialect);
  }

  /** Add FOR UPDATE locking. */
  forUpdate(): this {
    this._w.for_update();
    return this;
  }

  /** Combine with UNION. */
  union(other: SelectBuilder): SetOpBuilder {
    return new SetOpBuilder(this._w.union(other._w));
  }

  /** Combine with UNION ALL. */
  unionAll(other: SelectBuilder): SetOpBuilder {
    return new SetOpBuilder(this._w.union_all(other._w));
  }

  /** Combine with INTERSECT. */
  intersect(other: SelectBuilder): SetOpBuilder {
    return new SetOpBuilder(this._w.intersect(other._w));
  }

  /** Combine with EXCEPT. */
  except(other: SelectBuilder): SetOpBuilder {
    return new SetOpBuilder(this._w.except_(other._w));
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create a SelectBuilder, optionally pre-populated with columns. */
export function select(...columns: (ExprInput | '*')[]): SelectBuilder {
  const b = new SelectBuilder();
  if (columns.length > 0) b.select(...columns);
  return b;
}

// ============================================================================
// InsertBuilder
// ============================================================================

/**
 * Fluent builder for INSERT INTO statements.
 *
 * @example
 * ```typescript
 * const sql = insertInto('users')
 *   .columns('id', 'name')
 *   .values(lit(1), lit('Alice'))
 *   .toSql();
 * ```
 */
export class InsertBuilder {
  /** @internal */
  _w: WasmInsertBuilderType;

  constructor(tableName: string) {
    this._w = new (getWasmSync() as {
      WasmInsertBuilder: new (tableName: string) => WasmInsertBuilderType;
    }).WasmInsertBuilder(tableName);
  }

  /** Set the target column names. */
  columns(...cols: string[]): this {
    this._w.columns(cols);
    return this;
  }

  /** Append a row of values. Call multiple times for multiple rows. */
  values(...vals: ExprInput[]): this {
    this._w.values(toWasmArray(vals));
    return this;
  }

  /** Set the source query for INSERT ... SELECT. */
  query(q: SelectBuilder): this {
    this._w.query(q._w);
    return this;
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create an InsertBuilder for the given table. */
export function insertInto(tableName: string): InsertBuilder {
  return new InsertBuilder(tableName);
}

/** Alias for insertInto. */
export function insert(tableName: string): InsertBuilder {
  return insertInto(tableName);
}

// ============================================================================
// UpdateBuilder
// ============================================================================

/**
 * Fluent builder for UPDATE statements.
 *
 * @example
 * ```typescript
 * const sql = update('users')
 *   .set('name', lit('Bob'))
 *   .where(col('id').eq(lit(1)))
 *   .toSql();
 * ```
 */
export class UpdateBuilder {
  /** @internal */
  _w: WasmUpdateBuilderType;

  constructor(tableName: string) {
    this._w = new (getWasmSync() as {
      WasmUpdateBuilder: new (tableName: string) => WasmUpdateBuilderType;
    }).WasmUpdateBuilder(tableName);
  }

  /** Add a SET column = value assignment. */
  set(column: string, value: ExprInput): this {
    this._w.set(column, toWasm(value));
    return this;
  }

  /** Set the WHERE clause. */
  where(condition: ExprInput): this {
    this._w.where_expr(toWasm(condition));
    return this;
  }

  /** Set the FROM clause (PostgreSQL/Snowflake UPDATE ... FROM). */
  from(tableName: string): this {
    this._w.from(tableName);
    return this;
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create an UpdateBuilder for the given table. */
export function update(tableName: string): UpdateBuilder {
  return new UpdateBuilder(tableName);
}

// ============================================================================
// DeleteBuilder
// ============================================================================

/**
 * Fluent builder for DELETE FROM statements.
 *
 * @example
 * ```typescript
 * const sql = deleteFrom('users')
 *   .where(col('id').eq(lit(1)))
 *   .toSql();
 * ```
 */
export class DeleteBuilder {
  /** @internal */
  _w: WasmDeleteBuilderType;

  constructor(tableName: string) {
    this._w = new (getWasmSync() as {
      WasmDeleteBuilder: new (tableName: string) => WasmDeleteBuilderType;
    }).WasmDeleteBuilder(tableName);
  }

  /** Set the WHERE clause. */
  where(condition: ExprInput): this {
    this._w.where_expr(toWasm(condition));
    return this;
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create a DeleteBuilder for the given table. */
export function deleteFrom(tableName: string): DeleteBuilder {
  return new DeleteBuilder(tableName);
}

/** Alias for deleteFrom. */
export function del(tableName: string): DeleteBuilder {
  return deleteFrom(tableName);
}

// ============================================================================
// MergeBuilder
// ============================================================================

/**
 * Fluent builder for MERGE INTO statements.
 *
 * @example
 * ```typescript
 * const sql = mergeInto('target')
 *   .using('source', col('target.id').eq(col('source.id')))
 *   .whenMatchedUpdate({ name: col('source.name') })
 *   .whenNotMatchedInsert(['id', 'name'], [col('source.id'), col('source.name')])
 *   .toSql();
 * ```
 */
export class MergeBuilder {
  /** @internal */
  _w: WasmMergeBuilderType;

  constructor(target: string) {
    this._w = new (getWasmSync() as {
      WasmMergeBuilder: new (target: string) => WasmMergeBuilderType;
    }).WasmMergeBuilder(target);
  }

  /** Set the source table and ON condition. */
  using(source: string, on: ExprInput): this {
    this._w.using(source, toWasm(on));
    return this;
  }

  /** Add a WHEN MATCHED THEN UPDATE SET clause. */
  whenMatchedUpdate(assignments: Record<string, ExprInput>): this {
    const arr = new (getWasmSync() as {
      WasmAssignmentArray: new () => WasmAssignmentArrayType;
    }).WasmAssignmentArray();
    for (const [column, value] of Object.entries(assignments)) {
      arr.push(column, toWasm(value));
    }
    this._w.when_matched_update(arr);
    return this;
  }

  /** Add a WHEN MATCHED THEN DELETE clause. */
  whenMatchedDelete(): this {
    this._w.when_matched_delete();
    return this;
  }

  /** Add a WHEN NOT MATCHED THEN INSERT clause. */
  whenNotMatchedInsert(columns: string[], values: ExprInput[]): this {
    this._w.when_not_matched_insert(columns, toWasmArray(values));
    return this;
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create a MergeBuilder for the given target table. */
export function mergeInto(target: string): MergeBuilder {
  return new MergeBuilder(target);
}

// ============================================================================
// CaseBuilder
// ============================================================================

/**
 * Fluent builder for CASE expressions.
 *
 * @example
 * ```typescript
 * const expr = caseWhen()
 *   .when(col('x').gt(lit(0)), lit('positive'))
 *   .else_(lit('negative'))
 *   .build();
 * ```
 */
export class CaseBuilder {
  /** @internal */
  _w: WasmCaseBuilderType;

  /** @internal */
  constructor(w: WasmCaseBuilderType) {
    this._w = w;
  }

  /** Add a WHEN condition THEN result branch. */
  when(condition: ExprInput, result: ExprInput): this {
    this._w.when(toWasm(condition), toWasm(result));
    return this;
  }

  /** Set the ELSE result. */
  else_(result: ExprInput): this {
    this._w.else_(toWasm(result));
    return this;
  }

  /** Build the CASE expression as an Expr. */
  build(): Expr {
    return new Expr(this._w.build_expr());
  }

  /** Generate SQL string (generic dialect). */
  toSql(): string {
    return this._w.to_sql();
  }
}

/** Create a searched CASE builder (CASE WHEN ... THEN ...). */
export function caseWhen(): CaseBuilder {
  return new CaseBuilder(
    new (getWasmSync() as { WasmCaseBuilder: new () => WasmCaseBuilderType })
      .WasmCaseBuilder(),
  );
}

/** Create a simple CASE builder (CASE operand WHEN value THEN ...). */
export function caseOf(operand: ExprInput): CaseBuilder {
  return new CaseBuilder(getWasmSync().wasm_case_of(toWasm(operand)));
}

// ============================================================================
// SetOpBuilder
// ============================================================================

/**
 * Builder for set operations (UNION, INTERSECT, EXCEPT) with optional
 * ORDER BY, LIMIT, and OFFSET.
 */
export class SetOpBuilder {
  /** @internal */
  _w: WasmSetOpBuilderType;

  /** @internal */
  constructor(w: WasmSetOpBuilderType) {
    this._w = w;
  }

  /** Set the ORDER BY clause. */
  orderBy(...exprs: ExprInput[]): this {
    this._w.order_by_exprs(toWasmArray(exprs));
    return this;
  }

  /** Set the LIMIT. */
  limit(n: number): this {
    this._w.limit(n);
    return this;
  }

  /** Set the OFFSET. */
  offset(n: number): this {
    this._w.offset(n);
    return this;
  }

  /** Generate SQL string. Defaults to generic dialect. */
  toSql(dialect: string = 'generic'): string {
    return this._w.to_sql(dialect);
  }

  /** Return the Expression AST as a plain JS object. */
  build(): any {
    return this._w.build();
  }

  /** Free the underlying WASM handle. */
  free(): void {
    this._w.free();
  }
}

/** Create a UNION of two SELECT queries. */
export function union(left: SelectBuilder, right: SelectBuilder): SetOpBuilder {
  return left.union(right);
}

/** Create a UNION ALL of two SELECT queries. */
export function unionAll(
  left: SelectBuilder,
  right: SelectBuilder,
): SetOpBuilder {
  return left.unionAll(right);
}

/** Create an INTERSECT of two SELECT queries. */
export function intersect(
  left: SelectBuilder,
  right: SelectBuilder,
): SetOpBuilder {
  return left.intersect(right);
}

/** Create an EXCEPT of two SELECT queries. */
export function except(
  left: SelectBuilder,
  right: SelectBuilder,
): SetOpBuilder {
  return left.except(right);
}
