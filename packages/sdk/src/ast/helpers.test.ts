import { describe, expect, expectTypeOf, it } from 'vitest';
import type { Expression } from '../generated/Expression';
import { Dialect, type ParseResult, parse } from '../index';
import {
  clone,
  type ExpressionByKey,
  type ExpressionInner,
  type ExpressionType,
  findByType,
  findFirst,
  getExprData,
  getExprType,
  isExpressionType,
} from './index';

type IsAny<T> = 0 extends 1 & T ? true : false;

describe('AST helper types', () => {
  it('exposes a typed, discriminated parse result', () => {
    expectTypeOf<
      IsAny<NonNullable<ParseResult['ast']>>
    >().toEqualTypeOf<false>();
    const success = parse('SELECT a FROM t', Dialect.Generic);
    expect(success.success).toBe(true);
    if (!success.success) {
      throw new Error(success.error);
    }

    expectTypeOf(success.ast).toEqualTypeOf<Expression[]>();
    expect(success.ast).toHaveLength(1);

    const failure = parse('SELECT 1 + )', Dialect.Generic);
    expect(failure.success).toBe(false);
    if (failure.success) {
      throw new Error('expected malformed SQL to fail');
    }

    expectTypeOf(failure.ast).toEqualTypeOf<null | undefined>();
    expectTypeOf(failure.error).toEqualTypeOf<string>();
    expect(failure.ast).toBeNull();
    expect(failure.error).toBeTypeOf('string');
  });

  it('preserves a narrowed select payload and variant key', () => {
    const result = parse('SELECT a, b FROM t', Dialect.Generic);
    if (!result.success) {
      throw new Error(result.error);
    }

    const root = result.ast[0];
    expectTypeOf(getExprData(root)).toBeUnknown();
    expectTypeOf(findByType(root, 'select')).toEqualTypeOf<
      ExpressionByKey<'select'>[]
    >();
    expect(isExpressionType(root, 'select')).toBe(true);
    if (!isExpressionType(root, 'select')) {
      throw new Error('expected a select expression');
    }

    expectTypeOf(root).toEqualTypeOf<ExpressionByKey<'select'>>();
    expectTypeOf(getExprType(root)).toEqualTypeOf<'select'>();

    const select = getExprData(root);
    expectTypeOf(select).toEqualTypeOf<ExpressionInner<'select'>>();
    expect(select.expressions).toHaveLength(2);
    expect(select.joins).toEqual([]);

    // @ts-expect-error A Select payload cannot expose Lateral-only fields.
    expect(select.column_aliases).toBeUndefined();
    // @ts-expect-error Unknown expression variants must be rejected.
    expect(isExpressionType(root, 'not_an_expression_variant')).toBe(false);

    expectTypeOf<ExpressionByKey<'select' | 'lateral'>>().toEqualTypeOf<
      ExpressionByKey<'select'> | ExpressionByKey<'lateral'>
    >();
    expectTypeOf<ExpressionInner<'select' | 'lateral'>>().toEqualTypeOf<
      ExpressionInner<'select'> | ExpressionInner<'lateral'>
    >();
  });

  it('does not narrow either branch for union or broad type tags', () => {
    type DateLikeExpression = ExpressionByKey<
      'null' | 'current_date' | 'column_position'
    >;

    function verifyUnionTag(
      expression: DateLikeExpression,
      type: 'null' | 'current_date',
    ): void {
      if (isExpressionType(expression, type)) {
        expectTypeOf(expression).toEqualTypeOf<DateLikeExpression>();
      } else {
        expectTypeOf(expression).toEqualTypeOf<DateLikeExpression>();
      }
    }

    function verifyBroadTag(
      expression: Expression,
      type: ExpressionType,
    ): void {
      if (isExpressionType(expression, type)) {
        expectTypeOf(expression).toEqualTypeOf<Expression>();
      } else {
        expectTypeOf(expression).toEqualTypeOf<Expression>();
      }
    }

    const nullExpression: ExpressionByKey<'null'> = { null: null };
    verifyUnionTag(nullExpression, 'current_date');
    verifyBroadTag(nullExpression, 'current_date');
  });

  it('supports variants without dedicated named guards', () => {
    const result = parse(
      'SELECT f.VALUE FROM events, LATERAL FLATTEN(INPUT => payload) AS f',
      Dialect.Snowflake,
    );
    if (!result.success) {
      throw new Error(result.error);
    }

    const lateral = findFirst(result.ast[0], (node) =>
      isExpressionType(node, 'lateral'),
    );
    expect(lateral).toBeDefined();
    if (!lateral || !isExpressionType(lateral, 'lateral')) {
      throw new Error('expected a lateral expression');
    }

    expectTypeOf(lateral).toEqualTypeOf<ExpressionByKey<'lateral'>>();
    expectTypeOf(getExprType(lateral)).toEqualTypeOf<'lateral'>();

    const data = getExprData(lateral);
    expectTypeOf(data).toEqualTypeOf<ExpressionInner<'lateral'>>();
    expect(data.alias).toBe('f');
    expect(data.column_aliases ?? []).toEqual([]);

    // @ts-expect-error A Lateral payload cannot expose Select-only fields.
    expect(data.expressions).toBeUndefined();
  });

  it('preserves null payload variants', () => {
    const nullExpression: ExpressionByKey<'null'> = { null: null };

    expectTypeOf(getExprType(nullExpression)).toEqualTypeOf<'null'>();
    expectTypeOf(getExprData(nullExpression)).toEqualTypeOf<null>();
    expect(getExprData(nullExpression)).toBeNull();
  });

  it('preserves primitive payload variants', () => {
    const firstPosition: ExpressionByKey<'column_position'> = {
      column_position: 'First',
    };

    expectTypeOf(getExprData(firstPosition)).toEqualTypeOf<
      ExpressionInner<'column_position'>
    >();
    expect(getExprData(firstPosition)).toBe('First');
    expect(clone(firstPosition)).toEqual(firstPosition);
  });
});
