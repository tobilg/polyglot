/**
 * Semantic Validation Rules
 *
 * These rules check for semantic issues beyond syntax correctness.
 */

import type { Expression } from '../generated/Expression';
import type { ValidationError, ValidationOptions } from './types';
import { findByType, hasAggregates, getAggregateFunctions } from '../ast/visitor/walker';
import { getExprType, getExprData } from '../ast/helpers';

/**
 * Run all semantic validation rules on an expression
 */
export function validateSemantics(
  expr: Expression,
  options: ValidationOptions
): ValidationError[] {
  const errors: ValidationError[] = [];

  // Run rules based on statement type
  if (getExprType(expr) === 'select') {
    errors.push(...validateSelect(expr, options));
  }

  return errors;
}

/**
 * Validate SELECT statement semantics
 */
function validateSelect(
  select: Expression,
  _options: ValidationOptions
): ValidationError[] {
  const errors: ValidationError[] = [];

  // Rule W001: SELECT * is discouraged
  const stars = findByType(select, 'star');
  if (stars.length > 0) {
    errors.push({
      message: 'SELECT * is discouraged; specify columns explicitly for better performance and maintainability',
      severity: 'warning',
      code: 'W001',
    });
  }

  // Rule W002: Aggregate without GROUP BY
  const selectData = getExprData(select) as {
    expressions: Expression[];
    group_by: unknown;
    distinct: unknown;
    order_by: unknown;
    limit: unknown;
  };

  if (hasAggregates(select) && !selectData.group_by) {
    // Check if there are non-aggregate, non-literal columns in SELECT
    const aggregateFunctions = getAggregateFunctions(select);
    const selectCols = selectData.expressions || [];

    // Count expressions that are aggregates vs non-aggregates
    const hasNonAggregateColumn = selectCols.some((col) => {
      // Check if this expression contains any aggregate
      const colType = getExprType(col);
      if (colType === 'column' || colType === 'identifier') {
        // Check if this column is inside an aggregate
        const colAggregates = getAggregateFunctions(col);
        return colAggregates.length === 0;
      }
      return false;
    });

    if (hasNonAggregateColumn && aggregateFunctions.length > 0) {
      errors.push({
        message: 'Mixing aggregate functions with non-aggregated columns without GROUP BY may cause errors in strict SQL mode',
        severity: 'warning',
        code: 'W002',
      });
    }
  }

  // Rule W003: DISTINCT with ORDER BY columns not in SELECT
  if (selectData.distinct && selectData.order_by) {
    // This is a simplified check - full check would require column name matching
    errors.push({
      message: 'DISTINCT with ORDER BY: ensure ORDER BY columns are in SELECT list',
      severity: 'warning',
      code: 'W003',
    });
  }

  // Rule W004: LIMIT without ORDER BY
  if (selectData.limit && !selectData.order_by) {
    errors.push({
      message: 'LIMIT without ORDER BY produces non-deterministic results',
      severity: 'warning',
      code: 'W004',
    });
  }

  return errors;
}
