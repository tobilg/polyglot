/**
 * Query Execution Planner Module
 *
 * Converts SQL AST into an execution plan represented as a DAG of steps
 * (scan, join, aggregate, sort, set operations).
 */

import { plan as wasmPlan } from '../wasm/polyglot_sql_wasm.js';
import type { Expression } from './generated/Expression';

export type JoinType = 'inner' | 'left' | 'right' | 'full' | 'cross';
export type SetOperationType = 'union' | 'union_all' | 'intersect' | 'except';

/** Step kind — string for simple types, object for parameterized types */
export type StepKind =
  | 'scan'
  | 'aggregate'
  | 'sort'
  | { join: JoinType }
  | { set_operation: SetOperationType };

/** A single step in the execution plan */
export interface PlanStep {
  name: string;
  kind: StepKind;
  projections: Expression[];
  dependencies: PlanStep[];
  aggregations: Expression[];
  group_by: Expression[];
  condition: Expression | null;
  order_by: Expression[];
  limit: Expression | null;
}

/** Full query execution plan */
export interface QueryPlan {
  root: PlanStep;
  dag: Record<number, number[]>;
  leaves: PlanStep[];
}

/** Result from planning a query */
export interface PlanResult {
  success: boolean;
  plan?: QueryPlan;
  error?: string;
}

/**
 * Build an execution plan from a SQL query.
 *
 * @param sql - SQL string to plan
 * @param dialect - Dialect for parsing (default: 'generic')
 *
 * @example
 * ```typescript
 * const result = plan("SELECT x, SUM(y) FROM t GROUP BY x");
 * // result.plan.root.kind === "aggregate"
 * // result.plan.leaves[0].kind === "scan"
 * ```
 */
export function plan(sql: string, dialect: string = 'generic'): PlanResult {
  const resultJson = wasmPlan(sql, dialect);
  return JSON.parse(resultJson) as PlanResult;
}
