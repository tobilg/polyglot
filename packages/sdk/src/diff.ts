/**
 * SQL Diff Module
 *
 * Compares two SQL ASTs using the ChangeDistiller algorithm with Dice
 * coefficient matching, returning a list of edit operations (insert, remove,
 * move, update, keep).
 */

import { getWasmSync } from './wasm-loader';
import type { Expression } from './generated/Expression';

/** Types of edit operations */
export type EditType = 'insert' | 'remove' | 'move' | 'update' | 'keep';

/** A single edit in the diff result */
export interface DiffEdit {
  type: EditType;
  /** Present for insert/remove edits */
  expression?: Expression;
  /** Present for move/update/keep edits */
  source?: Expression;
  /** Present for move/update/keep edits */
  target?: Expression;
}

/** Configuration for diff sensitivity */
export interface DiffOptions {
  /** Exclude 'keep' edits from result (default: false) */
  deltaOnly?: boolean;
  /** Dice coefficient threshold for internal nodes (default: 0.6) */
  f?: number;
  /** Leaf similarity threshold (default: 0.6) */
  t?: number;
}

/** Result from diffing two SQL statements */
export interface DiffResult {
  success: boolean;
  edits?: DiffEdit[];
  error?: string;
}

/**
 * Diff two SQL statements and return edit operations.
 *
 * @param source - Source SQL string
 * @param target - Target SQL string
 * @param dialect - Dialect for parsing (default: 'generic')
 * @param options - Diff configuration options
 *
 * @example
 * ```typescript
 * const result = diff(
 *   "SELECT col_a FROM t",
 *   "SELECT col_b FROM t",
 * );
 * // result.edits contains update/keep edits
 * ```
 */
export function diff(
  source: string,
  target: string,
  dialect: string = 'generic',
  options: DiffOptions = {},
): DiffResult {
  const { deltaOnly = false, f = 0.6, t = 0.6 } = options;
  const wasm = getWasmSync();
  const resultJson = wasm.diff_sql(source, target, dialect, deltaOnly, f, t);
  return JSON.parse(resultJson) as DiffResult;
}

/** Check if any edits represent actual changes (not keeps) */
export function hasChanges(edits: DiffEdit[]): boolean {
  return edits.some((e) => e.type !== 'keep');
}

/** Filter to only change edits (exclude keeps) */
export function changesOnly(edits: DiffEdit[]): DiffEdit[] {
  return edits.filter((e) => e.type !== 'keep');
}
