/**
 * AST Type Definitions
 *
 * This module re-exports all generated types from the Rust AST
 * and provides type guards and utility types for working with the AST.
 */

// Re-export all generated types
export * from '../../generated';

// Re-export specific commonly-used types for convenience
export type { Expression } from '../../generated/Expression';
export type { Select } from '../../generated/Select';
export type { Insert } from '../../generated/Insert';
export type { Update } from '../../generated/Update';
export type { Delete } from '../../generated/Delete';
export type { Column } from '../../generated/Column';
export type { TableRef } from '../../generated/TableRef';
export type { Identifier } from '../../generated/Identifier';
export type { Literal } from '../../generated/Literal';
export type { BinaryOp } from '../../generated/BinaryOp';
export type { UnaryOp } from '../../generated/UnaryOp';
export type { Join } from '../../generated/Join';
export type { JoinKind } from '../../generated/JoinKind';
export type { From } from '../../generated/From';
export type { Where } from '../../generated/Where';
export type { GroupBy } from '../../generated/GroupBy';
export type { Having } from '../../generated/Having';
export type { OrderBy } from '../../generated/OrderBy';
export type { Ordered } from '../../generated/Ordered';
export type { Limit } from '../../generated/Limit';
export type { Offset } from '../../generated/Offset';
export type { DataType } from '../../generated/DataType';
export type { Cast } from '../../generated/Cast';
export type { Case } from '../../generated/Case';
export type { Alias } from '../../generated/Alias';

// Import Expression for type guards
import type { Expression } from '../../generated/Expression';

// Export type guards
export * from './guards';

// Export utility types
export * from './utilities';
