/**
 * AST Type Definitions
 *
 * This module re-exports selected generated types from the Rust AST
 * and provides type guards and utility types for working with the AST.
 */

// Re-export selected generated types
export type { Alias } from '../../generated/Alias';
export type { BinaryOp } from '../../generated/BinaryOp';
export type { Case } from '../../generated/Case';
export type { Cast } from '../../generated/Cast';
export type { Column } from '../../generated/Column';
export type { DataType } from '../../generated/DataType';
export type { Delete } from '../../generated/Delete';
// Re-export specific commonly-used types for convenience
export type { Expression } from '../../generated/Expression';
export type { From } from '../../generated/From';
export type { GroupBy } from '../../generated/GroupBy';
export type { Having } from '../../generated/Having';
export type { Identifier } from '../../generated/Identifier';
export type { Index } from '../../generated/Index';
export type { Insert } from '../../generated/Insert';
export type { Join } from '../../generated/Join';
export type { JoinKind } from '../../generated/JoinKind';
export type { Limit } from '../../generated/Limit';
export type { Literal } from '../../generated/Literal';
export type { Offset } from '../../generated/Offset';
export type { OrderBy } from '../../generated/OrderBy';
export type { Ordered } from '../../generated/Ordered';
export type { Select } from '../../generated/Select';
export type { TableRef } from '../../generated/TableRef';
export type { UnaryOp } from '../../generated/UnaryOp';
export type { Update } from '../../generated/Update';
export type { Where } from '../../generated/Where';

// Export type guards
export * from './guards';

// Export utility types
export * from './utilities';
