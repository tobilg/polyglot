/**
 * Schema Types for Schema-Aware Validation
 *
 * These types define the structure for database schema definitions
 * used in schema-aware SQL validation.
 */

/**
 * Column definition in a table schema.
 */
export interface ColumnSchema {
  /** Column name */
  name: string;

  /**
   * Column data type (e.g., "integer", "varchar", "timestamp").
   * Type names are case-insensitive.
   */
  type: string;

  /** Whether the column allows NULL values */
  nullable?: boolean;

  /** Whether this column is a primary key */
  primaryKey?: boolean;

  /** Whether this column has a uniqueness constraint */
  unique?: boolean;

  /**
   * Foreign key reference to another table.
   * Used for validating join conditions.
   */
  references?: {
    table: string;
    column: string;
    schema?: string;
  };
}

/**
 * Table-level foreign key constraint.
 */
export interface TableForeignKey {
  /** Optional constraint name */
  name?: string;

  /** Source columns in this table */
  columns: string[];

  /** Target reference */
  references: {
    table: string;
    columns: string[];
    schema?: string;
  };
}

/**
 * Table definition in the schema.
 */
export interface TableSchema {
  /** Table name */
  name: string;

  /** Schema/namespace name (e.g., "public", "dbo") */
  schema?: string;

  /** Column definitions */
  columns: ColumnSchema[];

  /** Aliases that can refer to this table */
  aliases?: string[];

  /** Primary key columns */
  primaryKey?: string[];

  /** Unique key groups (composite keys allowed) */
  uniqueKeys?: string[][];

  /** Table-level foreign keys */
  foreignKeys?: TableForeignKey[];
}

/**
 * Database schema definition for validation.
 */
export interface Schema {
  /** Table definitions */
  tables: TableSchema[];

  /**
   * If true (default), unknown tables/columns are errors.
   * If false, they are warnings.
   */
  strict?: boolean;
}

/**
 * Options for schema-aware validation.
 */
export interface SchemaValidationOptions {
  /**
   * Check column types in expressions for compatibility.
   * @default false
   */
  checkTypes?: boolean;

  /**
   * Check FK/reference integrity and JOIN/reference quality.
   * @default false
   */
  checkReferences?: boolean;

  /**
   * Treat unknown identifiers as errors (true) or warnings (false).
   * Overrides Schema.strict if provided.
   * @default true
   */
  strict?: boolean;

  /**
   * Enable semantic validation rules in addition to schema checks.
   * @default false
   */
  semantic?: boolean;
}
