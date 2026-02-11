/**
 * Validation types for SQL syntax and semantic validation.
 */

/**
 * Severity level for validation issues.
 */
export type ValidationSeverity = 'error' | 'warning';

/**
 * Validation severity constants for convenience.
 */
export const ValidationSeverity = {
  Error: 'error' as const,
  Warning: 'warning' as const,
};

/**
 * A single validation error or warning.
 */
export interface ValidationError {
  /** The error/warning message */
  message: string;
  /** Line number where the error occurred (1-based) */
  line?: number;
  /** Column number where the error occurred (1-based) */
  column?: number;
  /** Severity of the validation issue */
  severity: ValidationSeverity;
  /** Error code (e.g., "E001", "W001") */
  code: string;
}

/**
 * Result of validating SQL.
 */
export interface ValidationResult {
  /** Whether the SQL is valid (no errors, warnings are allowed) */
  valid: boolean;
  /** List of validation errors and warnings */
  errors: ValidationError[];
}

/**
 * Options for SQL validation.
 */
export interface ValidationOptions {
  /**
   * Enable semantic validation in addition to syntax checking.
   * Semantic validation checks for issues like SELECT * usage,
   * aggregate functions without GROUP BY, etc.
   * @default false
   */
  semantic?: boolean;

  /**
   * Enable dialect-specific validation rules.
   * When true, additional dialect-specific rules are applied.
   * @default false
   */
  dialectStrict?: boolean;
}
