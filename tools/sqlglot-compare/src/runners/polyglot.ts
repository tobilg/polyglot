import { ExecutionResult } from '../types/index.js';

// Import from the local polyglot package
// Note: This requires the package to be built first
let polyglot: typeof import('@polyglot-sql/sdk') | null = null;

async function getPolyglot(): Promise<typeof import('@polyglot-sql/sdk')> {
  if (!polyglot) {
    polyglot = await import('@polyglot-sql/sdk');
  }
  return polyglot;
}

/**
 * Map dialect names from sqlglot format to Polyglot format.
 */
function mapDialect(dialect: string): string {
  const mapping: Record<string, string> = {
    'postgres': 'postgresql',
    'postgresql': 'postgresql',
    'mysql': 'mysql',
    'bigquery': 'bigquery',
    'snowflake': 'snowflake',
    'duckdb': 'duckdb',
    'sqlite': 'sqlite',
    'hive': 'hive',
    'spark': 'spark',
    'trino': 'trino',
    'presto': 'presto',
    'redshift': 'redshift',
    'tsql': 'tsql',
    'mssql': 'tsql',
    'sqlserver': 'tsql',
    'oracle': 'oracle',
    'clickhouse': 'clickhouse',
    'databricks': 'databricks',
    '': 'generic',
  };

  return mapping[dialect.toLowerCase()] || dialect.toLowerCase();
}

/**
 * Run Polyglot TypeScript SDK directly.
 */
export class PolyglotRunner {
  /**
   * Execute a transpile operation using Polyglot.
   */
  async transpile(
    sql: string,
    readDialect: string,
    writeDialect: string
  ): Promise<ExecutionResult> {
    const startTime = performance.now();

    try {
      const pg = await getPolyglot();
      const result = pg.transpile(sql, {
        read: mapDialect(readDialect) as any,
        write: mapDialect(writeDialect) as any,
      });

      return {
        success: result.success,
        output: result.sql?.[0],
        error: result.error,
        executionTimeMs: performance.now() - startTime,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        executionTimeMs: performance.now() - startTime,
      };
    }
  }

  /**
   * Parse SQL and regenerate it (identity roundtrip).
   */
  async roundtrip(sql: string, dialect: string = ''): Promise<ExecutionResult> {
    const startTime = performance.now();

    try {
      const pg = await getPolyglot();
      const mappedDialect = mapDialect(dialect) as any;

      // Parse
      const parseResult = pg.parse(sql, mappedDialect);
      if (!parseResult.success || !parseResult.ast) {
        return {
          success: false,
          error: parseResult.error || 'Parse failed',
          executionTimeMs: performance.now() - startTime,
        };
      }

      // Generate
      const genResult = pg.generate(parseResult.ast, mappedDialect);
      return {
        success: genResult.success,
        output: genResult.sql?.[0],
        error: genResult.error,
        executionTimeMs: performance.now() - startTime,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        executionTimeMs: performance.now() - startTime,
      };
    }
  }

  /**
   * Format SQL with pretty printing.
   */
  async format(sql: string, dialect: string = ''): Promise<ExecutionResult> {
    const startTime = performance.now();

    try {
      const pg = await getPolyglot();
      const mappedDialect = mapDialect(dialect) as any;

      const result = pg.format(sql, mappedDialect);
      return {
        success: result.success,
        output: result.sql?.[0],
        error: result.error,
        executionTimeMs: performance.now() - startTime,
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message || String(error),
        executionTimeMs: performance.now() - startTime,
      };
    }
  }

  /**
   * Get Polyglot version.
   */
  async getVersion(): Promise<string> {
    try {
      const pg = await getPolyglot();
      return pg.getVersion();
    } catch {
      return 'unknown';
    }
  }
}
