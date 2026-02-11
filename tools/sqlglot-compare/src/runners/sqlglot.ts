import { spawn } from 'child_process';
import { ExecutionResult } from '../types/index.js';

/**
 * Run sqlglot Python library via subprocess.
 */
export class SqlglotRunner {
  private pythonPath: string;

  constructor(pythonPath: string = '.venv/bin/python') {
    this.pythonPath = pythonPath;
  }

  /**
   * Execute a transpile operation using Python sqlglot.
   */
  async transpile(
    sql: string,
    readDialect: string,
    writeDialect: string
  ): Promise<ExecutionResult> {
    const encoded = this.escapeForPython(sql);
    const script = `
import json
import base64
try:
    import sqlglot
    sql = base64.b64decode('${encoded}').decode('utf-8')
    result = sqlglot.transpile(sql, read='${readDialect}', write='${writeDialect}')
    output = result[0] if result else None
    print(json.dumps({'success': True, 'output': output}))
except Exception as e:
    print(json.dumps({'success': False, 'error': str(e)}))
`;
    return this.runPython(script);
  }

  /**
   * Parse SQL and regenerate it (identity roundtrip).
   */
  async roundtrip(sql: string, dialect: string = ''): Promise<ExecutionResult> {
    const encoded = this.escapeForPython(sql);
    const dialectArg = dialect ? `read='${dialect}'` : '';
    const writeArg = dialect ? `, dialect='${dialect}'` : '';

    const script = `
import json
import base64
try:
    import sqlglot
    sql = base64.b64decode('${encoded}').decode('utf-8')
    expr = sqlglot.parse_one(sql${dialectArg ? ', ' + dialectArg : ''})
    output = expr.sql(${dialectArg}${writeArg})
    print(json.dumps({'success': True, 'output': output}))
except Exception as e:
    print(json.dumps({'success': False, 'error': str(e)}))
`;
    return this.runPython(script);
  }

  /**
   * Format SQL with pretty printing.
   */
  async format(sql: string, dialect: string = ''): Promise<ExecutionResult> {
    const encoded = this.escapeForPython(sql);
    const dialectArg = dialect ? `'${dialect}'` : '';

    const script = `
import json
import base64
try:
    import sqlglot
    sql = base64.b64decode('${encoded}').decode('utf-8')
    expr = sqlglot.parse_one(sql${dialectArg ? ', read=' + dialectArg : ''})
    output = expr.sql(${dialectArg ? 'dialect=' + dialectArg + ', ' : ''}pretty=True)
    print(json.dumps({'success': True, 'output': output}))
except Exception as e:
    print(json.dumps({'success': False, 'error': str(e)}))
`;
    return this.runPython(script);
  }

  /**
   * Get sqlglot version.
   */
  async getVersion(): Promise<string> {
    const script = `
import json
try:
    import sqlglot
    print(json.dumps({'success': True, 'output': sqlglot.__version__}))
except Exception as e:
    print(json.dumps({'success': False, 'error': str(e)}))
`;
    const result = await this.runPython(script);
    if (result.success && result.output) {
      return result.output;
    }
    return 'unknown';
  }

  /**
   * Escape SQL string for embedding in Python.
   * Uses base64 encoding to avoid all quoting issues.
   */
  private escapeForPython(sql: string): string {
    // Base64 encode to avoid all quoting issues
    return Buffer.from(sql, 'utf-8').toString('base64');
  }

  /**
   * Generate Python code to decode the base64 SQL.
   */
  private decodeSqlInPython(varName: string, encoded: string): string {
    return `${varName} = __import__('base64').b64decode('${encoded}').decode('utf-8')`;
  }

  /**
   * Execute a Python script and return the result.
   */
  private async runPython(script: string): Promise<ExecutionResult> {
    const startTime = performance.now();

    return new Promise((resolve) => {
      const proc = spawn(this.pythonPath, ['-c', script], {
        stdio: ['pipe', 'pipe', 'pipe'],
        timeout: 30000, // 30 second timeout
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      proc.on('error', (error) => {
        resolve({
          success: false,
          error: `Process error: ${error.message}`,
          executionTimeMs: performance.now() - startTime,
        });
      });

      proc.on('close', (code) => {
        const executionTimeMs = performance.now() - startTime;

        if (code !== 0) {
          resolve({
            success: false,
            error: stderr || `Process exited with code ${code}`,
            executionTimeMs,
          });
          return;
        }

        try {
          const result = JSON.parse(stdout.trim());
          resolve({
            success: result.success,
            output: result.output,
            error: result.error,
            executionTimeMs,
          });
        } catch {
          resolve({
            success: false,
            error: `Failed to parse output: ${stdout}`,
            executionTimeMs,
          });
        }
      });
    });
  }
}
