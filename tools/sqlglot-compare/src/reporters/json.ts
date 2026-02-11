import { writeFileSync, mkdirSync } from 'fs';
import { dirname } from 'path';
import { ComparisonReport, TestResult, CategorySummary, DialectSummary, ReportSummary } from '../types/index.js';

/**
 * Generate a comparison report from test results.
 */
export function generateReport(
  results: TestResult[],
  sqlglotVersion: string,
  polyglotVersion: string
): ComparisonReport {
  const passedTests = results.filter(r => r.status === 'pass').length;
  const failedTests = results.filter(r => r.status === 'fail').length;
  const skippedTests = results.filter(r => r.status === 'skip').length;
  const totalTests = results.length;

  // Group by category
  const byCategory = {
    identity: createCategorySummary(results.filter(r => r.category === 'identity')),
    pretty: createCategorySummary(results.filter(r => r.category === 'pretty')),
    dialect_identity: createCategorySummary(results.filter(r => r.category === 'dialect_identity')),
    transpilation: createCategorySummary(results.filter(r => r.category === 'transpilation')),
  };

  // Group by dialect
  const dialectResults = new Map<string, TestResult[]>();
  for (const result of results) {
    const dialect = result.dialect || result.sourceDialect || 'generic';
    if (!dialectResults.has(dialect)) {
      dialectResults.set(dialect, []);
    }
    dialectResults.get(dialect)!.push(result);
  }

  const byDialect: Record<string, DialectSummary> = {};
  for (const [dialect, dialectTestResults] of dialectResults) {
    byDialect[dialect] = {
      dialect,
      ...createCategorySummary(dialectTestResults),
    };
  }

  // Find top failure patterns
  const failurePatterns = new Map<string, number>();
  for (const result of results) {
    if (result.status === 'fail') {
      // Try to categorize the failure
      const pattern = categorizeFailure(result);
      failurePatterns.set(pattern, (failurePatterns.get(pattern) || 0) + 1);
    }
  }

  const topFailurePatterns = Array.from(failurePatterns.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([pattern, count]) => ({ pattern, count }));

  const summary: ReportSummary = {
    byCategory,
    byDialect,
    topFailurePatterns,
  };

  return {
    metadata: {
      timestamp: new Date().toISOString(),
      sqlglotVersion,
      polyglotVersion,
      totalTests,
      passedTests,
      failedTests,
      skippedTests,
      passRate: totalTests > 0 ? passedTests / totalTests : 0,
    },
    summary,
    results,
  };
}

/**
 * Create a summary for a category of tests.
 */
function createCategorySummary(results: TestResult[]): CategorySummary {
  const total = results.length;
  const passed = results.filter(r => r.status === 'pass').length;
  const failed = results.filter(r => r.status === 'fail').length;
  const skipped = results.filter(r => r.status === 'skip').length;

  return {
    total,
    passed,
    failed,
    skipped,
    passRate: total > 0 ? passed / total : 0,
  };
}

/**
 * Try to categorize a failure based on the error or difference.
 */
function categorizeFailure(result: TestResult): string {
  const input = result.input.toLowerCase();
  const error = (result.polyglotResult.error || '').toLowerCase();

  // Check for common patterns
  if (input.includes('interval')) return 'INTERVAL parsing';
  if (input.includes('->') || input.includes('->>')) return 'JSON operators';
  if (input.includes('over') && input.includes('(')) return 'Window functions';
  if (input.includes('::')) return 'Cast operator';
  if (input.includes('array')) return 'Array operations';
  if (input.includes('struct')) return 'Struct operations';
  if (input.includes('lateral')) return 'LATERAL joins';
  if (input.includes('match_recognize')) return 'MATCH_RECOGNIZE';
  if (input.includes('connect by')) return 'CONNECT BY';
  if (input.includes('pivot') || input.includes('unpivot')) return 'PIVOT/UNPIVOT';
  if (error.includes('parse')) return 'Parse error';
  if (error.includes('unsupported')) return 'Unsupported feature';

  return 'Other';
}

/**
 * Write report to a JSON file.
 */
export function writeJsonReport(report: ComparisonReport, outputPath: string): void {
  // Ensure directory exists
  mkdirSync(dirname(outputPath), { recursive: true });

  writeFileSync(outputPath, JSON.stringify(report, null, 2));
}

/**
 * Generate a filename for the report.
 */
export function generateReportFilename(prefix: string = 'comparison'): string {
  const date = new Date().toISOString().split('T')[0];
  return `${prefix}-${date}.json`;
}
