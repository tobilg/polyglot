import { ComparisonReport, TestResult, CategorySummary } from '../types/index.js';

/**
 * Format a percentage with 1 decimal place.
 */
function formatPercent(value: number): string {
  return (value * 100).toFixed(1) + '%';
}

/**
 * Format a count ratio string.
 */
function formatRatio(passed: number, total: number): string {
  return `${passed}/${total}`;
}

/**
 * Create a progress bar string.
 */
function progressBar(ratio: number, width: number = 20): string {
  const filled = Math.round(ratio * width);
  const empty = width - filled;
  return '[' + '='.repeat(filled) + ' '.repeat(empty) + ']';
}

/**
 * Print a category summary line.
 */
function printCategorySummary(name: string, summary: CategorySummary): void {
  const ratio = formatRatio(summary.passed, summary.total);
  const percent = formatPercent(summary.passRate);
  const bar = progressBar(summary.passRate);

  console.log(`  ${name.padEnd(20)} ${ratio.padStart(12)} ${percent.padStart(8)} ${bar}`);
}

/**
 * Print the CLI summary report.
 */
export function printCliReport(report: ComparisonReport, verbose: boolean = false): void {
  const { metadata, summary, results } = report;

  console.log('');
  console.log('='.repeat(70));
  console.log('SQLGlot vs Polyglot Comparison Report');
  console.log('='.repeat(70));
  console.log('');

  // Metadata
  console.log(`Timestamp:        ${metadata.timestamp}`);
  console.log(`SQLGlot Version:  ${metadata.sqlglotVersion}`);
  console.log(`Polyglot Version: ${metadata.polyglotVersion}`);
  console.log('');

  // Overall summary
  console.log('Summary:');
  console.log('-'.repeat(70));
  console.log(`  Total Tests:    ${metadata.totalTests}`);
  console.log(`  Passed:         ${metadata.passedTests} (${formatPercent(metadata.passRate)})`);
  console.log(`  Failed:         ${metadata.failedTests} (${formatPercent(metadata.failedTests / metadata.totalTests)})`);
  console.log(`  Skipped:        ${metadata.skippedTests} (${formatPercent(metadata.skippedTests / metadata.totalTests)})`);
  console.log('');

  // By category
  console.log('By Category:');
  console.log('-'.repeat(70));
  printCategorySummary('Identity', summary.byCategory.identity);
  printCategorySummary('Pretty', summary.byCategory.pretty);
  printCategorySummary('Dialect Identity', summary.byCategory.dialect_identity);
  printCategorySummary('Transpilation', summary.byCategory.transpilation);
  console.log('');

  // By dialect (top 10)
  console.log('By Dialect (Top 10):');
  console.log('-'.repeat(70));

  const dialectEntries = Object.entries(summary.byDialect)
    .sort((a, b) => b[1].total - a[1].total)
    .slice(0, 10);

  for (const [dialect, dialectSummary] of dialectEntries) {
    printCategorySummary(dialect, dialectSummary);
  }
  console.log('');

  // Top failure patterns
  if (summary.topFailurePatterns.length > 0) {
    console.log('Top Failure Patterns:');
    console.log('-'.repeat(70));
    for (let i = 0; i < Math.min(5, summary.topFailurePatterns.length); i++) {
      const { pattern, count } = summary.topFailurePatterns[i];
      console.log(`  ${(i + 1).toString().padStart(2)}. ${pattern.padEnd(30)} (${count} failures)`);
    }
    console.log('');
  }

  // Verbose output - show individual failures
  if (verbose) {
    const failures = results.filter(r => r.status === 'fail');
    if (failures.length > 0) {
      console.log('Failed Tests (first 20):');
      console.log('-'.repeat(70));

      for (let i = 0; i < Math.min(20, failures.length); i++) {
        const result = failures[i];
        console.log(`\n[${result.id}] ${result.category}`);
        console.log(`  Input:    ${truncate(result.input, 60)}`);
        console.log(`  Expected: ${truncate(result.expectedOutput, 60)}`);
        console.log(`  SQLGlot:  ${truncate(result.sqlglotResult.output || result.sqlglotResult.error || '', 60)}`);
        console.log(`  Polyglot: ${truncate(result.polyglotResult.output || result.polyglotResult.error || '', 60)}`);

        if (result.comparison.differences.length > 0) {
          console.log(`  Diff:     ${result.comparison.differences[0].description}`);
        }
      }

      if (failures.length > 20) {
        console.log(`\n  ... and ${failures.length - 20} more failures`);
      }
      console.log('');
    }
  }

  console.log('='.repeat(70));
}

/**
 * Truncate a string to a maximum length.
 */
function truncate(str: string, maxLength: number): string {
  // Replace newlines with spaces for display
  const single = str.replace(/\n/g, ' ').replace(/\s+/g, ' ');
  if (single.length <= maxLength) {
    return single;
  }
  return single.substring(0, maxLength - 3) + '...';
}

/**
 * Print a simple pass/fail summary for CI.
 */
export function printCiSummary(report: ComparisonReport): void {
  const { metadata } = report;
  const status = metadata.passRate >= 0.9 ? 'PASS' : 'FAIL';

  console.log(`${status}: ${metadata.passedTests}/${metadata.totalTests} tests passed (${formatPercent(metadata.passRate)})`);
}

/**
 * Check if the report meets a minimum pass rate threshold.
 */
export function checkThreshold(report: ComparisonReport, minPassRate: number): boolean {
  return report.metadata.passRate >= minPassRate;
}
