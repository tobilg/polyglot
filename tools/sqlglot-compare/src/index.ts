#!/usr/bin/env node
import { Command } from 'commander';
import { existsSync, readFileSync } from 'fs';
import { resolve, join } from 'path';

import { extractIdentityTests, IDENTITY_FIXTURE_PATH } from './extractors/identity.js';
import { extractPrettyTests, PRETTY_FIXTURE_PATH } from './extractors/pretty.js';
import { loadAllDialectTests, DIALECTS_FIXTURE_DIR } from './extractors/dialect.js';
import { SqlglotRunner } from './runners/sqlglot.js';
import { PolyglotRunner } from './runners/polyglot.js';
import { compareSql } from './comparators/sql.js';
import { generateReport, writeJsonReport, generateReportFilename } from './reporters/json.js';
import { printCliReport, printCiSummary, checkThreshold } from './reporters/cli.js';
import {
  TestCase,
  TestResult,
  IdentityTest,
  PrettyTest,
  DialectIdentityTest,
  TranspilationTest,
  CompareOptions,
  KnownDifferencesConfig,
} from './types/index.js';

const program = new Command();

program
  .name('sqlglot-compare')
  .description('Compare SQLGlot Python library output with Polyglot')
  .version('1.0.0');

program
  .command('compare')
  .description('Run comparison tests between SQLGlot and Polyglot')
  .option('-f, --filter <pattern>', 'Filter tests by pattern (regex)')
  .option('-d, --dialect <dialect>', 'Filter tests by dialect')
  .option('-c, --category <category>', 'Filter tests by category (identity, pretty, dialect_identity, transpilation)')
  .option('-v, --verbose', 'Show detailed output including failures')
  .option('-o, --output-dir <dir>', 'Output directory for reports', './reports')
  .option('--format <format>', 'Report format: json, cli, or both', 'both')
  .option('--identity-only', 'Only run identity tests from identity.sql')
  .option('--max-tests <n>', 'Maximum number of tests to run', parseInt)
  .action(async (options) => {
    await runComparison(options);
  });

program
  .command('check-threshold')
  .description('Check if comparison results meet a minimum pass rate')
  .requiredOption('-r, --report <path>', 'Path to JSON report file')
  .option('--min-pass-rate <rate>', 'Minimum pass rate (0.0 - 1.0)', parseFloat, 0.9)
  .action((options) => {
    checkPassThreshold(options.report, options.minPassRate);
  });

program
  .command('extract')
  .description('Extract test cases from SQLGlot source (uses Python script)')
  .action(() => {
    console.log('Run the Python extraction script:');
    console.log('  python scripts/extract-tests.py');
  });

async function runComparison(options: CompareOptions & { identityOnly?: boolean; maxTests?: number }): Promise<void> {
  const projectRoot = resolve(process.cwd(), '../..');
  const toolRoot = process.cwd();

  console.log('SQLGlot vs Polyglot Comparison');
  console.log('==============================');
  console.log('');

  // Initialize runners
  const sqlglot = new SqlglotRunner();
  const polyglot = new PolyglotRunner();

  // Get versions
  const sqlglotVersion = await sqlglot.getVersion();
  const polyglotVersion = await polyglot.getVersion();

  console.log(`SQLGlot version:  ${sqlglotVersion}`);
  console.log(`Polyglot version: ${polyglotVersion}`);
  console.log('');

  // Load known differences config
  const knownDiffsPath = join(toolRoot, 'known-differences.json');
  let knownDiffs: KnownDifferencesConfig = { skip: [], acceptableDifferences: [], expectedFailures: [] };
  if (existsSync(knownDiffsPath)) {
    knownDiffs = JSON.parse(readFileSync(knownDiffsPath, 'utf-8'));
  }

  // Collect test cases
  const testCases: TestCase[] = [];

  // Identity tests from identity.sql
  const identityPath = join(projectRoot, IDENTITY_FIXTURE_PATH);
  if (existsSync(identityPath)) {
    const identityTests = extractIdentityTests(identityPath);
    testCases.push(...identityTests);
    console.log(`Loaded ${identityTests.length} identity tests from identity.sql`);
  }

  if (!options.identityOnly) {
    // Pretty tests from pretty.sql
    const prettyPath = join(projectRoot, PRETTY_FIXTURE_PATH);
    if (existsSync(prettyPath)) {
      const prettyTests = extractPrettyTests(prettyPath);
      testCases.push(...prettyTests);
      console.log(`Loaded ${prettyTests.length} pretty tests from pretty.sql`);
    }

    // Dialect tests from extracted JSON
    const dialectsPath = join(toolRoot, DIALECTS_FIXTURE_DIR);
    if (existsSync(dialectsPath)) {
      const dialectTests = loadAllDialectTests(dialectsPath);
      for (const dt of dialectTests) {
        testCases.push(...dt.identity);
        testCases.push(...dt.transpilation);
      }
      console.log(`Loaded dialect tests from ${dialectTests.length} dialects`);
    } else {
      console.log('No dialect test fixtures found. Run: python scripts/extract-tests.py');
    }
  }

  console.log(`Total test cases: ${testCases.length}`);
  console.log('');

  // Apply filters
  let filteredTests = testCases;

  if (options.filter) {
    const regex = new RegExp(options.filter, 'i');
    filteredTests = filteredTests.filter(t => {
      const sql = 'sql' in t ? t.sql : ('input' in t ? t.input : '');
      return regex.test(t.id) || regex.test(sql);
    });
  }

  if (options.dialect) {
    filteredTests = filteredTests.filter(t => {
      if ('dialect' in t) {
        return t.dialect?.toLowerCase() === options.dialect?.toLowerCase();
      }
      return false;
    });
  }

  if (options.category) {
    filteredTests = filteredTests.filter(t => t.category === options.category);
  }

  // Apply skip rules
  filteredTests = filteredTests.filter(t => {
    const sql = 'sql' in t ? t.sql : ('input' in t ? t.input : '');
    return !knownDiffs.skip.some(rule => new RegExp(rule.pattern, 'i').test(sql));
  });

  if (options.maxTests && options.maxTests > 0) {
    filteredTests = filteredTests.slice(0, options.maxTests);
  }

  console.log(`Running ${filteredTests.length} tests...`);
  console.log('');

  // Run tests
  const results: TestResult[] = [];
  let completed = 0;

  for (const test of filteredTests) {
    const result = await runTest(test, sqlglot, polyglot);
    results.push(result);

    completed++;
    if (completed % 100 === 0) {
      const percent = ((completed / filteredTests.length) * 100).toFixed(0);
      process.stdout.write(`\rProgress: ${completed}/${filteredTests.length} (${percent}%)`);
    }
  }

  console.log(`\rProgress: ${completed}/${filteredTests.length} (100%)   `);
  console.log('');

  // Generate report
  const report = generateReport(results, sqlglotVersion, polyglotVersion);

  // Output report
  const reportFormat = (options as any).format || 'both';

  if (reportFormat === 'json' || reportFormat === 'both') {
    const outputDir = options.outputDir || './reports';
    const reportPath = join(outputDir, generateReportFilename());
    writeJsonReport(report, reportPath);
    console.log(`JSON report written to: ${reportPath}`);
  }

  if (reportFormat === 'cli' || reportFormat === 'both') {
    printCliReport(report, options.verbose);
  }
}

async function runTest(
  test: TestCase,
  sqlglot: SqlglotRunner,
  polyglot: PolyglotRunner
): Promise<TestResult> {
  switch (test.category) {
    case 'identity':
      return runIdentityTest(test as IdentityTest, sqlglot, polyglot);
    case 'pretty':
      return runPrettyTest(test as PrettyTest, sqlglot, polyglot);
    case 'dialect_identity':
      return runDialectIdentityTest(test as DialectIdentityTest, sqlglot, polyglot);
    case 'transpilation':
      return runTranspilationTest(test as TranspilationTest, sqlglot, polyglot);
    default: {
      // TypeScript exhaustiveness check - cast to any for unexpected categories
      const unknownTest = test as any;
      return {
        id: unknownTest.id || 'unknown',
        category: unknownTest.category || 'unknown',
        input: '',
        expectedOutput: '',
        sqlglotResult: { success: false, error: 'Unknown test category', executionTimeMs: 0 },
        polyglotResult: { success: false, error: 'Unknown test category', executionTimeMs: 0 },
        comparison: { match: false, exactMatch: false, normalizedMatch: false, differences: [] },
        status: 'skip',
      };
    }
  }
}

async function runIdentityTest(
  test: IdentityTest,
  sqlglot: SqlglotRunner,
  polyglot: PolyglotRunner
): Promise<TestResult> {
  const [sqlglotResult, polyglotResult] = await Promise.all([
    sqlglot.roundtrip(test.sql),
    polyglot.roundtrip(test.sql),
  ]);

  const expectedOutput = sqlglotResult.output || test.sql;
  const comparison = compareSql(
    expectedOutput,
    polyglotResult.output || ''
  );

  let status: TestResult['status'] = 'fail';
  if (!sqlglotResult.success) {
    status = 'skip'; // SQLGlot itself can't handle this
  } else if (comparison.match) {
    status = 'pass';
  } else if (!polyglotResult.success) {
    status = 'error';
  }

  return {
    id: test.id,
    category: 'identity',
    input: test.sql,
    expectedOutput,
    sqlglotResult,
    polyglotResult,
    comparison,
    status,
  };
}

async function runPrettyTest(
  test: PrettyTest,
  sqlglot: SqlglotRunner,
  polyglot: PolyglotRunner
): Promise<TestResult> {
  // For pretty tests, we parse the input and generate with pretty=true (format)
  const [sqlglotResult, polyglotResult] = await Promise.all([
    sqlglot.format(test.input),
    polyglot.format(test.input),
  ]);

  // Expected output is the pre-formatted expected from the test
  const expectedOutput = test.expected;
  const comparison = compareSql(
    expectedOutput,
    polyglotResult.output || ''
  );

  let status: TestResult['status'] = 'fail';
  if (!sqlglotResult.success) {
    status = 'skip'; // SQLGlot itself can't handle this
  } else if (comparison.match) {
    status = 'pass';
  } else if (!polyglotResult.success) {
    status = 'error';
  }

  return {
    id: test.id,
    category: 'pretty',
    input: test.input,
    expectedOutput,
    sqlglotResult,
    polyglotResult,
    comparison,
    status,
  };
}

async function runDialectIdentityTest(
  test: DialectIdentityTest,
  sqlglot: SqlglotRunner,
  polyglot: PolyglotRunner
): Promise<TestResult> {
  const dialect = test.dialect;

  const [sqlglotResult, polyglotResult] = await Promise.all([
    sqlglot.roundtrip(test.sql, dialect),
    polyglot.roundtrip(test.sql, dialect),
  ]);

  const expectedOutput = test.expectedSql || sqlglotResult.output || test.sql;
  const comparison = compareSql(
    expectedOutput,
    polyglotResult.output || ''
  );

  let status: TestResult['status'] = 'fail';
  if (!sqlglotResult.success) {
    status = 'skip';
  } else if (comparison.match) {
    status = 'pass';
  } else if (!polyglotResult.success) {
    status = 'error';
  }

  return {
    id: test.id,
    category: 'dialect_identity',
    dialect,
    input: test.sql,
    expectedOutput,
    sqlglotResult,
    polyglotResult,
    comparison,
    status,
  };
}

async function runTranspilationTest(
  test: TranspilationTest,
  sqlglot: SqlglotRunner,
  polyglot: PolyglotRunner
): Promise<TestResult> {
  // For transpilation tests, pick the first write target
  const writeDialects = Object.keys(test.write);
  if (writeDialects.length === 0) {
    return {
      id: test.id,
      category: 'transpilation',
      dialect: test.dialect,
      input: test.sql,
      expectedOutput: '',
      sqlglotResult: { success: false, error: 'No write dialects', executionTimeMs: 0 },
      polyglotResult: { success: false, error: 'No write dialects', executionTimeMs: 0 },
      comparison: { match: false, exactMatch: false, normalizedMatch: false, differences: [] },
      status: 'skip',
    };
  }

  const targetDialect = writeDialects[0];
  const expectedOutput = test.write[targetDialect];

  const [sqlglotResult, polyglotResult] = await Promise.all([
    sqlglot.transpile(test.sql, test.dialect, targetDialect),
    polyglot.transpile(test.sql, test.dialect, targetDialect),
  ]);

  const comparison = compareSql(
    expectedOutput,
    polyglotResult.output || ''
  );

  let status: TestResult['status'] = 'fail';
  if (!sqlglotResult.success) {
    status = 'skip';
  } else if (comparison.match) {
    status = 'pass';
  } else if (!polyglotResult.success) {
    status = 'error';
  }

  return {
    id: test.id,
    category: 'transpilation',
    dialect: test.dialect,
    sourceDialect: test.dialect,
    targetDialect,
    input: test.sql,
    expectedOutput,
    sqlglotResult,
    polyglotResult,
    comparison,
    status,
  };
}

function checkPassThreshold(reportPath: string, minPassRate: number): void {
  if (!existsSync(reportPath)) {
    console.error(`Report file not found: ${reportPath}`);
    process.exit(1);
  }

  const report = JSON.parse(readFileSync(reportPath, 'utf-8'));
  printCiSummary(report);

  if (checkThreshold(report, minPassRate)) {
    console.log(`PASS: Pass rate ${(report.metadata.passRate * 100).toFixed(1)}% >= ${(minPassRate * 100).toFixed(1)}%`);
    process.exit(0);
  } else {
    console.log(`FAIL: Pass rate ${(report.metadata.passRate * 100).toFixed(1)}% < ${(minPassRate * 100).toFixed(1)}%`);
    process.exit(1);
  }
}

program.parse();
