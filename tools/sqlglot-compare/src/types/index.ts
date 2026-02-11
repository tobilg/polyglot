// Test case types
export interface IdentityTest {
  id: string;
  line: number;
  sql: string;
  category: 'identity';
}

export interface PrettyTest {
  id: string;
  line: number;
  input: string;
  expected: string;
  category: 'pretty';
}

export interface DialectIdentityTest {
  id: string;
  dialect: string;
  sql: string;
  expectedSql: string | null; // null means same as sql
  category: 'dialect_identity';
}

export interface TranspilationTest {
  id: string;
  dialect: string; // source dialect
  sql: string;
  read: Record<string, string>; // dialect -> input SQL
  write: Record<string, string>; // dialect -> expected output SQL
  category: 'transpilation';
}

export type TestCase = IdentityTest | PrettyTest | DialectIdentityTest | TranspilationTest;

// Execution results
export interface ExecutionResult {
  success: boolean;
  output?: string;
  error?: string;
  executionTimeMs: number;
}

// Comparison results
export interface Difference {
  type: 'keyword_case' | 'whitespace' | 'quote_style' | 'structure' | 'semantic' | 'missing' | 'error';
  description: string;
  expected?: string;
  actual?: string;
}

export interface ComparisonResult {
  match: boolean;
  exactMatch: boolean;
  normalizedMatch: boolean;
  differences: Difference[];
}

// Test result
export interface TestResult {
  id: string;
  category: TestCase['category'];
  dialect?: string;
  sourceDialect?: string;
  targetDialect?: string;
  input: string;
  expectedOutput: string;
  sqlglotResult: ExecutionResult;
  polyglotResult: ExecutionResult;
  comparison: ComparisonResult;
  status: 'pass' | 'fail' | 'skip' | 'error';
}

// Report types
export interface CategorySummary {
  total: number;
  passed: number;
  failed: number;
  skipped: number;
  passRate: number;
}

export interface DialectSummary extends CategorySummary {
  dialect: string;
}

export interface ReportMetadata {
  timestamp: string;
  sqlglotVersion: string;
  polyglotVersion: string;
  totalTests: number;
  passedTests: number;
  failedTests: number;
  skippedTests: number;
  passRate: number;
}

export interface ReportSummary {
  byCategory: {
    identity: CategorySummary;
    pretty: CategorySummary;
    dialect_identity: CategorySummary;
    transpilation: CategorySummary;
  };
  byDialect: Record<string, DialectSummary>;
  topFailurePatterns: Array<{ pattern: string; count: number }>;
}

export interface ComparisonReport {
  metadata: ReportMetadata;
  summary: ReportSummary;
  results: TestResult[];
}

// Known differences config
export interface SkipRule {
  pattern: string;
  reason: string;
}

export interface AcceptableDifference {
  category: Difference['type'];
  description: string;
  dialects?: string[];
  normalize?: boolean;
}

export interface ExpectedFailure {
  testId: string;
  reason: string;
  issue?: string;
}

export interface KnownDifferencesConfig {
  skip: SkipRule[];
  acceptableDifferences: AcceptableDifference[];
  expectedFailures: ExpectedFailure[];
}

// CLI options
export interface CompareOptions {
  filter?: string;
  dialect?: string;
  category?: string;
  verbose?: boolean;
  outputDir?: string;
  reportFormat?: 'json' | 'cli' | 'both';
  minPassRate?: number;
}
