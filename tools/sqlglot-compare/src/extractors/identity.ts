import { readFileSync } from 'fs';
import { IdentityTest } from '../types/index.js';

/**
 * Extract identity test cases from sqlglot's identity.sql fixture file.
 * Each non-empty, non-comment line is a SQL statement that should roundtrip identically.
 */
export function extractIdentityTests(filePath: string): IdentityTest[] {
  const content = readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  const tests: IdentityTest[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip empty lines and comments
    if (!line || line.startsWith('#') || line.startsWith('--')) {
      continue;
    }

    tests.push({
      id: `identity_${i + 1}`,
      line: i + 1,
      sql: line,
      category: 'identity',
    });
  }

  return tests;
}

/**
 * Default path to identity.sql fixture
 */
export const IDENTITY_FIXTURE_PATH = 'external-projects/sqlglot/tests/fixtures/identity.sql';
