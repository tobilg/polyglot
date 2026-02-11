import { readFileSync } from 'fs';
import { PrettyTest } from '../types/index.js';

/**
 * Extract pretty-print test cases from sqlglot's pretty.sql fixture file.
 * Format: pairs of input/output separated by blank lines.
 *
 * Example:
 *   SELECT * FROM t;
 *   SELECT
 *     *
 *   FROM t;
 *
 *   [blank line separates pairs]
 */
export function extractPrettyTests(filePath: string): PrettyTest[] {
  const content = readFileSync(filePath, 'utf-8');
  const tests: PrettyTest[] = [];

  // Split by double newlines to get pairs
  const blocks = content.split(/\n\n+/);
  let testIndex = 0;
  let lineNumber = 1;

  for (const block of blocks) {
    const trimmed = block.trim();
    if (!trimmed) {
      continue;
    }

    // Split block into lines
    const lines = trimmed.split('\n');

    // Handle metadata lines starting with #
    const metaLines: string[] = [];
    const sqlLines: string[] = [];

    for (const line of lines) {
      if (line.startsWith('#')) {
        metaLines.push(line);
      } else {
        sqlLines.push(line);
      }
    }

    // Find the split between input and output
    // The input is typically a single line (or few lines) ending with semicolon
    // The output is the pretty-printed version
    if (sqlLines.length >= 2) {
      // Try to find where input ends and output begins
      // Usually the first line/statement is input, rest is output
      let inputEnd = 0;
      let parenDepth = 0;
      let foundSemicolon = false;

      for (let i = 0; i < sqlLines.length; i++) {
        const line = sqlLines[i];
        for (const char of line) {
          if (char === '(') parenDepth++;
          if (char === ')') parenDepth--;
          if (char === ';' && parenDepth === 0) {
            foundSemicolon = true;
          }
        }
        if (foundSemicolon && parenDepth === 0) {
          inputEnd = i;
          break;
        }
      }

      // If we found a semicolon, split there; otherwise assume first line is input
      if (foundSemicolon && inputEnd < sqlLines.length - 1) {
        const input = sqlLines.slice(0, inputEnd + 1).join('\n');
        const expected = sqlLines.slice(inputEnd + 1).join('\n');

        if (input && expected) {
          tests.push({
            id: `pretty_${++testIndex}`,
            line: lineNumber,
            input: input.trim(),
            expected: expected.trim(),
            category: 'pretty',
          });
        }
      } else {
        // Fallback: first line is input, rest is output
        const input = sqlLines[0];
        const expected = sqlLines.slice(1).join('\n');

        if (input && expected) {
          tests.push({
            id: `pretty_${++testIndex}`,
            line: lineNumber,
            input: input.trim(),
            expected: expected.trim(),
            category: 'pretty',
          });
        }
      }
    }

    // Count lines for tracking
    lineNumber += block.split('\n').length + 1;
  }

  return tests;
}

/**
 * Default path to pretty.sql fixture
 */
export const PRETTY_FIXTURE_PATH = 'external-projects/sqlglot/tests/fixtures/pretty.sql';
