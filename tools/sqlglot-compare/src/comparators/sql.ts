import { ComparisonResult, Difference } from '../types/index.js';
import { normalizeSql, NormalizationOptions, DEFAULT_NORMALIZATION_OPTIONS } from './normalize.js';

/**
 * Compare two SQL strings and return detailed comparison result.
 */
export function compareSql(
  expected: string,
  actual: string,
  options: Partial<NormalizationOptions> = {}
): ComparisonResult {
  // Handle null/undefined
  if (expected === null || expected === undefined) {
    expected = '';
  }
  if (actual === null || actual === undefined) {
    actual = '';
  }

  // Check exact match first
  const exactMatch = expected === actual;

  if (exactMatch) {
    return {
      match: true,
      exactMatch: true,
      normalizedMatch: true,
      differences: [],
    };
  }

  // Normalize and compare
  const opts = { ...DEFAULT_NORMALIZATION_OPTIONS, ...options };
  const normalizedExpected = normalizeSql(expected, opts);
  const normalizedActual = normalizeSql(actual, opts);
  const normalizedMatch = normalizedExpected === normalizedActual;

  // Compute differences
  const differences = computeDifferences(expected, actual, normalizedExpected, normalizedActual);

  return {
    match: normalizedMatch,
    exactMatch,
    normalizedMatch,
    differences,
  };
}

/**
 * Compute the list of differences between two SQL strings.
 */
function computeDifferences(
  expected: string,
  actual: string,
  normalizedExpected: string,
  normalizedActual: string
): Difference[] {
  const differences: Difference[] = [];

  // Check for whitespace differences
  const wsNormExpected = expected.replace(/\s+/g, ' ').trim();
  const wsNormActual = actual.replace(/\s+/g, ' ').trim();

  if (wsNormExpected !== wsNormActual && expected !== actual) {
    // There are non-whitespace differences
    if (wsNormExpected === wsNormActual) {
      differences.push({
        type: 'whitespace',
        description: 'Whitespace differs',
        expected: expected.substring(0, 100),
        actual: actual.substring(0, 100),
      });
    }
  } else if (expected !== actual) {
    differences.push({
      type: 'whitespace',
      description: 'Whitespace differs',
      expected: expected.substring(0, 100),
      actual: actual.substring(0, 100),
    });
  }

  // Check for case differences
  if (expected.toLowerCase() === actual.toLowerCase() && expected !== actual) {
    differences.push({
      type: 'keyword_case',
      description: 'Keyword case differs',
      expected: findCaseDifference(expected, actual),
      actual: findCaseDifference(actual, expected),
    });
  }

  // Check for quote style differences
  const quotePattern = /[`"]/g;
  const expectedQuotes = expected.match(quotePattern) || [];
  const actualQuotes = actual.match(quotePattern) || [];

  if (expectedQuotes.join('') !== actualQuotes.join('')) {
    differences.push({
      type: 'quote_style',
      description: 'Quote style differs',
      expected: expectedQuotes.join(''),
      actual: actualQuotes.join(''),
    });
  }

  // If still different after normalization, it's a structural difference
  if (normalizedExpected !== normalizedActual) {
    // Find first difference position
    let diffPos = 0;
    while (diffPos < normalizedExpected.length &&
           diffPos < normalizedActual.length &&
           normalizedExpected[diffPos] === normalizedActual[diffPos]) {
      diffPos++;
    }

    const context = 30;
    const start = Math.max(0, diffPos - context);
    const expectedSnippet = normalizedExpected.substring(start, diffPos + context);
    const actualSnippet = normalizedActual.substring(start, diffPos + context);

    differences.push({
      type: 'structure',
      description: `Structural difference at position ${diffPos}`,
      expected: expectedSnippet,
      actual: actualSnippet,
    });
  }

  return differences;
}

/**
 * Find where case differs between two strings.
 */
function findCaseDifference(a: string, b: string): string {
  const words = a.split(/\s+/);
  const diffs: string[] = [];

  for (let i = 0; i < words.length && i < 5; i++) {
    const bWords = b.split(/\s+/);
    if (bWords[i] && words[i].toLowerCase() === bWords[i].toLowerCase() && words[i] !== bWords[i]) {
      diffs.push(words[i]);
    }
  }

  return diffs.join(', ') || a.substring(0, 50);
}

/**
 * Quick check if two SQL strings match after basic normalization.
 */
export function quickMatch(expected: string, actual: string): boolean {
  if (expected === actual) return true;

  // Quick whitespace + case normalization
  const normExpected = expected.replace(/\s+/g, ' ').trim().toLowerCase();
  const normActual = actual.replace(/\s+/g, ' ').trim().toLowerCase();

  return normExpected === normActual;
}
