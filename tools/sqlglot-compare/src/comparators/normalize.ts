/**
 * SQL normalization utilities for comparison.
 */

// SQL keywords that should be normalized to uppercase
const SQL_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'TRUE', 'FALSE',
  'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'CROSS', 'NATURAL',
  'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'NULLS', 'FIRST', 'LAST',
  'HAVING', 'LIMIT', 'OFFSET', 'FETCH', 'NEXT', 'ROWS', 'ONLY',
  'UNION', 'INTERSECT', 'EXCEPT', 'ALL', 'DISTINCT',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
  'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW', 'INDEX', 'SCHEMA', 'DATABASE',
  'IF', 'EXISTS', 'CASCADE', 'RESTRICT',
  'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
  'CONSTRAINT', 'ADD', 'COLUMN',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'CAST', 'CONVERT', 'COALESCE', 'NULLIF',
  'BETWEEN', 'LIKE', 'ILIKE', 'SIMILAR', 'TO', 'ESCAPE',
  'EXISTS', 'ANY', 'SOME',
  'OVER', 'PARTITION', 'WINDOW', 'ROWS', 'RANGE', 'GROUPS',
  'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW',
  'WITH', 'RECURSIVE', 'MATERIALIZED',
  'PIVOT', 'UNPIVOT', 'FOR',
  'LATERAL', 'APPLY',
  'TOP', 'PERCENT', 'TIES',
  'SAMPLE', 'TABLESAMPLE', 'BERNOULLI', 'SYSTEM',
  'QUALIFY',
]);

export interface NormalizationOptions {
  /** Normalize SQL keyword case to uppercase */
  normalizeKeywordCase: boolean;
  /** Collapse multiple whitespace to single space */
  normalizeWhitespace: boolean;
  /** Remove trailing semicolons */
  removeTrailingSemicolon: boolean;
  /** Normalize quote characters (treat " and ` as equivalent for identifiers) */
  normalizeQuotes: boolean;
  /** Remove extra parentheses */
  normalizeParentheses: boolean;
}

export const DEFAULT_NORMALIZATION_OPTIONS: NormalizationOptions = {
  normalizeKeywordCase: true,
  normalizeWhitespace: true,
  removeTrailingSemicolon: true,
  normalizeQuotes: false, // Be strict about quotes by default
  normalizeParentheses: false, // Parentheses can affect semantics
};

/**
 * Normalize SQL for comparison purposes.
 */
export function normalizeSql(
  sql: string,
  options: Partial<NormalizationOptions> = {}
): string {
  const opts = { ...DEFAULT_NORMALIZATION_OPTIONS, ...options };
  let result = sql;

  if (opts.normalizeWhitespace) {
    // Collapse multiple whitespace to single space
    result = result.replace(/\s+/g, ' ').trim();
  }

  if (opts.removeTrailingSemicolon) {
    result = result.replace(/;\s*$/, '');
  }

  if (opts.normalizeKeywordCase) {
    result = normalizeKeywords(result);
  }

  if (opts.normalizeQuotes) {
    // Normalize backticks to double quotes for identifiers
    result = normalizeQuoteCharacters(result);
  }

  return result;
}

/**
 * Normalize SQL keywords to uppercase.
 * Preserves case within string literals.
 */
function normalizeKeywords(sql: string): string {
  const result: string[] = [];
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];

    // Handle string literals - preserve everything inside
    if (char === "'" || char === '"') {
      const quote = char;
      result.push(char);
      i++;

      while (i < sql.length) {
        if (sql[i] === quote) {
          result.push(sql[i]);
          i++;
          // Handle escaped quotes
          if (i < sql.length && sql[i] === quote) {
            result.push(sql[i]);
            i++;
          } else {
            break;
          }
        } else if (sql[i] === '\\' && i + 1 < sql.length) {
          // Escape sequence
          result.push(sql[i], sql[i + 1]);
          i += 2;
        } else {
          result.push(sql[i]);
          i++;
        }
      }
      continue;
    }

    // Handle backtick-quoted identifiers
    if (char === '`') {
      result.push(char);
      i++;
      while (i < sql.length && sql[i] !== '`') {
        result.push(sql[i]);
        i++;
      }
      if (i < sql.length) {
        result.push(sql[i]);
        i++;
      }
      continue;
    }

    // Handle identifiers and keywords
    if (/[a-zA-Z_]/.test(char)) {
      let word = '';
      while (i < sql.length && /[a-zA-Z0-9_]/.test(sql[i])) {
        word += sql[i];
        i++;
      }

      // Check if it's a keyword
      if (SQL_KEYWORDS.has(word.toUpperCase())) {
        result.push(word.toUpperCase());
      } else {
        result.push(word);
      }
      continue;
    }

    // Copy other characters as-is
    result.push(char);
    i++;
  }

  return result.join('');
}

/**
 * Normalize quote characters.
 * Converts backticks to double quotes for identifiers.
 */
function normalizeQuoteCharacters(sql: string): string {
  const result: string[] = [];
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];

    // Handle string literals - preserve
    if (char === "'") {
      result.push(char);
      i++;
      while (i < sql.length) {
        if (sql[i] === "'") {
          result.push(sql[i]);
          i++;
          if (i < sql.length && sql[i] === "'") {
            result.push(sql[i]);
            i++;
          } else {
            break;
          }
        } else {
          result.push(sql[i]);
          i++;
        }
      }
      continue;
    }

    // Convert backticks to double quotes
    if (char === '`') {
      result.push('"');
      i++;
      while (i < sql.length && sql[i] !== '`') {
        result.push(sql[i]);
        i++;
      }
      if (i < sql.length) {
        result.push('"');
        i++;
      }
      continue;
    }

    result.push(char);
    i++;
  }

  return result.join('');
}

/**
 * Check if two SQL strings are equivalent after normalization.
 */
export function sqlEquals(
  a: string,
  b: string,
  options: Partial<NormalizationOptions> = {}
): boolean {
  return normalizeSql(a, options) === normalizeSql(b, options);
}
