/**
 * Mirrors GrowthBook's shared/sql.ts - top-level import from @polyglot-sql/sdk.
 * When this module is loaded during SSR, the SDK loads on the server → WASM URL error.
 */
import {
  format as polyglotFormat,
  init as polyglotInit,
  Dialect,
} from "@polyglot-sql/sdk";
import * as polyglot from "@polyglot-sql/sdk";

let polyglotInitPromise: Promise<void> | null = null;

export async function initPolyglotFormat(): Promise<void> {
  if (typeof polyglotInit !== "function") return;
  if (!polyglotInitPromise) {
    polyglotInitPromise = polyglotInit().catch(() => {});
  }
  await polyglotInitPromise;
}

export function formatWithPolyglot(
  sql: string,
  dialect: string,
): string | null {
  if (typeof polyglotFormat === "undefined") {
    throw new Error(
      "polyglotFormat (format) is undefined. polyglot is not a module but: " +
        JSON.stringify(polyglot),
    );
  }
  const dialectMap: Record<string, Dialect> = {
    mysql: Dialect.MySQL,
    postgresql: Dialect.PostgreSQL,
    bigquery: Dialect.BigQuery,
    snowflake: Dialect.Snowflake,
  };
  const pgDialect = dialectMap[dialect] ?? Dialect.PostgreSQL;
  const result = polyglotFormat(sql, pgDialect);
  if (result?.success && result?.sql?.length) {
    return result.sql[0];
  }
  return null;
}

export { polyglot, Dialect };
