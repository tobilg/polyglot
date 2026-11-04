import { getVersion, init } from "@polyglot-sql/sdk/manual";
import { version } from "../../../sdk/package.json";

export async function initializeSql(): Promise<void> {
  const urls = import.meta.env.DEV
    ? [__POLYGLOT_LOCAL_WASM_URL__]
    : [
        `https://cdn.jsdelivr.net/npm/@polyglot-sql/sdk@${version}/dist/polyglot_sql.wasm`,
        `https://unpkg.com/@polyglot-sql/sdk@${version}/dist/polyglot_sql.wasm`,
      ];

  let lastError: unknown;
  for (const wasmUrl of urls) {
    try {
      await init({ wasmUrl });
    } catch (error) {
      lastError = error;
      continue;
    }

    // A loaded module cannot be replaced through init(). Stop rather than
    // letting JavaScript bindings operate against a different SDK release.
    const actualVersion = getVersion();
    if (actualVersion !== version) {
      throw new Error(`SQL engine version mismatch: expected ${version}, received ${actualVersion}.`);
    }
    return;
  }

  throw Object.assign(
    new Error("Could not load the SQL engine. Check your connection and reload the page."),
    { cause: lastError },
  );
}
