import hljs from "highlight.js/lib/core";
import sql from "highlight.js/lib/languages/sql";
import typescript from "highlight.js/lib/languages/typescript";
import json from "highlight.js/lib/languages/json";

hljs.registerLanguage("sql", sql);
hljs.registerLanguage("typescript", typescript);
hljs.registerLanguage("json", json);

export function highlightSQL(code: string): string {
  return hljs.highlight(code, { language: "sql" }).value;
}

export function highlightTS(code: string): string {
  return hljs.highlight(code, { language: "typescript" }).value;
}

export function highlightJSON(code: string): string {
  return hljs.highlight(code, { language: "json" }).value;
}
