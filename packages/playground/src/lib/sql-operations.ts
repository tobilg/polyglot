import {
  format, getDialects, getVersion, lineage, parse, transpile,
  validate, validateWithSchema,
  type Dialect, type LineageNode,
} from "@polyglot-sql/sdk";

// Matches the core's collect_source_tables: use Table sources, deduplicate,
// and sort. Trimming intermediate SELECTs does not change Table sources.
export function sourceTablesFromLineage(root: LineageNode): string[] {
  const tables = new Set<string>();
  const stack = [root];
  while (stack.length) {
    const node = stack.pop()!;
    if ("table" in node.source) tables.add(node.source.table.name.name);
    for (const child of node.downstream) stack.push(child);
  }
  // Rust sorts UTF-8 strings by Unicode scalar value; JavaScript's default
  // UTF-16 ordering differs for names containing supplementary characters.
  return [...tables].sort((left, right) => {
    const rightCharacters = right[Symbol.iterator]();
    for (const character of left) {
      const other = rightCharacters.next();
      if (other.done) return 1;
      const difference = character.codePointAt(0)! - other.value.codePointAt(0)!;
      if (difference !== 0) return difference;
    }
    return rightCharacters.next().done ? 0 : -1;
  });
}

export const operations = {
  metadata: () => ({ dialects: getDialects(), version: getVersion() }),
  parse,
  format,
  validate,
  validateWithSchema,
  transpileFormatted(sql: string, source: Dialect, target: Dialect) {
    const result = transpile(sql, source, target);
    if (result.success && result.sql) {
      const raw = result.sql.join(";\n");
      const formatted = format(raw, target);
      return { ...result, output: formatted.success && formatted.sql ? formatted.sql.join(";\n") : raw };
    }
    return { ...result, output: "" };
  },
  lineageWithTables(...args: Parameters<typeof lineage>) {
    const result = lineage(...args);
    return { ...result, tables: result.lineage ? sourceTablesFromLineage(result.lineage) : [] };
  },
};

export type SqlOperations = typeof operations;
