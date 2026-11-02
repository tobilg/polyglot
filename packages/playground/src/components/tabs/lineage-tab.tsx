import { useSqlTask } from "@/hooks/use-sql-task";
import { useState, useCallback, useRef } from "react";
import {
  type Dialect,
  type LineageNode,
} from "@polyglot-sql/sdk";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { DialectSelect } from "@/components/shared/dialect-select";
import { SqlEditor } from "@/components/shared/sql-editor";
import { ErrorDisplay } from "@/components/shared/error-display";
import { LineageTree } from "@/components/shared/lineage-tree";
import { JsonTree } from "@/components/shared/json-tree";
import { DEFAULT_LINEAGE_SQL, DEFAULT_LINEAGE_COLUMN } from "@/lib/constants";
import { Play, Braces, TreeDeciduous } from "lucide-react";

interface LineageTabProps {
  dialects: string[];
}

export function LineageTab({ dialects }: LineageTabProps) {
  const [dialect, setDialect] = useState("generic");
  const [sql, setSql] = useState(DEFAULT_LINEAGE_SQL);
  const [column, setColumn] = useState(DEFAULT_LINEAGE_COLUMN);
  const [trimSelects, setTrimSelects] = useState(false);
  const [showJson, setShowJson] = useState(false);

  const [lineageResult, setLineageResult] = useState<{ data: LineageNode; key: number } | null>(null);
  const [sourceTables, setSourceTables] = useState<string[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runCountRef = useRef(0);

  const { run, pending } = useSqlTask([sql, dialect, column, trimSelects]);

  const handleTrace = useCallback(async () => {
    setError(null);
    setLineageResult(null);
    setSourceTables(null);

    const col = column.trim();
    if (!col) {
      setError("Please enter a column name to trace.");
      return;
    }

    try {
      const lr = await run("lineageWithTables", col, sql, dialect as Dialect, trimSelects);
      if (!lr) return;
      if (!lr.success || !lr.lineage) {
        setError(lr.error ?? "Lineage analysis failed.");
        return;
      }
      runCountRef.current += 1;
      setLineageResult({ data: lr.lineage, key: runCountRef.current });
      setSourceTables(lr.tables);
    } catch (e) {
      setError(String(e));
      return;
    }

  }, [sql, dialect, column, trimSelects, run]);

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <DialectSelect
          value={dialect}
          onValueChange={setDialect}
          dialects={dialects}
          label="Dialect"
        />
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground whitespace-nowrap">Column</span>
          <input
            type="text"
            value={column}
            onChange={(e) => setColumn(e.target.value)}
            placeholder="column name"
            className="h-8 rounded-md border border-input bg-background px-3 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 w-[180px]"
          />
        </div>
        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
          <input
            type="checkbox"
            checked={trimSelects}
            onChange={(e) => setTrimSelects(e.target.checked)}
            className="rounded border-zinc-700 bg-zinc-900"
          />
          Trim selects
        </label>
        <Button onClick={handleTrace} disabled={pending} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Trace Lineage
        </Button>
      </div>

      <ErrorDisplay error={error} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            SQL Input
          </div>
          <SqlEditor
            value={sql}
            onChange={setSql}
            placeholder="Enter SQL to trace column lineage..."
            className="flex-1 min-h-0"
          />
        </div>
        <div className="flex flex-col min-h-0">
          <div className="flex items-center justify-between mb-1.5 shrink-0">
            <span className="text-xs text-muted-foreground">
              Lineage Result
            </span>
            {lineageResult && (
              <Button
                variant="ghost"
                size="sm"
                className="h-5 px-1.5 text-xs text-muted-foreground"
                onClick={() => setShowJson((v) => !v)}
              >
                {showJson ? (
                  <><TreeDeciduous className="size-3 mr-1" />Tree</>
                ) : (
                  <><Braces className="size-3 mr-1" />JSON</>
                )}
              </Button>
            )}
          </div>
          <div className="flex flex-col flex-1 min-h-0 gap-2">
            {lineageResult === null ? (
              <div className="rounded-md border border-code-border bg-code-bg flex-1 min-h-0 flex items-start">
                <div className="p-4 text-sm text-zinc-500 italic">
                  Trace a column to see its lineage...
                </div>
              </div>
            ) : showJson ? (
              <JsonTree
                data={lineageResult.data}
                defaultExpanded={3}
                className="flex-1 min-h-0"
              />
            ) : (
              <LineageTree
                data={lineageResult.data}
                defaultExpanded={4}
                className="flex-1 min-h-0"
              />
            )}
            {sourceTables && sourceTables.length > 0 && (
              <div className="flex items-center gap-2 shrink-0 flex-wrap">
                <span className="text-xs text-muted-foreground">Source tables:</span>
                {sourceTables.map((t) => (
                  <Badge key={t} variant="secondary" className="text-xs">
                    {t}
                  </Badge>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
