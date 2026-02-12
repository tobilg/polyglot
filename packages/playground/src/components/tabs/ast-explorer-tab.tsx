import { useState, useCallback } from "react";
import { parse, type Dialect } from "@polyglot-sql/sdk";
import { Button } from "@/components/ui/button";
import { DialectSelect } from "@/components/shared/dialect-select";
import { SqlEditor } from "@/components/shared/sql-editor";
import { JsonTree } from "@/components/shared/json-tree";
import { ErrorDisplay } from "@/components/shared/error-display";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { DEFAULT_AST_SQL } from "@/lib/constants";
import { Play } from "lucide-react";

interface AstExplorerTabProps {
  dialects: string[];
}

export function AstExplorerTab({ dialects }: AstExplorerTabProps) {
  const [dialect, setDialect] = useLocalStorage("ast-dialect", "generic");
  const [sql, setSql] = useLocalStorage("ast-sql", DEFAULT_AST_SQL);
  const [ast, setAst] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);

  const handleParse = useCallback(() => {
    setError(null);
    try {
      const result = parse(sql, dialect as Dialect);
      if (result.success && result.ast) {
        setAst(result.ast);
      } else {
        setError(result.error ?? "Parse failed");
        setAst(null);
      }
    } catch (e) {
      setError(String(e));
      setAst(null);
    }
  }, [sql, dialect]);

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <DialectSelect
          value={dialect}
          onValueChange={setDialect}
          dialects={dialects}
          label="Dialect"
        />
        <Button onClick={handleParse} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Parse
        </Button>
      </div>

      <ErrorDisplay error={error} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            SQL Input
          </div>
          <SqlEditor value={sql} onChange={setSql} placeholder="Enter SQL to parse..." className="flex-1 min-h-0" />
        </div>
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            Abstract Syntax Tree
          </div>
          {ast ? (
            <JsonTree data={ast} defaultExpanded={3} className="flex-1 min-h-0" />
          ) : (
            <div className="rounded-md border border-code-border bg-code-bg flex-1 min-h-0 flex items-center justify-center text-sm text-zinc-500 italic">
              Parse SQL to see the AST...
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
