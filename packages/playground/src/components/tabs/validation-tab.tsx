import { useState, useCallback, useRef } from "react";
import { validate, type Dialect } from "@polyglot-sql/sdk";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { DialectSelect } from "@/components/shared/dialect-select";
import { SqlEditor } from "@/components/shared/sql-editor";
import { ErrorDisplay } from "@/components/shared/error-display";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { DEFAULT_VALIDATE_SQL } from "@/lib/constants";
import { Play, CheckCircle2 } from "lucide-react";

interface ValidationError {
  code: string;
  message: string;
  severity: "error" | "warning";
  line?: number;
  column?: number;
}

interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

interface ValidationTabProps {
  dialects: string[];
}

export function ValidationTab({ dialects }: ValidationTabProps) {
  const [dialect, setDialect] = useLocalStorage("validate-dialect", "generic");
  const [sql, setSql] = useLocalStorage("validate-sql", DEFAULT_VALIDATE_SQL);
  const [semantic, setSemantic] = useLocalStorage("validate-semantic", false);
  const [result, setResult] = useState<{ data: ValidationResult; key: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runCountRef = useRef(0);

  const handleValidate = useCallback(() => {
    setError(null);
    setResult(null);
    try {
      const r = validate(sql, dialect as Dialect, { semantic });
      runCountRef.current += 1;
      setResult({ data: r as ValidationResult, key: runCountRef.current });
    } catch (e) {
      setError(String(e));
      setResult(null);
    }
  }, [sql, dialect, semantic]);

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <DialectSelect
          value={dialect}
          onValueChange={setDialect}
          dialects={dialects}
          label="Dialect"
        />
        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
          <input
            type="checkbox"
            checked={semantic}
            onChange={(e) => setSemantic(e.target.checked)}
            className="rounded border-zinc-700 bg-zinc-900"
          />
          Semantic checks
        </label>
        <Button onClick={handleValidate} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Validate
        </Button>
      </div>

      <ErrorDisplay error={error} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            SQL Input
          </div>
          <SqlEditor value={sql} onChange={setSql} placeholder="Enter SQL to validate..." className="flex-1 min-h-0" />
        </div>
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            Validation Results
          </div>
          <div className="rounded-md border border-code-border bg-code-bg flex-1 min-h-0 flex flex-col">
            <ScrollArea className="flex-1">
              {result === null ? (
                <div className="p-4 text-sm text-zinc-500 italic">
                  Validate SQL to see results...
                </div>
              ) : result.data.valid && result.data.errors.length === 0 ? (
                <div className="p-4 flex items-center gap-2 text-sm text-emerald-400">
                  <CheckCircle2 className="size-4" />
                  SQL is valid!
                </div>
              ) : (
                <div className="divide-y divide-zinc-800">
                  {result.data.valid && result.data.errors.length === 0 ? null : (
                    <div className="p-3">
                      <Badge
                        variant={result.data.valid ? "secondary" : "destructive"}
                        className="text-xs"
                      >
                        {result.data.valid ? "Valid with warnings" : "Invalid"}
                      </Badge>
                    </div>
                  )}
                  {result.data.errors.map((err, i) => (
                    <div key={i} className="p-3 space-y-1">
                      <div className="flex items-center gap-2">
                        <Badge
                          variant={
                            err.severity === "error"
                              ? "destructive"
                              : "secondary"
                          }
                          className="text-xs"
                        >
                          {err.severity}
                        </Badge>
                        <span className="text-xs font-mono text-muted-foreground">
                          {err.code}
                        </span>
                        {err.line != null && (
                          <span className="text-xs text-muted-foreground">
                            Ln {err.line}
                            {err.column != null && `:${err.column}`}
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-zinc-300">{err.message}</p>
                    </div>
                  ))}
                </div>
              )}
            </ScrollArea>
          </div>
        </div>
      </div>
    </div>
  );
}
