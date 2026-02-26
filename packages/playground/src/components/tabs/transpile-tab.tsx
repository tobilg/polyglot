import { useState, useCallback } from "react";
import { transpile, format, type Dialect } from "@polyglot-sql/sdk";
import { Button } from "@/components/ui/button";
import { DialectSelect } from "@/components/shared/dialect-select";
import { SqlEditor } from "@/components/shared/sql-editor";
import { SqlOutput } from "@/components/shared/sql-output";
import { ErrorDisplay } from "@/components/shared/error-display";
import { DEFAULT_TRANSPILE_SQL } from "@/lib/constants";
import { ArrowRight, Play } from "lucide-react";

interface TranspileTabProps {
  dialects: string[];
}

export function TranspileTab({ dialects }: TranspileTabProps) {
  const [source, setSource] = useState("mysql");
  const [target, setTarget] = useState("postgres");
  const [sql, setSql] = useState(DEFAULT_TRANSPILE_SQL);
  const [output, setOutput] = useState("");
  const [error, setError] = useState<string | null>(null);

  const handleTranspile = useCallback(() => {
    setError(null);
    try {
      const result = transpile(sql, source as Dialect, target as Dialect);
      if (result.success && result.sql) {
        const raw = result.sql.join(";\n");
        const formatted = format(raw, target as Dialect);
        setOutput(formatted.success && formatted.sql ? formatted.sql.join(";\n") : raw);
      } else {
        setError(result.error ?? "Transpilation failed");
        setOutput("");
      }
    } catch (e) {
      setError(String(e));
      setOutput("");
    }
  }, [sql, source, target]);

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <DialectSelect
          value={source}
          onValueChange={setSource}
          dialects={dialects}
          label="From"
        />
        <ArrowRight className="size-4 text-muted-foreground" />
        <DialectSelect
          value={target}
          onValueChange={setTarget}
          dialects={dialects}
          label="To"
        />
        <Button onClick={handleTranspile} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Transpile
        </Button>
      </div>

      <ErrorDisplay error={error} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            Source SQL
          </div>
          <SqlEditor value={sql} onChange={setSql} placeholder="Enter SQL to transpile..." className="flex-1 min-h-0" />
        </div>
        <SqlOutput value={output} label="Transpiled SQL" className="flex-1 min-h-0" />
      </div>
    </div>
  );
}
