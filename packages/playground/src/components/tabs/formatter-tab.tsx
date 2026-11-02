import { useSqlTask } from "@/hooks/use-sql-task";
import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { SqlEditor } from "@/components/shared/sql-editor";
import { SqlOutput } from "@/components/shared/sql-output";
import { ErrorDisplay } from "@/components/shared/error-display";
import { DEFAULT_FORMAT_SQL } from "@/lib/constants";
import { Play } from "lucide-react";

export function FormatterTab() {
  const [sql, setSql] = useState(DEFAULT_FORMAT_SQL);
  const [output, setOutput] = useState("");
  const [error, setError] = useState<string | null>(null);

  const { run, pending } = useSqlTask([sql]);

  const handleFormat = useCallback(async () => {
    setError(null);
    try {
      const result = await run("format", sql);
      if (!result) return;
      if (result.success && result.sql) {
        setOutput(result.sql.join(";\n"));
      } else {
        setError(result.error ?? "Formatting failed");
        setOutput("");
      }
    } catch (e) {
      setError(String(e));
      setOutput("");
    }
  }, [sql, run]);

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <Button onClick={handleFormat} disabled={pending} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Format
        </Button>
      </div>

      <ErrorDisplay error={error} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            Messy SQL
          </div>
          <SqlEditor value={sql} onChange={setSql} placeholder="Enter SQL to format..." className="flex-1 min-h-0" />
        </div>
        <SqlOutput value={output} label="Formatted SQL" className="flex-1 min-h-0" />
      </div>
    </div>
  );
}
