import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import {
  validate,
  validateWithSchema,
  type Dialect,
  type Schema,
} from "@polyglot-sql/sdk";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { DialectSelect } from "@/components/shared/dialect-select";
import { SqlEditor } from "@/components/shared/sql-editor";
import { SchemaEditor } from "@/components/shared/schema-editor";
import { ErrorDisplay } from "@/components/shared/error-display";
import { ScrollArea } from "@/components/ui/scroll-area";
import { DEFAULT_VALIDATE_SQL, DEFAULT_VALIDATE_SCHEMA } from "@/lib/constants";
import { toCmDiagnostics } from "@/lib/diagnostics";
import { Play, CheckCircle2 } from "lucide-react";

interface ValidationError {
  code: string;
  message: string;
  severity: "error" | "warning";
  line?: number;
  column?: number;
  start?: number;
  end?: number;
}

interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

type ValidateMode = "syntax" | "semantic" | "schema";

interface ErrorGroup {
  label: string;
  errors: ValidationError[];
}

function groupErrors(errors: ValidationError[]): ErrorGroup[] {
  const groups: Record<string, { label: string; errors: ValidationError[] }> = {};

  const classify = (code: string): { key: string; label: string } => {
    if (/^E0\d\d$/.test(code)) return { key: "syntax", label: "Syntax Errors" };
    if (/^W00\d$/.test(code)) return { key: "semantic", label: "Semantic Warnings" };
    if (/^E20[01]$/.test(code)) return { key: "schema", label: "Schema Errors" };
    if (/^[EW]21\d$/.test(code)) return { key: "type", label: "Type Errors" };
    if (/^[EW]22\d$/.test(code)) return { key: "reference", label: "Reference Issues" };
    return { key: "other", label: "Other Issues" };
  };

  for (const err of errors) {
    const { key, label } = classify(err.code);
    if (!groups[key]) groups[key] = { label, errors: [] };
    groups[key].errors.push(err);
  }

  return Object.values(groups);
}

interface ValidationTabProps {
  dialects: string[];
  mode: ValidateMode;
  onModeChange: (mode: ValidateMode) => void;
}

export function ValidationTab({ dialects, mode, onModeChange }: ValidationTabProps) {
  const [dialect, setDialect] = useState("generic");
  const [sql, setSql] = useState(DEFAULT_VALIDATE_SQL);
  const [schemaJson, setSchemaJson] = useState(DEFAULT_VALIDATE_SCHEMA);
  const [checkTypes, setCheckTypes] = useState(false);
  const [checkRefs, setCheckRefs] = useState(false);
  const [strict, setStrict] = useState(true);

  const [result, setResult] = useState<{ data: ValidationResult; key: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [schemaParseError, setSchemaParseError] = useState<string | null>(null);
  const runCountRef = useRef(0);

  // Clear results when mode or sql changes
  useEffect(() => {
    setResult(null);
    setError(null);
    setSchemaParseError(null);
  }, [mode, sql]);

  const handleValidate = useCallback(() => {
    setError(null);
    setResult(null);
    setSchemaParseError(null);

    try {
      let r: ValidationResult;

      if (mode === "syntax") {
        r = validate(sql, dialect as Dialect, { strictSyntax: true }) as ValidationResult;
      } else if (mode === "semantic") {
        r = validate(sql, dialect as Dialect, {
          semantic: true,
          strictSyntax: true,
        }) as ValidationResult;
      } else {
        // schema mode — parse JSON first
        let schema: Schema;
        try {
          schema = JSON.parse(schemaJson) as Schema;
        } catch (e) {
          setSchemaParseError(`Invalid JSON: ${(e as Error).message}`);
          return;
        }
        r = validateWithSchema(sql, schema, dialect as Dialect, {
          checkTypes,
          checkReferences: checkRefs,
          strict,
          strictSyntax: true,
        }) as ValidationResult;
      }

      runCountRef.current += 1;
      setResult({ data: r, key: runCountRef.current });
    } catch (e) {
      setError(String(e));
      setResult(null);
    }
  }, [sql, dialect, mode, schemaJson, checkTypes, checkRefs, strict]);

  const errorCount = result?.data.errors.filter((e) => e.severity === "error").length ?? 0;
  const warningCount = result?.data.errors.filter((e) => e.severity === "warning").length ?? 0;
  const grouped = result ? groupErrors(result.data.errors) : [];

  const cmDiagnostics = useMemo(
    () => (result ? toCmDiagnostics(sql, result.data.errors) : undefined),
    [result, sql],
  );

  return (
    <div className="flex flex-col flex-1 min-h-0 gap-4">
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-3 shrink-0">
        <TooltipProvider>
          <Tabs value={mode} onValueChange={(v) => onModeChange(v as ValidateMode)}>
            <TabsList className="h-8">
              <Tooltip>
                <TooltipTrigger asChild>
                  <TabsTrigger value="syntax" className="text-xs px-2.5">Syntax</TabsTrigger>
                </TooltipTrigger>
                <TooltipContent side="bottom" className="max-w-64">
                  Checks for parse errors only. Does not analyze query logic or resolve table/column names.
                </TooltipContent>
              </Tooltip>
              <Tooltip>
                <TooltipTrigger asChild>
                  <TabsTrigger value="semantic" className="text-xs px-2.5">Semantic</TabsTrigger>
                </TooltipTrigger>
                <TooltipContent side="bottom" className="max-w-64">
                  Syntax checks plus quality warnings: SELECT *, aggregate without GROUP BY, DISTINCT with ORDER BY, LIMIT without ORDER BY. Does not validate against a schema.
                </TooltipContent>
              </Tooltip>
              <Tooltip>
                <TooltipTrigger asChild>
                  <TabsTrigger value="schema" className="text-xs px-2.5">Schema</TabsTrigger>
                </TooltipTrigger>
                <TooltipContent side="bottom" className="max-w-64">
                  Full schema-aware validation: resolves tables and columns, checks types, verifies foreign key integrity and join quality.
                </TooltipContent>
              </Tooltip>
            </TabsList>
          </Tabs>
        </TooltipProvider>
        <DialectSelect
          value={dialect}
          onValueChange={setDialect}
          dialects={dialects}
          label="Dialect"
        />
        {mode === "schema" && (
          <>
            <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
              <input
                type="checkbox"
                checked={checkTypes}
                onChange={(e) => setCheckTypes(e.target.checked)}
                className="rounded border-zinc-700 bg-zinc-900"
              />
              Type checks
            </label>
            <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
              <input
                type="checkbox"
                checked={checkRefs}
                onChange={(e) => setCheckRefs(e.target.checked)}
                className="rounded border-zinc-700 bg-zinc-900"
              />
              Reference checks
            </label>
            <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
              <input
                type="checkbox"
                checked={strict}
                onChange={(e) => setStrict(e.target.checked)}
                className="rounded border-zinc-700 bg-zinc-900"
              />
              Strict
            </label>
          </>
        )}
        <Button onClick={handleValidate} size="sm" className="ml-auto">
          <Play className="size-3.5" />
          Validate
        </Button>
      </div>

      <ErrorDisplay error={error} />

      {/* Content grid — 2 cols for syntax/semantic, 3 cols for schema */}
      <div className={`grid grid-cols-1 gap-4 flex-1 min-h-0 ${mode === "schema" ? "lg:grid-cols-3" : "lg:grid-cols-2"}`}>
        {/* SQL Editor */}
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
            SQL Input
          </div>
          <SqlEditor
            value={sql}
            onChange={setSql}
            placeholder="Enter SQL to validate..."
            className="flex-1 min-h-0"
            diagnostics={cmDiagnostics}
          />
        </div>

        {/* Schema Editor — only in schema mode */}
        {mode === "schema" && (
          <div className="flex flex-col min-h-0">
            <div className="text-xs text-muted-foreground mb-1.5 shrink-0">
              Schema (JSON)
            </div>
            <SchemaEditor
              value={schemaJson}
              onChange={setSchemaJson}
              parseError={schemaParseError}
              placeholder="Enter database schema as JSON..."
              className="flex-1 min-h-0"
            />
          </div>
        )}

        {/* Results */}
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
                  {/* Summary */}
                  <div className="p-3">
                    {result.data.valid && warningCount > 0 ? (
                      <Badge variant="secondary" className="text-xs">
                        Valid with {warningCount} warning{warningCount !== 1 ? "s" : ""}
                      </Badge>
                    ) : !result.data.valid ? (
                      <Badge variant="destructive" className="text-xs">
                        Invalid &mdash; {errorCount} error{errorCount !== 1 ? "s" : ""}{warningCount > 0 ? `, ${warningCount} warning${warningCount !== 1 ? "s" : ""}` : ""}
                      </Badge>
                    ) : null}
                  </div>

                  {/* Grouped errors */}
                  {grouped.map((group) => (
                    <div key={group.label}>
                      <div className="px-3 py-1.5 text-xs font-medium text-muted-foreground bg-muted/30 border-b border-zinc-800">
                        {group.label} ({group.errors.length})
                      </div>
                      <div className="divide-y divide-zinc-800/50">
                        {group.errors.map((err, i) => (
                          <div key={i} className="p-3 space-y-1">
                            <div className="flex items-center gap-2">
                              <Badge
                                variant={err.severity === "error" ? "destructive" : "secondary"}
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
