import { useCallback, useMemo, useState } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import { EditorState } from "@codemirror/state";
import { Button } from "@/components/ui/button";
import { Check, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { useEditorTheme } from "@/lib/codemirror-theme";

interface SqlOutputProps {
  value: string;
  className?: string;
  label?: string;
}

export function SqlOutput({ value, className, label }: SqlOutputProps) {
  const [copied, setCopied] = useState(false);
  const theme = useEditorTheme();

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [value]);

  const extensions = useMemo(
    () => [sql(), EditorView.lineWrapping, EditorState.readOnly.of(true), EditorView.editable.of(false)],
    [],
  );

  return (
    <div className={cn("relative group flex flex-col", className)}>
      {label && (
        <div className="text-xs text-muted-foreground mb-1.5 shrink-0">{label}</div>
      )}
      <div className="relative rounded-md border border-code-border bg-code-bg flex-1 min-h-0 flex flex-col overflow-hidden [&_.cm-editor]:!outline-none [&_.cm-editor]:rounded-md">
        {value && (
          <Button
            variant="ghost"
            size="icon-xs"
            className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity z-10 text-code-placeholder hover:text-code-text"
            onClick={handleCopy}
          >
            {copied ? (
              <Check className="size-3 text-emerald-400" />
            ) : (
              <Copy className="size-3" />
            )}
          </Button>
        )}
        {value ? (
          <CodeMirror
            value={value}
            readOnly
            editable={false}
            theme={theme}
            extensions={extensions}
            height="100%"
            style={{ height: "100%" }}
            basicSetup={{
              lineNumbers: false,
              foldGutter: false,
              highlightActiveLine: false,
              highlightActiveLineGutter: false,
              autocompletion: false,
            }}
          />
        ) : (
          <div className="p-4 text-sm text-code-placeholder italic">
            Output will appear here...
          </div>
        )}
      </div>
    </div>
  );
}
