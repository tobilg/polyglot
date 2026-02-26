import { useMemo } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";
import { EditorView } from "@codemirror/view";
import { cn } from "@/lib/utils";
import { useEditorTheme } from "@/lib/codemirror-theme";

interface SchemaEditorProps {
  value: string;
  onChange: (value: string) => void;
  parseError?: string | null;
  placeholder?: string;
  className?: string;
}

export function SchemaEditor({
  value,
  onChange,
  parseError,
  placeholder = "Enter JSON schema...",
  className,
}: SchemaEditorProps) {
  const theme = useEditorTheme();

  const extensions = useMemo(() => [json(), EditorView.lineWrapping], []);

  return (
    <div
      className={cn(
        "relative rounded-md border border-code-border bg-code-bg overflow-hidden [&_.cm-editor]:!outline-none [&_.cm-editor.cm-focused]:ring-1 [&_.cm-editor.cm-focused]:ring-ring [&_.cm-editor]:rounded-md",
        className,
      )}
    >
      {parseError && (
        <div className="absolute top-0 left-0 right-0 z-10 px-3 py-1.5 bg-red-950/90 border-b border-red-800 text-red-300 text-xs font-mono truncate">
          {parseError}
        </div>
      )}
      <CodeMirror
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        theme={theme}
        extensions={extensions}
        height="100%"
        style={{ height: "100%", paddingTop: parseError ? "2rem" : undefined }}
        basicSetup={{
          lineNumbers: false,
          foldGutter: false,
          highlightActiveLine: false,
          highlightActiveLineGutter: false,
          indentOnInput: true,
          bracketMatching: true,
          closeBrackets: true,
          autocompletion: false,
        }}
      />
    </div>
  );
}
