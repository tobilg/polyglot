import { useMemo, useRef, useEffect } from "react";
import CodeMirror, { type ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { lintGutter, setDiagnostics, type Diagnostic } from "@codemirror/lint";
import { EditorView } from "@codemirror/view";
import { cn } from "@/lib/utils";
import { useEditorTheme } from "@/lib/codemirror-theme";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  readOnly?: boolean;
  diagnostics?: Diagnostic[];
}

export function SqlEditor({
  value,
  onChange,
  placeholder = "Enter SQL...",
  className,
  readOnly = false,
  diagnostics,
}: SqlEditorProps) {
  const theme = useEditorTheme();
  const cmRef = useRef<ReactCodeMirrorRef>(null);

  const extensions = useMemo(() => [sql(), EditorView.lineWrapping, lintGutter()], []);

  // Push diagnostics into the editor view whenever they change
  useEffect(() => {
    const view = cmRef.current?.view;
    if (!view) return;
    view.dispatch(setDiagnostics(view.state, diagnostics ?? []));
  }, [diagnostics]);

  return (
    <div
      className={cn(
        "rounded-md border border-code-border bg-code-bg overflow-hidden [&_.cm-editor]:!outline-none [&_.cm-editor.cm-focused]:ring-1 [&_.cm-editor.cm-focused]:ring-ring [&_.cm-editor]:rounded-md",
        className,
      )}
    >
      <CodeMirror
        ref={cmRef}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        readOnly={readOnly}
        theme={theme}
        extensions={extensions}
        height="100%"
        style={{ height: "100%" }}
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
