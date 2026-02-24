import { useRef, useCallback } from "react";
import { cn } from "@/lib/utils";
import { highlightJSON } from "@/lib/highlight";

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
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const highlightRef = useRef<HTMLPreElement>(null);

  const syncScroll = useCallback(() => {
    const ta = textareaRef.current;
    const pre = highlightRef.current;
    if (ta && pre) {
      pre.scrollTop = ta.scrollTop;
      pre.scrollLeft = ta.scrollLeft;
    }
  }, []);

  const highlighted = highlightJSON(value) || "&nbsp;";

  return (
    <div className={cn("relative rounded-md border border-code-border bg-code-bg overflow-hidden", className)}>
      {parseError && (
        <div className="absolute top-0 left-0 right-0 z-10 px-3 py-1.5 bg-red-950/90 border-b border-red-800 text-red-300 text-xs font-mono truncate">
          {parseError}
        </div>
      )}
      <pre
        ref={highlightRef}
        aria-hidden
        className={cn(
          "absolute inset-0 m-0 overflow-hidden pointer-events-none p-3 font-mono text-sm leading-relaxed whitespace-pre-wrap break-words text-code-text",
          parseError && "pt-10",
        )}
        dangerouslySetInnerHTML={{ __html: highlighted + "\n" }}
      />
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={syncScroll}
        placeholder={placeholder}
        spellCheck={false}
        className={cn(
          "relative w-full h-full resize-none bg-transparent p-3",
          "font-mono text-sm leading-relaxed text-transparent caret-code-caret",
          "whitespace-pre-wrap break-words",
          "outline-none focus-visible:ring-1 focus-visible:ring-ring rounded-md",
          "placeholder:text-code-placeholder",
          parseError && "pt-10",
        )}
      />
    </div>
  );
}
