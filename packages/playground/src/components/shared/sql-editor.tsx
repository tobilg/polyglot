import { useRef, useCallback } from "react";
import { cn } from "@/lib/utils";
import { highlightSQL } from "@/lib/highlight";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  readOnly?: boolean;
}

export function SqlEditor({
  value,
  onChange,
  placeholder = "Enter SQL...",
  className,
  readOnly = false,
}: SqlEditorProps) {
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

  const highlighted = highlightSQL(value) || "&nbsp;";

  return (
    <div className={cn("relative rounded-md border border-code-border bg-code-bg overflow-hidden", className)}>
      <pre
        ref={highlightRef}
        aria-hidden
        className="absolute inset-0 m-0 overflow-hidden pointer-events-none p-3 font-mono text-sm leading-relaxed whitespace-pre-wrap break-words text-code-text"
        dangerouslySetInnerHTML={{ __html: highlighted + "\n" }}
      />
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={syncScroll}
        placeholder={placeholder}
        readOnly={readOnly}
        spellCheck={false}
        className={cn(
          "relative w-full h-full resize-none bg-transparent p-3",
          "font-mono text-sm leading-relaxed text-transparent caret-code-caret",
          "whitespace-pre-wrap break-words",
          "outline-none focus-visible:ring-1 focus-visible:ring-ring rounded-md",
          "placeholder:text-code-placeholder",
          readOnly && "cursor-default",
        )}
      />
    </div>
  );
}
