import { useCallback, useState } from "react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Check, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { highlightSQL } from "@/lib/highlight";

interface SqlOutputProps {
  value: string;
  className?: string;
  label?: string;
}

export function SqlOutput({ value, className, label }: SqlOutputProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [value]);

  return (
    <div className={cn("relative group flex flex-col", className)}>
      {label && (
        <div className="text-xs text-muted-foreground mb-1.5 shrink-0">{label}</div>
      )}
      <div className="relative rounded-md border border-code-border bg-code-bg flex-1 min-h-0 flex flex-col">
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
        <ScrollArea className="flex-1">
          {value ? (
            <pre
              className="p-4 text-sm font-mono text-code-text whitespace-pre-wrap"
              dangerouslySetInnerHTML={{ __html: highlightSQL(value) }}
            />
          ) : (
            <div className="p-4 text-sm text-code-placeholder italic">
              Output will appear here...
            </div>
          )}
        </ScrollArea>
      </div>
    </div>
  );
}
