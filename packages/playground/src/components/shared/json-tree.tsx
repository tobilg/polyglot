import { useState, useCallback } from "react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { ScrollArea } from "@/components/ui/scroll-area";

interface JsonNodeProps {
  keyName?: string;
  value: unknown;
  depth: number;
  defaultExpanded: number;
}

function JsonNode({ keyName, value, depth, defaultExpanded }: JsonNodeProps) {
  const [isOpen, setIsOpen] = useState(depth < defaultExpanded);

  const toggle = useCallback(() => setIsOpen((o) => !o), []);

  if (value === null) {
    return (
      <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
        {keyName && <span className="text-violet-400">{keyName}:</span>}
        <span className="text-zinc-500">null</span>
      </div>
    );
  }

  if (typeof value === "boolean") {
    return (
      <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
        {keyName && <span className="text-violet-400">{keyName}:</span>}
        <span className="text-amber-400">{String(value)}</span>
      </div>
    );
  }

  if (typeof value === "number") {
    return (
      <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
        {keyName && <span className="text-violet-400">{keyName}:</span>}
        <span className="text-amber-400">{value}</span>
      </div>
    );
  }

  if (typeof value === "string") {
    return (
      <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
        {keyName && <span className="text-violet-400">{keyName}:</span>}
        <span className="text-emerald-400">"{value}"</span>
      </div>
    );
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return (
        <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
          {keyName && <span className="text-violet-400">{keyName}:</span>}
          <span className="text-zinc-500">[]</span>
        </div>
      );
    }

    return (
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <CollapsibleTrigger
          onClick={toggle}
          className="flex items-center gap-1 py-0.5 hover:bg-zinc-800/50 rounded w-full text-left"
          style={{ paddingLeft: depth * 16 }}
        >
          <ChevronRight
            className={cn(
              "size-3 text-zinc-500 transition-transform",
              isOpen && "rotate-90"
            )}
          />
          {keyName && <span className="text-violet-400">{keyName}:</span>}
          <span className="text-zinc-500">
            Array[{value.length}]
          </span>
        </CollapsibleTrigger>
        <CollapsibleContent>
          {value.map((item, i) => (
            <JsonNode
              key={i}
              keyName={String(i)}
              value={item}
              depth={depth + 1}
              defaultExpanded={defaultExpanded}
            />
          ))}
        </CollapsibleContent>
      </Collapsible>
    );
  }

  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);

    if (entries.length === 0) {
      return (
        <div className="flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 16 }}>
          {keyName && <span className="text-violet-400">{keyName}:</span>}
          <span className="text-zinc-500">{"{}"}</span>
        </div>
      );
    }

    return (
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <CollapsibleTrigger
          onClick={toggle}
          className="flex items-center gap-1 py-0.5 hover:bg-zinc-800/50 rounded w-full text-left"
          style={{ paddingLeft: depth * 16 }}
        >
          <ChevronRight
            className={cn(
              "size-3 text-zinc-500 transition-transform",
              isOpen && "rotate-90"
            )}
          />
          {keyName && <span className="text-violet-400">{keyName}:</span>}
          <span className="text-zinc-500">
            {"{"}...{"}"}
          </span>
        </CollapsibleTrigger>
        <CollapsibleContent>
          {entries.map(([k, v]) => (
            <JsonNode
              key={k}
              keyName={k}
              value={v}
              depth={depth + 1}
              defaultExpanded={defaultExpanded}
            />
          ))}
        </CollapsibleContent>
      </Collapsible>
    );
  }

  return null;
}

interface JsonTreeProps {
  data: unknown;
  defaultExpanded?: number;
  className?: string;
}

export function JsonTree({ data, defaultExpanded = 2, className }: JsonTreeProps) {
  return (
    <div className={cn("rounded-md border border-code-border bg-code-bg flex flex-col", className)}>
      <ScrollArea className="flex-1">
        <div className="p-3 text-xs font-mono text-code-text">
          <JsonNode value={data} depth={0} defaultExpanded={defaultExpanded} />
        </div>
      </ScrollArea>
    </div>
  );
}
