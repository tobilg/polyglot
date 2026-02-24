import { useState, useCallback } from "react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { LineageNode } from "@polyglot-sql/sdk";

interface LineageNodeProps {
  node: LineageNode;
  depth: number;
  defaultExpanded: number;
}

function LineageNodeRow({ node, depth, defaultExpanded }: LineageNodeProps) {
  const [isOpen, setIsOpen] = useState(depth < defaultExpanded);
  const toggle = useCallback(() => setIsOpen((o) => !o), []);

  const hasChildren = node.downstream.length > 0;
  const isLeaf = !hasChildren;

  const label = (
    <div className="flex items-center gap-1.5">
      {depth === 0 ? (
        <span className="text-violet-400 font-semibold">{node.name}</span>
      ) : isLeaf ? (
        <>
          <span className="size-1.5 rounded-full bg-emerald-400 shrink-0" />
          <span className="text-emerald-400">{node.name}</span>
        </>
      ) : (
        <span className="text-amber-400">{node.name}</span>
      )}
      {node.reference_node_name && (
        <span className="text-zinc-500 text-[10px]">
          via {node.reference_node_name}
        </span>
      )}
      {isLeaf && node.source_name && (
        <span className="text-[10px] text-zinc-500 bg-zinc-800 rounded px-1 py-0.5">
          {node.source_name}
        </span>
      )}
      {hasChildren && (
        <span className="text-zinc-600 text-[10px]">({node.downstream.length})</span>
      )}
    </div>
  );

  if (!hasChildren) {
    return (
      <div
        className="flex items-center gap-1 py-0.5"
        style={{ paddingLeft: depth * 20 }}
      >
        {/* spacer matching chevron width */}
        <span className="size-3 shrink-0" />
        {label}
      </div>
    );
  }

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen}>
      <CollapsibleTrigger
        onClick={toggle}
        className="flex items-center gap-1 py-0.5 hover:bg-zinc-800/50 rounded w-full text-left"
        style={{ paddingLeft: depth * 20 }}
      >
        <ChevronRight
          className={cn(
            "size-3 text-zinc-500 transition-transform shrink-0",
            isOpen && "rotate-90"
          )}
        />
        {label}
      </CollapsibleTrigger>
      <CollapsibleContent>
        {node.downstream.map((child, i) => (
          <LineageNodeRow
            key={i}
            node={child}
            depth={depth + 1}
            defaultExpanded={defaultExpanded}
          />
        ))}
      </CollapsibleContent>
    </Collapsible>
  );
}

interface LineageTreeProps {
  data: LineageNode;
  defaultExpanded?: number;
  className?: string;
}

export function LineageTree({ data, defaultExpanded = 4, className }: LineageTreeProps) {
  return (
    <div className={cn("rounded-md border border-code-border bg-code-bg flex flex-col", className)}>
      <ScrollArea className="flex-1">
        <div className="p-3 text-xs font-mono text-code-text">
          <LineageNodeRow node={data} depth={0} defaultExpanded={defaultExpanded} />
        </div>
      </ScrollArea>
    </div>
  );
}
