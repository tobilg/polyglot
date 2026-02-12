import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { ModeToggle } from "@/components/mode-toggle";
import { ExternalLink } from "lucide-react";

interface HeaderProps {
  version: string;
}

export function Header({ version }: HeaderProps) {
  return (
    <header className="border-b border-border bg-background/80 backdrop-blur-sm sticky top-0 z-50 shrink-0">
      <div className="max-w-7xl mx-auto px-4 h-14 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="size-7 rounded-md bg-muted flex items-center justify-center text-sm font-bold">
              P
            </div>
            <h1 className="text-lg font-semibold tracking-tight">
              Polyglot SQL
            </h1>
          </div>
          <Separator orientation="vertical" className="h-5" />
          <span className="text-sm text-muted-foreground">Playground</span>
          <Badge variant="secondary" className="text-xs font-mono">
            v{version}
          </Badge>
        </div>
        <div className="flex items-center gap-1">
          <ModeToggle />
          <Button variant="ghost" size="sm" asChild>
            <a
              href="https://github.com/tobilg/polyglot"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5"
            >
              GitHub
              <ExternalLink className="size-3" />
            </a>
          </Button>
        </div>
      </div>
    </header>
  );
}
