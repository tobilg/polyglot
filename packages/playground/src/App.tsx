import { useEffect, useState } from "react";
import { runSql } from "@/lib/sql-client";
import { ErrorDisplay } from "@/components/shared/error-display";
import { useLocation, useNavigate, Navigate, Routes, Route } from "react-router-dom";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ThemeProvider } from "@/components/theme-provider";
import { Header } from "@/components/layout/header";
import { TranspileTab } from "@/components/tabs/transpile-tab";
import { AstExplorerTab } from "@/components/tabs/ast-explorer-tab";
import { FormatterTab } from "@/components/tabs/formatter-tab";
import { ValidationTab } from "@/components/tabs/validation-tab";
import { LineageTab } from "@/components/tabs/lineage-tab";
import {
  ArrowRightLeft,
  TreePine,
  AlignLeft,
  ShieldCheck,
  GitBranch,
} from "lucide-react";

type ValidateMode = "syntax" | "semantic" | "schema";

const VALID_TABS = ["transpile", "ast", "format", "validate", "lineage"] as const;
const VALID_VALIDATE_MODES: ValidateMode[] = ["syntax", "semantic", "schema"];

function parseTabFromPath(pathname: string): { mainTab: string; subTab: ValidateMode | null } {
  const segments = pathname.split("/").filter(Boolean);
  const mainTab = segments[0] || "transpile";
  if (mainTab === "validate") {
    const mode = segments[1] as ValidateMode | undefined;
    return {
      mainTab: "validate",
      subTab: mode && VALID_VALIDATE_MODES.includes(mode) ? mode : "syntax",
    };
  }
  if ((VALID_TABS as readonly string[]).includes(mainTab)) {
    return { mainTab, subTab: null };
  }
  return { mainTab: "transpile", subTab: null };
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function AppLayout() {
  const [{ dialects, version }, setMetadata] = useState({ dialects: [] as string[], version: "…" });
  const [startupError, setStartupError] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    runSql("metadata").then((metadata) => {
      if (active) setMetadata(metadata);
    }).catch((error) => {
      if (active) setStartupError(String(error));
    });
    return () => { active = false; };
  }, []);
  const location = useLocation();
  const navigate = useNavigate();

  const { mainTab, subTab } = parseTabFromPath(location.pathname);
  const [visited, setVisited] = useState(() => new Set([mainTab]));
  // Record visits during render so the first frame mounts the selected tab.
  if (!visited.has(mainTab)) setVisited(new Set([...visited, mainTab]));

  const setActiveTab = (tab: string) => {
    if (tab === "validate") navigate("/validate/syntax");
    else navigate(`/${tab}`);
  };

  const setSubTab = (mode: ValidateMode) => navigate(`/validate/${mode}`);

  return (
    <ThemeProvider defaultTheme="system" storageKey="polyglot-ui-theme">
    <TooltipProvider>
      <div className="h-screen flex flex-col bg-background text-foreground overflow-hidden">
        <Header version={version} />
        <main className="flex-1 min-h-0 flex flex-col max-w-7xl w-full mx-auto px-4 py-4">
          <ErrorDisplay error={startupError} />
          <Tabs value={mainTab} onValueChange={setActiveTab} className="flex-1 min-h-0 flex flex-col">
            <TabsList className="mb-4 shrink-0">
              <TabsTrigger value="transpile" className="gap-1.5">
                <ArrowRightLeft className="size-3.5" />
                Transpile
              </TabsTrigger>
              <TabsTrigger value="ast" className="gap-1.5">
                <TreePine className="size-3.5" />
                AST Explorer
              </TabsTrigger>
              <TabsTrigger value="format" className="gap-1.5">
                <AlignLeft className="size-3.5" />
                Formatter
              </TabsTrigger>
              <TabsTrigger value="validate" className="gap-1.5">
                <ShieldCheck className="size-3.5" />
                Validation
                {mainTab === "validate" && subTab && (
                  <span className="text-muted-foreground font-normal">· {capitalize(subTab)}</span>
                )}
              </TabsTrigger>
              <TabsTrigger value="lineage" className="gap-1.5">
                <GitBranch className="size-3.5" />
                Lineage
              </TabsTrigger>
            </TabsList>

            <TabsContent value="transpile" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              {visited.has("transpile") && <TranspileTab dialects={dialects} />}
            </TabsContent>
            <TabsContent value="ast" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              {visited.has("ast") && <AstExplorerTab dialects={dialects} />}
            </TabsContent>
            <TabsContent value="format" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              {visited.has("format") && <FormatterTab />}
            </TabsContent>
            <TabsContent value="validate" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              {visited.has("validate") && <ValidationTab dialects={dialects} mode={subTab ?? "syntax"} onModeChange={setSubTab} />}
            </TabsContent>
            <TabsContent value="lineage" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              {visited.has("lineage") && <LineageTab dialects={dialects} />}
            </TabsContent>
          </Tabs>
        </main>
      </div>
    </TooltipProvider>
    </ThemeProvider>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/transpile" replace />} />
      <Route path="/transpile" element={<AppLayout />} />
      <Route path="/ast" element={<AppLayout />} />
      <Route path="/format" element={<AppLayout />} />
      <Route path="/validate" element={<Navigate to="/validate/syntax" replace />} />
      <Route path="/validate/:mode" element={<AppLayout />} />
      <Route path="/lineage" element={<AppLayout />} />
      <Route path="*" element={<Navigate to="/transpile" replace />} />
    </Routes>
  );
}
