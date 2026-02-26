import { useMemo } from "react";
import { useLocation, useNavigate, Navigate, Routes, Route } from "react-router-dom";
import { getDialects, getVersion } from "@polyglot-sql/sdk";
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
  const dialects = useMemo(() => getDialects(), []);
  const version = useMemo(() => getVersion(), []);
  const location = useLocation();
  const navigate = useNavigate();

  const { mainTab, subTab } = parseTabFromPath(location.pathname);

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
              <TranspileTab dialects={dialects} />
            </TabsContent>
            <TabsContent value="ast" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              <AstExplorerTab dialects={dialects} />
            </TabsContent>
            <TabsContent value="format" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              <FormatterTab />
            </TabsContent>
            <TabsContent value="validate" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              <ValidationTab dialects={dialects} mode={subTab ?? "syntax"} onModeChange={setSubTab} />
            </TabsContent>
            <TabsContent value="lineage" forceMount className="data-[state=inactive]:hidden flex-1 min-h-0 flex flex-col">
              <LineageTab dialects={dialects} />
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
