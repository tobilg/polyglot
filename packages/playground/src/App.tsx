import { useMemo } from "react";
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
import { useLocalStorage } from "@/hooks/use-local-storage";
import {
  ArrowRightLeft,
  TreePine,
  AlignLeft,
  ShieldCheck,
  GitBranch,
} from "lucide-react";

export default function App() {
  const dialects = useMemo(() => getDialects(), []);
  const version = useMemo(() => getVersion(), []);
  const [activeTab, setActiveTab] = useLocalStorage("active-tab", "transpile");

  return (
    <ThemeProvider defaultTheme="system" storageKey="polyglot-ui-theme">
    <TooltipProvider>
      <div className="h-screen flex flex-col bg-background text-foreground overflow-hidden">
        <Header version={version} />
        <main className="flex-1 min-h-0 flex flex-col max-w-7xl w-full mx-auto px-4 py-4">
          <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 min-h-0 flex flex-col">
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
              <ValidationTab dialects={dialects} />
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
