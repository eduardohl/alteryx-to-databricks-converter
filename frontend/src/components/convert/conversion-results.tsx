import type { ConversionResult } from "@/lib/api";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { CodeBlock } from "@/components/shared/code-block";
import { MetricCard } from "@/components/shared/metric-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { motion, AnimatePresence } from "motion/react";
import { GitBranch, Network, FileCode, Percent, AlertTriangle, Download, Workflow } from "lucide-react";
import * as Collapsible from "@radix-ui/react-collapsible";
import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/cn";
import { downloadAsZip } from "@/lib/download";
import { WorkflowGraph } from "./workflow-graph";

type ViewMode = "code" | "workflow";

interface ConversionResultsProps {
  result: ConversionResult;
}

export function ConversionResults({ result }: ConversionResultsProps) {
  const [warningsOpen, setWarningsOpen] = useState(result.warnings.length > 0);
  const [viewMode, setViewMode] = useState<ViewMode>("code");
  const coverage = result.stats.coverage_percentage as number | undefined;
  const hasDag = result.dag_data && result.dag_data.nodes.length > 0;

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="space-y-6"
    >
      {/* Metrics + download */}
      <div className="flex items-start justify-between gap-4">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 flex-1">
          <MetricCard
            label="Nodes"
            value={result.node_count}
            icon={<Network className="h-5 w-5" />}
          />
          <MetricCard
            label="Edges"
            value={result.edge_count}
            icon={<GitBranch className="h-5 w-5" />}
          />
          <MetricCard
            label="Files"
            value={result.files.length}
            icon={<FileCode className="h-5 w-5" />}
          />
          <MetricCard
            label="Coverage"
            value={coverage ?? 100}
            suffix="%"
            icon={<Percent className="h-5 w-5" />}
          />
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => downloadAsZip(result.files, result.workflow_name)}
          className="shrink-0 mt-1"
        >
          <Download className="h-4 w-4" />
          Download ZIP
        </Button>
      </div>

      {/* Warnings */}
      {result.warnings.length > 0 && (
        <Collapsible.Root open={warningsOpen} onOpenChange={setWarningsOpen}>
          <Collapsible.Trigger className="flex items-center gap-2 text-sm font-medium text-warning hover:underline cursor-pointer">
            <AlertTriangle className="h-4 w-4" />
            {result.warnings.length} warning{result.warnings.length > 1 ? "s" : ""}
            <ChevronDown
              className={cn("h-4 w-4 transition-transform", warningsOpen && "rotate-180")}
            />
          </Collapsible.Trigger>
          <Collapsible.Content className="mt-2 space-y-1">
            <AnimatePresence>
              {result.warnings.map((w, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  className="rounded-lg bg-warning/5 border border-warning/20 px-3 py-2 text-sm text-[var(--fg-muted)]"
                >
                  {w}
                </motion.div>
              ))}
            </AnimatePresence>
          </Collapsible.Content>
        </Collapsible.Root>
      )}

      {/* View mode toggle */}
      {hasDag && (
        <div className="flex gap-1 rounded-lg bg-[var(--bg-sidebar)] p-1 w-fit">
          <button
            onClick={() => setViewMode("code")}
            className={cn(
              "px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              viewMode === "code"
                ? "bg-[var(--bg-card)] text-[var(--fg)] shadow-sm"
                : "text-[var(--fg-muted)] hover:text-[var(--fg)]",
            )}
          >
            Code
          </button>
          <button
            onClick={() => setViewMode("workflow")}
            className={cn(
              "flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              viewMode === "workflow"
                ? "bg-[var(--bg-card)] text-[var(--fg)] shadow-sm"
                : "text-[var(--fg-muted)] hover:text-[var(--fg)]",
            )}
          >
            <Workflow className="h-3.5 w-3.5" />
            Workflow
          </button>
        </div>
      )}

      {/* Code view */}
      {viewMode === "code" && (
        <Tabs defaultValue={result.files[0]?.filename}>
          <TabsList>
            {result.files.map((f) => (
              <TabsTrigger key={f.filename} value={f.filename}>
                {f.filename}
                <Badge variant="secondary" className="ml-2 text-[10px]">
                  {f.file_type}
                </Badge>
              </TabsTrigger>
            ))}
          </TabsList>
          {result.files.map((f) => (
            <TabsContent key={f.filename} value={f.filename}>
              <CodeBlock
                code={f.content}
                language={f.file_type}
                filename={f.filename}
              />
            </TabsContent>
          ))}
        </Tabs>
      )}

      {/* Workflow split view */}
      {viewMode === "workflow" && hasDag && (
        <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
          <div className="lg:col-span-3 rounded-xl border border-[var(--border)] overflow-hidden" style={{ height: 500 }}>
            <WorkflowGraph dagData={result.dag_data!} />
          </div>
          <div className="lg:col-span-2">
            <Tabs defaultValue={result.files[0]?.filename}>
              <TabsList>
                {result.files.map((f) => (
                  <TabsTrigger key={f.filename} value={f.filename}>
                    {f.filename}
                  </TabsTrigger>
                ))}
              </TabsList>
              {result.files.map((f) => (
                <TabsContent key={f.filename} value={f.filename}>
                  <CodeBlock
                    code={f.content}
                    language={f.file_type}
                    filename={f.filename}
                  />
                </TabsContent>
              ))}
            </Tabs>
          </div>
        </div>
      )}
    </motion.div>
  );
}
