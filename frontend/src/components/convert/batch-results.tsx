import { useState } from "react";
import { useBatchStore } from "@/stores/batch";
import { motion, AnimatePresence } from "motion/react";
import { MetricCard } from "@/components/shared/metric-card";
import { StatusBadge } from "@/components/shared/status-badge";
import { CodeBlock } from "@/components/shared/code-block";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  FileText,
  ChevronDown,
} from "lucide-react";

export function BatchResults() {
  const { status, fileResults, batchMetrics, errorsByKind } = useBatchStore();
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  if (status !== "completed" || !batchMetrics) return null;

  const errorChartData = errorsByKind
    ? Object.entries(errorsByKind).map(([kind, count]) => ({
        kind,
        count,
      }))
    : [];

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      className="space-y-6"
    >
      {/* Summary cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          label="Total"
          value={batchMetrics.total_files}
          icon={<FileText className="h-5 w-5" />}
        />
        <MetricCard
          label="Successful"
          value={batchMetrics.successful_files}
          icon={<CheckCircle className="h-5 w-5" />}
        />
        <MetricCard
          label="Partial"
          value={batchMetrics.partial_files}
          icon={<AlertTriangle className="h-5 w-5" />}
        />
        <MetricCard
          label="Failed"
          value={batchMetrics.failed_files}
          icon={<XCircle className="h-5 w-5" />}
        />
      </div>

      {/* Per-file table */}
      <div className="rounded-xl border border-[var(--border)] overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-[var(--bg-sidebar)] border-b border-[var(--border)]">
              <th scope="col" className="text-left px-4 py-3 font-medium text-[var(--fg-muted)]" colSpan={5}>
                <div className="flex items-center">
                  <span className="flex-1">Workflow</span>
                  <div className="flex items-center gap-6 text-xs">
                    <span>Status</span>
                    <span>Coverage</span>
                    <span>Nodes</span>
                    <span>Files</span>
                  </div>
                </div>
              </th>
            </tr>
          </thead>
          <tbody>
            {fileResults.map((fr, i) => {
              const isExpanded = expandedRow === fr.workflow_name;
              const hasFiles = fr.files && fr.files.length > 0;
              return (
                <motion.tr
                  key={fr.workflow_name}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: Math.min(i * 0.03, 0.5) }}
                  className={`border-b border-[var(--border)] last:border-b-0 ${hasFiles ? "cursor-pointer hover:bg-[var(--bg-sidebar)]/50" : ""} transition-colors`}
                  onClick={() => hasFiles && setExpandedRow(isExpanded ? null : fr.workflow_name)}
                >
                  <td className="px-4 py-3" colSpan={5}>
                    <div className="flex items-center">
                      <div className="flex items-center gap-2 font-medium text-[var(--fg)] flex-1">
                        {hasFiles && (
                          <ChevronDown className={`h-3.5 w-3.5 transition-transform ${isExpanded ? "rotate-180" : ""}`} />
                        )}
                        {fr.workflow_name}
                      </div>
                      <div className="flex items-center gap-6 text-sm">
                        <StatusBadge
                          success={fr.success}
                          hasWarnings={fr.warnings.length > 0}
                        />
                        <span className="text-[var(--fg)]">{fr.coverage.toFixed(1)}%</span>
                        <span className="text-[var(--fg-muted)]">{fr.node_count} nodes</span>
                        <span className="text-[var(--fg-muted)]">{fr.files_generated} files</span>
                      </div>
                    </div>
                    <AnimatePresence>
                      {isExpanded && fr.files && (
                        <motion.div
                          initial={{ opacity: 0, height: 0 }}
                          animate={{ opacity: 1, height: "auto" }}
                          exit={{ opacity: 0, height: 0 }}
                          className="mt-3 space-y-3 overflow-hidden"
                          onClick={(e) => e.stopPropagation()}
                        >
                          {fr.warnings.length > 0 && (
                            <div className="space-y-1">
                              {fr.warnings.map((w, wi) => (
                                <div key={wi} className="rounded-lg bg-warning/5 border border-warning/20 px-3 py-2 text-xs text-[var(--fg-muted)]">
                                  {w}
                                </div>
                              ))}
                            </div>
                          )}
                          {fr.files.map((f) => (
                            <CodeBlock
                              key={f.filename}
                              code={f.content}
                              language={f.file_type}
                              filename={f.filename}
                            />
                          ))}
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </td>
                </motion.tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Error breakdown chart */}
      <div className="rounded-xl border border-[var(--border)] bg-[var(--bg-card)] p-6">
        <h3 className="text-sm font-medium text-[var(--fg)] mb-4">
          Errors by Stage
        </h3>
        {errorChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={errorChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                dataKey="kind"
                tick={{ fill: "var(--fg-muted)", fontSize: 12 }}
              />
              <YAxis tick={{ fill: "var(--fg-muted)", fontSize: 12 }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "8px",
                }}
              />
              <Bar dataKey="count" fill="var(--ring)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex items-center justify-center h-[100px] text-sm text-[var(--fg-muted)]">
            <CheckCircle className="h-4 w-4 mr-2 text-green-500" />
            No errors encountered
          </div>
        )}
      </div>
    </motion.div>
  );
}
