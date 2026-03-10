import { useEffect, useRef, useState } from "react";
import { PageHeader } from "@/components/layout/page-header";
import { FileDropzone } from "@/components/shared/file-dropzone";
import { FormatSelector } from "@/components/convert/format-selector";
import { ConversionResults } from "@/components/convert/conversion-results";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useConversion } from "@/hooks/use-conversion";
import { useSettingsStore } from "@/stores/settings";
import { useToastStore } from "@/stores/toast";
import { useLocalHistoryStore } from "@/stores/local-history";
import { useConvertBridge } from "@/stores/convert-bridge";
import { Card } from "@/components/ui/card";
import { Play, Loader2, RotateCcw, ArrowRight, ChevronDown, Info } from "lucide-react";

const FORMAT_GUIDE = [
  {
    id: "pyspark",
    name: "PySpark",
    best: "Interactive notebooks, exploratory analysis, complex transformations",
    output: "Databricks .py notebooks with PySpark DataFrame API",
    when: "Default choice. Best for teams familiar with Python and needing full control.",
  },
  {
    id: "dlt",
    name: "Delta Live Tables",
    best: "Production ETL pipelines, data quality, automated orchestration",
    output: "DLT pipeline definitions with @dlt.table decorators",
    when: "Best for production data engineering. Handles dependencies, quality checks, and retries.",
  },
  {
    id: "sql",
    name: "Spark SQL",
    best: "SQL-first teams, simple transformations, quick migration",
    output: "SQL views, CTEs, and CREATE TABLE statements",
    when: "Easiest to understand. Best for analysts or simple transformation chains.",
  },
];

export function ConvertPage() {
  const [files, setFiles] = useState<File[]>([]);
  const settings = useSettingsStore();
  const [format, setFormat] = useState(settings.format);
  const toast = useToastStore();
  const localHistory = useLocalHistoryStore();
  const mutation = useConversion();
  const resultsRef = useRef<HTMLDivElement>(null);
  const bridge = useConvertBridge();

  // Clear bridge hint on unmount
  useEffect(() => {
    return () => bridge.clear();
  }, []);

  // Persist format choice
  useEffect(() => {
    settings.setFormat(format);
  }, [format]);

  const handleConvert = () => {
    if (files.length === 0) return;
    mutation.mutate({
      file: files[0],
      format,
      catalogName: settings.catalogName,
      schemaName: settings.schemaName,
      includeComments: settings.includeComments,
    });
  };

  // On success: toast + scroll + save to local history
  useEffect(() => {
    if (mutation.data) {
      toast.add(
        `Converted "${mutation.data.workflow_name}" successfully`,
        "success",
      );
      localHistory.add(mutation.data, format);
      setTimeout(() => {
        resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 100);
    }
  }, [mutation.data]);

  const handleReset = () => {
    mutation.reset();
    setFiles([]);
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Convert Workflow"
        description="Upload a single .yxmd file and generate equivalent Databricks code"
      >
        {mutation.data && (
          <Button variant="secondary" size="sm" onClick={handleReset}>
            <RotateCcw className="h-4 w-4" />
            Convert Another
          </Button>
        )}
      </PageHeader>

      {!mutation.data && (
        <>
          {bridge.workflowName && (
            <div className="flex items-center gap-2 rounded-lg border border-[var(--ring)]/30 bg-[var(--ring)]/5 px-4 py-3 text-sm text-[var(--fg)]">
              <ArrowRight className="h-4 w-4 text-[var(--ring)] shrink-0" />
              Ready to convert <strong>{bridge.workflowName}</strong> from your analysis. Upload the .yxmd file below.
            </div>
          )}
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto] gap-4 items-end">
            <FileDropzone files={files} onFilesChange={setFiles} />
            <div className="flex items-center gap-3">
              <FormatSelector value={format} onChange={setFormat} />
              <Button
                onClick={handleConvert}
                disabled={files.length === 0 || mutation.isPending}
              >
                {mutation.isPending ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Play className="h-4 w-4" />
                )}
                Convert
              </Button>
            </div>
          </div>

          <p className="text-xs text-[var(--fg-muted)]">
            Output: {format === "pyspark" ? "PySpark" : format === "dlt" ? "Delta Live Tables" : "Spark SQL"}
            {" | "}Catalog: {settings.catalogName}.{settings.schemaName}
            {" | "}
            <a href="/settings" className="underline hover:text-[var(--fg)]">Change settings</a>
          </p>

          {/* Format comparison guide */}
          <details className="group">
            <summary className="flex items-center gap-1.5 text-xs text-[var(--fg-muted)] cursor-pointer hover:text-[var(--fg)]">
              <Info className="h-3.5 w-3.5" />
              Which format should I choose?
              <ChevronDown className="h-3 w-3 transition-transform group-open:rotate-180" />
            </summary>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3">
              {FORMAT_GUIDE.map((fg) => (
                <Card
                  key={fg.id}
                  className={`cursor-pointer transition-all ${
                    format === fg.id
                      ? "ring-2 ring-[var(--ring)] shadow-md"
                      : "hover:shadow-sm"
                  }`}
                  onClick={() => setFormat(fg.id)}
                >
                  <h4 className="text-sm font-semibold text-[var(--fg)] mb-1">{fg.name}</h4>
                  <p className="text-xs text-[var(--fg-muted)] mb-2">{fg.output}</p>
                  <p className="text-xs text-[var(--fg)]"><strong>Best for:</strong> {fg.best}</p>
                  <p className="text-xs text-[var(--fg-muted)] mt-1">{fg.when}</p>
                </Card>
              ))}
            </div>
          </details>
        </>
      )}

      {mutation.isPending && (
        <div className="space-y-4">
          <div className="text-sm text-[var(--fg-muted)] animate-pulse">
            Converting {files[0]?.name}...
          </div>
          <Skeleton className="h-24 rounded-xl" />
          <Skeleton className="h-64 rounded-xl" />
        </div>
      )}

      {mutation.error && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 text-sm text-destructive">
          <p className="font-medium mb-1">Conversion failed</p>
          <p>{mutation.error.message}</p>
          <div className="mt-2 text-xs text-destructive/70 space-y-1">
            {mutation.error.message.includes("Failed to fetch") && (
              <p>The API server may be offline. Check that it is running and accessible.</p>
            )}
            {mutation.error.message.includes("422") && (
              <p>The file may not be a valid .yxmd Alteryx workflow. Ensure you are uploading an unmodified .yxmd file.</p>
            )}
            {mutation.error.message.includes("413") && (
              <p>File is too large. Try a smaller workflow file.</p>
            )}
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="mt-2"
            onClick={handleConvert}
          >
            Retry
          </Button>
        </div>
      )}

      <div ref={resultsRef}>
        {mutation.data && <ConversionResults result={mutation.data} />}
      </div>
    </div>
  );
}
