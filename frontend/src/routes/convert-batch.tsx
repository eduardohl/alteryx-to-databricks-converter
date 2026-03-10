import { useState } from "react";
import { PageHeader } from "@/components/layout/page-header";
import { FileDropzone } from "@/components/shared/file-dropzone";
import { FormatSelector } from "@/components/convert/format-selector";
import { BatchProgress } from "@/components/convert/batch-progress";
import { BatchResults } from "@/components/convert/batch-results";
import { Button } from "@/components/ui/button";
import { useBatchConversion } from "@/hooks/use-batch";
import { useBatchStore } from "@/stores/batch";
import { useSettingsStore } from "@/stores/settings";
import { useToastStore } from "@/stores/toast";
import { api } from "@/lib/api";
import { saveAs } from "file-saver";
import { Play, Loader2, RotateCcw, Download, XCircle, RefreshCcw } from "lucide-react";

export function ConvertBatchPage() {
  const [files, setFiles] = useState<File[]>([]);
  const settings = useSettingsStore();
  const [format, setFormat] = useState(settings.format);
  const mutation = useBatchConversion();
  const { status, jobId, fileResults, disconnect, reset } = useBatchStore();
  const toast = useToastStore();
  const [downloading, setDownloading] = useState(false);

  const handleCancel = () => {
    disconnect();
    toast.add("Batch conversion cancelled", "info");
    reset();
    setFiles([]);
    mutation.reset();
  };

  const failedFiles = fileResults.filter((fr) => !fr.success);
  const handleRetryFailed = () => {
    if (failedFiles.length === 0) return;
    // Filter original files to only those that failed
    const failedNames = new Set(failedFiles.map((fr) => fr.file_name));
    const retryFiles = files.filter((f) => failedNames.has(f.name));
    if (retryFiles.length > 0) {
      reset();
      mutation.reset();
      mutation.mutate({
        files: retryFiles,
        format,
        catalogName: settings.catalogName,
        schemaName: settings.schemaName,
        includeComments: settings.includeComments,
      });
    }
  };

  const handleConvert = () => {
    if (files.length === 0) return;
    mutation.mutate({
      files,
      format,
      catalogName: settings.catalogName,
      schemaName: settings.schemaName,
      includeComments: settings.includeComments,
    });
  };

  const handleReset = () => {
    reset();
    setFiles([]);
    mutation.reset();
  };

  const handleDownload = async () => {
    if (!jobId) return;
    setDownloading(true);
    try {
      const blob = await api.batchDownload(jobId);
      saveAs(blob, `batch-${jobId}.zip`);
      toast.add("Batch download started", "success");
    } catch {
      toast.add("Download failed", "error");
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Batch Convert"
        description="Upload multiple .yxmd files for batch conversion with real-time progress"
      >
        {status === "running" && (
          <Button variant="destructive" size="sm" onClick={handleCancel}>
            <XCircle className="h-4 w-4" />
            Cancel
          </Button>
        )}
        {status === "completed" && (
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              onClick={handleDownload}
              disabled={downloading}
            >
              {downloading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Download className="h-4 w-4" />
              )}
              Download All (ZIP)
            </Button>
            {failedFiles.length > 0 && (
              <Button variant="secondary" onClick={handleRetryFailed}>
                <RefreshCcw className="h-4 w-4" />
                Retry Failed ({failedFiles.length})
              </Button>
            )}
            <Button variant="secondary" onClick={handleReset}>
              <RotateCcw className="h-4 w-4" />
              New Batch
            </Button>
          </div>
        )}
      </PageHeader>

      {status === "idle" && (
        <>
          <FileDropzone files={files} onFilesChange={setFiles} multiple />
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
              Convert Batch ({files.length} files)
            </Button>
          </div>
          <p className="text-xs text-[var(--fg-muted)]">
            Catalog: {settings.catalogName}.{settings.schemaName}
            {" | "}
            <a href="/settings" className="underline hover:text-[var(--fg)]">Change settings</a>
          </p>
        </>
      )}

      {mutation.error && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 text-sm text-destructive">
          {mutation.error.message}
        </div>
      )}

      <BatchProgress />
      <BatchResults />
    </div>
  );
}
