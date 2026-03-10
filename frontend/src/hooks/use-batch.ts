import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useBatchStore } from "@/stores/batch";

interface BatchParams {
  files: File[];
  format: string;
  catalogName?: string;
  schemaName?: string;
  includeComments?: boolean;
}

export function useBatchConversion() {
  const { startJob, connectWs } = useBatchStore();

  return useMutation<void, Error, BatchParams>({
    mutationFn: async ({ files, format, catalogName, schemaName, includeComments }) => {
      const { job_id, total_files } = await api.convertBatch(files, format, {
        catalogName,
        schemaName,
        includeComments,
      });
      startJob(job_id, total_files);
      connectWs(job_id);
    },
  });
}
