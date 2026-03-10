import { useMutation } from "@tanstack/react-query";
import { api, type ConversionResult } from "@/lib/api";

interface ConvertParams {
  file: File;
  format: string;
  catalogName?: string;
  schemaName?: string;
  includeComments?: boolean;
}

export function useConversion() {
  return useMutation<ConversionResult, Error, ConvertParams>({
    mutationFn: ({ file, format, catalogName, schemaName, includeComments }) =>
      api.convert(file, format, { catalogName, schemaName, includeComments }),
  });
}
