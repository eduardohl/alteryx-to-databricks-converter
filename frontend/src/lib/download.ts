import JSZip from "jszip";
import { saveAs } from "file-saver";
import type { GeneratedFile } from "./api";

export async function downloadAsZip(
  files: GeneratedFile[],
  zipName: string,
) {
  const zip = new JSZip();
  for (const f of files) {
    zip.file(f.filename, f.content);
  }
  const blob = await zip.generateAsync({ type: "blob" });
  saveAs(blob, `${zipName}.zip`);
}

export async function downloadBatchAsZip(
  workflows: Array<{ workflowName: string; files: GeneratedFile[] }>,
  zipName: string,
) {
  const zip = new JSZip();
  for (const w of workflows) {
    const folder = zip.folder(w.workflowName);
    if (!folder) continue;
    for (const f of w.files) {
      folder.file(f.filename, f.content);
    }
  }
  const blob = await zip.generateAsync({ type: "blob" });
  saveAs(blob, `${zipName}.zip`);
}
