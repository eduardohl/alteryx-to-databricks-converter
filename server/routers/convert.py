"""Conversion endpoints — single file and batch."""

from __future__ import annotations

import asyncio
import io
import logging
import zipfile

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from server.models.requests import OutputFormatParam
from server.models.responses import (
    BatchStartResponse,
    BatchStatusResponse,
    ConversionResponse,
)
from server.services import batch as batch_service
from server.services.conversion import convert_file
from server.utils.validation import read_upload, validate_and_read_files, validate_yxmd_file

logger = logging.getLogger("a2d.server.convert")

router = APIRouter(prefix="/api", tags=["convert"])


@router.post("/convert", response_model=ConversionResponse)
async def convert_single(
    file: UploadFile = File(...),
    output_format: OutputFormatParam = Form(OutputFormatParam.pyspark, alias="format"),
    catalog_name: str = Form("main"),
    schema_name: str = Form("default"),
    include_comments: bool = Form(True),
) -> ConversionResponse:
    validate_yxmd_file(file)
    file_bytes = await read_upload(file)

    logger.info("Converting %s (format=%s, size=%d bytes)", file.filename, output_format.value, len(file_bytes))
    try:
        result = await asyncio.to_thread(
            convert_file,
            file_bytes,
            file.filename,
            output_format.value,
            catalog_name=catalog_name,
            schema_name=schema_name,
            include_comments=include_comments,
        )
    except ValueError as e:
        logger.warning("Validation error converting %s: %s", file.filename, e)
        raise HTTPException(status_code=422, detail=str(e))
    except Exception:
        logger.exception("Unexpected error converting %s", file.filename)
        raise HTTPException(status_code=500, detail="Internal conversion error")

    logger.info("Successfully converted %s", file.filename)
    return ConversionResponse(**result)


@router.post("/convert/batch", response_model=BatchStartResponse)
async def convert_batch(
    files: list[UploadFile] = File(...),
    output_format: OutputFormatParam = Form(OutputFormatParam.pyspark, alias="format"),
    catalog_name: str = Form("main"),
    schema_name: str = Form("default"),
    include_comments: bool = Form(True),
) -> BatchStartResponse:
    file_data = await validate_and_read_files(files)

    logger.info("Starting batch conversion: %d files, format=%s", len(file_data), output_format.value)
    job_id = await batch_service.create_batch_job(
        file_data,
        output_format.value,
        catalog_name=catalog_name,
        schema_name=schema_name,
        include_comments=include_comments,
    )

    return BatchStartResponse(job_id=job_id, total_files=len(file_data))


@router.get("/convert/batch/{job_id}", response_model=BatchStatusResponse)
async def batch_status(job_id: str) -> BatchStatusResponse:
    job = batch_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return BatchStatusResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        total=job.total,
        file_results=job.file_results,
        batch_metrics=job.batch_metrics,
        errors_by_kind=job.errors_by_kind,
    )


@router.get("/convert/batch/{job_id}/download")
async def batch_download(job_id: str) -> StreamingResponse:
    """Download all generated files from a completed batch job as a ZIP."""
    job = batch_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fr in job.file_results:
            if not fr.get("success") or "files" not in fr:
                continue
            folder = fr["workflow_name"]
            for f in fr["files"]:
                zf.writestr(f"{folder}/{f['filename']}", f["content"])

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="batch-{job_id}.zip"'},
    )
