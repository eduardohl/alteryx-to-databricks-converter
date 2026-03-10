"""Batch conversion service — wraps BatchOrchestrator with async job tracking."""

from __future__ import annotations

import asyncio
import logging
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from a2d.config import ConversionConfig, OutputFormat
from a2d.observability.batch import BatchOrchestrator, FileConversionResult
from a2d.observability.errors import ErrorSeverity
from server.constants import FORMAT_MAP
from server.settings import settings
from server.utils.validation import sanitize_filename

logger = logging.getLogger("a2d.server.services.batch")


@dataclass
class BatchJob:
    job_id: str
    status: str = "pending"  # pending | running | completed | failed
    progress: int = 0
    total: int = 0
    current_filename: str = ""
    file_results: list[dict] = field(default_factory=list)
    batch_metrics: dict | None = None
    errors_by_kind: dict[str, int] | None = None
    error_message: str | None = None
    subscribers: list[asyncio.Queue] = field(default_factory=list)
    created_at: float = field(default_factory=time.monotonic)


class JobStore:
    """Thread-safe in-memory store for batch jobs."""

    def __init__(self) -> None:
        self._jobs: dict[str, BatchJob] = {}
        self._lock = threading.Lock()

    def get(self, job_id: str) -> BatchJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def create(self, total: int) -> BatchJob:
        job_id = uuid.uuid4().hex[:12]
        job = BatchJob(job_id=job_id, total=total)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def subscribe(self, job_id: str, queue: asyncio.Queue) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            job.subscribers.append(queue)
            return True

    def unsubscribe(self, job_id: str, queue: asyncio.Queue) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job and queue in job.subscribers:
                job.subscribers.remove(queue)

    def evict_expired(self) -> int:
        """Remove jobs older than the configured TTL. Returns count of evicted jobs."""
        now = time.monotonic()
        expired: list[str] = []
        with self._lock:
            for job_id, job in self._jobs.items():
                if now - job.created_at > settings.job_ttl_seconds:
                    expired.append(job_id)
            for job_id in expired:
                del self._jobs[job_id]
        return len(expired)


_store = JobStore()


def get_store() -> JobStore:
    """Return the global job store (for use in lifespan cleanup)."""
    return _store


def get_job(job_id: str) -> BatchJob | None:
    return _store.get(job_id)


async def create_batch_job(
    files: list[tuple[str, bytes]],
    output_format: str,
    *,
    catalog_name: str = "main",
    schema_name: str = "default",
    include_comments: bool = True,
) -> str:
    """Create a batch job and return job_id. Starts conversion in background."""
    job = _store.create(total=len(files))
    logger.info("Created batch job %s: %d files, format=%s", job.job_id, len(files), output_format)
    asyncio.create_task(_run_batch(
        job, files, output_format,
        catalog_name=catalog_name,
        schema_name=schema_name,
        include_comments=include_comments,
    ))
    return job.job_id


def subscribe(job_id: str, queue: asyncio.Queue) -> bool:
    return _store.subscribe(job_id, queue)


def unsubscribe(job_id: str, queue: asyncio.Queue) -> None:
    _store.unsubscribe(job_id, queue)


async def _run_batch(
    job: BatchJob,
    files: list[tuple[str, bytes]],
    output_format: str,
    *,
    catalog_name: str = "main",
    schema_name: str = "default",
    include_comments: bool = True,
) -> None:
    """Run batch conversion in a background thread."""
    job.status = "running"
    logger.info("Batch job %s started", job.job_id)

    try:
        fmt = FORMAT_MAP.get(output_format, OutputFormat.PYSPARK)

        with tempfile.TemporaryDirectory() as tmpdir:
            file_paths: list[Path] = []
            for filename, content in files:
                p = Path(tmpdir) / sanitize_filename(filename)
                p.write_bytes(content)
                file_paths.append(p)

            config = ConversionConfig(
                input_path=Path(tmpdir),
                output_format=fmt,
                generate_orchestration=True,
                catalog_name=catalog_name,
                schema_name=schema_name,
                include_comments=include_comments,
            )
            orchestrator = BatchOrchestrator(config)

            def progress_callback(current: int, total: int, filename: str) -> None:
                job.progress = current
                job.current_filename = filename

            # Run CPU-bound work in a thread
            result = await asyncio.to_thread(
                orchestrator.convert_batch, file_paths, progress_callback
            )

        # Build file results (include files for download, summary for WS)
        for fr in result.file_results:
            file_result = _build_file_result(fr, include_files=True)
            job.file_results.append(file_result)
            # Send lightweight message over WS (no files payload)
            summary = {k: v for k, v in file_result.items() if k != "files"}
            msg = {"type": "file_complete", **summary}
            for q in job.subscribers:
                await q.put(msg)

        # Build batch metrics
        job.batch_metrics = result.batch_metrics.to_dict()
        job.errors_by_kind = {
            k.value: len(v) for k, v in result.errors_by_kind().items()
        }
        job.status = "completed"

        logger.info(
            "Batch job %s completed: %d files processed",
            job.job_id, len(job.file_results),
        )

        # Notify subscribers of completion
        complete_msg = {
            "type": "batch_complete",
            "batch_metrics": job.batch_metrics,
            "errors_by_kind": job.errors_by_kind,
            "file_results": job.file_results,
        }
        for q in job.subscribers:
            await q.put(complete_msg)
    except Exception:
        logger.exception("Batch job %s failed", job.job_id)
        job.status = "failed"
        job.error_message = "Internal batch conversion error"
        # Notify subscribers of failure
        error_msg = {"type": "error", "message": job.error_message}
        for q in job.subscribers:
            await q.put(error_msg)


def _build_file_result(fr: FileConversionResult, include_files: bool = False) -> dict:
    """Convert a FileConversionResult to a serializable dict."""
    warnings = [
        e.message for e in fr.errors if e.severity == ErrorSeverity.WARNING
    ]
    errors = [e.to_dict() for e in fr.errors if e.severity == ErrorSeverity.ERROR]

    result = {
        "file_name": Path(fr.file_path).name,
        "workflow_name": fr.workflow_name,
        "success": fr.success,
        "coverage": fr.metrics.coverage_percentage,
        "node_count": fr.metrics.node_count,
        "edge_count": fr.metrics.edge_count,
        "files_generated": fr.metrics.files_generated,
        "errors": errors,
        "warnings": warnings,
    }

    if include_files and fr.conversion_result and fr.conversion_result.output:
        result["files"] = [
            {
                "filename": f.filename,
                "content": f.content,
                "file_type": f.file_type,
            }
            for f in fr.conversion_result.output.files
        ]

    return result
