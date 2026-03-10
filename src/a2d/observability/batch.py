"""Batch orchestration with error accumulation and per-file metrics."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from a2d.config import ConversionConfig
from a2d.observability.errors import ConversionError, ErrorKind, ErrorSeverity
from a2d.observability.metrics import BatchMetrics, FileMetrics
from a2d.pipeline import ConversionPipeline, ConversionResult

logger = logging.getLogger("a2d.observability.batch")

ProgressCallback = Callable[[int, int, str], None]


@dataclass
class FileConversionResult:
    """Outcome of converting a single file within a batch."""

    file_path: str
    workflow_name: str
    success: bool
    conversion_result: ConversionResult | None = None
    errors: list[ConversionError] = field(default_factory=list)
    metrics: FileMetrics = field(default_factory=FileMetrics)


@dataclass
class BatchConversionResult:
    """Outcome of converting a batch of files."""

    file_results: list[FileConversionResult] = field(default_factory=list)
    batch_metrics: BatchMetrics = field(default_factory=BatchMetrics)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.file_results if r.success and not r.errors)

    @property
    def failure_count(self) -> int:
        return sum(1 for r in self.file_results if not r.success)

    @property
    def partial_count(self) -> int:
        return sum(1 for r in self.file_results if r.success and r.errors)

    def errors_by_kind(self) -> dict[ErrorKind, list[ConversionError]]:
        """Group all errors across files by their ErrorKind."""
        result: dict[ErrorKind, list[ConversionError]] = {}
        for fr in self.file_results:
            for err in fr.errors:
                result.setdefault(err.kind, []).append(err)
        return result

    def errors_by_severity(self) -> dict[ErrorSeverity, list[ConversionError]]:
        """Group all errors across files by their ErrorSeverity."""
        result: dict[ErrorSeverity, list[ConversionError]] = {}
        for fr in self.file_results:
            for err in fr.errors:
                result.setdefault(err.severity, []).append(err)
        return result


def _classify_exception(exc: Exception) -> ErrorKind:
    """Classify an exception into an ErrorKind based on type/message heuristics."""
    exc_type = type(exc).__name__
    msg = str(exc).lower()

    if "parse" in exc_type.lower() or "xml" in msg or "lxml" in msg:
        return ErrorKind.PARSING
    if "convert" in exc_type.lower() or "converter" in msg or "unsupported" in msg:
        return ErrorKind.CONVERSION
    if "generat" in exc_type.lower() or "template" in msg or "jinja" in msg:
        return ErrorKind.GENERATION
    if isinstance(exc, OSError | IOError | FileNotFoundError | PermissionError):
        return ErrorKind.IO
    return ErrorKind.INTERNAL


class BatchOrchestrator:
    """Orchestrates batch conversion with error accumulation and metrics."""

    def __init__(self, config: ConversionConfig) -> None:
        self.config = config
        self.pipeline = ConversionPipeline(config)

    def convert_batch(
        self,
        file_paths: list[Path],
        progress_callback: ProgressCallback | None = None,
    ) -> BatchConversionResult:
        """Convert multiple files, accumulating errors instead of raising."""
        batch_start = datetime.now(timezone.utc)
        file_results: list[FileConversionResult] = []

        total = len(file_paths)
        for idx, path in enumerate(file_paths):
            if progress_callback:
                progress_callback(idx + 1, total, path.name)

            file_result = self._convert_single_with_tracking(path)
            file_results.append(file_result)

        batch_end = datetime.now(timezone.utc)

        batch_metrics = self._compute_batch_metrics(file_results, batch_start, batch_end)

        return BatchConversionResult(
            file_results=file_results,
            batch_metrics=batch_metrics,
        )

    def _convert_single_with_tracking(self, path: Path) -> FileConversionResult:
        """Convert a single file with error trapping and metrics population."""
        file_start = datetime.now(timezone.utc)
        file_path_str = str(path)
        workflow_name = path.stem
        errors: list[ConversionError] = []

        try:
            result = self.pipeline.convert(path)

            # Collect warnings as INFO-severity errors
            for warning in result.warnings:
                errors.append(
                    ConversionError(
                        kind=ErrorKind.CONVERSION,
                        severity=ErrorSeverity.WARNING,
                        message=warning,
                        file_path=file_path_str,
                    )
                )

            file_end = datetime.now(timezone.utc)
            stats = result.output.stats

            total_nodes = stats.get("total_nodes", result.dag.node_count)
            supported_nodes = stats.get("supported_nodes", total_nodes)
            unsupported_nodes = total_nodes - supported_nodes
            coverage = (supported_nodes / total_nodes * 100) if total_nodes > 0 else 100.0

            metrics = FileMetrics(
                file_path=file_path_str,
                workflow_name=workflow_name,
                started_at=file_start,
                completed_at=file_end,
                duration_seconds=(file_end - file_start).total_seconds(),
                node_count=result.dag.node_count,
                edge_count=result.dag.edge_count,
                supported_node_count=supported_nodes,
                unsupported_node_count=unsupported_nodes,
                coverage_percentage=coverage,
                files_generated=len(result.output.files),
                success=True,
            )

            return FileConversionResult(
                file_path=file_path_str,
                workflow_name=workflow_name,
                success=True,
                conversion_result=result,
                errors=errors,
                metrics=metrics,
            )

        except Exception as exc:
            logger.exception("Failed to convert %s", path)
            file_end = datetime.now(timezone.utc)

            error = ConversionError.from_exception(
                exc,
                _classify_exception(exc),
                file_path=file_path_str,
            )
            errors.append(error)

            metrics = FileMetrics(
                file_path=file_path_str,
                workflow_name=workflow_name,
                started_at=file_start,
                completed_at=file_end,
                duration_seconds=(file_end - file_start).total_seconds(),
                success=False,
            )

            return FileConversionResult(
                file_path=file_path_str,
                workflow_name=workflow_name,
                success=False,
                errors=errors,
                metrics=metrics,
            )

    def _compute_batch_metrics(
        self,
        file_results: list[FileConversionResult],
        batch_start: datetime,
        batch_end: datetime,
    ) -> BatchMetrics:
        """Compute aggregate metrics from individual file results."""
        total_files = len(file_results)
        successful = sum(1 for r in file_results if r.success and not any(e.severity == ErrorSeverity.ERROR for e in r.errors))
        failed = sum(1 for r in file_results if not r.success)
        partial = total_files - successful - failed

        total_nodes = sum(r.metrics.node_count for r in file_results)

        all_errors = [e for r in file_results for e in r.errors if e.severity == ErrorSeverity.ERROR]
        all_warnings = [e for r in file_results for e in r.errors if e.severity == ErrorSeverity.WARNING]

        coverages = [r.metrics.coverage_percentage for r in file_results if r.success]
        avg_coverage = sum(coverages) / len(coverages) if coverages else 0.0

        return BatchMetrics(
            started_at=batch_start,
            completed_at=batch_end,
            duration_seconds=(batch_end - batch_start).total_seconds(),
            total_files=total_files,
            successful_files=successful,
            failed_files=failed,
            partial_files=partial,
            total_nodes=total_nodes,
            total_errors=len(all_errors),
            total_warnings=len(all_warnings),
            avg_coverage_percentage=avg_coverage,
        )
