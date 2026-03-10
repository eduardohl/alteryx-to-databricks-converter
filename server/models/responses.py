"""Pydantic response models for the API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str


class ReadinessResponse(BaseModel):
    ready: bool
    converters_loaded: int


class StatsResponse(BaseModel):
    supported_tools: int
    total_tools: int
    expression_functions: int
    output_formats: int
    version: str


class GeneratedFileResponse(BaseModel):
    filename: str
    content: str
    file_type: str


class ConversionStatsResponse(BaseModel):
    """Typed replacement for the untyped stats dict in ConversionResponse."""
    node_count: int = 0
    edge_count: int = 0
    coverage_percentage: float = 0.0
    files_generated: int = 0
    unsupported_tools: list[str] = []


class DagNodeResponse(BaseModel):
    node_id: int
    tool_type: str
    annotation: str | None = None
    position_x: float
    position_y: float
    conversion_confidence: float
    conversion_method: str


class DagEdgeResponse(BaseModel):
    source_id: int
    target_id: int
    origin_anchor: str
    destination_anchor: str


class DagDataResponse(BaseModel):
    nodes: list[DagNodeResponse]
    edges: list[DagEdgeResponse]


class ConversionResponse(BaseModel):
    workflow_name: str
    files: list[GeneratedFileResponse]
    stats: dict  # kept as dict for backward compat with pipeline output
    warnings: list[str]
    node_count: int
    edge_count: int
    dag_data: DagDataResponse | None = None


class BatchStartResponse(BaseModel):
    job_id: str
    total_files: int


class ErrorDetail(BaseModel):
    """Typed model for individual error entries."""
    message: str
    severity: str = "ERROR"
    tool_type: str | None = None
    node_id: int | None = None


class FileResultResponse(BaseModel):
    file_name: str
    workflow_name: str
    success: bool
    coverage: float
    node_count: int
    edge_count: int
    files_generated: int
    errors: list[ErrorDetail]
    warnings: list[str]


class BatchMetricsResponse(BaseModel):
    duration_seconds: float
    total_files: int
    successful_files: int
    failed_files: int
    partial_files: int
    total_nodes: int
    total_errors: int
    total_warnings: int
    avg_coverage_percentage: float


class BatchStatusResponse(BaseModel):
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    progress: int
    total: int
    file_results: list[FileResultResponse]
    batch_metrics: BatchMetricsResponse | None = None
    errors_by_kind: dict[str, int] | None = None


class ToolResponse(BaseModel):
    tool_type: str
    category: str
    supported: bool
    conversion_method: str | None = None
    description: str | None = None
    databricks_equivalent: str | None = None


class ToolMatrixResponse(BaseModel):
    categories: dict[str, list[ToolResponse]]
    total_tools: int
    supported_tools: int


class WorkflowAnalysisResponse(BaseModel):
    file_name: str
    workflow_name: str
    node_count: int
    connection_count: int
    coverage_percentage: float
    complexity_score: float
    complexity_level: str
    migration_priority: str
    estimated_effort: str
    tool_types: list[str]
    unsupported_types: list[str]
    warnings: list[str]


class AnalysisResponse(BaseModel):
    total_workflows: int
    total_nodes: int
    avg_coverage: float
    avg_complexity: float
    workflows: list[WorkflowAnalysisResponse]
    tool_frequency: dict[str, int]
    unsupported_tools: list[str]


class HistoryListItem(BaseModel):
    id: str
    workflow_name: str
    output_format: str
    created_at: str
    node_count: int
    edge_count: int
    coverage_percentage: float | None


class HistoryListResponse(BaseModel):
    items: list[HistoryListItem]
    total: int
