"""Conversion service — wraps ConversionPipeline."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from a2d.config import ConversionConfig, OutputFormat
from a2d.ir.graph import WorkflowDAG
from a2d.pipeline import ConversionPipeline, ConversionResult
from server.constants import FORMAT_MAP
from server.utils.validation import sanitize_filename

logger = logging.getLogger("a2d.server.services.conversion")


def _serialize_dag(dag: WorkflowDAG) -> dict:
    """Serialize WorkflowDAG into a JSON-friendly dict for the frontend."""
    nodes = []
    for ir_node in dag.all_nodes():
        nodes.append({
            "node_id": ir_node.node_id,
            "tool_type": ir_node.original_tool_type,
            "annotation": ir_node.annotation,
            "position_x": ir_node.position[0],
            "position_y": ir_node.position[1],
            "conversion_confidence": ir_node.conversion_confidence,
            "conversion_method": ir_node.conversion_method,
        })

    edges = []
    for source_id, target_id, data in dag._graph.edges(data=True):
        edge_info = data.get("info")
        edges.append({
            "source_id": source_id,
            "target_id": target_id,
            "origin_anchor": edge_info.origin_anchor if edge_info else "Output",
            "destination_anchor": edge_info.destination_anchor if edge_info else "Input",
        })

    return {"nodes": nodes, "edges": edges}


def convert_file(
    file_bytes: bytes,
    filename: str,
    output_format: str,
    *,
    catalog_name: str = "main",
    schema_name: str = "default",
    include_comments: bool = True,
) -> dict:
    """Convert a single uploaded .yxmd file and return result dict."""
    fmt = FORMAT_MAP.get(output_format, OutputFormat.PYSPARK)

    safe_name = sanitize_filename(filename)
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / safe_name
        file_path.write_bytes(file_bytes)

        config = ConversionConfig(
            input_path=file_path,
            output_format=fmt,
            generate_orchestration=True,
            catalog_name=catalog_name,
            schema_name=schema_name,
            include_comments=include_comments,
        )
        pipeline = ConversionPipeline(config)
        result: ConversionResult = pipeline.convert(file_path)

    files = [
        {
            "filename": f.filename,
            "content": f.content,
            "file_type": f.file_type,
        }
        for f in result.output.files
    ]

    logger.info(
        "Converted %s: %d files generated, %d nodes, %d edges",
        filename, len(files), result.dag.node_count, result.dag.edge_count,
    )

    response = {
        "workflow_name": Path(filename).stem,
        "files": files,
        "stats": result.output.stats,
        "warnings": result.warnings,
        "node_count": result.dag.node_count,
        "edge_count": result.dag.edge_count,
        "dag_data": _serialize_dag(result.dag),
        "output_format": output_format,
    }

    # Save to history (non-blocking — don't fail conversion if history save fails)
    try:
        from server.services.history import save_conversion
        save_conversion(response)
    except Exception:
        logger.debug("History save skipped (database not configured or save failed)")

    return response
