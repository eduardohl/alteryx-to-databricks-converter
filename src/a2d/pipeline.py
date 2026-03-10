"""Main conversion pipeline orchestrating Parse -> Convert -> Build DAG -> Generate Code."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from a2d.config import ConversionConfig, OutputFormat
from a2d.converters.registry import ConverterRegistry
from a2d.exceptions import A2dError
from a2d.generators.base import GeneratedOutput
from a2d.generators.dlt import DLTGenerator
from a2d.generators.pyspark import PySparkGenerator
from a2d.generators.sql import SQLGenerator
from a2d.generators.workflow_json import WorkflowJsonGenerator
from a2d.ir.graph import WorkflowDAG
from a2d.parser.schema import ParsedWorkflow
from a2d.parser.workflow_parser import WorkflowParser

logger = logging.getLogger("a2d.pipeline")


@dataclass
class ConversionResult:
    """Result of converting a single Alteryx workflow."""

    output: GeneratedOutput
    dag: WorkflowDAG
    parsed_workflow: ParsedWorkflow
    warnings: list[str] = field(default_factory=list)


class ConversionPipeline:
    """Main orchestration: Parse -> Convert -> Build DAG -> Generate Code."""

    def __init__(self, config: ConversionConfig) -> None:
        self.config = config
        self._parser = WorkflowParser()

    def convert(self, path: Path) -> ConversionResult:
        """Convert a single .yxmd file."""
        # 1. Parse XML
        parsed = self._parser.parse(path)

        # 2. Build IR DAG
        dag = self._build_dag(parsed)

        # 3. Validate DAG
        validation_issues = dag.validate()

        # 4. Generate code
        generator = self._get_generator()
        workflow_name = path.stem
        output = generator.generate(dag, workflow_name)

        # 5. Generate orchestration if configured
        if self.config.generate_orchestration:
            wf_gen = WorkflowJsonGenerator(self.config)
            wf_output = wf_gen.generate(dag, workflow_name)
            output.files.extend(wf_output.files)

        warnings = validation_issues + output.warnings

        return ConversionResult(
            output=output,
            dag=dag,
            parsed_workflow=parsed,
            warnings=warnings,
        )

    def convert_batch(self, directory: Path) -> list[ConversionResult]:
        """Convert all .yxmd files in a directory."""
        results = []
        for path in sorted(directory.glob("**/*.yxmd")):
            try:
                result = self.convert(path)
                results.append(result)
            except A2dError as e:
                logger.error(f"Failed to convert {path}: {e}")
        return results

    def _build_dag(self, parsed: ParsedWorkflow) -> WorkflowDAG:
        """Build an IR DAG from a parsed workflow."""
        dag = WorkflowDAG()
        # Node types to exclude from the DAG (visual-only, no data transformation)
        _SKIP_TYPES = {"ToolContainer"}

        disabled_ids: set[int] = set()
        for node in parsed.nodes:
            if node.disabled:
                disabled_ids.add(node.tool_id)
                logger.info("Skipping disabled node %d (%s)", node.tool_id, node.tool_type)
                continue
            if node.tool_type in _SKIP_TYPES:
                logger.debug("Skipping visual-only node %d (%s)", node.tool_id, node.tool_type)
                continue
            ir_node = ConverterRegistry.convert_node(node, self.config)
            dag.add_node(ir_node)

        node_ids = set(dag.all_node_ids())
        for conn in parsed.connections:
            src, dst = conn.origin.tool_id, conn.destination.tool_id
            if src not in node_ids or dst not in node_ids:
                if src not in disabled_ids and dst not in disabled_ids:
                    missing = [nid for nid in (src, dst) if nid not in node_ids]
                    logger.warning(
                        "Skipping edge %d→%d: node(s) %s not in graph",
                        src, dst, missing,
                    )
                continue
            dag.add_edge(
                src, dst,
                conn.origin.anchor_name,
                conn.destination.anchor_name,
            )
        return dag

    def _get_generator(self):
        """Return the appropriate code generator for the configured output format."""
        generators = {
            OutputFormat.PYSPARK: PySparkGenerator,
            OutputFormat.DLT: DLTGenerator,
            OutputFormat.SQL: SQLGenerator,
        }
        gen_class = generators[self.config.output_format]
        return gen_class(self.config)
