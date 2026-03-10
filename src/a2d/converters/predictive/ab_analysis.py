"""Converter for Alteryx ABAnalysis tool -> ABAnalysisNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import ABAnalysisNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ABAnalysisConverter(ToolConverter):
    """Converts Alteryx ABAnalysis tool to :class:`ABAnalysisNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ABAnalysis"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        treatment_field = safe_get(cfg, "TreatmentField")
        response_field = safe_get(cfg, "ResponseField")

        return ABAnalysisNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            treatment_field=treatment_field,
            response_field=response_field,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps A/B test analysis to pandas UDF with scipy.stats.",
            ],
        )
