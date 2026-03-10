"""Converter for Alteryx KCentroidsDiagnostics tool -> KCentroidsDiagnosticsNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list
from a2d.ir.nodes import IRNode, KCentroidsDiagnosticsNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class KCentroidsDiagnosticsConverter(ToolConverter):
    """Converts Alteryx KCentroidsDiagnostics tool to :class:`KCentroidsDiagnosticsNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["KCentroidsDiagnostics"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        feature_fields = parse_field_list(cfg, "FeatureFields")

        return KCentroidsDiagnosticsNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            feature_fields=feature_fields,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps clustering diagnostics to ClusteringEvaluator silhouette score.",
            ],
        )
