"""Converter for Alteryx AppendCluster tool -> AppendClusterNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import AppendClusterNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class AppendClusterConverter(ToolConverter):
    """Converts Alteryx AppendCluster tool to :class:`AppendClusterNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["AppendCluster"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        model_reference = safe_get(cfg, "ModelReference")
        feature_fields = parse_field_list(cfg, "FeatureFields")

        return AppendClusterNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            model_reference=model_reference,
            feature_fields=feature_fields,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps cluster assignment append to MLlib KMeansModel.transform.",
            ],
        )
