"""Converter for Alteryx PrincipalComponents tool -> PrincipalComponentsNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, PrincipalComponentsNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class PrincipalComponentsConverter(ToolConverter):
    """Converts Alteryx PrincipalComponents tool to :class:`PrincipalComponentsNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["PrincipalComponents"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        features = parse_field_list(cfg, "FeatureFields")
        num_components = int(safe_get(cfg, "NumComponents", "5") or "5")

        return PrincipalComponentsNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            feature_fields=features,
            num_components=num_components,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib PCA.",
                "Number of components may need adjustment based on explained variance.",
            ],
        )
