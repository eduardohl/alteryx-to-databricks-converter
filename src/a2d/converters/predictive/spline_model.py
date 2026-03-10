"""Converter for Alteryx SplineModel tool -> SplineModelNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, SplineModelNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class SplineModelConverter(ToolConverter):
    """Converts Alteryx SplineModel tool to :class:`SplineModelNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["SplineModel"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        max_knots = int(safe_get(cfg, "MaxKnots", "10") or "10")

        return SplineModelNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            max_knots=max_knots,
            conversion_confidence=0.3,
            conversion_method="mapping",
            conversion_notes=[
                "Spline regression has no direct Spark MLlib equivalent.",
                "Consider using polynomial features or a custom UDF.",
            ],
        )
