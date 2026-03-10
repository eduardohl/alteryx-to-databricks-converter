"""Converter for Alteryx GammaRegression tool -> GammaRegressionNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import GammaRegressionNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class GammaRegressionConverter(ToolConverter):
    """Converts Alteryx GammaRegression tool to :class:`GammaRegressionNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["GammaRegression"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        link_function = safe_get(cfg, "LinkFunction", "log")

        return GammaRegressionNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            link_function=link_function,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib GeneralizedLinearRegression with family=gamma.",
                "Link function may need adjustment.",
            ],
        )
