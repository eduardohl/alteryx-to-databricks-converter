"""Converter for Alteryx VarianceInflationFactors tool -> VarianceInflationFactorsNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list
from a2d.ir.nodes import IRNode, VarianceInflationFactorsNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class VarianceInflationFactorsConverter(ToolConverter):
    """Converts Alteryx VarianceInflationFactors tool to :class:`VarianceInflationFactorsNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["VarianceInflationFactors"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        feature_fields = parse_field_list(cfg, "FeatureFields")

        return VarianceInflationFactorsNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            feature_fields=feature_fields,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps VIF analysis to pandas UDF with statsmodels variance_inflation_factor.",
            ],
        )
