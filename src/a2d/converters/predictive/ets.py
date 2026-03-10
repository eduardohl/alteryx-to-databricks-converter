"""Converter for Alteryx ETS tool -> ETSNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import ETSNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ETSConverter(ToolConverter):
    """Converts Alteryx ETS tool to :class:`ETSNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ETS"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        time_field = safe_get(cfg, "TimeField")
        value_field = safe_get(cfg, "ValueField")
        error_type = safe_get(cfg, "ErrorType", "additive")
        trend_type = safe_get(cfg, "TrendType", "additive")
        seasonal_type = safe_get(cfg, "SeasonalType", "additive")

        return ETSNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            time_field=time_field,
            value_field=value_field,
            error_type=error_type,
            trend_type=trend_type,
            seasonal_type=seasonal_type,
            conversion_confidence=0.3,
            conversion_method="mapping",
            conversion_notes=[
                "ETS has no direct Spark MLlib equivalent.",
                "Consider using Prophet or pandas UDF with statsmodels.",
            ],
        )
