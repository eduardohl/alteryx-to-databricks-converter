"""Converter for Alteryx ARIMA tool -> ARIMANode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import ARIMANode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ARIMAConverter(ToolConverter):
    """Converts Alteryx ARIMA tool to :class:`ARIMANode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ARIMA"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        time_field = safe_get(cfg, "TimeField")
        value_field = safe_get(cfg, "ValueField")
        p = int(safe_get(cfg, "P", "1") or "1")
        d = int(safe_get(cfg, "D", "1") or "1")
        q = int(safe_get(cfg, "Q", "1") or "1")

        return ARIMANode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            time_field=time_field,
            value_field=value_field,
            p=p,
            d=d,
            q=q,
            conversion_confidence=0.3,
            conversion_method="mapping",
            conversion_notes=[
                "ARIMA has no direct Spark MLlib equivalent.",
                "Consider using Prophet or pandas UDF with statsmodels.",
            ],
        )
