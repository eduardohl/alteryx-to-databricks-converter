"""Converter for Alteryx TSForecast tool -> TSForecastNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import IRNode, TSForecastNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class TSForecastConverter(ToolConverter):
    """Converts Alteryx TSForecast tool to :class:`TSForecastNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["TSForecast"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        time_field = safe_get(cfg, "TimeField")
        value_field = safe_get(cfg, "ValueField")
        forecast_field = safe_get(cfg, "ForecastField")

        return TSForecastNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            time_field=time_field,
            value_field=value_field,
            forecast_field=forecast_field,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps time series forecast to Prophet / pandas UDF.",
            ],
        )
