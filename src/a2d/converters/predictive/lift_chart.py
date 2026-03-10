"""Converter for Alteryx LiftChart tool -> LiftChartNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import IRNode, LiftChartNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class LiftChartConverter(ToolConverter):
    """Converts Alteryx LiftChart tool to :class:`LiftChartNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LiftChart"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        prediction_field = safe_get(cfg, "PredictionField")
        actual_field = safe_get(cfg, "ActualField")

        return LiftChartNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            prediction_field=prediction_field,
            actual_field=actual_field,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps lift chart computation to binned prediction analysis.",
            ],
        )
