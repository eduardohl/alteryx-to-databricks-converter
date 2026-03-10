"""Converter for Alteryx TestOfMeans tool -> MeansTestNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import IRNode, MeansTestNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class TestOfMeansConverter(ToolConverter):
    """Converts Alteryx TestOfMeans tool to :class:`MeansTestNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["TestOfMeans"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        field_a = safe_get(cfg, "FieldA")
        field_b = safe_get(cfg, "FieldB")
        test_type = safe_get(cfg, "TestType", "two_sample")

        return MeansTestNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            field_a=field_a,
            field_b=field_b,
            test_type=test_type,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps t-test / test of means to pandas UDF with scipy.stats.ttest.",
            ],
        )
