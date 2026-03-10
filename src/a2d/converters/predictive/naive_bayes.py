"""Converter for Alteryx NaiveBayes tool -> NaiveBayesNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, NaiveBayesNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class NaiveBayesConverter(ToolConverter):
    """Converts Alteryx NaiveBayes tool to :class:`NaiveBayesNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["NaiveBayes"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        smoothing_val = safe_get(cfg, "Smoothing", "1.0")
        smoothing = float(smoothing_val) if smoothing_val else 1.0

        return NaiveBayesNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            smoothing=smoothing,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib NaiveBayes.",
                "Features must be non-negative for multinomial model.",
            ],
        )
