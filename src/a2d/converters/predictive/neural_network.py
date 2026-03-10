"""Converter for Alteryx NeuralNetwork tool -> NeuralNetworkNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, parse_int_list, safe_get
from a2d.ir.nodes import IRNode, NeuralNetworkNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class NeuralNetworkConverter(ToolConverter):
    """Converts Alteryx NeuralNetwork tool to :class:`NeuralNetworkNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["NeuralNetwork"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        hidden_layers = parse_int_list(cfg, "HiddenLayers", [64, 32])
        max_iter = int(safe_get(cfg, "MaxIterations", "100") or "100")

        return NeuralNetworkNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            hidden_layers=hidden_layers,
            max_iterations=max_iter,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib MultilayerPerceptronClassifier.",
                "Layer sizes must include input and output layers; adjust manually.",
            ],
        )
