"""Converter for Alteryx ModelCoefficients tool -> ModelCoefficientsNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import IRNode, ModelCoefficientsNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ModelCoefficientsConverter(ToolConverter):
    """Converts Alteryx ModelCoefficients tool to :class:`ModelCoefficientsNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ModelCoefficients"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        model_reference = safe_get(cfg, "ModelReference")

        return ModelCoefficientsNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            model_reference=model_reference,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps coefficient extraction to MLlib model.coefficients access.",
            ],
        )
