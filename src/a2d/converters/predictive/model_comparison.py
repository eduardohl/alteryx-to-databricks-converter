"""Converter for Alteryx ModelComparison tool -> ModelComparisonNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list
from a2d.ir.nodes import IRNode, ModelComparisonNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ModelComparisonConverter(ToolConverter):
    """Converts Alteryx ModelComparison tool to :class:`ModelComparisonNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ModelComparison"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        model_references = parse_field_list(cfg, "ModelReferences")

        return ModelComparisonNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            model_references=model_references,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps model comparison to MLflow metric comparison.",
            ],
        )
