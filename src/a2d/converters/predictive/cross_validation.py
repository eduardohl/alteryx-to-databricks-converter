"""Converter for Alteryx CrossValidation tool -> CrossValidationNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import CrossValidationNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class CrossValidationConverter(ToolConverter):
    """Converts Alteryx CrossValidation tool to :class:`CrossValidationNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["CrossValidation"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        num_folds = int(safe_get(cfg, "NumFolds", "5") or "5")
        model_reference = safe_get(cfg, "ModelReference")

        return CrossValidationNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            num_folds=num_folds,
            model_reference=model_reference,
            conversion_confidence=0.4,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib CrossValidator.",
                "Model reference and evaluator need manual configuration.",
            ],
        )
