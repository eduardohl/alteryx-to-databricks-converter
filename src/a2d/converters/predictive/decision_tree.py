"""Converter for Alteryx DecisionTree tool -> DecisionTreeNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import DecisionTreeNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class DecisionTreeConverter(ToolConverter):
    """Converts Alteryx DecisionTree tool to :class:`DecisionTreeNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["DecisionTree"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        model_type = safe_get(cfg, "ModelType", "classification").lower()
        max_depth = int(safe_get(cfg, "MaxDepth", "5") or "5")

        return DecisionTreeNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            model_type=model_type,
            max_depth=max_depth,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib DecisionTreeClassifier/Regressor.",
                "Alteryx uses R-based implementation; hyperparameters may need tuning.",
            ],
        )
