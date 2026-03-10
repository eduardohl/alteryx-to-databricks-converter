"""Converter for Alteryx ForestModel tool -> ForestModelNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import ForestModelNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ForestModelConverter(ToolConverter):
    """Converts Alteryx ForestModel tool to :class:`ForestModelNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ForestModel"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        model_type = safe_get(cfg, "ModelType", "classification").lower()
        num_trees = int(safe_get(cfg, "NumTrees", "100") or "100")
        max_depth = int(safe_get(cfg, "MaxDepth", "5") or "5")

        return ForestModelNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            model_type=model_type,
            num_trees=num_trees,
            max_depth=max_depth,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib RandomForestClassifier/Regressor.",
                "Alteryx uses R-based implementation; hyperparameters may need tuning.",
            ],
        )
