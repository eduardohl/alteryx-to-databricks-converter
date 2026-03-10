"""Converter for Alteryx BoostedModel tool -> BoostedModelNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import BoostedModelNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class BoostedModelConverter(ToolConverter):
    """Converts Alteryx BoostedModel tool to :class:`BoostedModelNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["BoostedModel"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        model_type = safe_get(cfg, "ModelType", "classification")
        num_iterations = int(safe_get(cfg, "NumIterations", "100") or "100")
        max_depth = int(safe_get(cfg, "MaxDepth", "5") or "5")
        lr_val = safe_get(cfg, "LearningRate", "0.1")
        learning_rate = float(lr_val) if lr_val else 0.1

        return BoostedModelNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            model_type=model_type,
            num_iterations=num_iterations,
            max_depth=max_depth,
            learning_rate=learning_rate,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib GBTClassifier/GBTRegressor.",
                "Number of iterations and learning rate may need tuning.",
            ],
        )
