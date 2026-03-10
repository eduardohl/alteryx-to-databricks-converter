"""Converter for Alteryx ScoreModel tool -> ScoreModelNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, ScoreModelNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class ScoreModelConverter(ToolConverter):
    """Converts Alteryx ScoreModel tool to :class:`ScoreModelNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["ScoreModel"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        model_ref = safe_get(cfg, "ModelReference")
        features = parse_field_list(cfg, "FeatureFields")

        return ScoreModelNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            model_reference=model_ref,
            feature_fields=features,
            conversion_confidence=0.5,
            conversion_method="mapping",
            conversion_notes=[
                "Uses model.transform() from Spark MLlib or MLflow model.predict().",
                "Model loading path must be configured for Databricks (DBFS/MLflow).",
            ],
        )
