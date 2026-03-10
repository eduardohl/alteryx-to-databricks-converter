"""Converter for Alteryx KCentroids tool -> KCentroidsNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, KCentroidsNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class KCentroidsConverter(ToolConverter):
    """Converts Alteryx KCentroids tool to :class:`KCentroidsNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["KCentroids"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        features = parse_field_list(cfg, "FeatureFields")
        k = int(safe_get(cfg, "K", "5") or "5")
        max_iter = int(safe_get(cfg, "MaxIterations", "100") or "100")

        return KCentroidsNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            feature_fields=features,
            k=k,
            max_iterations=max_iter,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib KMeans clustering.",
                "Number of clusters (K) may need tuning.",
            ],
        )
