"""Converter for Alteryx SupportVectorMachine tool -> SupportVectorMachineNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import parse_field_list, safe_get
from a2d.ir.nodes import IRNode, SupportVectorMachineNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class SupportVectorMachineConverter(ToolConverter):
    """Converts Alteryx SupportVectorMachine tool to :class:`SupportVectorMachineNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["SupportVectorMachine"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        target = safe_get(cfg, "TargetField")
        features = parse_field_list(cfg, "FeatureFields")
        reg_val = safe_get(cfg, "Regularization", "1.0")
        regularization = float(reg_val) if reg_val else 1.0
        max_iter = int(safe_get(cfg, "MaxIterations", "100") or "100")

        return SupportVectorMachineNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            target_field=target,
            feature_fields=features,
            regularization=regularization,
            max_iterations=max_iter,
            conversion_confidence=0.6,
            conversion_method="mapping",
            conversion_notes=[
                "Maps to Spark MLlib LinearSVC.",
                "Only supports linear kernel; for non-linear kernels use sklearn UDF.",
            ],
        )
