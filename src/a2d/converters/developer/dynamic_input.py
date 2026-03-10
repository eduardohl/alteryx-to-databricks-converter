"""Converter for Alteryx DynamicInput tool -> DynamicInputNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get
from a2d.ir.nodes import DynamicInputNode, IRNode
from a2d.parser.schema import ParsedNode


@ConverterRegistry.register
class DynamicInputConverter(ToolConverter):
    """Converts Alteryx DynamicInput tool to :class:`DynamicInputNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["DynamicInput"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        file_path_pattern = safe_get(cfg, "FilePath", safe_get(cfg, "PathField", ""))
        file_format = safe_get(cfg, "FileFormat", "csv").lower()
        template_file = safe_get(cfg, "TemplateFile", safe_get(cfg, "Template", ""))

        return DynamicInputNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            file_path_pattern=file_path_pattern,
            file_format=file_format,
            template_file=template_file,
            conversion_notes=["DynamicInput: file list may need adjustment for Databricks paths."],
        )
