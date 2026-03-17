"""Converter for Alteryx Filter tool -> FilterNode."""

from __future__ import annotations

import html

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import safe_get_nested
from a2d.ir.nodes import FilterNode, IRNode
from a2d.parser.schema import ParsedNode


_UNARY_NULL_OPS: dict[str, str] = {
    "IsNull": "IsNull([{field}])",
    "IsNotNull": "!IsNull([{field}])",
    "IsEmpty": "IsEmpty([{field}])",
    "IsNotEmpty": "!IsEmpty([{field}])",
}


def _build_simple_expression(cfg: dict) -> str:
    """Build a filter expression from Alteryx simple-mode fields.

    Simple mode stores: Field, Operator, Operands -> Operand
    Example: [Age] > 18

    Some XML variants nest these under a ``<Simple>`` element.
    """
    # If fields are nested under a <Simple> element, unwrap first
    simple = cfg.get("Simple", {})
    if isinstance(simple, dict) and simple:
        cfg = simple

    field_name = safe_get_nested(cfg, "Field")
    operator = safe_get_nested(cfg, "Operator")
    operands = cfg.get("Operands", {})

    if isinstance(operands, dict):
        operand = safe_get_nested(operands, "Operand")
    else:
        operand = str(operands) if operands else ""

    if not field_name or not operator:
        return ""

    # Unary null/empty operators don't take an operand — build directly
    if operator in _UNARY_NULL_OPS:
        return _UNARY_NULL_OPS[operator].format(field=field_name)

    expr = f"[{field_name}] {operator}"
    if operand:
        expr = f"{expr} {operand}"
    return html.unescape(expr)


@ConverterRegistry.register
class FilterConverter(ToolConverter):
    """Converts Alteryx Filter to :class:`FilterNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["Filter"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        mode = safe_get_nested(cfg, "Mode", default="Simple")
        mode_lower = mode.lower() if mode else "simple"

        # Always try to get the Expression element first (present in both modes)
        expression = safe_get_nested(cfg, "Expression")
        expression = html.unescape(expression) if expression else ""

        if mode_lower == "custom":
            ir_mode = "custom"
        else:
            ir_mode = "simple"
            # If no top-level expression, try building from Simple mode sub-fields
            if not expression:
                expression = _build_simple_expression(cfg)

        return FilterNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            expression=expression,
            mode=ir_mode,
        )
