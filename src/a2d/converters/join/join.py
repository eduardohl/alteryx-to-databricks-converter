"""Converter for Alteryx Join tool -> JoinNode."""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import ensure_list
from a2d.ir.nodes import (
    FieldAction,
    FieldOperation,
    IRNode,
    JoinKey,
    JoinNode,
)
from a2d.parser.schema import ParsedNode


def _parse_join_keys(cfg: dict) -> list[JoinKey]:
    """Extract join key pairs from the configuration."""
    join_info = cfg.get("JoinInfo", cfg.get("JoinFields", {}))
    if isinstance(join_info, dict):
        raw_keys = ensure_list(join_info.get("Field", join_info.get("JoinField", [])))
    else:
        raw_keys = []

    keys: list[JoinKey] = []
    for k in raw_keys:
        if isinstance(k, dict):
            left = k.get("@left", k.get("@LeftField", ""))
            right = k.get("@right", k.get("@RightField", ""))
            if left and right:
                keys.append(JoinKey(left_field=left, right_field=right))
    return keys


def _parse_field_ops(cfg: dict, section_key: str) -> list[FieldOperation]:
    """Parse field selection operations for one side of a join."""
    section = cfg.get(section_key, {})
    if not isinstance(section, dict):
        return []
    raw = ensure_list(section.get("SelectField", section.get("Field", [])))
    ops: list[FieldOperation] = []
    for f in raw:
        if isinstance(f, dict):
            name = f.get("@field", f.get("@name", ""))
            selected = f.get("@selected", "True").lower() != "false"
            rename_to = f.get("@rename", None) or None
            action = FieldAction.RENAME if rename_to else (FieldAction.SELECT if selected else FieldAction.DESELECT)
            ops.append(
                FieldOperation(
                    field_name=name,
                    action=action,
                    rename_to=rename_to,
                    selected=selected,
                )
            )
    return ops


@ConverterRegistry.register
class JoinConverter(ToolConverter):
    """Converts Alteryx Join to :class:`JoinNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["Join"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        join_keys = _parse_join_keys(cfg)

        # Alteryx Join always does an inner join; the unmatched rows go to
        # separate output anchors (Left=L, Right=R, Join=J).
        # We map it as "inner" by default.
        join_type = "inner"

        select_left = _parse_field_ops(cfg, "SelectLeftFields")
        select_right = _parse_field_ops(cfg, "SelectRightFields")

        return JoinNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            join_keys=join_keys,
            join_type=join_type,
            select_left=select_left,
            select_right=select_right,
            conversion_notes=[
                "Alteryx Join produces 3 outputs (J/L/R); downstream connections determine effective join type."
            ],
        )
