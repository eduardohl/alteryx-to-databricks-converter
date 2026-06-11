"""Converters for Alteryx LockIn plugin family -> existing IR nodes.

LockIn tools are Alteryx's push-down execution layer — they run operations
server-side in the source DB, but semantically are identical to regular
Alteryx tools and map directly to the same IR nodes.
"""

from __future__ import annotations

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry, ToolConverter
from a2d.converters.utils import ensure_list, safe_get
from a2d.ir.nodes import (
    AggAction,
    AggregationField,
    IRNode,
    ReadNode,
    SortField,
    SortNode,
    SummarizeNode,
    UnionNode,
)
from a2d.converters.join.join import JoinConverter
from a2d.converters.preparation.select import SelectConverter
from a2d.parser.schema import ParsedNode

# Reuse summarize action map
_ACTION_MAP: dict[str, AggAction] = {
    "GroupBy": AggAction.GROUP_BY,
    "Sum": AggAction.SUM,
    "Count": AggAction.COUNT,
    "CountDistinct": AggAction.COUNT_DISTINCT,
    "Min": AggAction.MIN,
    "Max": AggAction.MAX,
    "Avg": AggAction.AVG,
    "First": AggAction.FIRST,
    "Last": AggAction.LAST,
    "Concat": AggAction.CONCAT,
    "StdDev": AggAction.STD_DEV,
    "Variance": AggAction.VARIANCE,
    "Median": AggAction.MEDIAN,
    "Mode": AggAction.MODE,
    "Percentile": AggAction.PERCENTILE,
    "CountNonNull": AggAction.COUNT_NON_NULL,
    "CountNull": AggAction.COUNT_NULL,
}


@ConverterRegistry.register
class LockInInputConverter(ToolConverter):
    """Converts LockIn Input (server-side DB query) to :class:`ReadNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInInput"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        # LockIn Input stores the connection and SQL query
        connection_string = safe_get(cfg, "Connection") or safe_get(cfg, "ConnectionString") or ""
        query = safe_get(cfg, "Query") or safe_get(cfg, "SQL") or ""
        table_name = safe_get(cfg, "TableName") or ""

        return ReadNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            conversion_confidence=0.7,
            conversion_notes=["LockIn database source; connection string needs manual mapping to Databricks."],
            source_type="database",
            file_path="",
            connection_string=connection_string,
            table_name=table_name,
            query=query,
            file_format="",
            has_header=True,
            delimiter=",",
            encoding="utf-8",
            record_limit=None,
        )


@ConverterRegistry.register
class LockInStreamOutConverter(ToolConverter):
    """Converts LockIn StreamOut (server-side sort) to :class:`SortNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInStreamOut"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        # LockIn StreamOut XML: <Sort value="True/False"><SortInfo><Field field="..." order="..."/></SortInfo></Sort>
        sort_section = cfg.get("Sort", {})
        if isinstance(sort_section, dict):
            sort_enabled = sort_section.get("@value", "True").lower() != "false"
            sort_info = sort_section.get("SortInfo", {}) if sort_enabled else {}
        else:
            sort_info = {}
            sort_enabled = False

        if isinstance(sort_info, dict):
            raw_fields = ensure_list(sort_info.get("Field", [])) if sort_enabled else []
        else:
            raw_fields = []

        sort_fields: list[SortField] = []
        for f in raw_fields:
            if isinstance(f, dict):
                name = f.get("@field", f.get("@name", ""))
                order = f.get("@order", "Ascending")
                ascending = order.lower() != "descending"
                sort_fields.append(SortField(field_name=name, ascending=ascending))

        return SortNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            sort_fields=sort_fields,
        )


@ConverterRegistry.register
class LockInSummarizeConverter(ToolConverter):
    """Converts LockIn Summarize (server-side aggregation) to :class:`SummarizeNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInSummarize"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        summary_section = cfg.get("SummarizeFields", {})
        if isinstance(summary_section, dict):
            raw_fields = ensure_list(summary_section.get("SummarizeField", []))
        else:
            raw_fields = ensure_list(summary_section)

        aggregations: list[AggregationField] = []
        for f in raw_fields:
            if isinstance(f, dict):
                field_name = safe_get(f, "@field") or safe_get(f, "@name")
                action_str = safe_get(f, "@action") or safe_get(f, "@type")
                output_name = safe_get(f, "@rename") or None
                separator = safe_get(f, "@separator", default=",")
                pct_str = safe_get(f, "@percentile")
                pct_val = float(pct_str) if pct_str else None

                action = _ACTION_MAP.get(action_str, AggAction.GROUP_BY)

                aggregations.append(
                    AggregationField(
                        field_name=field_name,
                        action=action,
                        output_field_name=output_name,
                        separator=separator,
                        percentile_value=pct_val,
                    )
                )

        return SummarizeNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            aggregations=aggregations,
        )


@ConverterRegistry.register
class LockInJoinConverter(ToolConverter):
    """Converts LockIn Join (server-side join) to :class:`JoinNode`.

    Delegates to JoinConverter since the XML structure is identical.
    """

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInJoin"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        # LockIn Join uses the same XML structure as regular Join.
        # Reuse JoinConverter by temporarily aliasing the tool_type.
        return JoinConverter().convert(parsed_node, config)


@ConverterRegistry.register
class LockInUnionConverter(ToolConverter):
    """Converts LockIn Union (server-side union) to :class:`UnionNode`."""

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInUnion"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration

        # LockIn Union XML: <Mode>ByName</Mode>
        raw_mode = safe_get(cfg, "Mode", default="ByName")
        mode_map = {
            "ByName": "name",
            "ByPosition": "position",
            "Auto Config by Name": "name",
            "Auto Config by Position": "position",
            "Manually Configure": "manual",
        }
        mode = mode_map.get(raw_mode, "name")

        return UnionNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            mode=mode,
            allow_missing=True,
        )


@ConverterRegistry.register
class LockInDynamicInputConverter(ToolConverter):
    """Converts LockIn DynamicInput (runtime query from upstream column) to :class:`ReadNode`.

    This tool executes a SQL string from an upstream data column against a database.
    It cannot be auto-translated; a manual conversion note is emitted.
    """

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInDynamicInput"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        cfg = parsed_node.configuration
        connection_string = safe_get(cfg, "Connection") or safe_get(cfg, "ConnectionString") or ""

        return ReadNode(
            node_id=parsed_node.tool_id,
            original_tool_type=parsed_node.tool_type,
            original_plugin_name=parsed_node.plugin_name,
            annotation=parsed_node.annotation,
            position=parsed_node.position,
            conversion_confidence=0.2,
            conversion_notes=[
                "LockInDynamicInput executes a SQL query string stored in an upstream column value.",
                "Manual conversion required: collect the query string from the upstream DataFrame row,",
                "then call spark.sql(query_string) to execute it.",
            ],
            source_type="database",
            file_path="",
            connection_string=connection_string,
            table_name="",
            query="",
            file_format="",
            has_header=True,
            delimiter=",",
            encoding="utf-8",
            record_limit=None,
        )


@ConverterRegistry.register
class LockInSelectConverter(ToolConverter):
    """Converts LockIn Select (server-side column selection) to :class:`SelectNode`.

    Delegates to SelectConverter since the XML structure is identical.
    """

    @property
    def supported_tool_types(self) -> list[str]:
        return ["LockInSelect"]

    def convert(self, parsed_node: ParsedNode, config: ConversionConfig) -> IRNode:
        return SelectConverter().convert(parsed_node, config)
