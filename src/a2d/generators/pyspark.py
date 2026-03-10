"""PySpark notebook code generator.

Walks a :class:`~a2d.ir.graph.WorkflowDAG` in topological order and emits a
Databricks notebook with ``# COMMAND ----------`` cell separators.
"""

from __future__ import annotations

import logging
import re

from a2d.config import ConversionConfig
from a2d.expressions.base_translator import BaseTranslationError
from a2d.expressions.translator import PySparkTranslator
from a2d.generators.base import CodeGenerator, GeneratedFile, GeneratedOutput, NodeCodeResult
from a2d.ir.graph import WorkflowDAG
from a2d.ir.nodes import (
    ABAnalysisNode,
    AggAction,
    AppendClusterNode,
    AppendFieldsNode,
    ARIMANode,
    AutoFieldNode,
    BoostedModelNode,
    BrowseNode,
    BufferNode,
    ChartNode,
    CloudStorageNode,
    CommentNode,
    CountRecordsNode,
    CountRegressionNode,
    CreatePointsNode,
    CrossTabNode,
    CrossValidationNode,
    DataCleansingNode,
    DateTimeNode,
    DecisionTreeNode,
    DistanceNode,
    DownloadNode,
    DynamicInputNode,
    DynamicOutputNode,
    EmailOutputNode,
    ETSNode,
    FieldAction,
    FieldSummaryNode,
    FilterNode,
    FindNearestNeighborsNode,
    FindNearestNode,
    FindReplaceNode,
    ForestModelNode,
    FormulaNode,
    GammaRegressionNode,
    GenerateRowsNode,
    GeocoderNode,
    ImputationNode,
    IRNode,
    JoinMultipleNode,
    JoinNode,
    JsonParseNode,
    KCentroidsDiagnosticsNode,
    KCentroidsNode,
    LiftChartNode,
    LinearRegressionNode,
    LiteralDataNode,
    LogisticRegressionNode,
    MacroIONode,
    MakeGridNode,
    MeansTestNode,
    ModelCoefficientsNode,
    ModelComparisonNode,
    MultiFieldFormulaNode,
    MultiRowFormulaNode,
    NaiveBayesNode,
    NeuralNetworkNode,
    PrincipalComponentsNode,
    PythonToolNode,
    ReadNode,
    RecordIDNode,
    RegExNode,
    ReportNode,
    RunCommandNode,
    RunningTotalNode,
    SampleNode,
    ScoreModelNode,
    SelectNode,
    SortNode,
    SpatialMatchNode,
    SplineModelNode,
    StepwiseNode,
    SummarizeNode,
    SupportVectorMachineNode,
    TextToColumnsNode,
    TileNode,
    TradeAreaNode,
    TransposeNode,
    TSForecastNode,
    UnionNode,
    UniqueNode,
    UnsupportedNode,
    VarianceInflationFactorsNode,
    WeightedAverageNode,
    WidgetNode,
    WorkflowControlNode,
    WriteNode,
    XMLParseNode,
)

logger = logging.getLogger("a2d.generators.pyspark")


# ---------------------------------------------------------------------------
# PySpark Generator
# ---------------------------------------------------------------------------


class PySparkGenerator(CodeGenerator):
    """Generate a Databricks PySpark notebook from a WorkflowDAG."""

    def __init__(self, config: ConversionConfig) -> None:
        super().__init__(config)
        self._translator = PySparkTranslator()

    # -- Public API ---------------------------------------------------------

    def generate(self, dag: WorkflowDAG, workflow_name: str = "workflow") -> GeneratedOutput:
        ordered_nodes = dag.topological_order()
        warnings: list[str] = []
        all_imports: set[str] = {
            "from pyspark.sql import functions as F",
            "from pyspark.sql import Window",
        }
        var_map: dict[int, dict[str, str]] = {}  # node_id -> {anchor: var_name}
        cells: list[str] = []
        node_count = 0
        unsupported_count = 0

        for node in ordered_nodes:
            if isinstance(node, CommentNode):
                # Canvas comments become code comments, no variable output
                if node.comment_text:
                    cells.append(f"# {node.comment_text}")
                continue

            input_vars = self._resolve_input_vars(node.node_id, dag, var_map)
            result = self._generate_node_code(node, input_vars)

            if result.imports:
                all_imports.update(result.imports)
            if result.warnings:
                warnings.extend(result.warnings)
            if isinstance(node, UnsupportedNode):
                unsupported_count += 1

            # Register output variables
            var_map[node.node_id] = result.output_vars

            # Build cell content
            annotation = ""
            if self.config.include_comments and node.annotation:
                annotation = f"# {node.annotation}\n"
            comment = ""
            if self.config.include_comments:
                comment = f"# Step {node.node_id}: {node.original_tool_type or type(node).__name__}\n"

            cell = annotation + comment + "\n".join(result.code_lines)
            cells.append(cell)
            node_count += 1

        # Build notebook content
        import_cell = "\n".join(sorted(all_imports))
        separator = "\n\n# COMMAND ----------\n\n"
        notebook_body = separator.join([import_cell] + cells)
        notebook_content = "# Databricks notebook source\n" + separator + notebook_body + "\n"

        files = [
            GeneratedFile(
                filename=f"{workflow_name}.py",
                content=notebook_content,
                file_type="python",
            )
        ]

        stats = {
            "total_nodes": node_count,
            "unsupported_nodes": unsupported_count,
            "warnings": len(warnings),
        }

        return GeneratedOutput(files=files, warnings=warnings, stats=stats)

    # -- Input resolution ---------------------------------------------------

    def _resolve_input_vars(self, node_id: int, dag: WorkflowDAG, var_map: dict[int, dict[str, str]]) -> dict[str, str]:
        """Determine input DataFrame variable names from predecessors.

        Returns a dict mapping destination anchor name -> variable name.
        For example ``{"Input": "df_1", "Left": "df_2", "Right": "df_3"}``.
        """
        result: dict[str, str] = {}
        preds = dag.get_predecessors(node_id)
        for pred in preds:
            edge_info = dag.get_edge_info(pred.node_id, node_id)
            origin_anchor = edge_info.origin_anchor
            dest_anchor = edge_info.destination_anchor

            pred_vars = var_map.get(pred.node_id, {})
            # Try to match origin anchor to a specific output variable
            if origin_anchor in pred_vars:
                var_name = pred_vars[origin_anchor]
            elif "Output" in pred_vars:
                var_name = pred_vars["Output"]
            elif pred_vars:
                # Take the first available output
                var_name = next(iter(pred_vars.values()))
            else:
                var_name = f"df_{pred.node_id}"

            result[dest_anchor] = var_name
        return result

    # -- Node dispatch ------------------------------------------------------

    def _generate_node_code(self, node: IRNode, input_vars: dict[str, str]) -> NodeCodeResult:
        """Generate PySpark code for a single IR node."""
        type_name = type(node).__name__
        method_name = f"_generate_{type_name}"
        method = getattr(self, method_name, None)
        if method is not None:
            return method(node, input_vars)
        # Fallback for unknown node types
        return self._generate_fallback(node, input_vars)

    def _generate_fallback(self, node: IRNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# TODO: Unsupported node type '{type(node).__name__}' (tool: {node.original_tool_type})",
            "# Manual conversion required.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"No generator for node type {type(node).__name__} (node {node.node_id})"],
        )

    # -- IO nodes -----------------------------------------------------------

    def _generate_ReadNode(self, node: ReadNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        fmt = self._map_file_format(node.file_format)
        path = node.file_path or node.table_name or "UNKNOWN_PATH"
        warnings: list[str] = []

        options: list[str] = []
        if fmt == "csv":
            options.append(f'"header", "{str(node.has_header).lower()}"')
            if node.delimiter and node.delimiter != ",":
                options.append(f'"delimiter", "{node.delimiter}"')
            if node.encoding and node.encoding != "utf-8":
                options.append(f'"encoding", "{node.encoding}"')

        if node.source_type == "database" and node.query:
            lines = []
            if node.connection_string:
                lines.append(f"# Source database: {node.connection_string}")
                lines.append("# TODO: Map the connection to a Unity Catalog table or external connection.")
                warnings.append(f"Input node {node.node_id}: database connection '{node.connection_string}' needs manual mapping")
            lines.append(f'{out_var} = spark.sql("""{node.query}""")')
        elif node.source_type == "database" and node.table_name:
            lines = [f'{out_var} = spark.table("{node.table_name}")']
        else:
            option_chain = ""
            if options:
                opt_parts = ", ".join(f".option({o})" for o in options)
                option_chain = opt_parts
            lines = [f'{out_var} = spark.read.format("{fmt}"){option_chain}.load("{path}")']

        if node.record_limit is not None:
            lines.append(f"{out_var} = {out_var}.limit({node.record_limit})")

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_WriteNode(self, node: WriteNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        fmt = self._map_file_format(node.file_format)
        mode = node.write_mode or "overwrite"
        warnings: list[str] = []

        if node.destination_type == "database" and node.table_name:
            lines = [f'{inp}.write.mode("{mode}").saveAsTable("{node.table_name}")']
        else:
            path = node.file_path or "UNKNOWN_PATH"
            # Unsupported output formats get a comment + Delta fallback
            if fmt in ("hyper", "yxdb", "unknown"):
                catalog = self.config.catalog_name
                schema = self.config.schema_name
                table = f"output_{node.node_id}"
                lines = [
                    f"# Original output: {path}",
                    f"# Format '{fmt}' is not natively supported in Databricks.",
                    f"# Writing as Delta table instead. Export to {fmt} if needed.",
                    f'{inp}.write.mode("{mode}").saveAsTable("{catalog}.{schema}.{table}")',
                ]
                warnings.append(f"Output node {node.node_id}: '{fmt}' format replaced with Delta table")
            else:
                lines = [f'{inp}.write.format("{fmt}").mode("{mode}").save("{path}")']

        return NodeCodeResult(code_lines=lines, output_vars={}, warnings=warnings)

    def _generate_LiteralDataNode(self, node: LiteralDataNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"

        rows_repr = repr(node.data_rows) if node.data_rows else "[]"
        schema_repr = repr(node.field_names) if node.field_names else "[]"

        lines = [f"{out_var} = spark.createDataFrame({rows_repr}, schema={schema_repr})"]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_BrowseNode(self, node: BrowseNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        lines = [
            "# Browse tool -- display data preview",
            f"display({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": inp},
        )

    # -- Preparation nodes --------------------------------------------------

    def _generate_FilterNode(self, node: FilterNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_true = f"df_{node.node_id}_true"
        out_false = f"df_{node.node_id}_false"
        warnings: list[str] = []

        try:
            expr = self._translator.translate_string(node.expression)
            warnings.extend(self._translator.warnings)
        except BaseTranslationError as exc:
            expr = f'F.expr("{node.expression}")'
            warnings.append(f"Filter expression fallback for node {node.node_id}: {exc}")

        lines = [
            f"_filter_cond_{node.node_id} = {expr}",
            f"{out_true} = {inp}.filter(_filter_cond_{node.node_id})",
            f"{out_false} = {inp}.filter(~(_filter_cond_{node.node_id}))",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={
                "True": out_true,
                "False": out_false,
                "Output": out_true,
            },
            warnings=warnings,
        )

    def _generate_FormulaNode(self, node: FormulaNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"{out_var} = {inp}"]
        warnings: list[str] = []

        for formula in node.formulas:
            try:
                expr = self._translator.translate_string(formula.expression)
                warnings.extend(self._translator.warnings)
            except BaseTranslationError as exc:
                expr = f'F.expr("{formula.expression}")'
                warnings.append(f"Formula expression fallback for '{formula.output_field}': {exc}")
            lines.append(f'{out_var} = {out_var}.withColumn("{formula.output_field}", {expr})')

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_SelectNode(self, node: SelectNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"{out_var} = {inp}"]

        renames: list[tuple[str, str]] = []
        drops: list[str] = []

        for op in node.field_operations:
            if not op.selected or op.action == FieldAction.DESELECT:
                drops.append(op.field_name)
            elif op.action == FieldAction.RENAME and op.rename_to:
                renames.append((op.field_name, op.rename_to))

        for old, new in renames:
            lines.append(f'{out_var} = {out_var}.withColumnRenamed("{old}", "{new}")')

        if drops:
            drop_args = ", ".join(f'"{d}"' for d in drops)
            lines.append(f"{out_var} = {out_var}.drop({drop_args})")

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_SortNode(self, node: SortNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        sort_exprs: list[str] = []
        for sf in node.sort_fields:
            direction = "asc" if sf.ascending else "desc"
            sort_exprs.append(f'F.col("{sf.field_name}").{direction}()')

        sort_str = ", ".join(sort_exprs) if sort_exprs else ""
        lines = [f"{out_var} = {inp}.orderBy({sort_str})"]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_SampleNode(self, node: SampleNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        if node.sample_method in ("first",) and node.n_records is not None:
            lines = [f"{out_var} = {inp}.limit({node.n_records})"]
        elif node.sample_method == "percent" and node.percentage is not None:
            frac = node.percentage / 100.0 if node.percentage > 1 else node.percentage
            lines = [f"{out_var} = {inp}.sample(fraction={frac})"]
        elif node.sample_method == "random" and node.n_records is not None:
            lines = [
                f"# Random sample of {node.n_records} records",
                f"_frac_{node.node_id} = min(1.0, {node.n_records} * 2 / max(1, {inp}.count()))",
                f"{out_var} = {inp}.sample(fraction=_frac_{node.node_id}).limit({node.n_records})",
            ]
        else:
            lines = [
                f"# Sample method: {node.sample_method}",
                f"{out_var} = {inp}.limit({node.n_records or 100})",
            ]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_UniqueNode(self, node: UniqueNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_unique = f"df_{node.node_id}_unique"
        out_dup = f"df_{node.node_id}_duplicate"

        keys_repr = repr(node.key_fields) if node.key_fields else "[]"

        lines = [
            f"{out_unique} = {inp}.dropDuplicates({keys_repr})",
            f"{out_dup} = {inp}.subtract({out_unique})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={
                "Unique": out_unique,
                "Duplicate": out_dup,
                "Output": out_unique,
            },
        )

    def _generate_RecordIDNode(self, node: RecordIDNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        start = node.starting_value

        lines = [f'{out_var} = {inp}.withColumn("{node.output_field}", F.monotonically_increasing_id() + {start})']
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_MultiRowFormulaNode(self, node: MultiRowFormulaNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        warnings: list[str] = []

        # Build window spec
        if node.group_fields:
            partition = ", ".join(f'"{gf}"' for gf in node.group_fields)
            window_def = f"Window.partitionBy({partition}).orderBy(F.monotonically_increasing_id())"
        else:
            window_def = "Window.orderBy(F.monotonically_increasing_id())"

        try:
            expr = self._translator.translate_string(node.expression)
            warnings.extend(self._translator.warnings)
        except BaseTranslationError as exc:
            expr = f'F.expr("{node.expression}")'
            warnings.append(f"MultiRowFormula expression fallback: {exc}")

        # Replace the placeholder 'window' with actual window variable
        expr = expr.replace(".over(window)", f".over(_window_{node.node_id})")

        lines = [
            f"_window_{node.node_id} = {window_def}",
            f'{out_var} = {inp}.withColumn("{node.output_field}", {expr})',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_MultiFieldFormulaNode(
        self, node: MultiFieldFormulaNode, input_vars: dict[str, str]
    ) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        warnings: list[str] = []
        lines = [f"{out_var} = {inp}"]

        for fld in node.fields:
            try:
                expr_str = node.expression.replace("[_CurrentField_]", f"[{fld}]")
                expr = self._translator.translate_string(expr_str)
                warnings.extend(self._translator.warnings)
            except BaseTranslationError as exc:
                expr = f'F.col("{fld}")'
                warnings.append(f"MultiFieldFormula fallback for field '{fld}': {exc}")

            output_name = f"{fld}_out" if node.copy_output else fld
            lines.append(f'{out_var} = {out_var}.withColumn("{output_name}", {expr})')

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_DataCleansingNode(self, node: DataCleansingNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"{out_var} = {inp}"]

        for fld in node.fields:
            if node.trim_whitespace:
                lines.append(f'{out_var} = {out_var}.withColumn("{fld}", F.trim(F.col("{fld}")))')
            if node.remove_null and node.replace_nulls_with is not None:
                lines.append(f'{out_var} = {out_var}.na.fill({{"{fld}": "{node.replace_nulls_with}"}})')
            elif node.remove_null:
                lines.append(f'{out_var} = {out_var}.na.fill({{"{fld}": ""}})')
            if node.modify_case == "upper":
                lines.append(f'{out_var} = {out_var}.withColumn("{fld}", F.upper(F.col("{fld}")))')
            elif node.modify_case == "lower":
                lines.append(f'{out_var} = {out_var}.withColumn("{fld}", F.lower(F.col("{fld}")))')
            elif node.modify_case == "title":
                lines.append(f'{out_var} = {out_var}.withColumn("{fld}", F.initcap(F.col("{fld}")))')
            if node.remove_tabs:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{fld}", F.regexp_replace(F.col("{fld}"), "\\\\t", ""))'
                )
            if node.remove_line_breaks:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{fld}", F.regexp_replace(F.col("{fld}"), "[\\\\r\\\\n]+", ""))'
                )
            if node.remove_duplicate_whitespace:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{fld}", F.regexp_replace(F.col("{fld}"), "\\\\s+", " "))'
                )

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_AutoFieldNode(self, node: AutoFieldNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            "# AutoField: automatic type sizing (no-op in Spark)",
            f"{out_var} = {inp}",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_GenerateRowsNode(self, node: GenerateRowsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        warnings: list[str] = []
        lines = []

        # Try to parse init/condition/loop as simple counter pattern
        # Common pattern: i=1, i<=N, i=i+1
        init_expr = node.init_expression.strip()
        cond_expr = node.condition_expression.strip()
        loop_expr = node.loop_expression.strip()

        lines.append(f"# GenerateRows: init='{init_expr}', cond='{cond_expr}', loop='{loop_expr}'")

        # Attempt to detect simple range pattern
        range_match = re.search(r'(\w+)\s*<=?\s*(\d+)', cond_expr)
        init_match = re.search(r'(\w+)\s*=\s*(\d+)', init_expr)

        if init_match and range_match:
            start_val = int(init_match.group(2))
            end_val = int(range_match.group(2))
            if '<=' in cond_expr:
                end_val += 1
            output_field = node.output_field or init_match.group(1)
            lines.append(
                f'{out_var} = spark.range({start_val}, {end_val})'
                f'.withColumnRenamed("id", "{output_field}")'
            )
        else:
            # Fallback: generate a UDF-based approach with manual guidance
            output_field = node.output_field or "GeneratedRow"
            lines.append("# Complex row generation - using spark.range with expression application")
            lines.append(f'{out_var} = spark.range(0, 1000).withColumnRenamed("id", "{output_field}")')
            if cond_expr:
                try:
                    expr = self._translator.translate_string(cond_expr.replace(node.output_field or "i", f"[{output_field}]"))
                    lines.append(f"{out_var} = {out_var}.filter({expr})")
                except BaseTranslationError:
                    lines.append(f'# Apply condition filter: {cond_expr}')
                    warnings.append(f"GenerateRows (node {node.node_id}): complex condition may need manual adjustment")
            if loop_expr and loop_expr != f"{output_field}+1" and loop_expr != f"{output_field} + 1":
                warnings.append(f"GenerateRows (node {node.node_id}): loop expression '{loop_expr}' may need manual adjustment")

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    # -- Join nodes ---------------------------------------------------------

    def _generate_JoinNode(self, node: JoinNode, input_vars: dict[str, str]) -> NodeCodeResult:
        left_var = input_vars.get("Left", input_vars.get("Input", "MISSING_LEFT"))
        right_var = input_vars.get("Right", "MISSING_RIGHT")
        out_join = f"df_{node.node_id}_join"
        out_left = f"df_{node.node_id}_left"
        out_right = f"df_{node.node_id}_right"

        join_type = node.join_type or "inner"

        if node.join_keys:
            key_pairs = [f'{left_var}["{jk.left_field}"] == {right_var}["{jk.right_field}"]' for jk in node.join_keys]
            condition = " & ".join(f"({kp})" for kp in key_pairs)
            if len(key_pairs) == 1:
                condition = key_pairs[0]
        else:
            condition = "F.lit(True)"

        lines = [
            f'{out_join} = {left_var}.join({right_var}, {condition}, "{join_type}")',
            "# Left unmatched (anti join)",
            f'{out_left} = {left_var}.join({right_var}, {condition}, "left_anti")',
            "# Right unmatched (anti join)",
            f'{out_right} = {right_var}.join({left_var}, {condition}, "left_anti")',
        ]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={
                "Join": out_join,
                "Left": out_left,
                "Right": out_right,
                "Output": out_join,
            },
        )

    def _generate_UnionNode(self, node: UnionNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        sorted_vars = list(input_vars.values())

        if len(sorted_vars) == 0:
            lines = [f"{out_var} = spark.createDataFrame([], schema=[])  # empty union"]
        elif len(sorted_vars) == 1:
            lines = [f"{out_var} = {sorted_vars[0]}"]
        else:
            allow_missing = "True" if node.allow_missing else "False"
            parts = [sorted_vars[0]]
            for sv in sorted_vars[1:]:
                parts.append(f".unionByName({sv}, allowMissingColumns={allow_missing})")
            lines = [f"{out_var} = " + "".join(parts)]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_AppendFieldsNode(self, node: AppendFieldsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        target = input_vars.get("Target", input_vars.get("Input", "MISSING_TARGET"))
        source = input_vars.get("Source", "MISSING_SOURCE")
        out_var = f"df_{node.node_id}"

        lines = [
            "# AppendFields: cross join (target x source)",
            f"{out_var} = {target}.crossJoin({source})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_FindReplaceNode(self, node: FindReplaceNode, input_vars: dict[str, str]) -> NodeCodeResult:
        target_var = input_vars.get("Input", input_vars.get("Target", "MISSING_TARGET"))
        lookup_var = input_vars.get("Source", input_vars.get("Right", target_var))
        out_var = f"df_{node.node_id}"
        warnings: list[str] = []

        find_field = node.find_field or "find_field"
        replace_field = node.replace_field or "replace_field"

        lines = [f"# FindReplace: find '{find_field}', replace with '{replace_field}' (mode: {node.find_mode})"]

        if node.find_mode == "exact":
            if not node.case_sensitive:
                lines.append(f'_lookup_{node.node_id} = {lookup_var}.withColumn("_find_key", F.lower(F.col("{find_field}")))')
                lines.append(f'_main_{node.node_id} = {target_var}.withColumn("_find_key", F.lower(F.col("{find_field}")))')
                join_cond = f'_main_{node.node_id}["_find_key"] == _lookup_{node.node_id}["_find_key"]'
                lines.append(f'{out_var} = _main_{node.node_id}.join(_lookup_{node.node_id}.select("_find_key", F.col("{replace_field}").alias("_replace_val_{node.node_id}")), {join_cond}, "left")')
                lines.append(f'{out_var} = {out_var}.withColumn("{find_field}", F.coalesce(F.col("_replace_val_{node.node_id}"), F.col("{find_field}")))')
                lines.append(f'{out_var} = {out_var}.drop("_find_key", "_replace_val_{node.node_id}")')
            else:
                lines.append(f'_lookup_{node.node_id} = {lookup_var}.select(F.col("{find_field}").alias("_find_val"), F.col("{replace_field}").alias("_replace_val_{node.node_id}"))')
                lines.append(f'{out_var} = {target_var}.join(_lookup_{node.node_id}, {target_var}["{find_field}"] == _lookup_{node.node_id}["_find_val"], "left")')
                lines.append(f'{out_var} = {out_var}.withColumn("{find_field}", F.coalesce(F.col("_replace_val_{node.node_id}"), F.col("{find_field}")))')
                lines.append(f'{out_var} = {out_var}.drop("_find_val", "_replace_val_{node.node_id}")')
        elif node.find_mode == "contains":
            lines.append("# Contains-mode find/replace using regexp_replace")
            lines.append(f'{out_var} = {target_var}')
            lines.append("# Note: For contains mode with lookup table, iterate over lookup values")
            warnings.append(f"FindReplace contains mode (node {node.node_id}) may need manual adjustment for lookup-based replace")
        elif node.find_mode == "regex":
            lines.append("# Regex-mode find/replace")
            lines.append(f'{out_var} = {target_var}')
            lines.append("# Note: For regex mode with lookup table, iterate over lookup patterns")
            warnings.append(f"FindReplace regex mode (node {node.node_id}) may need manual adjustment")
        else:
            lines.append(f'{out_var} = {target_var}')
            warnings.append(f"FindReplace mode '{node.find_mode}' (node {node.node_id}) may need manual adjustment")

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_JoinMultipleNode(self, node: JoinMultipleNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        sorted_inputs = list(input_vars.items())

        if len(sorted_inputs) < 2:
            var = sorted_inputs[0][1] if sorted_inputs else "MISSING_INPUT"
            lines = [f"{out_var} = {var}"]
        else:
            join_type = node.join_type or "inner"
            lines = [f"{out_var} = {sorted_inputs[0][1]}"]

            for i, (anchor, var_name) in enumerate(sorted_inputs[1:], start=1):
                if node.join_keys:
                    # Use key fields - handle potential column name conflicts with aliases
                    key_parts = [f'{out_var}["{jk.left_field}"] == {var_name}["{jk.right_field}"]' for jk in node.join_keys]
                    condition = " & ".join(f"({kp})" for kp in key_parts)
                    if len(key_parts) == 1:
                        condition = key_parts[0]
                    lines.append(f'{out_var} = {out_var}.join({var_name}, {condition}, "{join_type}")')
                else:
                    # No explicit keys - join on common column names
                    lines.append(f"# Join input #{i + 1} (no explicit keys - joining on common columns)")
                    lines.append(f'{out_var} = {out_var}.join({var_name}, "{join_type}")')

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    # -- Parse nodes --------------------------------------------------------

    def _generate_RegExNode(self, node: RegExNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        if node.mode == "replace":
            lines = [
                f'{out_var} = {inp}.withColumn("{node.field_name}", '
                f'F.regexp_replace(F.col("{node.field_name}"), "{node.expression}", "{node.replacement}"))'
            ]
        elif node.mode == "parse":
            lines = [f"{out_var} = {inp}"]
            for idx, out_field in enumerate(node.output_fields):
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{out_field}", '
                    f'F.regexp_extract(F.col("{node.field_name}"), "{node.expression}", {idx + 1}))'
                )
        elif node.mode == "match":
            lines = [
                f'{out_var} = {inp}.withColumn("_regex_match_{node.node_id}", '
                f'F.col("{node.field_name}").rlike("{node.expression}"))',
            ]
        else:
            lines = [
                f"# RegEx mode '{node.mode}' for field '{node.field_name}'",
                "# TODO: Manual conversion required.",
                f"{out_var} = {inp}",
            ]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_TextToColumnsNode(self, node: TextToColumnsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        root = node.output_root_name or node.field_name

        if node.split_to == "rows":
            lines = [
                f'{out_var} = {inp}.withColumn("{root}", F.explode(F.split(F.col("{node.field_name}"), "{node.delimiter}")))',
            ]
        else:
            lines = [
                f'_split_{node.node_id} = F.split(F.col("{node.field_name}"), "{node.delimiter}")',
                f'{out_var} = {inp}.withColumn("_split_arr_{node.node_id}", _split_{node.node_id})',
            ]
            num = node.num_columns or 5
            for i in range(num):
                col_name = f"{root}_{i + 1}"
                lines.append(f'{out_var} = {out_var}.withColumn("{col_name}", F.col("_split_arr_{node.node_id}")[{i}])')
            lines.append(f'{out_var} = {out_var}.drop("_split_arr_{node.node_id}")')

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_DateTimeNode(self, node: DateTimeNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        out_field = node.output_field or f"{node.input_field}_converted"

        if node.conversion_mode == "parse":
            fmt = node.format_string or "yyyy-MM-dd"
            lines = [f'{out_var} = {inp}.withColumn("{out_field}", F.to_date(F.col("{node.input_field}"), "{fmt}"))']
        elif node.conversion_mode == "format":
            fmt = node.format_string or "yyyy-MM-dd"
            lines = [
                f'{out_var} = {inp}.withColumn("{out_field}", F.date_format(F.col("{node.input_field}"), "{fmt}"))'
            ]
        else:
            lines = [
                f"# DateTime conversion mode: {node.conversion_mode}",
                f'{out_var} = {inp}.withColumn("{out_field}", F.col("{node.input_field}"))',
            ]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_JsonParseNode(self, node: JsonParseNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        out_field = node.output_field or f"{node.input_field}_parsed"

        lines = [
            f"# JSON parse from field '{node.input_field}' (flatten: {node.flatten_mode})",
            f'{out_var} = {inp}.withColumn("{out_field}", F.get_json_object(F.col("{node.input_field}"), "$"))',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    # -- Transform nodes ----------------------------------------------------

    def _generate_SummarizeNode(self, node: SummarizeNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        group_by_fields: list[str] = []
        agg_exprs: list[str] = []

        agg_map = {
            AggAction.SUM: "F.sum",
            AggAction.COUNT: "F.count",
            AggAction.COUNT_DISTINCT: "F.countDistinct",
            AggAction.MIN: "F.min",
            AggAction.MAX: "F.max",
            AggAction.AVG: "F.avg",
            AggAction.FIRST: "F.first",
            AggAction.LAST: "F.last",
            AggAction.STD_DEV: "F.stddev",
            AggAction.VARIANCE: "F.variance",
        }

        for agg in node.aggregations:
            if agg.action == AggAction.GROUP_BY:
                group_by_fields.append(f'"{agg.field_name}"')
            elif agg.action in agg_map:
                func = agg_map[agg.action]
                alias = agg.output_field_name or f"{agg.action.value}_{agg.field_name}"
                agg_exprs.append(f'{func}("{agg.field_name}").alias("{alias}")')
            elif agg.action == AggAction.CONCAT:
                alias = agg.output_field_name or f"Concat_{agg.field_name}"
                sep = agg.separator or ","
                agg_exprs.append(f'F.concat_ws("{sep}", F.collect_list("{agg.field_name}")).alias("{alias}")')
            elif agg.action == AggAction.COUNT_NON_NULL:
                alias = agg.output_field_name or f"CountNonNull_{agg.field_name}"
                agg_exprs.append(f'F.count(F.when(F.col("{agg.field_name}").isNotNull(), 1)).alias("{alias}")')
            elif agg.action == AggAction.COUNT_NULL:
                alias = agg.output_field_name or f"CountNull_{agg.field_name}"
                agg_exprs.append(f'F.count(F.when(F.col("{agg.field_name}").isNull(), 1)).alias("{alias}")')
            else:
                alias = agg.output_field_name or f"{agg.action.value}_{agg.field_name}"
                agg_exprs.append(f'F.expr("{agg.action.value.lower()}(`{agg.field_name}`)").alias("{alias}")')

        gb = ", ".join(group_by_fields)
        aggs = ", ".join(agg_exprs) if agg_exprs else 'F.count("*").alias("count")'

        if group_by_fields:
            lines = [f"{out_var} = {inp}.groupBy({gb}).agg({aggs})"]
        else:
            lines = [f"{out_var} = {inp}.agg({aggs})"]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_CrossTabNode(self, node: CrossTabNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        group_cols = ", ".join(f'"{gf}"' for gf in node.group_fields) if node.group_fields else ""
        agg_func = node.aggregation.lower() if node.aggregation else "sum"

        lines = [
            f'{out_var} = {inp}.groupBy({group_cols}).pivot("{node.header_field}").agg(F.{agg_func}("{node.value_field}"))',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_TransposeNode(self, node: TransposeNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        key_cols = ", ".join(f'"`{kf}`"' for kf in node.key_fields) if node.key_fields else ""
        data_fields = node.data_fields if node.data_fields else []

        if data_fields:
            cols_str = ", ".join(f"'{df}', `{df}`" for df in data_fields)
            lines = [
                "# Transpose (unpivot): data fields -> rows",
                f"{out_var} = {inp}.selectExpr({key_cols + ', ' if key_cols else ''}"
                f'"stack({len(data_fields)}, {cols_str}) as (`{node.header_name}`, `{node.value_name}`)")',
            ]
        else:
            lines = [
                "# Transpose: TODO - determine data columns at runtime",
                f"{out_var} = {inp}  # placeholder for transpose",
            ]

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_RunningTotalNode(self, node: RunningTotalNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        if node.group_fields:
            partition = ", ".join(f'"{gf}"' for gf in node.group_fields)
            window_def = (
                f"Window.partitionBy({partition})"
                f".orderBy(F.monotonically_increasing_id())"
                f".rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            )
        else:
            window_def = (
                "Window.orderBy(F.monotonically_increasing_id())"
                ".rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            )

        lines = [f"_window_{node.node_id} = {window_def}", f"{out_var} = {inp}"]

        func_map = {"Sum": "F.sum", "Avg": "F.avg", "Count": "F.count", "Min": "F.min", "Max": "F.max"}

        for rf in node.running_fields:
            func = func_map.get(rf.running_type, "F.sum")
            alias = rf.output_field_name or f"Running{rf.running_type}_{rf.field_name}"
            lines.append(
                f'{out_var} = {out_var}.withColumn("{alias}", {func}("{rf.field_name}").over(_window_{node.node_id}))'
            )

        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_CountRecordsNode(self, node: CountRecordsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f'{out_var} = spark.createDataFrame([({inp}.count(),)], ["{node.output_field}"])']
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    # -- Developer nodes ----------------------------------------------------

    def _generate_PythonToolNode(self, node: PythonToolNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"

        code_lines_raw = node.code.split("\n") if node.code else ["# (no code)"]
        commented = [f"#   {line}" for line in code_lines_raw]

        lines = [
            f"# ---- PythonTool (node {node.node_id}) ----",
            "# MANUAL REVIEW REQUIRED: Original Alteryx Python code below",
            *commented,
            "# ---- End PythonTool ----",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"PythonTool (node {node.node_id}) requires manual review"],
        )

    def _generate_DownloadNode(self, node: DownloadNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        url = node.url_static or f"<from field: {node.url_field}>"
        lines = [
            f"# DownloadTool: {node.method} {url}",
            "# TODO: Replace with requests/urllib UDF or Databricks external access.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"DownloadTool (node {node.node_id}) requires manual conversion"],
        )

    def _generate_RunCommandNode(self, node: RunCommandNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# RunCommand: '{node.command} {node.command_arguments}'",
            "# TODO: Replace with subprocess or %sh magic in Databricks.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"RunCommand (node {node.node_id}) requires manual conversion"],
        )

    # -- New tool nodes -----------------------------------------------------

    def _generate_ImputationNode(self, node: ImputationNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"# Imputation: fill missing values (method: {node.method})"]
        lines.append(f"{out_var} = {inp}")
        if node.method == "custom" and node.custom_value is not None:
            fill_dict = ", ".join(f'"{f}": "{node.custom_value}"' for f in node.fields)
            lines.append(f"{out_var} = {out_var}.na.fill({{{fill_dict}}})")
        elif node.method in ("mean", "avg"):
            for fld in node.fields:
                lines.append(f'_mean_{fld}_{node.node_id} = {out_var}.agg(F.avg("{fld}")).first()[0]')
                lines.append(f'{out_var} = {out_var}.na.fill({{"{fld}": _mean_{fld}_{node.node_id}}})')
        elif node.method == "median":
            for fld in node.fields:
                lines.append(f'_median_{fld}_{node.node_id} = {out_var}.approxQuantile("{fld}", [0.5], 0.001)[0]')
                lines.append(f'{out_var} = {out_var}.na.fill({{"{fld}": _median_{fld}_{node.node_id}}})')
        elif node.method == "mode":
            for fld in node.fields:
                lines.append(f'_mode_{fld}_{node.node_id} = {out_var}.groupBy("{fld}").count().orderBy(F.desc("count")).first()[0]')
                lines.append(f'{out_var} = {out_var}.na.fill({{"{fld}": _mode_{fld}_{node.node_id}}})')
        else:
            lines.append(f'{out_var} = {out_var}.na.fill("")')
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_XMLParseNode(self, node: XMLParseNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"# XMLParse: extract XPath values from '{node.input_field}'"]
        lines.append(f"{out_var} = {inp}")
        if node.xpath_expressions:
            for xpath, name in node.xpath_expressions:
                lines.append(f'{out_var} = {out_var}.withColumn("{name}", F.xpath_string(F.col("{node.input_field}"), F.lit("{xpath}")))')
        elif node.output_field:
            lines.append(f'{out_var} = {out_var}.withColumn("{node.output_field}", F.col("{node.input_field}"))')
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"XMLParse (node {node.node_id}): verify xpath_string function availability"] if node.xpath_expressions else [],
        )

    def _generate_TileNode(self, node: TileNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        imports = ["from pyspark.sql import Window"]
        order = f'"{node.order_field}"' if node.order_field else f'"{node.tile_field}"' if node.tile_field else "F.monotonically_increasing_id()"
        if node.group_fields:
            partition = ", ".join(f'"{gf}"' for gf in node.group_fields)
            window = f"Window.partitionBy({partition}).orderBy({order})"
        else:
            window = f"Window.orderBy({order})"
        lines = [
            f"# Tile: assign rows to {node.tile_count} equal-sized groups",
            f'{out_var} = {inp}.withColumn("{node.output_field}", F.ntile({node.tile_count}).over({window}))',
        ]
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var}, imports=imports)

    def _generate_WeightedAverageNode(self, node: WeightedAverageNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [f"# WeightedAverage: sum({node.value_field} * {node.weight_field}) / sum({node.weight_field})"]
        if node.group_fields:
            gb = ", ".join(f'"{gf}"' for gf in node.group_fields)
            lines.append(
                f'{out_var} = {inp}.groupBy({gb}).agg('
                f'(F.sum(F.col("{node.value_field}") * F.col("{node.weight_field}")) / F.sum(F.col("{node.weight_field}"))).alias("{node.output_field}")'
                f')'
            )
        else:
            lines.append(
                f'{out_var} = {inp}.agg('
                f'(F.sum(F.col("{node.value_field}") * F.col("{node.weight_field}")) / F.sum(F.col("{node.weight_field}"))).alias("{node.output_field}")'
                f')'
            )
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_DynamicInputNode(self, node: DynamicInputNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        fmt = self._map_file_format(node.file_format)
        pattern = node.file_path_pattern or "*.csv"
        lines = [
            "# DynamicInput: read multiple files matching pattern",
            f'{out_var} = spark.read.format("{fmt}").option("header", "true").load("{pattern}")',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"DynamicInput (node {node.node_id}): adjust file path pattern for Databricks"],
        )

    def _generate_DynamicOutputNode(self, node: DynamicOutputNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        fmt = self._map_file_format(node.file_format)
        path = node.file_path_expression or "dynamic_output"
        lines = ["# DynamicOutput: write to partitioned destination"]
        if node.partition_field:
            lines.append(f'{inp}.write.format("{fmt}").partitionBy("{node.partition_field}").mode("overwrite").save("{path}")')
        else:
            lines.append(f'{inp}.write.format("{fmt}").mode("overwrite").save("{path}")')
        lines.append(f"{out_var} = {inp}  # passthrough after write")
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"DynamicOutput (node {node.node_id}): adjust output path for Databricks"],
        )

    # -- Workflow / Interface / Reporting nodes ------------------------------

    def _generate_WorkflowControlNode(self, node: WorkflowControlNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars) if input_vars else None
        out_var = f"df_{node.node_id}"
        lines = [f"# {node.original_tool_type}: {node.control_type} - no Databricks equivalent"]
        if inp:
            lines.append(f"{out_var} = {inp}  # passthrough")
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var} if inp else {})

    def _generate_MacroIONode(self, node: MacroIONode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        if node.direction == "input":
            default = f', "{node.default_value}"' if node.default_value else ""
            lines = [
                f"# MacroInput: {node.field_name}",
                f'dbutils.widgets.text("{node.field_name}"{default})',
                f'{out_var}_param = dbutils.widgets.get("{node.field_name}")',
            ]
        else:
            inp = self._get_single_input(input_vars) if input_vars else None
            lines = [f"# MacroOutput: {node.field_name}"]
            if inp:
                lines.append(f"{out_var} = {inp}")
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var} if node.direction == "output" and input_vars else {})

    def _generate_FieldSummaryNode(self, node: FieldSummaryNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        if node.fields:
            cols = ", ".join(f'"{f}"' for f in node.fields)
            lines = [f'{out_var} = {inp}.select({cols}).describe()']
        else:
            lines = [f"{out_var} = {inp}.describe()"]
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_WidgetNode(self, node: WidgetNode, input_vars: dict[str, str]) -> NodeCodeResult:
        default = f', "{node.default_value}"' if node.default_value else ""
        if node.widget_type in ("dropdown", "listbox") and node.options:
            choices = ", ".join(f'"{o}"' for o in node.options)
            lines = [f'dbutils.widgets.dropdown("{node.field_name}", "{node.options[0] if node.options else ""}",  [{choices}])']
        elif node.widget_type == "checkbox":
            lines = [f'dbutils.widgets.dropdown("{node.field_name}", "False", ["True", "False"])']
        else:
            lines = [f'dbutils.widgets.text("{node.field_name}"{default})']
        lines.append(f'# Widget: {node.widget_type} - "{node.label or node.field_name}"')
        return NodeCodeResult(code_lines=lines, output_vars={})

    def _generate_CloudStorageNode(self, node: CloudStorageNode, input_vars: dict[str, str]) -> NodeCodeResult:
        out_var = f"df_{node.node_id}"
        fmt = self._map_file_format(node.file_format)
        if node.provider == "s3":
            path_prefix = "s3://"
        elif node.provider == "azure":
            path_prefix = "abfss://"
        else:
            path_prefix = ""
        full_path = f"{path_prefix}{node.bucket_or_container}/{node.path}" if node.bucket_or_container else node.path
        if node.direction == "input":
            lines = [f'{out_var} = spark.read.format("{fmt}").option("header", "true").load("{full_path}")']
            return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})
        else:
            inp = self._get_single_input(input_vars)
            lines = [f'{inp}.write.format("{fmt}").mode("overwrite").save("{full_path}")', f"{out_var} = {inp}"]
            return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_ChartNode(self, node: ChartNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        title = node.title or "Chart"
        lines = [
            f"# Chart: {node.chart_type or 'auto'} - {title}",
            f"# Use display({inp}) or convert to pandas for matplotlib/plotly",
            f"display({inp})",
            f"{out_var} = {inp}",
        ]
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_ReportNode(self, node: ReportNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# Report ({node.report_type}): {node.title or 'Untitled'}",
            f"display({inp})",
            f"{out_var} = {inp}",
        ]
        return NodeCodeResult(code_lines=lines, output_vars={"Output": out_var})

    def _generate_EmailOutputNode(self, node: EmailOutputNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            "# EmailOutput: use Databricks notifications or smtplib",
            f"# To: {node.to_field}, Subject: {node.subject_field}",
            "# Consider using dbutils.notebook.exit() with results or Databricks alerts",
            f"{out_var} = {inp}",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"EmailOutput (node {node.node_id}): manual conversion needed for email sending"],
        )

    # -- Spatial nodes ------------------------------------------------------

    def _generate_BufferNode(self, node: BufferNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            "# Buffer: create buffer zone using Databricks Mosaic library",
            "# Requires: import mosaic; mosaic.enable_mosaic(spark)",
            f'{out_var} = {inp}.withColumn("{node.input_field}_buffer", '
            f'F.expr("st_buffer({node.input_field}, {node.buffer_distance})"))',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"BufferNode (node {node.node_id}): requires Databricks Mosaic library (st_buffer)"],
        )

    def _generate_SpatialMatchNode(self, node: SpatialMatchNode, input_vars: dict[str, str]) -> NodeCodeResult:
        left_var = input_vars.get("Target", input_vars.get("Left", input_vars.get("Input", "MISSING_TARGET")))
        right_var = input_vars.get("Universe", input_vars.get("Right", "MISSING_UNIVERSE"))
        out_var = f"df_{node.node_id}"
        lines = [
            "# Spatial match using Mosaic st_intersects",
            "# Requires: import mosaic; mosaic.enable_mosaic(spark)",
            f'{out_var} = {left_var}.join({right_var}, '
            f'F.expr("st_intersects({left_var}.{node.spatial_field_target}, '
            f'{right_var}.{node.spatial_field_universe})"), "inner")',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"SpatialMatchNode (node {node.node_id}): requires Databricks Mosaic library (st_intersects)"],
        )

    def _generate_CreatePointsNode(self, node: CreatePointsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            "# CreatePoints: build point geometry from lat/lon",
            f'{out_var} = {inp}.withColumn("{node.output_field}", '
            f'F.struct(F.col("{node.lat_field}"), F.col("{node.lon_field}")))',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
        )

    def _generate_DistanceNode(self, node: DistanceNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        # Earth radius: 3959 miles, 6371 km
        radius = "3959" if node.distance_units == "miles" else "6371"
        lines = [
            f"# Distance: Haversine formula ({node.distance_units})",
            f"# Using source='{node.source_field}' and target='{node.target_field}'",
            f"_lat1_{node.node_id} = F.radians(F.col(\"{node.source_field}.lat\"))",
            f"_lon1_{node.node_id} = F.radians(F.col(\"{node.source_field}.lon\"))",
            f"_lat2_{node.node_id} = F.radians(F.col(\"{node.target_field}.lat\"))",
            f"_lon2_{node.node_id} = F.radians(F.col(\"{node.target_field}.lon\"))",
            f"_distance_expr_{node.node_id} = F.acos(",
            f"    F.sin(_lat1_{node.node_id}) * F.sin(_lat2_{node.node_id}) +",
            f"    F.cos(_lat1_{node.node_id}) * F.cos(_lat2_{node.node_id}) *",
            f"    F.cos(_lon2_{node.node_id} - _lon1_{node.node_id})",
            f") * {radius}",
            f'{out_var} = {inp}.withColumn("{node.output_field}", _distance_expr_{node.node_id})',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"DistanceNode (node {node.node_id}): verify lat/lon struct field names match your schema"],
        )

    def _generate_FindNearestNode(self, node: FindNearestNode, input_vars: dict[str, str]) -> NodeCodeResult:
        target_var = input_vars.get("Target", input_vars.get("Left", input_vars.get("Input", "MISSING_TARGET")))
        universe_var = input_vars.get("Universe", input_vars.get("Right", "MISSING_UNIVERSE"))
        out_var = f"df_{node.node_id}"
        radius = "3959" if node.distance_units == "miles" else "6371"
        lines = [
            f"# FindNearest: cross join + Haversine distance + rank (max_matches={node.max_matches})",
            f"_cross_{node.node_id} = {target_var}.crossJoin({universe_var})",
            "# Compute Haversine distance between target and universe points",
            f"_lat1_{node.node_id} = F.radians(F.col(\"{node.target_field}.lat\"))",
            f"_lon1_{node.node_id} = F.radians(F.col(\"{node.target_field}.lon\"))",
            f"_lat2_{node.node_id} = F.radians(F.col(\"{node.universe_field}.lat\"))",
            f"_lon2_{node.node_id} = F.radians(F.col(\"{node.universe_field}.lon\"))",
            f"_dist_expr_{node.node_id} = F.acos(",
            f"    F.sin(_lat1_{node.node_id}) * F.sin(_lat2_{node.node_id}) +",
            f"    F.cos(_lat1_{node.node_id}) * F.cos(_lat2_{node.node_id}) *",
            f"    F.cos(_lon2_{node.node_id} - _lon1_{node.node_id})",
            f") * {radius}",
            f'_with_dist_{node.node_id} = _cross_{node.node_id}.withColumn("{node.output_distance_field}", _dist_expr_{node.node_id})',
            f"_window_{node.node_id} = Window.partitionBy({target_var}.columns[0]).orderBy(F.col(\"{node.output_distance_field}\"))",
            f'_ranked_{node.node_id} = _with_dist_{node.node_id}.withColumn("_rank_{node.node_id}", F.row_number().over(_window_{node.node_id}))',
            f'{out_var} = _ranked_{node.node_id}.filter(F.col("_rank_{node.node_id}") <= {node.max_matches}).drop("_rank_{node.node_id}")',
        ]
        warnings = [f"FindNearestNode (node {node.node_id}): verify partition key and lat/lon struct field names"]
        if node.max_distance is not None:
            lines.insert(-1, f'{out_var} = _with_dist_{node.node_id}.filter(F.col("{node.output_distance_field}") <= {node.max_distance})')
            warnings.append(f"FindNearestNode (node {node.node_id}): max_distance={node.max_distance} {node.distance_units} filter applied")
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=warnings,
        )

    def _generate_GeocoderNode(self, node: GeocoderNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        address_fields = [f for f in [node.address_field, node.city_field, node.state_field, node.zip_field, node.country_field] if f]
        lines = [
            "# Geocoder: convert address fields to coordinates",
            f"# Input fields: {', '.join(address_fields)}",
            "# TODO: Implement geocoding UDF using a geocoding service (e.g., Databricks Labs Mosaic, Google Maps API, HERE, etc.)",
            "# Example: register a pandas UDF that calls a geocoding API",
            f"{out_var} = {inp}  # passthrough placeholder",
            f'# {out_var} = {out_var}.withColumn("{node.output_lat_field}", <geocode_lat_udf>)',
            f'# {out_var} = {out_var}.withColumn("{node.output_lon_field}", <geocode_lon_udf>)',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"GeocoderNode (node {node.node_id}): requires manual implementation of geocoding UDF"],
        )

    def _generate_TradeAreaNode(self, node: TradeAreaNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# TradeArea: create trade area polygon (radius={node.radius} {node.radius_units}, rings={node.ring_count})",
            "# Requires: import mosaic; mosaic.enable_mosaic(spark)",
            f'{out_var} = {inp}.withColumn("{node.output_field}", '
            f'F.expr("st_buffer({node.input_field}, {node.radius})"))',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"TradeAreaNode (node {node.node_id}): requires Databricks Mosaic library (st_buffer); multi-ring trade areas need manual adjustment"],
        )

    def _generate_MakeGridNode(self, node: MakeGridNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        # Convert grid_size to an approximate H3 resolution
        # H3 resolutions: ~1km = res 7, ~5km = res 5, ~10km = res 4
        resolution = 7  # default
        lines = [
            f"# Grid generation using H3 polyfill (grid_size={node.grid_size} {node.grid_units})",
            "# Requires: h3-pyspark or Databricks Mosaic H3 functions",
            f'{out_var} = {inp}.selectExpr("explode(h3_polyfill({node.extent_field}, {resolution})) as {node.output_field}")',
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"MakeGridNode (node {node.node_id}): requires H3 library; adjust resolution ({resolution}) based on grid_size={node.grid_size} {node.grid_units}"],
        )

    # -- Predictive / ML nodes ----------------------------------------------

    def _generate_DecisionTreeNode(self, node: DecisionTreeNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        model_class = "DecisionTreeClassifier" if node.model_type == "classification" else "DecisionTreeRegressor"
        model_import = f"from pyspark.ml.classification import {model_class}" if node.model_type == "classification" else f"from pyspark.ml.regression import {model_class}"
        lines = [
            f"# DecisionTree ({node.model_type}): target='{node.target_field}', max_depth={node.max_depth}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_dt_{node.node_id} = {model_class}(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"maxDepth={node.max_depth}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _dt_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            imports={
                model_import,
                "from pyspark.ml.feature import VectorAssembler",
                "from pyspark.ml import Pipeline",
            },
            warnings=[f"DecisionTreeNode (node {node.node_id}): verify feature columns and label column types"],
        )

    def _generate_ForestModelNode(self, node: ForestModelNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        model_class = "RandomForestClassifier" if node.model_type == "classification" else "RandomForestRegressor"
        model_import = f"from pyspark.ml.classification import {model_class}" if node.model_type == "classification" else f"from pyspark.ml.regression import {model_class}"
        lines = [
            f"# RandomForest ({node.model_type}): target='{node.target_field}', trees={node.num_trees}, max_depth={node.max_depth}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_rf_{node.node_id} = {model_class}(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"numTrees={node.num_trees}, maxDepth={node.max_depth}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _rf_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            imports={
                model_import,
                "from pyspark.ml.feature import VectorAssembler",
                "from pyspark.ml import Pipeline",
            },
            warnings=[f"ForestModelNode (node {node.node_id}): verify feature columns and label column types"],
        )

    def _generate_LinearRegressionNode(self, node: LinearRegressionNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# LinearRegression: target='{node.target_field}', regularization={node.regularization}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_lr_{node.node_id} = LinearRegression(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"regParam={node.regularization}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _lr_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            imports={
                "from pyspark.ml.regression import LinearRegression",
                "from pyspark.ml.feature import VectorAssembler",
                "from pyspark.ml import Pipeline",
            },
            warnings=[f"LinearRegressionNode (node {node.node_id}): verify feature columns are numeric"],
        )

    def _generate_LogisticRegressionNode(self, node: LogisticRegressionNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# LogisticRegression: target='{node.target_field}', regularization={node.regularization}, maxIter={node.max_iterations}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_logr_{node.node_id} = LogisticRegression(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"regParam={node.regularization}, maxIter={node.max_iterations}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _logr_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            imports={
                "from pyspark.ml.classification import LogisticRegression",
                "from pyspark.ml.feature import VectorAssembler",
                "from pyspark.ml import Pipeline",
            },
            warnings=[f"LogisticRegressionNode (node {node.node_id}): verify label column is binary/categorical"],
        )

    def _generate_ScoreModelNode(self, node: ScoreModelNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        model_ref = node.model_reference or "UNKNOWN_MODEL_PATH"
        lines = [
            "# ScoreModel: load model and score data",
            f"# Model reference: {model_ref}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f"_prepared_{node.node_id} = _assembler_{node.node_id}.transform({inp})",
            "# TODO: Load the model from the appropriate path/registry",
            f'# _model_{node.node_id} = PipelineModel.load("{model_ref}")',
            f"# {out_var} = _model_{node.node_id}.transform(_prepared_{node.node_id})",
            f"{out_var} = _prepared_{node.node_id}  # passthrough placeholder until model is loaded",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            imports={
                "from pyspark.ml.feature import VectorAssembler",
                "from pyspark.ml import PipelineModel",
            },
            warnings=[f"ScoreModelNode (node {node.node_id}): requires manual model loading from '{model_ref}'"],
        )

    def _generate_BoostedModelNode(self, node: BoostedModelNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        model_class = "GBTClassifier" if node.model_type == "classification" else "GBTRegressor"
        model_import = f"from pyspark.ml.classification import {model_class}" if node.model_type == "classification" else f"from pyspark.ml.regression import {model_class}"
        lines = [
            f"# BoostedModel ({node.model_type}): target='{node.target_field}', iterations={node.num_iterations}, lr={node.learning_rate}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_gbt_{node.node_id} = {model_class}(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"maxIter={node.num_iterations}, maxDepth={node.max_depth}, stepSize={node.learning_rate}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _gbt_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={model_import, "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"BoostedModelNode (node {node.node_id}): verify feature columns and label column types"],
        )

    def _generate_NaiveBayesNode(self, node: NaiveBayesNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# NaiveBayes: target='{node.target_field}', smoothing={node.smoothing}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_nb_{node.node_id} = NaiveBayes(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"smoothing={node.smoothing}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _nb_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.classification import NaiveBayes", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"NaiveBayesNode (node {node.node_id}): features must be non-negative for multinomial model"],
        )

    def _generate_SupportVectorMachineNode(self, node: SupportVectorMachineNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# SVM: target='{node.target_field}', regularization={node.regularization}, maxIter={node.max_iterations}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_svc_{node.node_id} = LinearSVC(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"regParam={node.regularization}, maxIter={node.max_iterations}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _svc_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.classification import LinearSVC", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"SVMNode (node {node.node_id}): LinearSVC only supports linear kernel"],
        )

    def _generate_NeuralNetworkNode(self, node: NeuralNetworkNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        layers_repr = repr(node.hidden_layers)
        lines = [
            f"# NeuralNetwork: target='{node.target_field}', hidden_layers={layers_repr}, maxIter={node.max_iterations}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            "# NOTE: layers must be [input_size, ...hidden..., output_size]; adjust manually",
            f"_layers_{node.node_id} = [len({features_repr})] + {layers_repr} + [2]  # adjust output classes",
            f'_mlp_{node.node_id} = MultilayerPerceptronClassifier(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f"layers=_layers_{node.node_id}, maxIter={node.max_iterations}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _mlp_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.classification import MultilayerPerceptronClassifier", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"NeuralNetworkNode (node {node.node_id}): adjust layer sizes (input/output) manually"],
        )

    def _generate_GammaRegressionNode(self, node: GammaRegressionNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# GammaRegression: target='{node.target_field}', link='{node.link_function}'",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_glm_{node.node_id} = GeneralizedLinearRegression(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f'family="gamma", link="{node.link_function}", predictionCol="{node.output_field}")',
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _glm_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.regression import GeneralizedLinearRegression", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"GammaRegressionNode (node {node.node_id}): verify target values are positive"],
        )

    def _generate_CountRegressionNode(self, node: CountRegressionNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# CountRegression (Poisson): target='{node.target_field}', link='{node.link_function}'",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_glm_{node.node_id} = GeneralizedLinearRegression(featuresCol="features_{node.node_id}", labelCol="{node.target_field}", '
            f'family="poisson", link="{node.link_function}", predictionCol="{node.output_field}")',
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _glm_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.regression import GeneralizedLinearRegression", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"CountRegressionNode (node {node.node_id}): verify target values are non-negative integers"],
        )

    def _generate_SplineModelNode(self, node: SplineModelNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# SplineModel: target='{node.target_field}', max_knots={node.max_knots}",
            "# TODO: No direct Spark MLlib equivalent for spline regression.",
            "# Consider using polynomial features or a custom UDF.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"SplineModelNode (node {node.node_id}): requires manual implementation (no MLlib equivalent)"],
        )

    def _generate_StepwiseNode(self, node: StepwiseNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# Stepwise feature selection: target='{node.target_field}', direction='{node.direction}'",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_selector_{node.node_id} = ChiSqSelector(featuresCol="features_{node.node_id}", outputCol="selected_features_{node.node_id}", '
            f'labelCol="{node.target_field}", numTopFeatures=len({features_repr}))',
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _selector_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.feature import ChiSqSelector, VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"StepwiseNode (node {node.node_id}): ChiSqSelector approximates stepwise; direction='{node.direction}' not directly supported"],
        )

    def _generate_KCentroidsNode(self, node: KCentroidsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# KMeans clustering: k={node.k}, max_iterations={node.max_iterations}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_kmeans_{node.node_id} = KMeans(featuresCol="features_{node.node_id}", k={node.k}, '
            f"maxIter={node.max_iterations}, predictionCol=\"{node.output_field}\")",
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _kmeans_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.clustering import KMeans", "from pyspark.ml.feature import VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"KCentroidsNode (node {node.node_id}): verify k={node.k} is appropriate for your dataset"],
        )

    def _generate_PrincipalComponentsNode(self, node: PrincipalComponentsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        features_repr = repr(node.feature_fields) if node.feature_fields else '["feature1", "feature2"]'
        lines = [
            f"# PCA: num_components={node.num_components}",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols={features_repr}, outputCol=\"features_{node.node_id}\")",
            f'_pca_{node.node_id} = PCA(inputCol="features_{node.node_id}", outputCol="{node.output_field}", k={node.num_components})',
            f"_pipeline_{node.node_id} = Pipeline(stages=[_assembler_{node.node_id}, _pca_{node.node_id}])",
            f"_model_{node.node_id} = _pipeline_{node.node_id}.fit({inp})",
            f"{out_var} = _model_{node.node_id}.transform({inp})",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.feature import PCA, VectorAssembler", "from pyspark.ml import Pipeline"},
            warnings=[f"PCANode (node {node.node_id}): verify num_components={node.num_components} is appropriate"],
        )

    def _generate_CrossValidationNode(self, node: CrossValidationNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        model_ref = node.model_reference or "UNKNOWN_MODEL"
        lines = [
            f"# CrossValidation: num_folds={node.num_folds}, model='{model_ref}'",
            "# TODO: Configure CrossValidator with appropriate estimator and evaluator",
            f"# _cv_{node.node_id} = CrossValidator(estimator=..., evaluator=..., numFolds={node.num_folds})",
            f"# _cv_model_{node.node_id} = _cv_{node.node_id}.fit({inp})",
            f"{out_var} = {inp}  # passthrough placeholder until CrossValidator is configured",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.tuning import CrossValidator"},
            warnings=[f"CrossValidationNode (node {node.node_id}): requires manual estimator/evaluator configuration"],
        )

    def _generate_ARIMANode(self, node: ARIMANode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# ARIMA({node.p},{node.d},{node.q}): time='{node.time_field}', value='{node.value_field}'",
            "# TODO: No direct Spark MLlib equivalent for ARIMA.",
            "# Consider using Prophet or pandas UDF with statsmodels.",
            "# Example with pandas UDF:",
            '# @F.pandas_udf("double")',
            "# def arima_forecast(ts: pd.Series) -> pd.Series:",
            "#     from statsmodels.tsa.arima.model import ARIMA",
            f"#     model = ARIMA(ts, order=({node.p}, {node.d}, {node.q}))",
            "#     return pd.Series(model.fit().predict())",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"ARIMANode (node {node.node_id}): requires manual implementation with Prophet or statsmodels"],
        )

    def _generate_ETSNode(self, node: ETSNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# ETS: time='{node.time_field}', value='{node.value_field}', "
            f"error={node.error_type}, trend={node.trend_type}, seasonal={node.seasonal_type}",
            "# TODO: No direct Spark MLlib equivalent for ETS.",
            "# Consider using Prophet or pandas UDF with statsmodels.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"ETSNode (node {node.node_id}): requires manual implementation with Prophet or statsmodels"],
        )

    def _generate_AppendClusterNode(self, node: AppendClusterNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        feats = ", ".join(f'"{f}"' for f in node.feature_fields)
        lines = [
            "# AppendCluster: apply pre-trained clustering model",
            "from pyspark.ml.feature import VectorAssembler",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols=[{feats}], outputCol='features')",
            f"_assembled_{node.node_id} = _assembler_{node.node_id}.transform({inp})",
            f"# TODO: Load pre-trained KMeans model (model_reference='{node.model_reference}')",
            "# model = KMeansModel.load('...')",
            f"# {out_var} = model.transform(_assembled_{node.node_id})",
            f"{out_var} = _assembled_{node.node_id}  # passthrough until model loaded",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.feature import VectorAssembler"},
            warnings=[f"AppendClusterNode (node {node.node_id}): requires loading pre-trained model"],
        )

    def _generate_FindNearestNeighborsNode(self, node: FindNearestNeighborsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        feats = ", ".join(f'"{f}"' for f in node.feature_fields)
        lines = [
            f"# FindNearestNeighbors: k={node.k}, metric='{node.distance_metric}'",
            "from pyspark.ml.feature import VectorAssembler, BucketedRandomProjectionLSH",
            f"_assembler_{node.node_id} = VectorAssembler(inputCols=[{feats}], outputCol='features')",
            f"_assembled_{node.node_id} = _assembler_{node.node_id}.transform({inp})",
            f"_lsh_{node.node_id} = BucketedRandomProjectionLSH(inputCol='features', outputCol='hashes', bucketLength=2.0)",
            f"_lsh_model_{node.node_id} = _lsh_{node.node_id}.fit(_assembled_{node.node_id})",
            f"{out_var} = _assembled_{node.node_id}  # TODO: use approxSimilarityJoin for neighbor search",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.feature import VectorAssembler, BucketedRandomProjectionLSH"},
            warnings=[f"FindNearestNeighborsNode (node {node.node_id}): approximate NN via LSH"],
        )

    def _generate_KCentroidsDiagnosticsNode(self, node: KCentroidsDiagnosticsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        feats = ", ".join(f'"{f}"' for f in node.feature_fields)
        lines = [
            "# KCentroidsDiagnostics: silhouette score and cluster evaluation",
            "from pyspark.ml.evaluation import ClusteringEvaluator",
            "# TODO: Ensure 'prediction' and 'features' columns exist from upstream KMeans",
            f"_evaluator_{node.node_id} = ClusteringEvaluator()",
            f"# silhouette = _evaluator_{node.node_id}.evaluate({inp})",
            f"{out_var} = {inp}  # passthrough - diagnostics computed above",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            imports={"from pyspark.ml.evaluation import ClusteringEvaluator"},
            warnings=[f"KCentroidsDiagnosticsNode (node {node.node_id}): ensure upstream KMeans output has prediction column"],
        )

    def _generate_LiftChartNode(self, node: LiftChartNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# LiftChart: prediction='{node.prediction_field}', actual='{node.actual_field}'",
            "from pyspark.sql import functions as F, Window as W",
            f"_decile_{node.node_id} = F.ntile(10).over(W.orderBy(F.col('{node.prediction_field}').desc()))",
            f"{out_var} = {inp}.withColumn('decile', _decile_{node.node_id})",
            "# TODO: Group by decile and compute cumulative gain/lift",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"LiftChartNode (node {node.node_id}): lift/gain computation may need refinement"],
        )

    def _generate_ModelComparisonNode(self, node: ModelComparisonNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        refs = ", ".join(f"'{r}'" for r in node.model_references)
        lines = [
            f"# ModelComparison: models=[{refs}]",
            "# TODO: Load models from MLflow and compare metrics",
            "# Use mlflow.search_runs() to compare across experiments",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"ModelComparisonNode (node {node.node_id}): use MLflow for model comparison"],
        )

    def _generate_ModelCoefficientsNode(self, node: ModelCoefficientsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# ModelCoefficients: extract from model_reference='{node.model_reference}'",
            "# TODO: Load fitted model and access .coefficients / .intercept",
            "# coeffs = model.coefficients.toArray()",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"ModelCoefficientsNode (node {node.node_id}): load model to extract coefficients"],
        )

    def _generate_TSForecastNode(self, node: TSForecastNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# TSForecast: time='{node.time_field}', value='{node.value_field}', forecast='{node.forecast_field}'",
            "# TODO: Use Prophet or pandas UDF for time series forecasting",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"TSForecastNode (node {node.node_id}): use Prophet or pandas UDF for forecasting"],
        )

    def _generate_MeansTestNode(self, node: MeansTestNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# TestOfMeans: field_a='{node.field_a}', field_b='{node.field_b}', test='{node.test_type}'",
            "# TODO: Use pandas UDF with scipy.stats.ttest_ind / ttest_rel",
            "# from scipy.stats import ttest_ind",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"MeansTestNode (node {node.node_id}): use pandas UDF with scipy.stats"],
        )

    def _generate_VarianceInflationFactorsNode(self, node: VarianceInflationFactorsNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        feats = ", ".join(f'"{f}"' for f in node.feature_fields)
        lines = [
            f"# VIF: features=[{feats}]",
            "# TODO: Use pandas UDF with statsmodels.stats.outliers_influence.variance_inflation_factor",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"VarianceInflationFactorsNode (node {node.node_id}): use pandas UDF with statsmodels"],
        )

    def _generate_ABAnalysisNode(self, node: ABAnalysisNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        lines = [
            f"# ABAnalysis: treatment='{node.treatment_field}', response='{node.response_field}'",
            "# TODO: Use pandas UDF with scipy.stats for A/B test analysis",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines, output_vars={"Output": out_var},
            warnings=[f"ABAnalysisNode (node {node.node_id}): use pandas UDF with scipy.stats"],
        )

    # -- Special nodes ------------------------------------------------------

    def _generate_UnsupportedNode(self, node: UnsupportedNode, input_vars: dict[str, str]) -> NodeCodeResult:
        inp = self._get_single_input(input_vars)
        out_var = f"df_{node.node_id}"
        reason = node.unsupported_reason or "No auto-conversion available"
        lines = [
            f"# UNSUPPORTED: {node.original_tool_type or node.original_plugin_name}",
            f"# Reason: {reason}",
            "# TODO: Manual conversion required.",
            f"{out_var} = {inp}  # passthrough placeholder",
        ]
        return NodeCodeResult(
            code_lines=lines,
            output_vars={"Output": out_var},
            warnings=[f"Unsupported node {node.node_id} ({node.original_tool_type}): {reason}"],
        )

    def _generate_CommentNode(self, node: CommentNode, input_vars: dict[str, str]) -> NodeCodeResult:
        # Handled specially in generate() but provide fallback
        lines = [f"# {node.comment_text}"] if node.comment_text else []
        return NodeCodeResult(code_lines=lines, output_vars={})

    # _map_file_format and _get_single_input inherited from CodeGenerator base class
