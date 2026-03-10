"""Workflow complexity scoring for migration readiness assessment."""

from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from a2d.ir.graph import WorkflowDAG
from a2d.ir.nodes import (
    FilterNode,
    FormulaNode,
    MultiFieldFormulaNode,
    MultiRowFormulaNode,
    RegExNode,
    UnsupportedNode,
)


@dataclass
class ComplexityScore:
    """Scored complexity of a workflow on a 0-100 scale."""

    total_score: float  # 0-100
    level: str  # Low, Medium, High, Very High
    node_count: int
    edge_count: int
    unique_tool_types: int
    unsupported_count: int
    expression_count: int
    max_dag_depth: int
    has_macro_refs: bool
    detail: dict = field(default_factory=dict)


class ComplexityAnalyzer:
    """Score workflow complexity on a 0-100 scale.

    Scoring weights:
        - Node count: 20%
        - Tool diversity: 15%
        - Expressions: 20%
        - Unsupported nodes: 25%
        - Macro references: 10%
        - DAG depth: 10%

    Thresholds:
        - 0-25: Low
        - 25-50: Medium
        - 50-75: High
        - 75-100: Very High
    """

    def analyze(self, dag: WorkflowDAG, macro_refs: list[str] | None = None) -> ComplexityScore:
        """Analyze a DAG and return a complexity score."""
        macro_refs = macro_refs or []

        nodes = list(dag.all_nodes())
        node_count = dag.node_count
        edge_count = dag.edge_count

        # Collect metrics
        tool_types: set[str] = set()
        unsupported_count = 0
        expression_count = 0

        for node in nodes:
            tool_type = node.original_tool_type or type(node).__name__
            tool_types.add(tool_type)

            if isinstance(node, UnsupportedNode):
                unsupported_count += 1

            # Count expressions
            if isinstance(node, FormulaNode):
                expression_count += len(node.formulas)
            elif isinstance(node, FilterNode) or isinstance(node, MultiRowFormulaNode):
                if node.expression:
                    expression_count += 1
            elif isinstance(node, MultiFieldFormulaNode):
                if node.expression:
                    expression_count += len(node.fields) if node.fields else 1
            elif isinstance(node, RegExNode):
                if node.expression:
                    expression_count += 1

        unique_tool_types = len(tool_types)
        max_dag_depth = self._compute_dag_depth(dag)
        has_macro_refs = len(macro_refs) > 0

        # --- Compute component scores (each 0-100) ---

        # Node count score: 0-5 nodes=0, 5-15=linear, 15+=100
        node_score = self._scale(node_count, low=5, high=30)

        # Tool diversity: 1-3 types=0, 3-10=linear, 10+=100
        diversity_score = self._scale(unique_tool_types, low=3, high=12)

        # Expression count: 0=0, 1-5=linear, 5+=100
        expression_score = self._scale(expression_count, low=0, high=10)

        # Unsupported ratio: 0%=0, 50%+=100
        unsupported_ratio = (unsupported_count / max(node_count, 1)) * 100
        unsupported_score = self._scale(unsupported_ratio, low=0, high=50)

        # Macro refs: 0=0, any=50, 3+=100
        macro_score = 0.0
        if has_macro_refs:
            macro_score = min(100.0, 50.0 + len(macro_refs) * 16.67)

        # DAG depth: 1-3=0, 3-10=linear, 10+=100
        depth_score = self._scale(max_dag_depth, low=3, high=15)

        # Weighted total
        total = (
            node_score * 0.20
            + diversity_score * 0.15
            + expression_score * 0.20
            + unsupported_score * 0.25
            + macro_score * 0.10
            + depth_score * 0.10
        )

        # Clamp to 0-100
        total = max(0.0, min(100.0, total))

        # Determine level
        if total < 25:
            level = "Low"
        elif total < 50:
            level = "Medium"
        elif total < 75:
            level = "High"
        else:
            level = "Very High"

        detail = {
            "node_score": round(node_score, 1),
            "diversity_score": round(diversity_score, 1),
            "expression_score": round(expression_score, 1),
            "unsupported_score": round(unsupported_score, 1),
            "macro_score": round(macro_score, 1),
            "depth_score": round(depth_score, 1),
        }

        return ComplexityScore(
            total_score=round(total, 1),
            level=level,
            node_count=node_count,
            edge_count=edge_count,
            unique_tool_types=unique_tool_types,
            unsupported_count=unsupported_count,
            expression_count=expression_count,
            max_dag_depth=max_dag_depth,
            has_macro_refs=has_macro_refs,
            detail=detail,
        )

    @staticmethod
    def _scale(value: float, low: float, high: float) -> float:
        """Scale a value linearly from [low, high] to [0, 100]."""
        if value <= low:
            return 0.0
        if value >= high:
            return 100.0
        return ((value - low) / (high - low)) * 100.0

    @staticmethod
    def _compute_dag_depth(dag: WorkflowDAG) -> int:
        """Compute the longest path in the DAG."""
        if dag.node_count == 0:
            return 0
        try:
            return nx.dag_longest_path_length(dag._graph) + 1
        except (nx.NetworkXError, nx.NetworkXUnfeasible):
            return dag.node_count
