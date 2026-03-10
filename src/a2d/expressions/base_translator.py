"""Base expression translator with shared visitor dispatch logic.

Concrete translators (PySpark, SQL) override only output-format-specific
methods: ``_visit_FieldRef``, ``_visit_RowRef``, ``_visit_Literal``,
``_visit_FunctionCall``, and the Switch helper.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from a2d.expressions.ast import (
    BinaryOp,
    ComparisonOp,
    Expr,
    FieldRef,
    IfExpr,
    InExpr,
    Literal,
    LogicalOp,
    NotOp,
    RowRef,
    UnaryOp,
)
from a2d.expressions.parser import ExpressionParser


class BaseTranslationError(Exception):
    """Raised when the translator cannot handle an AST node."""


class BaseExpressionTranslator(ABC):
    """Shared dispatch and structural visitors for expression translators."""

    def __init__(self) -> None:
        self._warnings: list[str] = []

    # -- Public API ----------------------------------------------------------

    def translate(self, expr: Expr) -> str:
        """Translate an AST node to a target-format expression string."""
        self._warnings = []
        return self._visit(expr)

    def translate_string(self, expression: str) -> str:
        """Parse and translate an Alteryx expression string."""
        parser = ExpressionParser()
        ast = parser.parse(expression)
        return self.translate(ast)

    @property
    def warnings(self) -> list[str]:
        """Return any warnings generated during the last translation."""
        return list(self._warnings)

    # -- Visitor dispatch ----------------------------------------------------

    def _visit(self, node: Expr) -> str:
        method_name = f"_visit_{type(node).__name__}"
        method = getattr(self, method_name, None)
        if method is None:
            raise self._make_error(f"Unsupported AST node type: {type(node).__name__}")
        return method(node)

    @abstractmethod
    def _make_error(self, message: str) -> Exception:
        """Create a format-specific translation error."""

    # -- Shared structural visitors -----------------------------------------

    def _visit_BinaryOp(self, node: BinaryOp) -> str:
        left = self._visit(node.left)
        right = self._visit(node.right)
        return f"({left} {node.operator} {right})"

    def _visit_UnaryOp(self, node: UnaryOp) -> str:
        operand = self._visit(node.operand)
        return f"({node.operator}{operand})"

    def _visit_ComparisonOp(self, node: ComparisonOp) -> str:
        left = self._visit(node.left)
        right = self._visit(node.right)
        op = self._cmp_map.get(node.operator, node.operator)
        return f"({left} {op} {right})"

    @property
    @abstractmethod
    def _cmp_map(self) -> dict[str, str]:
        """Map Alteryx comparison operators to target format operators."""

    @abstractmethod
    def _visit_LogicalOp(self, node: LogicalOp) -> str: ...

    @abstractmethod
    def _visit_NotOp(self, node: NotOp) -> str: ...

    @abstractmethod
    def _visit_FieldRef(self, node: FieldRef) -> str: ...

    @abstractmethod
    def _visit_RowRef(self, node: RowRef) -> str: ...

    @abstractmethod
    def _visit_Literal(self, node: Literal) -> str: ...

    @abstractmethod
    def _visit_IfExpr(self, node: IfExpr) -> str: ...

    @abstractmethod
    def _visit_InExpr(self, node: InExpr) -> str: ...
