"""PySpark column-expression translator.

Walks an Alteryx expression AST and emits a PySpark ``F.col`` / ``F.when`` /
``F.lit`` string that can be embedded in generated code.
"""

from __future__ import annotations

from a2d.expressions.ast import (
    FieldRef,
    FunctionCall,
    IfExpr,
    InExpr,
    Literal,
    LogicalOp,
    NotOp,
    RowRef,
)
from a2d.expressions.base_translator import BaseExpressionTranslator, BaseTranslationError
from a2d.expressions.functions import get_function_mapping


class TranslationError(BaseTranslationError):
    """Raised when the translator cannot handle an AST node."""


# Map Alteryx comparison operators to PySpark operators
_CMP_MAP = {
    "=": "==",
    "!=": "!=",
    "<>": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
}


class PySparkTranslator(BaseExpressionTranslator):
    """Translates Alteryx expression AST to PySpark column expressions.

    Usage::

        translator = PySparkTranslator()
        pyspark_code = translator.translate_string('[Age] > 25')
        # -> 'F.col("Age") > 25'
    """

    def _make_error(self, message: str) -> Exception:
        return TranslationError(message)

    @property
    def _cmp_map(self) -> dict[str, str]:
        return _CMP_MAP

    # -- Format-specific visitors -------------------------------------------

    def _visit_FieldRef(self, node: FieldRef) -> str:
        return f'F.col("{node.field_name}")'

    def _visit_RowRef(self, node: RowRef) -> str:
        offset = node.row_offset
        if offset < 0:
            return f'F.lag(F.col("{node.field_name}"), {abs(offset)}).over(window)'
        return f'F.lead(F.col("{node.field_name}"), {offset}).over(window)'

    def _visit_Literal(self, node: Literal) -> str:
        if node.literal_type == "string":
            escaped = str(node.value).replace("\\", "\\\\").replace('"', '\\"')
            return f'F.lit("{escaped}")'
        if node.literal_type == "number":
            return str(node.value)
        if node.literal_type == "boolean":
            return f"F.lit({node.value})"
        if node.literal_type == "null":
            return "F.lit(None)"
        raise TranslationError(f"Unknown literal type: {node.literal_type}")

    def _visit_LogicalOp(self, node: LogicalOp) -> str:
        left = self._visit(node.left)
        right = self._visit(node.right)
        if node.operator == "AND":
            return f"({left} & {right})"
        return f"({left} | {right})"

    def _visit_NotOp(self, node: NotOp) -> str:
        operand = self._visit(node.operand)
        return f"~({operand})"

    def _visit_FunctionCall(self, node: FunctionCall) -> str:
        mapping = get_function_mapping(node.function_name)
        if mapping is None:
            self._warnings.append(f"Unknown function: {node.function_name}")
            args = ", ".join(self._visit(a) for a in node.arguments)
            return f"F.expr('{node.function_name}({args})')"

        translated_args = [self._visit(a) for a in node.arguments]

        # Special case: Switch(value, default, val1, result1, val2, result2, ...)
        if mapping.pyspark_template == "__SWITCH__":
            return self._translate_switch_pyspark(translated_args)

        template = mapping.pyspark_template

        # Handle variable-args placeholder
        if "{args}" in template:
            return template.replace("{args}", ", ".join(translated_args))

        # Substitute positional placeholders
        result = template
        for i, arg_str in enumerate(translated_args):
            result = result.replace(f"{{{i}}}", arg_str)

        return result

    def _translate_switch_pyspark(self, args: list[str]) -> str:
        """Translate Switch(value, default, val1, res1, val2, res2, ...) to nested F.when()."""
        if len(args) < 2:
            return "F.lit(None)"
        value = args[0]
        default = args[1]
        pairs = list(zip(args[2::2], args[3::2], strict=False))
        if not pairs:
            return default
        result = f"F.when({value} == {pairs[0][0]}, {pairs[0][1]})"
        for val, res in pairs[1:]:
            result += f".when({value} == {val}, {res})"
        result += f".otherwise({default})"
        return result

    def _visit_IfExpr(self, node: IfExpr) -> str:
        cond = self._visit(node.condition)
        then = self._visit(node.then_expr)
        result = f"F.when({cond}, {then})"

        for elseif_cond, elseif_then in node.elseif_clauses:
            ec = self._visit(elseif_cond)
            et = self._visit(elseif_then)
            result += f".when({ec}, {et})"

        if node.else_expr is not None:
            else_val = self._visit(node.else_expr)
            result += f".otherwise({else_val})"

        return result

    def _visit_InExpr(self, node: InExpr) -> str:
        value = self._visit(node.value)
        items = ", ".join(self._visit(item) for item in node.items)
        return f"{value}.isin([{items}])"
