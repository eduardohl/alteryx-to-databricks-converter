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


# Map Alteryx DateTimeAdd unit strings to PySpark expression templates
# {col} = translated column expression, {n} = translated count expression
_DATETIMEADD_UNIT_MAP: dict[str, str] = {
    "day":     "F.date_add({col}, {n})",
    "days":    "F.date_add({col}, {n})",
    "month":   "F.add_months({col}, {n})",
    "months":  "F.add_months({col}, {n})",
    "year":    "F.add_months({col}, ({n}) * 12)",
    "years":   "F.add_months({col}, ({n}) * 12)",
    "hour":    "({col}.cast('timestamp').cast('long') + ({n}) * 3600).cast('timestamp')",
    "hours":   "({col}.cast('timestamp').cast('long') + ({n}) * 3600).cast('timestamp')",
    "minute":  "({col}.cast('timestamp').cast('long') + ({n}) * 60).cast('timestamp')",
    "minutes": "({col}.cast('timestamp').cast('long') + ({n}) * 60).cast('timestamp')",
    "second":  "({col}.cast('timestamp').cast('long') + ({n})).cast('timestamp')",
    "seconds": "({col}.cast('timestamp').cast('long') + ({n})).cast('timestamp')",
}

# Map Alteryx DateTimeTrim mode strings to Spark date_trunc format strings
_DATETIMETRIM_MODE_MAP: dict[str, str] = {
    "firstofmonth": "month",
    "firstofyear": "year",
    "firstofhour": "hour",
    "firstofminute": "minute",
    "firstofsecond": "second",
    "firstofday": "day",
    "firstofweek": "week",
}

# Map Alteryx/strftime date format tokens to Java SimpleDateFormat tokens
# (used by Spark's date_format / to_date / to_timestamp)
_STRFTIME_TO_JAVA: dict[str, str] = {
    "%Y": "yyyy",
    "%y": "yy",
    "%m": "MM",
    "%d": "dd",
    "%H": "HH",
    "%I": "hh",
    "%M": "mm",
    "%S": "ss",
    "%p": "a",
    "%A": "EEEE",
    "%a": "EEE",
    "%B": "MMMM",
    "%b": "MMM",
    "%j": "DDD",
    "%u": "u",   # ISO weekday (1=Monday … 7=Sunday)
    "%w": "e",   # weekday (1=Sunday in strftime, use 'e' as approx)
    "%Z": "z",
    "%z": "Z",
    "%f": "SSSSSS",
}


def _strftime_to_java(fmt: str) -> str:
    """Convert a strftime-style format string to Java SimpleDateFormat."""
    result = fmt
    for strftime_token, java_token in _STRFTIME_TO_JAVA.items():
        result = result.replace(strftime_token, java_token)
    return result


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

        # Special case: DateTimeTrim(col, mode) — map Alteryx mode to Spark format
        if mapping.pyspark_template == "__DATETIMETRIM__":
            return self._translate_datetimetrim_pyspark(node, translated_args)

        # Special case: DateTimeAdd(col, n, unit) — map unit string to correct Spark function
        if mapping.pyspark_template == "__DATETIMEADD__":
            return self._translate_datetimeadd_pyspark(node, translated_args)

        # Special case: DateTimeFormat(col, fmt) — convert strftime tokens to Java format
        if node.function_name == "DateTimeFormat" and len(node.arguments) == 2:
            fmt_arg = node.arguments[1]
            if isinstance(fmt_arg, Literal) and fmt_arg.literal_type == "string":
                java_fmt = _strftime_to_java(str(fmt_arg.value))
                return f'F.date_format({translated_args[0]}, "{java_fmt}")'

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

    def _translate_datetimetrim_pyspark(self, node: FunctionCall, translated_args: list[str]) -> str:
        """Translate DateTimeTrim(col, mode) to F.date_trunc or F.last_day."""
        if len(translated_args) < 2:
            return "F.lit(None)"
        col_expr = translated_args[0]
        # The mode arg is a string literal — extract the raw string value
        mode_arg = node.arguments[1]
        if isinstance(mode_arg, Literal) and mode_arg.literal_type == "string":
            mode_key = str(mode_arg.value).lower()
        else:
            # Non-literal mode: fall back to the translated expression
            mode_expr = translated_args[1]
            self._warnings.append(
                "DateTimeTrim: mode is not a string literal — emitting F.date_trunc with raw expression"
            )
            return f"F.date_trunc({mode_expr}, {col_expr})"

        if mode_key == "lastofmonth":
            return f"F.last_day({col_expr})"
        spark_mode = _DATETIMETRIM_MODE_MAP.get(mode_key)
        if spark_mode is None:
            self._warnings.append(f"DateTimeTrim: unknown mode '{mode_key}' — emitting as-is")
            return f'F.date_trunc("{mode_key}", {col_expr})'
        return f'F.date_trunc("{spark_mode}", {col_expr})'

    def _translate_datetimeadd_pyspark(self, node: FunctionCall, translated_args: list[str]) -> str:
        """Translate DateTimeAdd(col, n, unit) to the appropriate Spark date function."""
        if len(translated_args) < 2:
            return "F.lit(None)"
        col_expr = translated_args[0]
        n_expr = translated_args[1] if len(translated_args) >= 2 else "1"
        if len(translated_args) < 3:
            return f"F.date_add({col_expr}, {n_expr})"

        unit_arg = node.arguments[2]
        if isinstance(unit_arg, Literal) and unit_arg.literal_type == "string":
            unit_key = str(unit_arg.value).lower()
        else:
            self._warnings.append(
                "DateTimeAdd: unit is not a string literal — defaulting to date_add (days)"
            )
            return f"F.date_add({col_expr}, {n_expr})"

        template = _DATETIMEADD_UNIT_MAP.get(unit_key)
        if template is None:
            self._warnings.append(
                f"DateTimeAdd: unknown unit '{unit_key}' — defaulting to date_add (days)"
            )
            return f"F.date_add({col_expr}, {n_expr})"

        return template.replace("{col}", col_expr).replace("{n}", n_expr)

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
