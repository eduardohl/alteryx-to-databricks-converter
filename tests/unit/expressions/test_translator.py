"""Tests for the PySpark and SQL translators.

Loads the expression corpus from tests/fixtures/expressions/expression_corpus.json
and verifies each case against both translators.  Also includes targeted unit tests
for individual translation methods.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from a2d.expressions.ast import (
    BinaryOp,
    ComparisonOp,
    FieldRef,
    FunctionCall,
    IfExpr,
    InExpr,
    Literal,
    LogicalOp,
    NotOp,
    RowRef,
    UnaryOp,
)
from a2d.expressions.sql_translator import SparkSQLTranslator
from a2d.expressions.translator import PySparkTranslator

CORPUS_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "expressions" / "expression_corpus.json"


@pytest.fixture
def corpus() -> list[dict]:
    """Load the expression test corpus."""
    with open(CORPUS_PATH) as f:
        return json.load(f)


@pytest.fixture
def pyspark_translator() -> PySparkTranslator:
    return PySparkTranslator()


@pytest.fixture
def sql_translator() -> SparkSQLTranslator:
    return SparkSQLTranslator()


# ---------------------------------------------------------------------------
# Corpus-driven tests
# ---------------------------------------------------------------------------


class TestPySparkCorpus:
    """Test the PySpark translator against the expression corpus."""

    def test_corpus_pyspark_translations(self, corpus: list[dict], pyspark_translator: PySparkTranslator) -> None:
        """Every corpus entry with a 'pyspark' field must translate correctly."""
        failures: list[str] = []
        for i, case in enumerate(corpus):
            if "pyspark" not in case:
                continue
            try:
                result = pyspark_translator.translate_string(case["alteryx"])
                if result != case["pyspark"]:
                    failures.append(
                        f"  Case {i} ({case['description']}):\n"
                        f"    alteryx:  {case['alteryx']}\n"
                        f"    expected: {case['pyspark']}\n"
                        f"    got:      {result}"
                    )
            except Exception as exc:
                failures.append(
                    f"  Case {i} ({case['description']}):\n    alteryx:  {case['alteryx']}\n    ERROR:    {exc}"
                )

        if failures:
            pytest.fail("PySpark corpus failures:\n" + "\n".join(failures))


class TestSQLCorpus:
    """Test the SQL translator against the expression corpus."""

    def test_corpus_sql_translations(self, corpus: list[dict], sql_translator: SparkSQLTranslator) -> None:
        """Every corpus entry with a 'sql' field must translate correctly."""
        failures: list[str] = []
        for i, case in enumerate(corpus):
            if "sql" not in case:
                continue
            try:
                result = sql_translator.translate_string(case["alteryx"])
                if result != case["sql"]:
                    failures.append(
                        f"  Case {i} ({case['description']}):\n"
                        f"    alteryx:  {case['alteryx']}\n"
                        f"    expected: {case['sql']}\n"
                        f"    got:      {result}"
                    )
            except Exception as exc:
                failures.append(
                    f"  Case {i} ({case['description']}):\n    alteryx:  {case['alteryx']}\n    ERROR:    {exc}"
                )

        if failures:
            pytest.fail("SQL corpus failures:\n" + "\n".join(failures))


# ---------------------------------------------------------------------------
# Targeted PySpark translator unit tests
# ---------------------------------------------------------------------------


class TestPySparkFieldRef:
    """Test PySpark translation of field references."""

    def test_simple_field(self, pyspark_translator: PySparkTranslator) -> None:
        node = FieldRef(field_name="Age")
        assert pyspark_translator.translate(node) == 'F.col("Age")'

    def test_field_with_spaces(self, pyspark_translator: PySparkTranslator) -> None:
        node = FieldRef(field_name="First Name")
        assert pyspark_translator.translate(node) == 'F.col("First Name")'


class TestPySparkRowRef:
    """Test PySpark translation of row references."""

    def test_lag(self, pyspark_translator: PySparkTranslator) -> None:
        node = RowRef(field_name="Amount", row_offset=-1)
        result = pyspark_translator.translate(node)
        assert "F.lag" in result
        assert '"Amount"' in result

    def test_lead(self, pyspark_translator: PySparkTranslator) -> None:
        node = RowRef(field_name="Amount", row_offset=1)
        result = pyspark_translator.translate(node)
        assert "F.lead" in result


class TestPySparkLiterals:
    """Test PySpark translation of literals."""

    def test_string_literal(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value="hello", literal_type="string")
        assert pyspark_translator.translate(node) == 'F.lit("hello")'

    def test_number_literal(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value=42, literal_type="number")
        assert pyspark_translator.translate(node) == "42"

    def test_float_literal(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value=3.14, literal_type="number")
        assert pyspark_translator.translate(node) == "3.14"

    def test_boolean_true(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value=True, literal_type="boolean")
        assert pyspark_translator.translate(node) == "F.lit(True)"

    def test_boolean_false(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value=False, literal_type="boolean")
        assert pyspark_translator.translate(node) == "F.lit(False)"

    def test_null_literal(self, pyspark_translator: PySparkTranslator) -> None:
        node = Literal(value=None, literal_type="null")
        assert pyspark_translator.translate(node) == "F.lit(None)"


class TestPySparkOperations:
    """Test PySpark translation of operations."""

    def test_binary_add(self, pyspark_translator: PySparkTranslator) -> None:
        node = BinaryOp(
            left=FieldRef("A"),
            operator="+",
            right=FieldRef("B"),
        )
        assert pyspark_translator.translate(node) == '(F.col("A") + F.col("B"))'

    def test_comparison_equals(self, pyspark_translator: PySparkTranslator) -> None:
        node = ComparisonOp(
            left=FieldRef("Status"),
            operator="=",
            right=Literal("Active", "string"),
        )
        result = pyspark_translator.translate(node)
        assert "==" in result

    def test_logical_and(self, pyspark_translator: PySparkTranslator) -> None:
        node = LogicalOp(
            left=ComparisonOp(FieldRef("A"), ">", Literal(0, "number")),
            operator="AND",
            right=ComparisonOp(FieldRef("B"), "<", Literal(100, "number")),
        )
        result = pyspark_translator.translate(node)
        assert "&" in result

    def test_not_op(self, pyspark_translator: PySparkTranslator) -> None:
        node = NotOp(operand=FieldRef("Flag"))
        result = pyspark_translator.translate(node)
        assert result.startswith("~(")

    def test_unary_minus(self, pyspark_translator: PySparkTranslator) -> None:
        node = UnaryOp(operator="-", operand=FieldRef("Value"))
        result = pyspark_translator.translate(node)
        assert result == '(-F.col("Value"))'


class TestPySparkFunctions:
    """Test PySpark translation of function calls."""

    def test_known_function(self, pyspark_translator: PySparkTranslator) -> None:
        node = FunctionCall(
            function_name="ABS",
            arguments=[FieldRef("Value")],
        )
        result = pyspark_translator.translate(node)
        assert result == 'F.abs(F.col("Value"))'

    def test_unknown_function_warning(self, pyspark_translator: PySparkTranslator) -> None:
        node = FunctionCall(
            function_name="UnknownFunc",
            arguments=[FieldRef("X")],
        )
        result = pyspark_translator.translate(node)
        assert "UnknownFunc" in result
        assert len(pyspark_translator.warnings) == 1


class TestPySparkIfExpr:
    """Test PySpark translation of IF expressions."""

    def test_simple_if_else(self, pyspark_translator: PySparkTranslator) -> None:
        node = IfExpr(
            condition=ComparisonOp(FieldRef("A"), ">", Literal(0, "number")),
            then_expr=Literal("yes", "string"),
            else_expr=Literal("no", "string"),
        )
        result = pyspark_translator.translate(node)
        assert "F.when(" in result
        assert ".otherwise(" in result

    def test_if_without_else(self, pyspark_translator: PySparkTranslator) -> None:
        node = IfExpr(
            condition=ComparisonOp(FieldRef("A"), ">", Literal(0, "number")),
            then_expr=Literal(1, "number"),
        )
        result = pyspark_translator.translate(node)
        assert "F.when(" in result
        assert ".otherwise(" not in result


class TestPySparkInExpr:
    """Test PySpark translation of IN expressions."""

    def test_in_expression(self, pyspark_translator: PySparkTranslator) -> None:
        node = InExpr(
            value=FieldRef("Color"),
            items=[
                Literal("Red", "string"),
                Literal("Blue", "string"),
            ],
        )
        result = pyspark_translator.translate(node)
        assert ".isin([" in result


# ---------------------------------------------------------------------------
# Targeted SQL translator unit tests
# ---------------------------------------------------------------------------


class TestSQLFieldRef:
    """Test SQL translation of field references."""

    def test_backtick_quoting(self, sql_translator: SparkSQLTranslator) -> None:
        node = FieldRef(field_name="Age")
        assert sql_translator.translate(node) == "`Age`"


class TestSQLLiterals:
    """Test SQL translation of literals."""

    def test_string_single_quotes(self, sql_translator: SparkSQLTranslator) -> None:
        node = Literal(value="hello", literal_type="string")
        assert sql_translator.translate(node) == "'hello'"

    def test_boolean_true(self, sql_translator: SparkSQLTranslator) -> None:
        node = Literal(value=True, literal_type="boolean")
        assert sql_translator.translate(node) == "TRUE"

    def test_null(self, sql_translator: SparkSQLTranslator) -> None:
        node = Literal(value=None, literal_type="null")
        assert sql_translator.translate(node) == "NULL"


class TestSQLOperations:
    """Test SQL translation of operations."""

    def test_logical_and(self, sql_translator: SparkSQLTranslator) -> None:
        node = LogicalOp(
            left=ComparisonOp(FieldRef("A"), ">", Literal(0, "number")),
            operator="AND",
            right=ComparisonOp(FieldRef("B"), "<", Literal(100, "number")),
        )
        result = sql_translator.translate(node)
        assert "AND" in result

    def test_not_op(self, sql_translator: SparkSQLTranslator) -> None:
        node = NotOp(operand=FieldRef("Flag"))
        result = sql_translator.translate(node)
        assert result == "NOT (`Flag`)"


class TestSQLIfExpr:
    """Test SQL translation of IF expressions."""

    def test_case_when(self, sql_translator: SparkSQLTranslator) -> None:
        node = IfExpr(
            condition=ComparisonOp(FieldRef("A"), ">", Literal(0, "number")),
            then_expr=Literal("yes", "string"),
            else_expr=Literal("no", "string"),
        )
        result = sql_translator.translate(node)
        assert result.startswith("CASE WHEN")
        assert "THEN" in result
        assert "ELSE" in result
        assert result.endswith("END")


class TestSQLInExpr:
    """Test SQL translation of IN expressions."""

    def test_in_expression(self, sql_translator: SparkSQLTranslator) -> None:
        node = InExpr(
            value=FieldRef("Color"),
            items=[
                Literal("Red", "string"),
                Literal("Blue", "string"),
            ],
        )
        result = sql_translator.translate(node)
        assert "IN" in result
        assert "'Red'" in result
        assert "'Blue'" in result
