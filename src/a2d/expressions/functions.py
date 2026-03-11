"""Alteryx-to-PySpark function mapping registry.

Maps 80+ Alteryx expression functions to their PySpark column-expression
and Spark SQL equivalents.  Each mapping includes argument count constraints
and optional translator notes.

Templates use ``{0}``, ``{1}``, ... for positional arguments and the
special ``{args}`` placeholder for variable-length argument lists.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FunctionMapping:
    """Maps an Alteryx function to PySpark equivalent."""

    alteryx_name: str
    pyspark_template: str  # Template with {0}, {1}, etc. for args
    sql_template: str | None = None  # Spark SQL equivalent
    min_args: int = 0
    max_args: int | None = None
    notes: str = ""


FUNCTION_REGISTRY: dict[str, FunctionMapping] = {}


def _register(
    name: str,
    pyspark: str,
    sql: str | None = None,
    min_args: int = 0,
    max_args: int | None = None,
    notes: str = "",
) -> None:
    """Register a function mapping in the global registry."""
    FUNCTION_REGISTRY[name.lower()] = FunctionMapping(
        alteryx_name=name,
        pyspark_template=pyspark,
        sql_template=sql,
        min_args=min_args,
        max_args=max_args,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# String functions (25+)
# ---------------------------------------------------------------------------
_register("Contains", "({0}).contains({1})", "{0} LIKE CONCAT('%', {1}, '%')", 2, 2)
_register("EndsWith", "({0}).endswith({1})", "{0} LIKE CONCAT('%', {1})", 2, 2)
_register("StartsWith", "({0}).startswith({1})", "{0} LIKE CONCAT({1}, '%')", 2, 2)
_register("FindString", "(F.locate({1}, {0}) - 1)", "(LOCATE({1}, {0}) - 1)", 2, 2)
_register("Left", "F.substring({0}, 1, {1})", "LEFT({0}, {1})", 2, 2)
_register("Right", "F.expr(f'RIGHT({{{0}}}, {{{1}}})')", "RIGHT({0}, {1})", 2, 2, notes="Uses SQL expr")
_register("Length", "F.length({0})", "LENGTH({0})", 1, 1)
_register("LowerCase", "F.lower({0})", "LOWER({0})", 1, 1)
_register("Uppercase", "F.upper({0})", "UPPER({0})", 1, 1)
_register("TitleCase", "F.initcap({0})", "INITCAP({0})", 1, 1)
_register("Trim", "F.trim({0})", "TRIM({0})", 1, 2)
_register("TrimLeft", "F.ltrim({0})", "LTRIM({0})", 1, 2)
_register("TrimRight", "F.rtrim({0})", "RTRIM({0})", 1, 2)
_register("Replace", "F.regexp_replace({0}, {1}, {2})", "REGEXP_REPLACE({0}, {1}, {2})", 3, 3)
_register(
    "ReplaceFirst",
    "F.regexp_replace({0}, {1}, {2})",
    "REGEXP_REPLACE({0}, {1}, {2})",
    3,
    3,
    notes="PySpark replaces all by default",
)
_register("PadLeft", "F.lpad({0}, {1}, {2})", "LPAD({0}, {1}, {2})", 3, 3)
_register("PadRight", "F.rpad({0}, {1}, {2})", "RPAD({0}, {1}, {2})", 3, 3)
_register(
    "Substring",
    "F.substring({0}, ({1}) + 1, {2})",
    "SUBSTRING({0}, ({1}) + 1, {2})",
    3,
    3,
    notes="Alteryx is 0-indexed, Spark is 1-indexed",
)
_register("ReverseString", "F.reverse({0})", "REVERSE({0})", 1, 1)
_register(
    "CountWords",
    "F.size(F.split(F.trim({0}), r'\\s+'))",
    "SIZE(SPLIT(TRIM({0}), '\\\\s+'))",
    1,
    1,
)
_register("GetWord", "F.split({0}, F.lit(' '))[{1}]", "SPLIT({0}, ' ')[{1}]", 2, 2)
_register("REGEX_Match", "({0}).rlike({1})", "{0} RLIKE {1}", 2, 2)
_register("REGEX_Replace", "F.regexp_replace({0}, {1}, {2})", "REGEXP_REPLACE({0}, {1}, {2})", 3, 3)
_register(
    "REGEX_CountMatches",
    "F.size(F.expr(f'regexp_extract_all({{{0}}}, {{{1}}})'))",
    "SIZE(REGEXP_EXTRACT_ALL({0}, {1}))",
    2,
    2,
)
_register("Concat", "F.concat({args})", "CONCAT({args})", 1, None, notes="Variable args")

# ---------------------------------------------------------------------------
# Math functions (20+)
# ---------------------------------------------------------------------------
_register("ABS", "F.abs({0})", "ABS({0})", 1, 1)
_register("CEIL", "F.ceil({0})", "CEIL({0})", 1, 1)
_register("FLOOR", "F.floor({0})", "FLOOR({0})", 1, 1)
_register("Round", "F.round({0}, {1})", "ROUND({0}, {1})", 1, 2)
_register("POW", "F.pow({0}, {1})", "POWER({0}, {1})", 2, 2)
_register("SQRT", "F.sqrt({0})", "SQRT({0})", 1, 1)
_register("LOG", "F.log({0})", "LN({0})", 1, 1)
_register("LOG10", "F.log10({0})", "LOG10({0})", 1, 1)
_register("LOG2", "F.log2({0})", "LOG2({0})", 1, 1)
_register("EXP", "F.exp({0})", "EXP({0})", 1, 1)
_register("Mod", "({0} % {1})", "({0} % {1})", 2, 2)
_register("RAND", "F.rand()", "RAND()", 0, 0)
_register("RandInt", "F.floor(F.rand() * {0})", "FLOOR(RAND() * {0})", 1, 1)
_register("PI", "F.lit(3.141592653589793)", "3.141592653589793", 0, 0)
_register("SIN", "F.sin({0})", "SIN({0})", 1, 1)
_register("COS", "F.cos({0})", "COS({0})", 1, 1)
_register("TAN", "F.tan({0})", "TAN({0})", 1, 1)
_register("ASIN", "F.asin({0})", "ASIN({0})", 1, 1)
_register("ACOS", "F.acos({0})", "ACOS({0})", 1, 1)
_register("ATAN", "F.atan({0})", "ATAN({0})", 1, 1)
_register("ATAN2", "F.atan2({0}, {1})", "ATAN2({0}, {1})", 2, 2)

# ---------------------------------------------------------------------------
# Conversion functions (10+)
# ---------------------------------------------------------------------------
_register("ToNumber", "({0}).cast('double')", "CAST({0} AS DOUBLE)", 1, 1)
_register("ToInteger", "({0}).cast('int')", "CAST({0} AS INT)", 1, 1)
_register("ToString", "({0}).cast('string')", "CAST({0} AS STRING)", 1, 2)
_register("ToDate", "F.to_date({0}, {1})", "TO_DATE({0}, {1})", 1, 2)
_register("ToDateTime", "F.to_timestamp({0}, {1})", "TO_TIMESTAMP({0}, {1})", 1, 2)
_register("CharToInt", "F.ascii({0})", "ASCII({0})", 1, 1)
_register("IntToChar", "F.chr({0})", "CHR({0})", 1, 1)
_register("HexToNumber", "F.conv({0}, 16, 10)", "CONV({0}, 16, 10)", 1, 1)
_register("BinToInt", "F.conv({0}, 2, 10)", "CONV({0}, 2, 10)", 1, 1)

# ---------------------------------------------------------------------------
# DateTime functions (15+)
# ---------------------------------------------------------------------------
_register("DateTimeNow", "F.current_timestamp()", "CURRENT_TIMESTAMP()", 0, 0)
_register("DateTimeToday", "F.current_date()", "CURRENT_DATE()", 0, 0)
_register("DateTimeYear", "F.year({0})", "YEAR({0})", 1, 1)
_register("DateTimeMonth", "F.month({0})", "MONTH({0})", 1, 1)
_register("DateTimeDay", "F.dayofmonth({0})", "DAYOFMONTH({0})", 1, 1)
_register("DateTimeHour", "F.hour({0})", "HOUR({0})", 1, 1)
_register("DateTimeMinutes", "F.minute({0})", "MINUTE({0})", 1, 1)
_register("DateTimeSeconds", "F.second({0})", "SECOND({0})", 1, 1)
_register(
    "DateTimeAdd",
    "F.expr(f'dateadd({{{2}}}, {{{1}}}, {{{0}}})')",
    "DATEADD({2}, {1}, {0})",
    3,
    3,
    notes="Args: datetime, interval_count, interval_type",
)
_register(
    "DateTimeDiff",
    "F.datediff({0}, {1})",
    "DATEDIFF({2}, {0}, {1})",
    2,
    3,
    notes="Args: start, end, unit",
)
_register("DateTimeFormat", "F.date_format({0}, {1})", "DATE_FORMAT({0}, {1})", 2, 2)
_register("DateTimeParse", "F.to_timestamp({0}, {1})", "TO_TIMESTAMP({0}, {1})", 2, 2)
_register("DateTimeTrim", "F.date_trunc({1}, {0})", "DATE_TRUNC({1}, {0})", 2, 2)
_register("DateTimeFirstOfMonth", "F.trunc({0}, 'month')", "TRUNC({0}, 'month')", 1, 1)
_register("DateTimeDayOfWeek", "F.dayofweek({0})", "DAYOFWEEK({0})", 1, 1)

# ---------------------------------------------------------------------------
# Test / Null functions (10+)
# ---------------------------------------------------------------------------
_register("IsNull", "F.isnull({0})", "{0} IS NULL", 1, 1)
_register(
    "IsEmpty",
    "(F.isnull({0}) | (F.trim({0}) == F.lit('')))",
    "({0} IS NULL OR TRIM({0}) = '')",
    1,
    1,
)
_register(
    "IsNumber",
    "({0}).cast('double').isNotNull()",
    "CAST({0} AS DOUBLE) IS NOT NULL",
    1,
    1,
)
_register(
    "IsInteger",
    "({0}).cast('int').isNotNull()",
    "CAST({0} AS INT) IS NOT NULL",
    1,
    1,
)
_register(
    "IsString",
    "F.lit(True)",
    "TRUE",
    1,
    1,
    notes="In Spark everything can be treated as string",
)
_register("Coalesce", "F.coalesce({args})", "COALESCE({args})", 1, None, notes="Variable args")
_register("IIF", "F.when({0}, {1}).otherwise({2})", "CASE WHEN {0} THEN {1} ELSE {2} END", 3, 3)
_register("IFNULL", "F.coalesce({0}, {1})", "COALESCE({0}, {1})", 2, 2)

# ---------------------------------------------------------------------------
# Min / Max (scalar)
# ---------------------------------------------------------------------------
_register("Min", "F.least({0}, {1})", "LEAST({0}, {1})", 2, 2)
_register("Max", "F.greatest({0}, {1})", "GREATEST({0}, {1})", 2, 2)

# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------
_register("Null", "F.lit(None)", "NULL", 0, 0)

# ---------------------------------------------------------------------------
# Spatial (stub)
# ---------------------------------------------------------------------------
_register(
    "Distance",
    "F.lit('UNSUPPORTED: Distance')",
    "'UNSUPPORTED: Distance'",
    2,
    4,
    notes="Spatial functions not supported in Spark natively",
)

# ---------------------------------------------------------------------------
# Conditional functions
# ---------------------------------------------------------------------------
_register(
    "Switch",
    "__SWITCH__",
    "__SWITCH__",
    2,
    None,
    notes="Special-cased in translators: Switch(value, default, val1, result1, val2, result2, ...)",
)

# ---------------------------------------------------------------------------
# Additional string functions
# ---------------------------------------------------------------------------
_register(
    "Mid",
    "F.substring({0}, ({1}) + 1, {2})",
    "SUBSTRING({0}, ({1}) + 1, {2})",
    3,
    3,
    notes="Alias for Substring; Alteryx is 0-indexed, Spark is 1-indexed",
)

# ---------------------------------------------------------------------------
# Additional test / validation functions
# ---------------------------------------------------------------------------
_register(
    "IsAlpha",
    "({0}).rlike('^[A-Za-z]+$')",
    "{0} RLIKE '^[A-Za-z]+$'",
    1,
    1,
)
_register(
    "IsUpperCase",
    "({0} == F.upper({0}))",
    "({0} = UPPER({0}))",
    1,
    1,
)
_register(
    "IsLowerCase",
    "({0} == F.lower({0}))",
    "({0} = LOWER({0}))",
    1,
    1,
)

# ---------------------------------------------------------------------------
# Additional null handling
# ---------------------------------------------------------------------------
_register(
    "NullIf",
    "F.when({0} == {1}, F.lit(None)).otherwise({0})",
    "NULLIF({0}, {1})",
    2,
    2,
)

# ---------------------------------------------------------------------------
# Conversion aliases
# ---------------------------------------------------------------------------
_register("CharFromInt", "F.chr({0})", "CHR({0})", 1, 1, notes="Alias for IntToChar")

# ---------------------------------------------------------------------------
# File functions
# ---------------------------------------------------------------------------
_register(
    "FileGetFileName",
    "F.element_at(F.split(F.regexp_replace({0}, '\\\\\\\\', '/'), '/'), -1)",
    "ELEMENT_AT(SPLIT(REGEXP_REPLACE({0}, '\\\\\\\\', '/'), '/'), -1)",
    1,
    1,
    notes="Extracts filename from a path; normalizes backslash to forward-slash first",
)


def get_function_mapping(name: str) -> FunctionMapping | None:
    """Look up a function mapping by (case-insensitive) Alteryx name.

    Returns ``None`` when the function is not in the registry.
    """
    return FUNCTION_REGISTRY.get(name.lower())
