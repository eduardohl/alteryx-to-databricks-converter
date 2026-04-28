"""Shared type aliases and utilities."""

import re

# Alteryx uses strftime-style format tokens; Spark uses Java datetime patterns.
_ALTERYX_FMT_MAP = [
    ("%Y", "yyyy"),
    ("%y", "yy"),
    ("%m", "MM"),
    ("%B", "MMMM"),
    ("%b", "MMM"),
    ("%d", "dd"),
    ("%H", "HH"),
    ("%I", "hh"),
    ("%M", "mm"),
    ("%S", "ss"),
    ("%p", "a"),
    ("%j", "DDD"),
    ("%A", "EEEE"),
    ("%a", "EEE"),
    ("%f", "SSSSSS"),
]

# Only convert if the string contains at least one strftime token
_ALTERYX_FMT_RE = re.compile(r"%[YymbBdHIMSpjAaf]")


# SQL normalization patterns for converting source-DB SQL to Spark SQL
_SQL_GETDATE_RE = re.compile(r"\bGETDATE\s*\(\s*\)", re.IGNORECASE)
_SQL_NOW_RE = re.compile(r"\bNOW\s*\(\s*\)", re.IGNORECASE)
_SQL_SYSDATE_RE = re.compile(r"\bSYSDATE\b", re.IGNORECASE)
# Double-quoted SQL identifier (no spaces inside, at least one char)
_SQL_DQUOTE_ID_RE = re.compile(r'"([^"\s][^"]*[^"\s]|[^"\s])"')


def normalize_sql_for_spark(query: str) -> tuple[str, list[str]]:
    """Apply targeted fixes to an Alteryx SQL query for Spark SQL compatibility.

    Fixes:
    - GETDATE() / NOW() / SYSDATE  →  CURRENT_TIMESTAMP()
    - Double-quoted SQL identifiers →  backtick identifiers
    - Hyphens in identifier names   →  underscores

    Returns (normalized_query, warnings).
    """
    result = query
    result = _SQL_GETDATE_RE.sub("CURRENT_TIMESTAMP()", result)
    result = _SQL_NOW_RE.sub("CURRENT_TIMESTAMP()", result)
    result = _SQL_SYSDATE_RE.sub("CURRENT_TIMESTAMP()", result)

    def _to_backtick(m: re.Match) -> str:
        return f"`{m.group(1).replace('-', '_')}`"

    result = _SQL_DQUOTE_ID_RE.sub(_to_backtick, result)
    return result, []


def alteryx_fmt_to_spark(fmt: str) -> str:
    """Convert an Alteryx/strftime datetime format string to a Spark Java datetime pattern.

    If the format contains no strftime tokens (e.g. already a Spark pattern like
    'yyyy-MM-dd'), it is returned unchanged.
    """
    if not _ALTERYX_FMT_RE.search(fmt):
        return fmt
    result = fmt
    for alteryx_token, spark_token in _ALTERYX_FMT_MAP:
        result = result.replace(alteryx_token, spark_token)
    return result
