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
]

# Only convert if the string contains at least one strftime token
_ALTERYX_FMT_RE = re.compile(r"%[YymbBdHIMSpjAa]")


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
