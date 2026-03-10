"""Pydantic request models for the API."""

from __future__ import annotations

from enum import Enum


class OutputFormatParam(str, Enum):
    pyspark = "pyspark"
    dlt = "dlt"
    sql = "sql"
