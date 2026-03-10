"""Abstract base class for all code generators.

Each concrete generator walks a :class:`~a2d.ir.graph.WorkflowDAG` and
produces one or more output files (PySpark notebooks, SQL scripts, JSON
workflow definitions, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from a2d.config import ConversionConfig
from a2d.ir.graph import WorkflowDAG


@dataclass
class GeneratedFile:
    """A single generated output file."""

    filename: str
    content: str
    file_type: str  # "python", "sql", "json"


@dataclass
class GeneratedOutput:
    """Collection of generated files plus metadata."""

    files: list[GeneratedFile] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


@dataclass
class NodeCodeResult:
    """Result of generating code for a single IR node."""

    code_lines: list[str] = field(default_factory=list)
    output_vars: dict[str, str] = field(default_factory=dict)
    imports: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


# Shared file format mapping (superset of all generators)
_FILE_FORMAT_MAP: dict[str, str] = {
    "csv": "csv",
    "xlsx": "com.crealytics.spark.excel",
    "xls": "com.crealytics.spark.excel",
    "yxdb": "parquet",  # yxdb -> parquet as best approximation
    "json": "json",
    "parquet": "parquet",
    "avro": "avro",
    "sas7bdat": "com.github.saurfang.sas.spark",
    "dbf": "com.github.saurfang.spark.dbf",
    "": "csv",  # default
}


class CodeGenerator(ABC):
    """Abstract base for code generators."""

    def __init__(self, config: ConversionConfig) -> None:
        self.config = config

    @abstractmethod
    def generate(self, dag: WorkflowDAG, workflow_name: str = "workflow") -> GeneratedOutput:
        """Walk the DAG and produce generated files."""
        ...

    @staticmethod
    def _get_single_input(input_vars: dict[str, str]) -> str:
        """Return the single input variable name (most common case)."""
        if "Input" in input_vars:
            return input_vars["Input"]
        if input_vars:
            return next(iter(input_vars.values()))
        return "MISSING_INPUT"

    @staticmethod
    def _map_file_format(fmt: str) -> str:
        """Map Alteryx file format strings to Spark format names."""
        return _FILE_FORMAT_MAP.get(fmt.lower(), fmt.lower()) if fmt else "csv"
