"""Configuration data classes for the Alteryx-to-Databricks conversion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class OutputFormat(Enum):
    """Target output format for code generation."""

    PYSPARK = "pyspark"
    DLT = "dlt"
    SQL = "sql"


class CatalogMode(Enum):
    """Databricks catalog mode."""

    UNITY_CATALOG = "unity_catalog"
    HIVE_METASTORE = "hive_metastore"


@dataclass(frozen=True)
class ConversionConfig:
    """Settings that govern how Alteryx nodes are converted to IR and then to code.

    Attributes:
        input_path: Path to the .yxmd file (or directory of files).
        output_dir: Where generated code is written.
        output_format: Target code style (PySpark, DLT, SQL).
        catalog_mode: Unity Catalog or legacy Hive metastore.
        catalog_name: Default catalog for Unity Catalog mode.
        schema_name: Default schema/database name.
        generate_orchestration: Whether to emit a DatabricksWorkflow YAML.
        include_comments: Add explanatory comments to generated code.
        spark_version: Target Apache Spark version string.
        dbr_version: Target Databricks Runtime version string.
        connection_overrides: Map of Alteryx connection names to Databricks equivalents.
    """

    input_path: Path = field(default_factory=lambda: Path("."))
    output_dir: Path = field(default_factory=lambda: Path("./a2d-output"))
    output_format: OutputFormat = OutputFormat.PYSPARK
    catalog_mode: CatalogMode = CatalogMode.UNITY_CATALOG
    catalog_name: str = "main"
    schema_name: str = "default"
    generate_orchestration: bool = True
    include_comments: bool = False
    spark_version: str = "3.5"
    dbr_version: str = "14.3"
    verbose_unsupported: bool = False
    connection_overrides: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.output_format, OutputFormat):
            raise ValueError(f"output_format must be an OutputFormat enum, got {type(self.output_format).__name__}")
        if not isinstance(self.catalog_mode, CatalogMode):
            raise ValueError(f"catalog_mode must be a CatalogMode enum, got {type(self.catalog_mode).__name__}")
        if not self.catalog_name.strip():
            raise ValueError("catalog_name must not be empty")
        if not self.schema_name.strip():
            raise ValueError("schema_name must not be empty")


@dataclass(frozen=True)
class AnalysisConfig:
    """Settings for the pre-conversion analysis / assessment report.

    Attributes:
        input_path: Path to the .yxmd file (or directory).
        output_dir: Where the report is written.
        report_format: Report type (html, json, markdown).
        include_expression_detail: Show per-expression breakdown.
        batch_mode: Process all .yxmd files in a directory.
    """

    input_path: Path = field(default_factory=lambda: Path("."))
    output_dir: Path = field(default_factory=lambda: Path("./a2d-report"))
    report_format: str = "html"
    include_expression_detail: bool = True
    batch_mode: bool = False
