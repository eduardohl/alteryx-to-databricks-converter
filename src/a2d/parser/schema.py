"""Data model for parsed Alteryx XML workflows and plugin name mapping."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

# ---------------------------------------------------------------------------
# Alteryx data types
# ---------------------------------------------------------------------------


class AlteryxDataType(Enum):
    """Alteryx field data types as they appear in .yxmd XML."""

    BOOL = "Bool"
    BYTE = "Byte"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    FIXED_DECIMAL = "FixedDecimal"
    FLOAT = "Float"
    DOUBLE = "Double"
    STRING = "String"
    WSTRING = "WString"
    V_STRING = "V_String"
    V_WSTRING = "V_WString"
    DATE = "Date"
    TIME = "Time"
    DATETIME = "DateTime"
    BLOB = "Blob"
    SPATIAL_OBJ = "SpatialObj"


# ---------------------------------------------------------------------------
# Field metadata
# ---------------------------------------------------------------------------


@dataclass
class FieldInfo:
    """Metadata about a single field/column in an Alteryx stream."""

    name: str
    data_type: AlteryxDataType
    size: int | None = None
    scale: int | None = None
    source: str | None = None
    description: str | None = None
    rename_to: str | None = None


# ---------------------------------------------------------------------------
# Connection model
# ---------------------------------------------------------------------------


@dataclass
class ConnectionAnchor:
    """Identifies one end of a connection (tool + anchor name)."""

    tool_id: int
    anchor_name: str


@dataclass
class ParsedConnection:
    """A directed edge between two tool anchors."""

    origin: ConnectionAnchor
    destination: ConnectionAnchor
    is_wireless: bool = False


# ---------------------------------------------------------------------------
# Node model
# ---------------------------------------------------------------------------


@dataclass
class ParsedNode:
    """A single tool/node as extracted from the .yxmd XML."""

    tool_id: int
    plugin_name: str
    tool_type: str
    category: str
    position: tuple[float, float] = (0.0, 0.0)
    configuration: dict = field(default_factory=dict)
    annotation: str | None = None
    raw_xml: str | None = None
    disabled: bool = False


# ---------------------------------------------------------------------------
# Workflow model
# ---------------------------------------------------------------------------


@dataclass
class ParsedWorkflow:
    """Top-level container for a fully parsed .yxmd file."""

    file_path: str
    alteryx_version: str
    nodes: list[ParsedNode] = field(default_factory=list)
    connections: list[ParsedConnection] = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    macro_references: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Plugin name map  (Alteryx plugin string -> (tool_type, category))
# ---------------------------------------------------------------------------

PLUGIN_NAME_MAP: dict[str, tuple[str, str]] = {
    # ── IO ──────────────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.DbFileInput.DbFileInput": ("Input", "io"),
    "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput": ("Output", "io"),
    "AlteryxBasePluginsGui.TextInput.TextInput": ("TextInput", "io"),
    "AlteryxBasePluginsGui.BrowseV2.BrowseV2": ("Browse", "io"),
    "AlteryxBasePluginsGui.DynamicInput.DynamicInput": ("DynamicInput", "developer"),
    "AlteryxBasePluginsGui.DynamicOutput.DynamicOutput": ("DynamicOutput", "developer"),
    # ── Preparation ─────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": ("Select", "preparation"),
    "AlteryxBasePluginsGui.Filter.Filter": ("Filter", "preparation"),
    "AlteryxBasePluginsGui.Formula.Formula": ("Formula", "preparation"),
    "AlteryxBasePluginsGui.Sort.Sort": ("Sort", "preparation"),
    "AlteryxBasePluginsGui.Sample.Sample": ("Sample", "preparation"),
    "AlteryxBasePluginsGui.Unique.Unique": ("Unique", "preparation"),
    "AlteryxBasePluginsGui.RecordID.RecordID": ("RecordID", "preparation"),
    "AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula": ("MultiRowFormula", "preparation"),
    "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula": ("MultiFieldFormula", "preparation"),
    "AlteryxBasePluginsGui.DataCleansing.DataCleansing": ("DataCleansing", "preparation"),
    "AlteryxBasePluginsGui.GenerateRows.GenerateRows": ("GenerateRows", "preparation"),
    "AlteryxBasePluginsGui.AutoField.AutoField": ("AutoField", "preparation"),
    "AlteryxBasePluginsGui.Imputation.Imputation": ("Imputation", "preparation"),
    # ── Join ────────────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.Join.Join": ("Join", "join"),
    "AlteryxBasePluginsGui.UnionV2.UnionV2": ("Union", "join"),
    "AlteryxBasePluginsGui.FindReplace.FindReplace": ("FindReplace", "join"),
    "AlteryxBasePluginsGui.AppendFields.AppendFields": ("AppendFields", "join"),
    "AlteryxBasePluginsGui.JoinMultiple.JoinMultiple": ("JoinMultiple", "join"),
    # ── Parse ───────────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.RegEx.RegEx": ("RegEx", "parse"),
    "AlteryxBasePluginsGui.TextToColumns.TextToColumns": ("TextToColumns", "parse"),
    "AlteryxBasePluginsGui.DateTime.DateTime": ("DateTime", "parse"),
    "AlteryxBasePluginsGui.JsonParse.JsonParse": ("JsonParse", "parse"),
    "AlteryxBasePluginsGui.XMLParse.XMLParse": ("XMLParse", "parse"),
    # ── Transform ───────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.Summarize.Summarize": ("Summarize", "transform"),
    "AlteryxBasePluginsGui.CrossTab.CrossTab": ("CrossTab", "transform"),
    "AlteryxBasePluginsGui.Transpose.Transpose": ("Transpose", "transform"),
    "AlteryxBasePluginsGui.RunningTotal.RunningTotal": ("RunningTotal", "transform"),
    "AlteryxBasePluginsGui.CountRecords.CountRecords": ("CountRecords", "transform"),
    "AlteryxBasePluginsGui.Tile.Tile": ("Tile", "transform"),
    "AlteryxBasePluginsGui.WeightedAverage.WeightedAverage": ("WeightedAverage", "transform"),
    # ── Developer ───────────────────────────────────────────────────────
    "AlteryxBasePluginsGui.PythonTool.PythonTool": ("PythonTool", "developer"),
    "AlteryxBasePluginsGui.RunCommand.RunCommand": ("RunCommand", "developer"),
    "AlteryxBasePluginsGui.Download.Download": ("Download", "developer"),
    # ── Spatial ─────────────────────────────────────────────────────────
    "AlteryxSpatialPluginsGui.Buffer.Buffer": ("Buffer", "spatial"),
    "AlteryxSpatialPluginsGui.SpatialMatch.SpatialMatch": ("SpatialMatch", "spatial"),
    "AlteryxSpatialPluginsGui.CreatePoints.CreatePoints": ("CreatePoints", "spatial"),
    "AlteryxSpatialPluginsGui.Distance.Distance": ("Distance", "spatial"),
    "AlteryxSpatialPluginsGui.FindNearest.FindNearest": ("FindNearest", "spatial"),
    "AlteryxSpatialPluginsGui.Geocoder.Geocoder": ("Geocoder", "spatial"),
    "AlteryxSpatialPluginsGui.TradeArea.TradeArea": ("TradeArea", "spatial"),
    "AlteryxSpatialPluginsGui.MakeGrid.MakeGrid": ("MakeGrid", "spatial"),
    # ── Reporting ───────────────────────────────────────────────────────
    "AlteryxReportingPluginsGui.Table.Table": ("Table", "reporting"),
    "AlteryxReportingPluginsGui.Layout.Layout": ("Layout", "reporting"),
    "AlteryxReportingPluginsGui.Render.Render": ("Render", "reporting"),
    "AlteryxReportingPluginsGui.EmailOutput.EmailOutput": ("EmailOutput", "reporting"),
    # ── Container ──────────────────────────────────────────────────────
    "AlteryxGuiToolkit.ToolContainer.ToolContainer": ("ToolContainer", "container"),
    # ── Interface ───────────────────────────────────────────────────────
    "AlteryxGuiToolkit.CheckBox.CheckBox": ("CheckBox", "interface"),
    "AlteryxGuiToolkit.DropDown.DropDown": ("DropDown", "interface"),
    "AlteryxGuiToolkit.FileInput.FileInput": ("FileInput", "interface"),
    "AlteryxGuiToolkit.ListBox.ListBox": ("ListBox", "interface"),
    "AlteryxGuiToolkit.TextBox.TextBox": ("TextBox", "interface"),
    "AlteryxGuiToolkit.NumericUpDown.NumericUpDown": ("NumericUpDown", "interface"),
    "AlteryxBasePluginsGui.MacroInput.MacroInput": ("MacroInput", "interface"),
    "AlteryxBasePluginsGui.MacroOutput.MacroOutput": ("MacroOutput", "interface"),
    # ── Workflow orchestration ────────────────────────────────────────
    "AlteryxBasePluginsGui.BlockUntilDone.BlockUntilDone": ("BlockUntilDone", "workflow"),
    "AlteryxBasePluginsGui.ControlParam.ControlParam": ("ControlParam", "workflow"),
    "AlteryxBasePluginsGui.Action.Action": ("Action", "workflow"),
    # ── Interface (additional) ────────────────────────────────────────
    "AlteryxGuiToolkit.RadioButton.RadioButton": ("RadioButton", "interface"),
    "AlteryxGuiToolkit.Tree.Tree": ("Tree", "interface"),
    "AlteryxGuiToolkit.Date.Date": ("Date", "interface"),
    # ── Parse (additional) ────────────────────────────────────────────
    "AlteryxBasePluginsGui.FieldSummary.FieldSummary": ("FieldSummary", "parse"),
    # ── Transform (additional) ────────────────────────────────────────
    "AlteryxBasePluginsGui.Arrange.Arrange": ("Arrange", "transform"),
    # ── Connectors ────────────────────────────────────────────────────
    "AlteryxConnectorPluginsGui.AmazonS3Upload.AmazonS3Upload": ("AmazonS3Upload", "connectors"),
    "AlteryxConnectorPluginsGui.AmazonS3Download.AmazonS3Download": ("AmazonS3Download", "connectors"),
    "AlteryxConnectorPluginsGui.AzureBlobOutput.AzureBlobOutput": ("AzureBlobOutput", "connectors"),
    "AlteryxConnectorPluginsGui.AzureBlobInput.AzureBlobInput": ("AzureBlobInput", "connectors"),
    "AlteryxConnectorPluginsGui.SharePointInput.SharePointInput": ("SharePointInput", "connectors"),
    # ── Reporting (additional) ────────────────────────────────────────
    "AlteryxReportingPluginsGui.Chart.Chart": ("Chart", "reporting"),
    "AlteryxReportingPluginsGui.InteractiveChart.InteractiveChart": ("InteractiveChart", "reporting"),
    # ── Predictive (supported) ─────────────────────────────────────────
    "AlteryxPredictivePluginsGui.DecisionTree.DecisionTree": ("DecisionTree", "predictive"),
    "AlteryxPredictivePluginsGui.ForestModel.ForestModel": ("ForestModel", "predictive"),
    "AlteryxPredictivePluginsGui.LinearRegression.LinearRegression": ("LinearRegression", "predictive"),
    "AlteryxPredictivePluginsGui.LogisticRegression.LogisticRegression": ("LogisticRegression", "predictive"),
    "AlteryxPredictivePluginsGui.ScoreModel.ScoreModel": ("ScoreModel", "predictive"),
    "AlteryxPredictivePluginsGui.BoostedModel.BoostedModel": ("BoostedModel", "predictive"),
    "AlteryxPredictivePluginsGui.NaiveBayes.NaiveBayes": ("NaiveBayes", "predictive"),
    "AlteryxPredictivePluginsGui.SupportVectorMachine.SupportVectorMachine": ("SupportVectorMachine", "predictive"),
    "AlteryxPredictivePluginsGui.NeuralNetwork.NeuralNetwork": ("NeuralNetwork", "predictive"),
    "AlteryxPredictivePluginsGui.GammaRegression.GammaRegression": ("GammaRegression", "predictive"),
    "AlteryxPredictivePluginsGui.CountRegression.CountRegression": ("CountRegression", "predictive"),
    "AlteryxPredictivePluginsGui.SplineModel.SplineModel": ("SplineModel", "predictive"),
    "AlteryxPredictivePluginsGui.Stepwise.Stepwise": ("Stepwise", "predictive"),
    "AlteryxPredictivePluginsGui.KCentroids.KCentroids": ("KCentroids", "predictive"),
    "AlteryxPredictivePluginsGui.PrincipalComponents.PrincipalComponents": ("PrincipalComponents", "predictive"),
    "AlteryxPredictivePluginsGui.CrossValidation.CrossValidation": ("CrossValidation", "predictive"),
    "AlteryxPredictivePluginsGui.ARIMA.ARIMA": ("ARIMA", "predictive"),
    "AlteryxPredictivePluginsGui.ETS.ETS": ("ETS", "predictive"),
    # ── Predictive (unsupported / recognized only) ─────────────────────
    "AlteryxPredictivePluginsGui.ABAnalysis.ABAnalysis": ("ABAnalysis", "predictive"),
    "AlteryxPredictivePluginsGui.ABControls.ABControls": ("ABControls", "predictive"),
    "AlteryxPredictivePluginsGui.ABTreatments.ABTreatments": ("ABTreatments", "predictive"),
    "AlteryxPredictivePluginsGui.ABTrend.ABTrend": ("ABTrend", "predictive"),
    "AlteryxPredictivePluginsGui.SurvivalAnalysis.SurvivalAnalysis": ("SurvivalAnalysis", "predictive"),
    "AlteryxPredictivePluginsGui.SurvivalScore.SurvivalScore": ("SurvivalScore", "predictive"),
    "AlteryxPredictivePluginsGui.LiftChart.LiftChart": ("LiftChart", "predictive"),
    "AlteryxPredictivePluginsGui.ModelComparison.ModelComparison": ("ModelComparison", "predictive"),
    "AlteryxPredictivePluginsGui.ModelCoefficients.ModelCoefficients": ("ModelCoefficients", "predictive"),
    "AlteryxPredictivePluginsGui.NestedTest.NestedTest": ("NestedTest", "predictive"),
    "AlteryxPredictivePluginsGui.TestOfMeans.TestOfMeans": ("TestOfMeans", "predictive"),
    "AlteryxPredictivePluginsGui.VarianceInflationFactors.VarianceInflationFactors": ("VarianceInflationFactors", "predictive"),
    "AlteryxPredictivePluginsGui.AppendCluster.AppendCluster": ("AppendCluster", "predictive"),
    "AlteryxPredictivePluginsGui.FindNearestNeighbors.FindNearestNeighbors": ("FindNearestNeighbors", "predictive"),
    "AlteryxPredictivePluginsGui.KCentroidsDiagnostics.KCentroidsDiagnostics": ("KCentroidsDiagnostics", "predictive"),
    "AlteryxPredictivePluginsGui.MBAffinity.MBAffinity": ("MBAffinity", "predictive"),
    "AlteryxPredictivePluginsGui.MBInspect.MBInspect": ("MBInspect", "predictive"),
    "AlteryxPredictivePluginsGui.MBRules.MBRules": ("MBRules", "predictive"),
    "AlteryxPredictivePluginsGui.MultidimensionalScaling.MultidimensionalScaling": ("MultidimensionalScaling", "predictive"),
    "AlteryxPredictivePluginsGui.Optimization.Optimization": ("Optimization", "predictive"),
    "AlteryxPredictivePluginsGui.SimulationSampling.SimulationSampling": ("SimulationSampling", "predictive"),
    "AlteryxPredictivePluginsGui.SimulationScoring.SimulationScoring": ("SimulationScoring", "predictive"),
    "AlteryxPredictivePluginsGui.SimulationSummary.SimulationSummary": ("SimulationSummary", "predictive"),
    "AlteryxPredictivePluginsGui.NetworkAnalysis.NetworkAnalysis": ("NetworkAnalysis", "predictive"),
    "AlteryxPredictivePluginsGui.TSForecast.TSForecast": ("TSForecast", "predictive"),
}


# ---------------------------------------------------------------------------
# Tool metadata  (conversion approach + description for each tool type)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolMetadata:
    """Static metadata about an Alteryx tool's conversion approach.

    Attributes:
        conversion_method: One of "deterministic", "expression-engine",
            "template", or "mapping".
        short_description: One-line explanation of what the conversion does.
        databricks_equivalent: The Databricks/PySpark API it maps to.
    """

    conversion_method: str
    short_description: str
    databricks_equivalent: str


TOOL_METADATA: dict[str, ToolMetadata] = {
    # ── IO ──────────────────────────────────────────────────────────────
    "Input": ToolMetadata(
        "deterministic",
        "Parses file/DB config into spark.read call",
        "spark.read / spark.table",
    ),
    "Output": ToolMetadata(
        "deterministic",
        "Maps write config to DataFrame.write",
        "DataFrame.write / saveAsTable",
    ),
    "TextInput": ToolMetadata(
        "deterministic",
        "Converts inline data to spark.createDataFrame",
        "spark.createDataFrame",
    ),
    "Browse": ToolMetadata(
        "deterministic",
        "Maps to display() for data preview",
        "display()",
    ),
    "DynamicInput": ToolMetadata(
        "deterministic",
        "Maps wildcard file reads to spark.read.load(pattern)",
        "spark.read.load with glob",
    ),
    "DynamicOutput": ToolMetadata(
        "deterministic",
        "Maps partitioned writes to partitionBy().save()",
        "DataFrame.write.partitionBy",
    ),
    # ── Preparation ─────────────────────────────────────────────────────
    "Select": ToolMetadata(
        "deterministic",
        "Maps field select/rename/drop to withColumnRenamed/drop",
        "withColumnRenamed / drop",
    ),
    "Filter": ToolMetadata(
        "expression-engine",
        "Translates Alteryx filter expressions to PySpark filter conditions",
        "DataFrame.filter",
    ),
    "Formula": ToolMetadata(
        "expression-engine",
        "Translates Alteryx formula expressions to withColumn calls",
        "DataFrame.withColumn",
    ),
    "Sort": ToolMetadata(
        "deterministic",
        "Maps sort fields and directions to orderBy",
        "DataFrame.orderBy",
    ),
    "Sample": ToolMetadata(
        "deterministic",
        "Maps sample config to limit/sample calls",
        "DataFrame.limit / sample",
    ),
    "Unique": ToolMetadata(
        "deterministic",
        "Maps unique key fields to dropDuplicates",
        "DataFrame.dropDuplicates",
    ),
    "RecordID": ToolMetadata(
        "deterministic",
        "Adds monotonically_increasing_id column",
        "monotonically_increasing_id",
    ),
    "MultiRowFormula": ToolMetadata(
        "expression-engine",
        "Translates row-reference expressions to Window functions",
        "Window functions",
    ),
    "MultiFieldFormula": ToolMetadata(
        "expression-engine",
        "Applies same expression across multiple columns",
        "withColumn loop",
    ),
    "DataCleansing": ToolMetadata(
        "deterministic",
        "Maps cleansing options to trim/upper/lower/regexp_replace",
        "trim / upper / lower",
    ),
    "AutoField": ToolMetadata(
        "deterministic",
        "No-op passthrough (Spark handles type sizing automatically)",
        "passthrough",
    ),
    "GenerateRows": ToolMetadata(
        "deterministic",
        "Maps loop expressions to spark.range or UDF",
        "spark.range",
    ),
    "Imputation": ToolMetadata(
        "deterministic",
        "Maps imputation methods to na.fill with computed statistics",
        "DataFrame.na.fill",
    ),
    "Arrange": ToolMetadata(
        "deterministic",
        "Maps field arrangement to column reorder select",
        "DataFrame.select reorder",
    ),
    # ── Join ────────────────────────────────────────────────────────────
    "Join": ToolMetadata(
        "deterministic",
        "Maps join keys and type to DataFrame.join",
        "DataFrame.join",
    ),
    "Union": ToolMetadata(
        "deterministic",
        "Maps union config to unionByName",
        "DataFrame.unionByName",
    ),
    "FindReplace": ToolMetadata(
        "deterministic",
        "Maps find/replace to join-based lookup",
        "join + coalesce",
    ),
    "AppendFields": ToolMetadata(
        "deterministic",
        "Maps to crossJoin",
        "DataFrame.crossJoin",
    ),
    "JoinMultiple": ToolMetadata(
        "deterministic",
        "Maps multi-input join to chained DataFrame.join calls",
        "chained DataFrame.join",
    ),
    # ── Parse ───────────────────────────────────────────────────────────
    "RegEx": ToolMetadata(
        "deterministic",
        "Maps regex modes to regexp_extract/regexp_replace/rlike",
        "regexp_extract / regexp_replace",
    ),
    "TextToColumns": ToolMetadata(
        "deterministic",
        "Maps split config to split/explode",
        "split / explode",
    ),
    "DateTime": ToolMetadata(
        "deterministic",
        "Maps date/time operations to to_date/date_format",
        "to_date / date_format",
    ),
    "JsonParse": ToolMetadata(
        "deterministic",
        "Maps JSON parsing to get_json_object/from_json",
        "get_json_object",
    ),
    "XMLParse": ToolMetadata(
        "deterministic",
        "Maps XPath extraction to xpath_string",
        "xpath_string",
    ),
    "FieldSummary": ToolMetadata(
        "deterministic",
        "Maps to DataFrame.describe() for column statistics",
        "DataFrame.describe",
    ),
    # ── Transform ───────────────────────────────────────────────────────
    "Summarize": ToolMetadata(
        "deterministic",
        "Maps group-by and aggregation specs to groupBy.agg",
        "groupBy / agg",
    ),
    "CrossTab": ToolMetadata(
        "deterministic",
        "Maps pivot config to groupBy.pivot.agg",
        "groupBy.pivot",
    ),
    "Transpose": ToolMetadata(
        "deterministic",
        "Maps unpivot to stack() selectExpr",
        "stack / UNPIVOT",
    ),
    "RunningTotal": ToolMetadata(
        "deterministic",
        "Maps running calculations to Window aggregate functions",
        "Window functions",
    ),
    "CountRecords": ToolMetadata(
        "deterministic",
        "Maps to DataFrame.count()",
        "DataFrame.count",
    ),
    "Tile": ToolMetadata(
        "deterministic",
        "Maps quantile binning to ntile Window function",
        "ntile Window",
    ),
    "WeightedAverage": ToolMetadata(
        "deterministic",
        "Maps to sum(val*weight)/sum(weight) aggregation",
        "weighted sum agg",
    ),
    # ── Developer ───────────────────────────────────────────────────────
    "PythonTool": ToolMetadata(
        "template",
        "Embeds original Python code as scaffolding for manual review",
        "Databricks notebook cell",
    ),
    "Download": ToolMetadata(
        "template",
        "Generates scaffolding for HTTP requests using UDF",
        "requests UDF / external access",
    ),
    "RunCommand": ToolMetadata(
        "template",
        "Generates scaffolding for subprocess or %sh magic",
        "subprocess / %sh",
    ),
    # ── Connectors ──────────────────────────────────────────────────────
    "AmazonS3Download": ToolMetadata(
        "mapping",
        "Maps S3 read config to spark.read with s3:// path",
        "spark.read(s3://)",
    ),
    "AmazonS3Upload": ToolMetadata(
        "mapping",
        "Maps S3 write config to DataFrame.write with s3:// path",
        "DataFrame.write(s3://)",
    ),
    "AzureBlobInput": ToolMetadata(
        "mapping",
        "Maps Azure Blob read to spark.read with abfss:// path",
        "spark.read(abfss://)",
    ),
    "AzureBlobOutput": ToolMetadata(
        "mapping",
        "Maps Azure Blob write to DataFrame.write with abfss:// path",
        "DataFrame.write(abfss://)",
    ),
    "SharePointInput": ToolMetadata(
        "mapping",
        "Maps SharePoint read to spark.read with connector",
        "SharePoint connector",
    ),
    # ── Third-party connectors (pattern-matched) ──────────────────────
    "PublishToTableauServer": ToolMetadata(
        "mapping",
        "Publishes data to Tableau Server; maps to Delta table with Databricks-Tableau connector",
        "Delta table / Tableau connector",
    ),
    # ── Reporting ───────────────────────────────────────────────────────
    "Table": ToolMetadata(
        "template",
        "Maps report table to display() call",
        "display()",
    ),
    "Layout": ToolMetadata(
        "template",
        "Maps report layout to display() call",
        "display()",
    ),
    "Render": ToolMetadata(
        "template",
        "Maps report render to display() or file write",
        "display() / file write",
    ),
    "Chart": ToolMetadata(
        "template",
        "Maps chart config to display() for Databricks visualization",
        "display() / plotly",
    ),
    "InteractiveChart": ToolMetadata(
        "template",
        "Maps interactive chart to display() visualization",
        "display() / plotly",
    ),
    "EmailOutput": ToolMetadata(
        "template",
        "Generates scaffolding for email via Databricks notifications",
        "Databricks alerts / smtplib",
    ),
    # ── Interface / Widgets ─────────────────────────────────────────────
    "CheckBox": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.dropdown with True/False",
        "dbutils.widgets",
    ),
    "DropDown": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.dropdown",
        "dbutils.widgets",
    ),
    "FileInput": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.text for file path",
        "dbutils.widgets",
    ),
    "ListBox": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.dropdown",
        "dbutils.widgets",
    ),
    "TextBox": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.text",
        "dbutils.widgets",
    ),
    "NumericUpDown": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.text with numeric validation",
        "dbutils.widgets",
    ),
    "RadioButton": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.dropdown",
        "dbutils.widgets",
    ),
    "Tree": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.dropdown (flattened)",
        "dbutils.widgets",
    ),
    "Date": ToolMetadata(
        "mapping",
        "Maps to dbutils.widgets.text with date validation",
        "dbutils.widgets",
    ),
    "MacroInput": ToolMetadata(
        "mapping",
        "Maps macro input parameters to dbutils.widgets",
        "dbutils.widgets / notebook params",
    ),
    "MacroOutput": ToolMetadata(
        "mapping",
        "Maps macro output to notebook return value",
        "dbutils.notebook.exit",
    ),
    # ── Container ──────────────────────────────────────────────────────
    "ToolContainer": ToolMetadata(
        "unsupported",
        "Visual grouping container — no data transformation; children are processed independently",
        "no-op (visual only)",
    ),
    # ── Workflow ─────────────────────────────────────────────────────────
    "BlockUntilDone": ToolMetadata(
        "template",
        "No direct equivalent; generates comment placeholder",
        "Workflows task dependency",
    ),
    "ControlParam": ToolMetadata(
        "template",
        "Maps to notebook widget parameter",
        "dbutils.widgets",
    ),
    "Action": ToolMetadata(
        "template",
        "Maps to notebook action placeholder",
        "Databricks Workflows",
    ),
    # ── Spatial ─────────────────────────────────────────────────────────
    "Buffer": ToolMetadata(
        "mapping",
        "Maps buffer zone creation to Mosaic st_buffer / H3 functions",
        "Mosaic st_buffer / H3",
    ),
    "SpatialMatch": ToolMetadata(
        "mapping",
        "Maps spatial relationship matching to Mosaic st_intersects/st_contains",
        "Mosaic st_intersects",
    ),
    "CreatePoints": ToolMetadata(
        "mapping",
        "Maps lat/lon fields to struct point column or Mosaic st_point",
        "Mosaic st_point / struct",
    ),
    "Distance": ToolMetadata(
        "mapping",
        "Maps distance calculation to Haversine UDF or Mosaic st_distance",
        "Mosaic st_distance / Haversine",
    ),
    "FindNearest": ToolMetadata(
        "mapping",
        "Maps nearest-neighbor search to H3 indexing with distance ranking",
        "H3 index + distance rank",
    ),
    "Geocoder": ToolMetadata(
        "mapping",
        "Generates scaffolding for geocoding via external API UDF",
        "Geocoding API UDF",
    ),
    "TradeArea": ToolMetadata(
        "mapping",
        "Maps trade area polygon creation to Mosaic st_buffer or H3 k-ring",
        "Mosaic st_buffer / H3 k-ring",
    ),
    "MakeGrid": ToolMetadata(
        "mapping",
        "Maps grid creation to H3 polyfill or Mosaic grid functions",
        "H3 polyfill / Mosaic grid",
    ),
    # ── Predictive ──────────────────────────────────────────────────────
    "DecisionTree": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib DecisionTreeClassifier/Regressor pipeline",
        "MLlib DecisionTree",
    ),
    "ForestModel": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib RandomForestClassifier/Regressor pipeline",
        "MLlib RandomForest",
    ),
    "LinearRegression": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib LinearRegression pipeline",
        "MLlib LinearRegression",
    ),
    "LogisticRegression": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib LogisticRegression pipeline",
        "MLlib LogisticRegression",
    ),
    "ScoreModel": ToolMetadata(
        "mapping",
        "Maps to model.transform() for scoring with a trained model",
        "MLlib model.transform",
    ),
    "BoostedModel": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib GBTClassifier/Regressor pipeline",
        "MLlib GBTClassifier/Regressor",
    ),
    "NaiveBayes": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib NaiveBayes classifier pipeline",
        "MLlib NaiveBayes",
    ),
    "SupportVectorMachine": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib LinearSVC pipeline",
        "MLlib LinearSVC",
    ),
    "NeuralNetwork": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib MultilayerPerceptronClassifier pipeline",
        "MLlib MultilayerPerceptronClassifier",
    ),
    "GammaRegression": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib GeneralizedLinearRegression with gamma family",
        "MLlib GLM (gamma)",
    ),
    "CountRegression": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib GeneralizedLinearRegression with poisson family",
        "MLlib GLM (poisson)",
    ),
    "SplineModel": ToolMetadata(
        "mapping",
        "Spline regression - passthrough stub with TODO for manual conversion",
        "Manual (spline regression)",
    ),
    "Stepwise": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib ChiSqSelector for feature selection",
        "MLlib ChiSqSelector",
    ),
    "KCentroids": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib KMeans clustering pipeline",
        "MLlib KMeans",
    ),
    "PrincipalComponents": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib PCA for dimensionality reduction",
        "MLlib PCA",
    ),
    "CrossValidation": ToolMetadata(
        "mapping",
        "Maps to Spark MLlib CrossValidator for model evaluation",
        "MLlib CrossValidator",
    ),
    "ARIMA": ToolMetadata(
        "mapping",
        "Time series ARIMA - passthrough stub referencing Prophet/pandas UDF",
        "Prophet / pandas UDF",
    ),
    "ETS": ToolMetadata(
        "mapping",
        "Exponential smoothing ETS - passthrough stub referencing Prophet/pandas UDF",
        "Prophet / pandas UDF",
    ),
    # ── Predictive (unsupported) ───────────────────────────────────────
    "ABAnalysis": ToolMetadata(
        "mapping",
        "Maps A/B test analysis to pandas UDF with scipy.stats",
        "pandas UDF / scipy.stats",
    ),
    "ABControls": ToolMetadata(
        "unsupported",
        "A/B test controls - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "ABTreatments": ToolMetadata(
        "unsupported",
        "A/B test treatments - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "ABTrend": ToolMetadata(
        "unsupported",
        "A/B trend analysis - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "SurvivalAnalysis": ToolMetadata(
        "unsupported",
        "Survival analysis model - no direct MLlib equivalent",
        "Manual conversion",
    ),
    "SurvivalScore": ToolMetadata(
        "unsupported",
        "Score survival model - no direct MLlib equivalent",
        "Manual conversion",
    ),
    "LiftChart": ToolMetadata(
        "mapping",
        "Maps lift chart computation to binned prediction analysis",
        "PySpark binning + aggregation",
    ),
    "ModelComparison": ToolMetadata(
        "mapping",
        "Maps model comparison to MLflow metric comparison",
        "MLflow model comparison",
    ),
    "ModelCoefficients": ToolMetadata(
        "mapping",
        "Maps coefficient extraction to MLlib model.coefficients access",
        "MLlib model.coefficients",
    ),
    "NestedTest": ToolMetadata(
        "unsupported",
        "Nested model significance test - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "TestOfMeans": ToolMetadata(
        "mapping",
        "Maps t-test / test of means to pandas UDF with scipy.stats.ttest",
        "pandas UDF / scipy.stats",
    ),
    "VarianceInflationFactors": ToolMetadata(
        "mapping",
        "Maps VIF analysis to pandas UDF with statsmodels variance_inflation_factor",
        "pandas UDF / statsmodels VIF",
    ),
    "AppendCluster": ToolMetadata(
        "mapping",
        "Maps cluster assignment append to MLlib KMeansModel.transform",
        "MLlib KMeansModel.transform",
    ),
    "FindNearestNeighbors": ToolMetadata(
        "mapping",
        "Maps K-NN search to BucketedRandomProjectionLSH approximate NN",
        "MLlib BucketedRandomProjectionLSH",
    ),
    "KCentroidsDiagnostics": ToolMetadata(
        "mapping",
        "Maps clustering diagnostics to ClusteringEvaluator silhouette score",
        "MLlib ClusteringEvaluator",
    ),
    "MBAffinity": ToolMetadata(
        "unsupported",
        "Market basket affinity analysis - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "MBInspect": ToolMetadata(
        "unsupported",
        "Market basket inspection - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "MBRules": ToolMetadata(
        "unsupported",
        "Market basket association rules - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "MultidimensionalScaling": ToolMetadata(
        "unsupported",
        "MDS dimensionality reduction - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "Optimization": ToolMetadata(
        "unsupported",
        "Prescriptive optimization tool - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "SimulationSampling": ToolMetadata(
        "unsupported",
        "Monte Carlo simulation sampling - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "SimulationScoring": ToolMetadata(
        "unsupported",
        "Monte Carlo simulation scoring - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "SimulationSummary": ToolMetadata(
        "unsupported",
        "Monte Carlo simulation summary - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "NetworkAnalysis": ToolMetadata(
        "unsupported",
        "Social network analysis - no direct Databricks equivalent",
        "Manual conversion",
    ),
    "TSForecast": ToolMetadata(
        "mapping",
        "Maps time series forecast to Prophet / pandas UDF",
        "Prophet / pandas UDF",
    ),
}
