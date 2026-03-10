"""IR node hierarchy for Alteryx-to-Databricks migration.

Every Alteryx tool type is represented by a concrete :class:`IRNode` subclass.
The base class carries metadata common to all tools; subclasses add fields
specific to their semantics (e.g. join keys, aggregation specs, filter
expressions).
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# ═══════════════════════════════════════════════════════════════════════════
# Base
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class IRNode(ABC):
    """Abstract base for all IR nodes.

    Attributes:
        node_id: Unique integer identifier (matches Alteryx ToolID).
        original_tool_type: Resolved human-readable tool type (e.g. "Filter").
        original_plugin_name: Raw Alteryx plugin string.
        annotation: Optional user-supplied annotation from the canvas.
        position: (x, y) canvas position.
        conversion_confidence: 0.0 .. 1.0 indicating how reliably this node
            can be auto-converted.
        conversion_notes: Free-text notes about conversion caveats.
    """

    node_id: int
    original_tool_type: str = ""
    original_plugin_name: str = ""
    annotation: str | None = None
    position: tuple[float, float] = (0.0, 0.0)
    conversion_confidence: float = 1.0
    conversion_notes: list[str] = field(default_factory=list)
    conversion_method: str = "deterministic"  # "deterministic", "expression-engine", "template", "mapping"


# ═══════════════════════════════════════════════════════════════════════════
# IO nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ReadNode(IRNode):
    """Read data from a source (file, database, etc.)."""

    source_type: str = ""  # "file", "database", "odbc", etc.
    file_path: str = ""
    connection_string: str = ""
    table_name: str = ""
    query: str = ""
    file_format: str = ""  # csv, xlsx, yxdb, etc.
    has_header: bool = True
    delimiter: str = ","
    encoding: str = "utf-8"
    record_limit: int | None = None
    field_info: list[Any] = field(default_factory=list)


@dataclass
class WriteNode(IRNode):
    """Write data to a destination."""

    destination_type: str = ""  # "file", "database", etc.
    file_path: str = ""
    connection_string: str = ""
    table_name: str = ""
    file_format: str = ""
    write_mode: str = "overwrite"  # overwrite, append, create_new
    has_header: bool = True
    delimiter: str = ","
    encoding: str = "utf-8"
    field_info: list[Any] = field(default_factory=list)


@dataclass
class LiteralDataNode(IRNode):
    """Inline / text-input data embedded in the workflow."""

    num_fields: int = 0
    num_records: int = 0
    field_names: list[str] = field(default_factory=list)
    field_types: list[str] = field(default_factory=list)
    data_rows: list[list[str]] = field(default_factory=list)


@dataclass
class BrowseNode(IRNode):
    """Browse / preview tool -- typically becomes df.display() or omitted."""

    pass


# ═══════════════════════════════════════════════════════════════════════════
# Preparation nodes
# ═══════════════════════════════════════════════════════════════════════════


class FieldAction(Enum):
    """Actions that can be applied to a field in a Select tool."""

    SELECT = "select"
    DESELECT = "deselect"
    RENAME = "rename"
    RETYPE = "retype"
    RESIZE = "resize"
    REORDER = "reorder"


@dataclass
class FieldOperation:
    """A single field operation inside a Select node."""

    field_name: str
    action: FieldAction = FieldAction.SELECT
    rename_to: str | None = None
    new_type: str | None = None
    new_size: int | None = None
    selected: bool = True
    description: str | None = None


@dataclass
class SelectNode(IRNode):
    """Select / rename / retype / reorder columns."""

    field_operations: list[FieldOperation] = field(default_factory=list)
    select_all_unknown: bool = True


@dataclass
class FilterNode(IRNode):
    """Filter rows based on an expression."""

    expression: str = ""
    mode: str = "simple"  # "simple" or "custom"


@dataclass
class FormulaField:
    """A single formula assignment inside a Formula tool."""

    output_field: str
    expression: str
    data_type: str = ""
    size: int | None = None


@dataclass
class FormulaNode(IRNode):
    """Apply one or more formula expressions to create/update columns."""

    formulas: list[FormulaField] = field(default_factory=list)


@dataclass
class SortField:
    """Sort specification for a single field."""

    field_name: str
    ascending: bool = True


@dataclass
class SortNode(IRNode):
    """Sort the dataset."""

    sort_fields: list[SortField] = field(default_factory=list)


@dataclass
class SampleNode(IRNode):
    """Sample / limit rows."""

    sample_method: str = "first"  # "first", "last", "random", "percent", "every_nth"
    n_records: int | None = None
    percentage: float | None = None
    group_fields: list[str] = field(default_factory=list)


@dataclass
class UniqueNode(IRNode):
    """Deduplicate rows based on key fields."""

    key_fields: list[str] = field(default_factory=list)


@dataclass
class RecordIDNode(IRNode):
    """Add a sequential record ID column."""

    output_field: str = "RecordID"
    starting_value: int = 1
    output_type: str = "Int64"


@dataclass
class MultiRowFormulaNode(IRNode):
    """Apply a formula referencing rows above/below the current row."""

    expression: str = ""
    output_field: str = ""
    rows_above: int = 1
    rows_below: int = 0
    group_fields: list[str] = field(default_factory=list)
    output_type: str = ""
    output_size: int | None = None


@dataclass
class MultiFieldFormulaNode(IRNode):
    """Apply the same formula across multiple fields."""

    expression: str = ""
    fields: list[str] = field(default_factory=list)
    output_type: str = ""
    copy_output: bool = False


@dataclass
class DataCleansingNode(IRNode):
    """Clean data: trim whitespace, remove nulls, etc."""

    fields: list[str] = field(default_factory=list)
    remove_null: bool = False
    trim_whitespace: bool = False
    remove_tabs: bool = False
    remove_line_breaks: bool = False
    remove_duplicate_whitespace: bool = False
    replace_nulls_with: str | None = None
    modify_case: str | None = None  # "upper", "lower", "title"


@dataclass
class AutoFieldNode(IRNode):
    """Automatically resize field types to their minimum required size."""

    pass


@dataclass
class GenerateRowsNode(IRNode):
    """Generate rows using an iterative expression."""

    init_expression: str = ""
    condition_expression: str = ""
    loop_expression: str = ""
    output_field: str = ""
    output_type: str = "Int64"


# ═══════════════════════════════════════════════════════════════════════════
# Join nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class JoinKey:
    """A single key pair for a join operation."""

    left_field: str
    right_field: str


@dataclass
class JoinNode(IRNode):
    """Join two inputs on key fields.

    Alteryx Join produces three outputs: Left (unmatched), Join (matched),
    Right (unmatched).
    """

    join_keys: list[JoinKey] = field(default_factory=list)
    join_type: str = "inner"  # inner, left, right, full
    select_left: list[FieldOperation] = field(default_factory=list)
    select_right: list[FieldOperation] = field(default_factory=list)


@dataclass
class UnionNode(IRNode):
    """Union (concatenate) multiple inputs."""

    mode: str = "auto"  # "auto", "name", "position"
    allow_missing: bool = True


@dataclass
class FindReplaceNode(IRNode):
    """Find and replace values using a lookup input."""

    find_field: str = ""
    replace_field: str = ""
    find_mode: str = "exact"  # "exact", "starts_with", "contains", "regex"
    case_sensitive: bool = True
    append_unused: bool = False


@dataclass
class AppendFieldsNode(IRNode):
    """Cross-join (append all fields from a secondary input)."""

    allow_all_appends: bool = True
    select_target: list[FieldOperation] = field(default_factory=list)
    select_source: list[FieldOperation] = field(default_factory=list)


@dataclass
class JoinMultipleNode(IRNode):
    """Join more than two inputs simultaneously."""

    join_keys: list[JoinKey] = field(default_factory=list)
    join_type: str = "inner"
    input_count: int = 2


# ═══════════════════════════════════════════════════════════════════════════
# Parse nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class RegExNode(IRNode):
    """Apply a regular expression to parse, match, replace, or tokenize."""

    field_name: str = ""
    expression: str = ""
    mode: str = "parse"  # "parse", "match", "replace", "tokenize"
    output_fields: list[str] = field(default_factory=list)
    case_insensitive: bool = False
    replacement: str = ""


@dataclass
class TextToColumnsNode(IRNode):
    """Split a field into multiple columns or rows."""

    field_name: str = ""
    delimiter: str = ","
    split_to: str = "columns"  # "columns" or "rows"
    num_columns: int | None = None
    output_root_name: str = ""
    skip_empty: bool = False


@dataclass
class DateTimeNode(IRNode):
    """Parse, format, or compute date/time values."""

    input_field: str = ""
    output_field: str = ""
    conversion_mode: str = ""  # "parse", "format", "date_add", etc.
    format_string: str = ""
    language: str = "English"


@dataclass
class JsonParseNode(IRNode):
    """Parse JSON content from a field."""

    input_field: str = ""
    output_field: str = ""
    flatten_mode: str = "auto"  # "auto", "keys", "values"


# ═══════════════════════════════════════════════════════════════════════════
# Transform nodes
# ═══════════════════════════════════════════════════════════════════════════


class AggAction(Enum):
    """Aggregation action types for Summarize tool."""

    GROUP_BY = "GroupBy"
    SUM = "Sum"
    COUNT = "Count"
    COUNT_DISTINCT = "CountDistinct"
    MIN = "Min"
    MAX = "Max"
    AVG = "Avg"
    FIRST = "First"
    LAST = "Last"
    CONCAT = "Concat"
    STD_DEV = "StdDev"
    VARIANCE = "Variance"
    MEDIAN = "Median"
    MODE = "Mode"
    PERCENTILE = "Percentile"
    COUNT_NON_NULL = "CountNonNull"
    COUNT_NULL = "CountNull"
    SPATIAL_COMBINE = "SpatialObjCombine"


@dataclass
class AggregationField:
    """A single aggregation specification for the Summarize tool."""

    field_name: str
    action: AggAction = AggAction.GROUP_BY
    output_field_name: str | None = None
    separator: str = ","  # for Concat action
    percentile_value: float | None = None


@dataclass
class SummarizeNode(IRNode):
    """Group-by and aggregate data."""

    aggregations: list[AggregationField] = field(default_factory=list)


@dataclass
class CrossTabNode(IRNode):
    """Pivot data: rows become columns."""

    group_fields: list[str] = field(default_factory=list)
    header_field: str = ""
    value_field: str = ""
    aggregation: str = "Sum"  # Sum, Count, Avg, etc.
    separator: str = "_"


@dataclass
class TransposeNode(IRNode):
    """Unpivot data: columns become rows."""

    key_fields: list[str] = field(default_factory=list)
    data_fields: list[str] = field(default_factory=list)
    header_name: str = "Name"
    value_name: str = "Value"
    enable_key_fields: bool = True


@dataclass
class RunningField:
    """A single running-total specification."""

    field_name: str
    running_type: str = "Sum"  # Sum, Avg, Count, Min, Max
    output_field_name: str | None = None


@dataclass
class RunningTotalNode(IRNode):
    """Compute running totals / moving calculations."""

    running_fields: list[RunningField] = field(default_factory=list)
    group_fields: list[str] = field(default_factory=list)


@dataclass
class CountRecordsNode(IRNode):
    """Count the number of records in the stream."""

    output_field: str = "Count"


# ═══════════════════════════════════════════════════════════════════════════
# Developer nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class PythonToolNode(IRNode):
    """Embedded Python code."""

    code: str = ""
    mode: str = "script"  # "script" or "jupyter"


@dataclass
class DownloadNode(IRNode):
    """HTTP request / download tool."""

    url_field: str = ""
    url_static: str = ""
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    output_field: str = "DownloadData"
    connection_timeout: int = 30
    max_retries: int = 0


@dataclass
class RunCommandNode(IRNode):
    """Execute an external command / process."""

    command: str = ""
    command_arguments: str = ""
    working_directory: str = ""
    write_source: str = ""  # file path to write input to
    read_results: str = ""  # file path to read output from


# ═══════════════════════════════════════════════════════════════════════════
# Additional Preparation nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ImputationNode(IRNode):
    """Fill missing values with statistical measures or custom values."""

    fields: list[str] = field(default_factory=list)
    method: str = "mean"  # mean, median, mode, custom
    custom_value: str | None = None


# ═══════════════════════════════════════════════════════════════════════════
# Additional Parse nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class XMLParseNode(IRNode):
    """Parse XML columns and extract nodes via XPath."""

    input_field: str = ""
    xpath_expressions: list[tuple[str, str]] = field(default_factory=list)
    root_element: str = ""
    output_field: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# Additional Transform nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class TileNode(IRNode):
    """Assign rows to equal-sized tiles (quantile binning)."""

    tile_count: int = 4
    tile_field: str = ""
    group_fields: list[str] = field(default_factory=list)
    order_field: str = ""
    output_field: str = "Tile"


@dataclass
class WeightedAverageNode(IRNode):
    """Compute weighted average."""

    value_field: str = ""
    weight_field: str = ""
    group_fields: list[str] = field(default_factory=list)
    output_field: str = "WeightedAvg"


# ═══════════════════════════════════════════════════════════════════════════
# Additional IO nodes (Dynamic)
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class DynamicInputNode(IRNode):
    """Read from a dynamic list of files/sources."""

    file_path_pattern: str = ""
    file_format: str = "csv"
    template_file: str = ""


@dataclass
class DynamicOutputNode(IRNode):
    """Write to a dynamic destination based on field values."""

    file_path_expression: str = ""
    file_format: str = "csv"
    partition_field: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# Workflow control nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowControlNode(IRNode):
    """Workflow orchestration tool (BlockUntilDone, ControlParam, Action)."""

    control_type: str = ""  # "block_until_done", "control_param", "action"
    parameter_name: str = ""
    parameter_value: str = ""


@dataclass
class MacroIONode(IRNode):
    """Macro input/output boundary - maps to notebook parameters."""

    direction: str = "input"  # "input" or "output"
    field_name: str = ""
    default_value: str = ""
    question_text: str = ""
    data_type: str = ""


@dataclass
class FieldSummaryNode(IRNode):
    """Generate column-level statistics."""

    fields: list[str] = field(default_factory=list)
    statistics: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Interface / Widget nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class WidgetNode(IRNode):
    """UI widget for Alteryx Analytic Apps - maps to Databricks widgets."""

    widget_type: str = ""  # checkbox, date, dropdown, file_input, listbox, numeric, radio, textbox, tree
    field_name: str = ""
    label: str = ""
    default_value: str = ""
    options: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Cloud storage nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class CloudStorageNode(IRNode):
    """Cloud storage read/write (S3, Azure Blob, SharePoint)."""

    provider: str = ""  # "s3", "azure", "sharepoint"
    direction: str = "input"  # "input" or "output"
    bucket_or_container: str = ""
    path: str = ""
    file_format: str = "csv"
    auth_config: dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════
# Reporting nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ChartNode(IRNode):
    """Chart / visualization specification."""

    chart_type: str = ""  # bar, line, scatter, pie, etc.
    x_field: str = ""
    y_field: str = ""
    series_fields: list[str] = field(default_factory=list)
    title: str = ""


@dataclass
class ReportNode(IRNode):
    """Report element (Table, Layout, Render)."""

    report_type: str = ""  # "table", "layout", "render"
    title: str = ""
    fields: list[str] = field(default_factory=list)
    output_format: str = ""  # "pdf", "html", etc.


@dataclass
class EmailOutputNode(IRNode):
    """Send output via email."""

    to_field: str = ""
    subject_field: str = ""
    body_field: str = ""
    smtp_server: str = ""
    attachment_field: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# Spatial nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class BufferNode(IRNode):
    """Create buffer zones around spatial objects."""

    input_field: str = "SpatialObj"
    buffer_distance: float = 1.0
    buffer_units: str = "miles"
    buffer_style: str = "circle"


@dataclass
class SpatialMatchNode(IRNode):
    """Match records based on spatial relationships."""

    spatial_field_target: str = "SpatialObj"
    spatial_field_universe: str = "SpatialObj"
    match_type: str = "intersects"
    output_fields: list[str] = field(default_factory=list)


@dataclass
class CreatePointsNode(IRNode):
    """Create point objects from latitude/longitude fields."""

    lat_field: str = ""
    lon_field: str = ""
    output_field: str = "SpatialObj"


@dataclass
class DistanceNode(IRNode):
    """Calculate distance between spatial objects."""

    source_field: str = "SpatialObj"
    target_field: str = "SpatialObj"
    output_field: str = "Distance"
    distance_units: str = "miles"


@dataclass
class FindNearestNode(IRNode):
    """Find nearest spatial objects from a universe dataset."""

    target_field: str = "SpatialObj"
    universe_field: str = "SpatialObj"
    max_distance: float | None = None
    max_matches: int = 1
    distance_units: str = "miles"
    output_distance_field: str = "Distance"


@dataclass
class GeocoderNode(IRNode):
    """Convert addresses to coordinates."""

    address_field: str = ""
    city_field: str = ""
    state_field: str = ""
    zip_field: str = ""
    country_field: str = ""
    output_lat_field: str = "Latitude"
    output_lon_field: str = "Longitude"


@dataclass
class TradeAreaNode(IRNode):
    """Create trade area polygons around points."""

    input_field: str = "SpatialObj"
    radius: float = 1.0
    radius_units: str = "miles"
    ring_count: int = 1
    output_field: str = "TradeArea"


@dataclass
class MakeGridNode(IRNode):
    """Create a grid of spatial polygons."""

    extent_field: str = "SpatialObj"
    grid_size: float = 1.0
    grid_units: str = "miles"
    output_field: str = "GridCell"


# ═══════════════════════════════════════════════════════════════════════════
# Predictive / ML nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class DecisionTreeNode(IRNode):
    """Decision tree classification or regression."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    model_type: str = "classification"
    max_depth: int = 5
    output_field: str = "Prediction"


@dataclass
class ForestModelNode(IRNode):
    """Random forest classification or regression."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    model_type: str = "classification"
    num_trees: int = 100
    max_depth: int = 5
    output_field: str = "Prediction"


@dataclass
class LinearRegressionNode(IRNode):
    """Linear regression model."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    regularization: float = 0.0
    output_field: str = "Prediction"


@dataclass
class LogisticRegressionNode(IRNode):
    """Logistic regression model."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    regularization: float = 0.0
    max_iterations: int = 100
    output_field: str = "Prediction"


@dataclass
class ScoreModelNode(IRNode):
    """Score data using a previously trained model."""

    model_reference: str = ""
    feature_fields: list[str] = field(default_factory=list)
    output_field: str = "Score"


@dataclass
class BoostedModelNode(IRNode):
    """Gradient boosted tree classification or regression."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    model_type: str = "classification"
    num_iterations: int = 100
    max_depth: int = 5
    learning_rate: float = 0.1
    output_field: str = "Prediction"


@dataclass
class NaiveBayesNode(IRNode):
    """Naive Bayes classification."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    smoothing: float = 1.0
    output_field: str = "Prediction"


@dataclass
class SupportVectorMachineNode(IRNode):
    """Support vector machine classification."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    regularization: float = 1.0
    max_iterations: int = 100
    output_field: str = "Prediction"


@dataclass
class NeuralNetworkNode(IRNode):
    """Neural network classification."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    hidden_layers: list[int] = field(default_factory=lambda: [64, 32])
    max_iterations: int = 100
    output_field: str = "Prediction"


@dataclass
class GammaRegressionNode(IRNode):
    """Gamma regression (GLM with gamma family)."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    link_function: str = "log"
    output_field: str = "Prediction"


@dataclass
class CountRegressionNode(IRNode):
    """Count regression (GLM with Poisson family)."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    link_function: str = "log"
    output_field: str = "Prediction"


@dataclass
class SplineModelNode(IRNode):
    """Spline-based regression model."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    max_knots: int = 10
    output_field: str = "Prediction"


@dataclass
class StepwiseNode(IRNode):
    """Stepwise feature selection."""

    target_field: str = ""
    feature_fields: list[str] = field(default_factory=list)
    direction: str = "both"  # "forward", "backward", "both"
    output_field: str = "Prediction"


@dataclass
class KCentroidsNode(IRNode):
    """K-means clustering."""

    feature_fields: list[str] = field(default_factory=list)
    k: int = 5
    max_iterations: int = 100
    output_field: str = "Cluster"


@dataclass
class PrincipalComponentsNode(IRNode):
    """Principal Component Analysis (PCA)."""

    feature_fields: list[str] = field(default_factory=list)
    num_components: int = 5
    output_field: str = "PC"


@dataclass
class CrossValidationNode(IRNode):
    """Cross-validation wrapper for model evaluation."""

    num_folds: int = 5
    model_reference: str = ""
    output_field: str = "CV_Score"


@dataclass
class ARIMANode(IRNode):
    """ARIMA time series model."""

    time_field: str = ""
    value_field: str = ""
    p: int = 1
    d: int = 1
    q: int = 1
    output_field: str = "Forecast"


@dataclass
class ETSNode(IRNode):
    """Exponential smoothing (ETS) time series model."""

    time_field: str = ""
    value_field: str = ""
    error_type: str = "additive"
    trend_type: str = "additive"
    seasonal_type: str = "additive"
    output_field: str = "Forecast"


@dataclass
class AppendClusterNode(IRNode):
    """Append cluster assignments from a pre-trained clustering model."""

    model_reference: str = ""
    feature_fields: list[str] = field(default_factory=list)
    output_field: str = "Cluster"


@dataclass
class FindNearestNeighborsNode(IRNode):
    """K-nearest neighbors search."""

    feature_fields: list[str] = field(default_factory=list)
    k: int = 5
    distance_metric: str = "euclidean"
    output_field: str = "Neighbor"


@dataclass
class KCentroidsDiagnosticsNode(IRNode):
    """Clustering diagnostics (silhouette score, elbow analysis)."""

    feature_fields: list[str] = field(default_factory=list)
    output_field: str = "Diagnostics"


@dataclass
class LiftChartNode(IRNode):
    """Model evaluation lift chart."""

    prediction_field: str = ""
    actual_field: str = ""
    output_field: str = "Lift"


@dataclass
class ModelComparisonNode(IRNode):
    """Compare multiple models side by side."""

    model_references: list[str] = field(default_factory=list)
    output_field: str = "Comparison"


@dataclass
class ModelCoefficientsNode(IRNode):
    """Extract coefficients from a fitted regression model."""

    model_reference: str = ""
    output_field: str = "Coefficients"


@dataclass
class TSForecastNode(IRNode):
    """Time series forecast visualization."""

    time_field: str = ""
    value_field: str = ""
    forecast_field: str = ""
    output_field: str = "Forecast"


@dataclass
class MeansTestNode(IRNode):
    """Statistical t-test / test of means."""

    field_a: str = ""
    field_b: str = ""
    test_type: str = "two_sample"  # "one_sample", "two_sample", "paired"
    output_field: str = "PValue"


@dataclass
class VarianceInflationFactorsNode(IRNode):
    """Variance inflation factor analysis for multicollinearity."""

    feature_fields: list[str] = field(default_factory=list)
    output_field: str = "VIF"


@dataclass
class ABAnalysisNode(IRNode):
    """A/B test analysis."""

    treatment_field: str = ""
    response_field: str = ""
    output_field: str = "ABResult"


# ═══════════════════════════════════════════════════════════════════════════
# Special nodes
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class UnsupportedNode(IRNode):
    """Placeholder for Alteryx tools that cannot be auto-converted.

    The original configuration is preserved so that a human can inspect it
    and write a manual conversion.
    """

    original_configuration: dict = field(default_factory=dict)
    unsupported_reason: str = ""


@dataclass
class CommentNode(IRNode):
    """A comment / annotation on the canvas (no data transformation)."""

    comment_text: str = ""
    background_color: str = ""
    font_size: int = 12
