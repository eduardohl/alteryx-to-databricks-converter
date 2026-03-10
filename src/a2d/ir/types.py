"""Data type system for Alteryx-to-Spark type mapping."""

from __future__ import annotations

from enum import Enum

from a2d.parser.schema import AlteryxDataType


class SparkDataType(Enum):
    """PySpark SQL data types."""

    STRING = "StringType()"
    INTEGER = "IntegerType()"
    SHORT = "ShortType()"
    LONG = "LongType()"
    FLOAT = "FloatType()"
    DOUBLE = "DoubleType()"
    BOOLEAN = "BooleanType()"
    DATE = "DateType()"
    TIMESTAMP = "TimestampType()"
    BINARY = "BinaryType()"
    DECIMAL = "DecimalType({precision}, {scale})"


ALTERYX_TO_SPARK_TYPE: dict[AlteryxDataType, SparkDataType] = {
    AlteryxDataType.BOOL: SparkDataType.BOOLEAN,
    AlteryxDataType.BYTE: SparkDataType.SHORT,
    AlteryxDataType.INT16: SparkDataType.SHORT,
    AlteryxDataType.INT32: SparkDataType.INTEGER,
    AlteryxDataType.INT64: SparkDataType.LONG,
    AlteryxDataType.FIXED_DECIMAL: SparkDataType.DECIMAL,
    AlteryxDataType.FLOAT: SparkDataType.FLOAT,
    AlteryxDataType.DOUBLE: SparkDataType.DOUBLE,
    AlteryxDataType.STRING: SparkDataType.STRING,
    AlteryxDataType.WSTRING: SparkDataType.STRING,
    AlteryxDataType.V_STRING: SparkDataType.STRING,
    AlteryxDataType.V_WSTRING: SparkDataType.STRING,
    AlteryxDataType.DATE: SparkDataType.DATE,
    AlteryxDataType.TIME: SparkDataType.STRING,  # No direct Spark Time type
    AlteryxDataType.DATETIME: SparkDataType.TIMESTAMP,
    AlteryxDataType.BLOB: SparkDataType.BINARY,
    # SpatialObj has no Spark equivalent
}


def map_alteryx_type(
    alteryx_type: AlteryxDataType,
    size: int | None = None,
    scale: int | None = None,
) -> str:
    """Map an Alteryx data type to its PySpark type string.

    Returns the PySpark type constructor string, e.g. ``"StringType()"``
    or ``"DecimalType(18, 2)"``.
    """
    spark_type = ALTERYX_TO_SPARK_TYPE.get(alteryx_type)
    if spark_type is None:
        return "StringType()  # Unsupported Alteryx type: " + alteryx_type.value

    if spark_type == SparkDataType.DECIMAL:
        precision = size if size is not None else 18
        sc = scale if scale is not None else 2
        return spark_type.value.format(precision=precision, scale=sc)

    return spark_type.value
