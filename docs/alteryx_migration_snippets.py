# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx → Databricks Migration Snippet Library
# MAGIC
# MAGIC This notebook contains copy-paste-ready code snippets for the patterns that the
# MAGIC **Alteryx-to-Databricks Migration Accelerator** tool cannot convert automatically.
# MAGIC
# MAGIC Each section corresponds to a `# TODO` or `# WARNING` comment you will find in the
# MAGIC generated notebook. Find the matching section below, copy the relevant cell into your
# MAGIC migrated notebook, and adapt the catalog/schema/table/path values to your environment.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Data Source Connections (`aka:`, ODBC, DSN)
# MAGIC 2. File I/O — Unity Catalog Volumes (replaces UNC/Windows paths)
# MAGIC 3. Excel Files (read and write)
# MAGIC 4. DynamicInput — Parameterized SQL per Row
# MAGIC 5. Stored Procedures (PostSQL blocks)
# MAGIC 6. Date & Type Coercion
# MAGIC 7. HTTP / DownloadTool
# MAGIC 8. RunCommand → Shell
# MAGIC 9. Iterative Macros → Python Loops
# MAGIC 10. Two-Part SQL Table Names → Unity Catalog Three-Part Names

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Data Source Connections
# MAGIC
# MAGIC **Converter flag:** `# TODO: map to Unity Catalog` on Input tool steps
# MAGIC
# MAGIC Alteryx uses named connections (`aka:UUID|||schema.table`), ODBC DSNs, and connection
# MAGIC strings that are inaccessible in Databricks. Replace them with one of the patterns below.

# COMMAND ----------

# 1a. UC managed or external table (most common — use this first)
# If the source table has already been onboarded to Rahona/Unity Catalog:
df = spark.table("catalog_name.schema_name.table_name")

# 1b. Inline SQL query against a UC table
df = spark.sql("""
    SELECT *
    FROM catalog_name.schema_name.table_name
    WHERE date_col >= '2024-01-01'
""")

# COMMAND ----------

# 1c. JDBC — source is still in an on-prem/cloud database (not yet in UC)
# Requires the JDBC driver on the cluster and network access from Databricks.
jdbc_url = "jdbc:sqlserver://hostname:1433;databaseName=your_db"
conn_props = {
    "user": dbutils.secrets.get(scope="your-scope", key="db-user"),
    "password": dbutils.secrets.get(scope="your-scope", key="db-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}
df = spark.read.jdbc(url=jdbc_url, table="schema_name.table_name", properties=conn_props)

# For a filtered read (avoids full table scan):
df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM schema_name.table_name WHERE year_col = 2024) AS t",
    properties=conn_props,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — File I/O: Unity Catalog Volumes
# MAGIC
# MAGIC **Converter flag:** `# WARNING: local/network path detected` / `# TODO: /Volumes/...`
# MAGIC
# MAGIC UNC paths (`\\server\share\...`) and Windows paths (`C:\Users\...`) are not accessible
# MAGIC from Databricks. Upload files to a Unity Catalog Volume first, then use the Volume path.
# MAGIC
# MAGIC **Path mapping pattern:**
# MAGIC ```
# MAGIC \\nasoc01\share\dept\reports\file.csv  →  /Volumes/catalog/schema/volume/reports/file.csv
# MAGIC C:\Users\analyst\data\input.xlsx       →  /Volumes/catalog/schema/volume/input.xlsx
# MAGIC ```

# COMMAND ----------

# 2a. Read CSV from UC Volume
df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/catalog/schema/volume/filename.csv"))

# Read multiple CSVs matching a pattern
df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/catalog/schema/volume/reports/*.csv"))

# COMMAND ----------

# 2b. Read Parquet from UC Volume
df = spark.read.parquet("/Volumes/catalog/schema/volume/filename.parquet")

# COMMAND ----------

# 2c. Write CSV to UC Volume (single file, no partitioning)
(df.coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/output/"))

# COMMAND ----------

# 2d. Write as Delta table — recommended over CSV for analytical workloads
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.table_name")

# Append mode:
df.write.format("delta").mode("append").saveAsTable("catalog.schema.table_name")

# With schema evolution (safe for adding new columns):
(df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("catalog.schema.table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Excel Files
# MAGIC
# MAGIC **Converter flag:** `# TODO: Excel write not supported natively in Databricks`
# MAGIC
# MAGIC Databricks does not have a built-in Excel reader/writer. Use pandas for small files
# MAGIC or the `com.crealytics.spark.excel` library for large ones (requires cluster install).

# COMMAND ----------

# 3a. Read Excel from UC Volume via pandas (small-to-medium files)
import pandas as pd

pdf = pd.read_excel(
    "/Volumes/catalog/schema/volume/file.xlsx",
    sheet_name="Sheet1",   # or sheet index 0
    dtype=str,             # read all as string first, then cast in Spark
)
df = spark.createDataFrame(pdf)

# COMMAND ----------

# 3b. Write Excel to UC Volume via pandas (small DataFrames only — collects to driver)
pdf = df.toPandas()
pdf.to_excel(
    "/Volumes/catalog/schema/volume/output.xlsx",
    sheet_name="Data",
    index=False,
)

# COMMAND ----------

# 3c. Read/write Excel with com.crealytics library (if installed on cluster)
# Install: Maven coordinate com.crealytics:spark-excel_2.12:3.4.1_0.20.3

# Read:
df = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .load("/Volumes/catalog/schema/volume/file.xlsx"))

# Write:
(df.write
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Data'!A1")
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/output.xlsx"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — DynamicInput (ModifySQL)
# MAGIC
# MAGIC **Converter flag:** `# TODO: DynamicInput (ModifySQL) cannot be represented as static SQL`
# MAGIC
# MAGIC The DynamicInput tool in ModifySQL mode executes a SQL template once per row of an
# MAGIC input DataFrame, substituting field values into the SQL at runtime.
# MAGIC
# MAGIC The PySpark output format handles this automatically. The SQL output format cannot —
# MAGIC **use PySpark output for workflows containing DynamicInput.**
# MAGIC
# MAGIC The pattern below shows the full working implementation (identical to what the converter
# MAGIC emits in PySpark mode). Adapt the SQL template and field names to your workflow.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def _to_iso_date(val):
    """Normalize a date value to ISO yyyy-MM-dd string for SQL substitution.

    Handles common date formats produced by Alteryx DateTimeFormat:
    dd-MMM-yyyy (e.g. 08-Apr-2026), mm/dd/yyyy, yyyymmdd, and ISO (pass-through).
    """
    if val is None:
        return "NULL"
    s = str(val)
    if len(s) == 10 and s[4:5] == "-" and s[7:8] == "-":
        return s  # already ISO yyyy-MM-dd
    from datetime import datetime
    for fmt in ("%d-%b-%Y", "%b-%d-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s  # return as-is if no format matched


# -- Adapt these to your workflow -----------------------------------------------
_sql_template = """
    SELECT *
    FROM catalog.schema.source_table
    WHERE report_date = '2024-01-01'
      AND region = 'PLACEHOLDER_REGION'
"""

_rows = df_input.collect()
_dfs = []

for _row in _rows:
    _sql = _sql_template
    # Date placeholders: use _to_iso_date() to normalize from any Alteryx date format
    _sql = _sql.replace("2024-01-01", _to_iso_date(_row["ReportDate"]))
    # String placeholders: use str() directly
    _sql = _sql.replace("PLACEHOLDER_REGION", str(_row["Region"]))
    _dfs.append(spark.sql(_sql))

df_result = _dfs[0] if _dfs else spark.createDataFrame([], StructType([]))
for _df in _dfs[1:]:
    df_result = df_result.unionByName(_df, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Stored Procedures (PostSQL)
# MAGIC
# MAGIC **Converter flag:** PostSQL blocks are not extracted from workflow XML (no TODO emitted
# MAGIC — they are silently skipped). Check your original Alteryx workflow for any PostSQL
# MAGIC tools and implement the equivalent logic using one of the patterns below.

# COMMAND ----------

# 5a. Call a stored procedure via JDBC and read the result set
jdbc_url = "jdbc:sqlserver://hostname:1433;databaseName=your_db"
conn_props = {
    "user": dbutils.secrets.get(scope="your-scope", key="db-user"),
    "password": dbutils.secrets.get(scope="your-scope", key="db-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}
df_sp_result = spark.read.jdbc(
    url=jdbc_url,
    table="(EXEC your_schema.your_stored_procedure @param1='value', @param2=123) AS sp_result",
    properties=conn_props,
)

# COMMAND ----------

# 5b. Rewrite SP insert/update logic as Databricks SQL (preferred — runs in UC)
spark.sql("""
    INSERT INTO catalog.schema.target_table
    SELECT
        col1,
        col2,
        CURRENT_DATE() AS load_date
    FROM catalog.schema.source_table
    WHERE status = 'PENDING'
""")

# COMMAND ----------

# 5c. Rewrite SP logic as PySpark
from pyspark.sql import functions as F

df_transformed = (df_source
    .filter(F.col("status") == "PENDING")
    .withColumn("load_date", F.current_date())
    .select("col1", "col2", "load_date"))

df_transformed.write.format("delta").mode("append").saveAsTable("catalog.schema.target_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 — Date & Type Coercion
# MAGIC
# MAGIC **Converter flag:** Type mismatches in `IF/THEN` expressions involving DateType columns.
# MAGIC These cannot be auto-fixed without schema information. The two most common patterns
# MAGIC found in TD workflows are shown below.

# COMMAND ----------

from pyspark.sql import functions as F

# 6a. "Is today?" check — DateType column vs formatted string (always returns false)
#
# BEFORE (converter output — wrong, string vs DateType):
#   F.when(F.date_format(F.current_timestamp(), "yyyy-MM-dd") == F.col("DateType"), ...)
#
# AFTER — compare DateType to DateType:
df = df.withColumn(
    "DateCheck",
    F.when(F.current_date() == F.col("DateType"), F.col("DateType"))
     .otherwise(F.lit(None).cast("date"))
)

# COMMAND ----------

# 6b. String sentinel "0" in ELSE branch of a DateType WHEN
#
# BEFORE (converter output — type mismatch, "0" is StringType):
#   F.when(condition, F.col("DateType")).otherwise(F.lit("0"))
#
# Option A — use null as the "no date" sentinel (cleanest):
df = df.withColumn(
    "DateCheck",
    F.when(F.current_date() == F.col("DateType"), F.col("DateType"))
     .otherwise(F.lit(None).cast("date"))
)

# Option B — keep "0" sentinel, cast DateType to string in THEN branch:
df = df.withColumn(
    "DateCheck",
    F.when(F.current_date() == F.col("DateType"), F.col("DateType").cast("string"))
     .otherwise(F.lit("0"))
)

# COMMAND ----------

# 6c. DateTimeNow() — timestamp vs date
#
# Alteryx DateTimeNow() translates to F.current_timestamp() (TimestampType).
# If your logic only needs the date portion, replace with F.current_date():

df = df.withColumn("today", F.current_date())                          # DateType
df = df.withColumn("now_ts", F.current_timestamp())                    # TimestampType
df = df.withColumn("today_str", F.date_format(F.current_date(), "yyyy-MM-dd"))  # StringType

# COMMAND ----------

# 6d. Concatenating a DateType column with strings
#
# DateType columns must be cast to string before F.concat():
from pyspark.sql import functions as F

# Cast to string, then concat:
df = df.withColumn(
    "FilePath",
    F.concat(
        F.lit("/Volumes/catalog/schema/volume/report_"),
        F.date_format(F.col("DateType"), "yyyyMMdd"),   # "20240408"
        F.lit(".csv")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7 — HTTP Requests (DownloadTool)
# MAGIC
# MAGIC **Converter flag:** `# TODO: Replace with requests/urllib UDF or Databricks external access`
# MAGIC
# MAGIC Requires: External network access enabled for the target host in your Databricks workspace.
# MAGIC Contact your workspace admin to allowlist the required URLs.

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import functions as F


@pandas_udf("string")
def fetch_url(urls: pd.Series) -> pd.Series:
    """Fetch the response body for each URL in the column. Returns error string on failure."""
    def get(url):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            return f"ERROR: {exc}"
    return urls.apply(get)


# Apply to a DataFrame column containing URLs:
df_with_response = df.withColumn("response_body", fetch_url(F.col("url_column")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8 — Shell Commands (RunCommand)
# MAGIC
# MAGIC **Converter flag:** `# TODO: Replace with subprocess or %sh magic in Databricks`

# COMMAND ----------

# Option 1: %sh magic cell — interactive notebooks only, not supported in Jobs
# Uncomment the cell below and adapt the command:
# %sh
# cp /Volumes/catalog/schema/volume/input.csv /tmp/staging/input.csv

# Option 2: subprocess — works in both interactive and job contexts
import subprocess

result = subprocess.run(
    ["cp", "/Volumes/catalog/schema/volume/input.csv", "/tmp/staging/input.csv"],
    capture_output=True,
    text=True,
)
if result.returncode != 0:
    raise RuntimeError(f"Shell command failed:\n{result.stderr}")

# Option 3: dbutils for common file operations within Volumes/DBFS
dbutils.fs.cp(
    "dbfs:/Volumes/catalog/schema/volume/input.csv",
    "dbfs:/tmp/staging/input.csv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9 — Iterative Macros → Python Loops
# MAGIC
# MAGIC **Converter flag:** Iterative macros are flagged as unsupported; no code is generated.
# MAGIC
# MAGIC Alteryx iterative macros run a sub-workflow repeatedly until a stopping condition is met.
# MAGIC The equivalent pattern in PySpark is a Python `while` loop that transforms a DataFrame
# MAGIC until the termination condition is satisfied.

# COMMAND ----------

from pyspark.sql import functions as F

# Pattern: process rows until none remain that need further action
max_iterations = 100
df_current = df_seed  # starting point — adapt to your workflow

for iteration in range(max_iterations):
    # Apply the iterative transformation (adapt to your business logic)
    df_next = (df_current
        .withColumn("status", F.when(F.col("value") > 100, F.lit("DONE")).otherwise(F.col("status")))
        .withColumn("value", F.col("value") * 1.1)   # example: compound growth
    )

    # Stopping condition: no more rows need processing
    remaining = df_next.filter(F.col("status") != "DONE").count()
    if remaining == 0:
        print(f"Converged after {iteration + 1} iterations")
        break

    df_current = df_next
else:
    print(f"Warning: reached max_iterations ({max_iterations}) without converging")

df_result = df_current

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10 — Two-Part SQL Table Names in DynamicInput SQL Templates
# MAGIC
# MAGIC **Converter flag:** SQL templates inside DynamicInput use two-part names (`SCHEMA.TABLE`)
# MAGIC which are invalid in Unity Catalog (requires three-part: `CATALOG.SCHEMA.TABLE`).
# MAGIC
# MAGIC Update your SQL template strings in the generated notebook — find all `FROM` and `JOIN`
# MAGIC clauses and add the catalog prefix.

# COMMAND ----------

# Pattern:
#   BEFORE: FROM RRDW_DLV.V_DLV_DEP_AGMT A
#   AFTER:  FROM your_catalog.RRDW_DLV.V_DLV_DEP_AGMT A

# To discover which catalog a table belongs to in your workspace:
spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW TABLES IN RRDW_DLV").show()

# To verify a specific table exists and inspect its metadata:
spark.sql("DESCRIBE TABLE EXTENDED your_catalog.RRDW_DLV.V_DLV_DEP_AGMT").show(truncate=False)

# Quick check — if the table is accessible:
spark.table("your_catalog.RRDW_DLV.V_DLV_DEP_AGMT").limit(1).display()
