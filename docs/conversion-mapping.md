# Alteryx → Databricks Conversion Mapping

This document describes how each Alteryx tool maps to generated PySpark / Databricks code, and which tools require manual intervention after conversion.

---

## Fully Converted Tools

These tools are automatically converted with high confidence (≥ 0.9).

| Alteryx Tool | PySpark / Databricks Equivalent | Notes |
|---|---|---|
| **Input Data** (CSV, TSV, TXT) | `spark.read.format("csv").option(...).load(path)` | Header, delimiter, encoding preserved |
| **Input Data** (Parquet) | `spark.read.format("parquet").load(path)` | |
| **Input Data** (JSON) | `spark.read.format("json").load(path)` | |
| **Input Data** (Avro) | `spark.read.format("avro").load(path)` | |
| **Input Data** (SAS `.sas7bdat`) | `spark.read.format("sas")...` | Requires `spark-sas7bdat` library |
| **Output Data** (CSV, Parquet, etc.) | `df.write.format(...).mode(...).save(path)` | Overwrite/append mode preserved |
| **Select** | `df.drop(...)` / `df.withColumnRenamed(...)` | Deselected and renamed columns applied |
| **Filter** | `df_pass = df.filter(expr)` / `df_fail = df.filter(~expr)` | True/False anchors split into separate DataFrames |
| **Formula** | `df.withColumn("field", expr)` | Expression translated via Alteryx→PySpark expression engine |
| **Sort** | `df.orderBy(...)` | Ascending/descending per field |
| **Sample** (First N) | `df.limit(n)` | |
| **Sample** (Random N / Percent) | `df.sample(fraction, seed)` | Seed from Alteryx deterministic setting |
| **Summarize** (GroupBy + Aggregates) | `df.groupBy(...).agg(F.sum(), F.avg(), ...)` | Count, Sum, Avg, Min, Max, CountDistinct |
| **Join** (Inner / Left / Right / Full) | `df_left.join(df_right, cond, how=...)` | Join keys extracted from XML |
| **Union** | `df1.unionByName(df2, allowMissingColumns=True)` | All inputs merged; column alignment handled |
| **Unique** (Dedup) | `df.dropDuplicates(key_fields)` | Key fields from configuration |
| **Text To Columns** | `F.split(col, delimiter)[index]` | Multi-column split via explode |
| **RegexReplace / RegexMatch** | `F.regexp_replace(...)` / `F.rlike(...)` | |
| **DateTime Parse / Format** | `F.to_timestamp(col, fmt)` / `F.date_format(col, fmt)` | Format strings preserved |
| **DateTimeAdd** | `F.date_add(col, n)` / `F.add_months(col, n)` | Days/months/years mapped to native PySpark |
| **Multi-Field Formula** | Multiple `df.withColumn(...)` calls | One per formula expression |
| **Multi-Row Formula** | `F.lag(col, n).over(window)` / `F.lead(col, n).over(window)` | Row references translated via window functions |
| **Append Fields** | `df_left.crossJoin(df_right)` or broadcast join | |
| **Running Total** | `F.sum(col).over(Window.rowsBetween(unboundedPreceding, currentRow))` | |
| **Tile** | `F.ntile(n).over(window)` | Quantile/equal-count tiles |
| **Record ID** | `F.monotonically_increasing_id()` | Sequential numbering approximated |
| **Generate Rows** | `spark.range(start, end, step)` | |
| **Comment / Annotation** | `# comment text` in notebook cell | Canvas annotations preserved as code comments |
| **Tool Container** | Skipped (layout-only) | |

---

## Partially Converted Tools (Review Required)

These tools are converted but the output needs manual review before running in Databricks.

| Alteryx Tool | Generated Code | What to Review |
|---|---|---|
| **Join** (with Select post-join) | `df.drop(...)` / `df.withColumnRenamed(...)` appended after join | Verify selected/dropped columns match intent |
| **Formula with complex expressions** | `F.lit(None)  # PLACEHOLDER` with `# TODO` block | Review expressions containing unsupported functions |
| **Filter with complex expressions** | `F.lit(True)  # PLACEHOLDER` with `# TODO` block | Replace placeholder with correct PySpark condition |
| **Summarize (Concatenate)** | `F.concat_ws(sep, col)` | Multi-row string concat may need `groupBy` + `agg` pattern |
| **Summarize (Count Missing)** | `F.sum(F.when(col.isNull(), 1).otherwise(0))` | Verify null semantics match Alteryx behavior |
| **Cross Tab** | `df.groupBy(...).pivot(...).agg(...)` | Pivot key columns must be known at runtime |
| **DateTime (edge cases)** | `F.expr(f"dateadd({unit}, {n}, col)")` | Non-standard units (quarters, weeks) use SQL fallback |

---

## Manual Conversion Required

The following Alteryx tools or configurations **cannot be automatically converted** and require manual implementation in Databricks. The converter emits `# TODO` placeholder blocks with the original Alteryx expression preserved.

### Input Sources

| Scenario | Generated Output | Manual Steps |
|---|---|---|
| **ODBC / DSN connections** | `spark.sql("""SELECT ...""")  # TODO: replace DSN with catalog.schema.table` | Replace DSN connection with Unity Catalog table reference or JDBC connection string |
| **Excel files (`.xlsx`, `.xls`)** | `# TODO: manual conversion required` + `df = None  # PLACEHOLDER` | Use `openpyxl` / `pandas` bridge or mount file to DBFS; consider converting to CSV/Parquet first |
| **Alteryx DB format (`.yxdb`)** | Low confidence warning emitted | Convert `.yxdb` to Parquet or Delta using Alteryx export before migrating |
| **Local / UNC network paths** | `# WARNING: local/network path detected` + escaped path in comment | Upload file to DBFS, Unity Catalog Volume, or cloud storage; update path in generated code |
| **`aka:` named connections** | `spark.sql("""...""")  # TODO: map connection alias` | Map Alteryx connection alias to Databricks catalog/schema or JDBC URL |

### Expression Engine Limitations

| Alteryx Feature | Behavior | Manual Steps |
|---|---|---|
| **Unknown / custom functions** | `F.expr('FunctionName(args)')`  + warning logged | Implement equivalent PySpark UDF or SQL function |
| **Complex multi-row formulas** | Window function emitted; window spec may be incorrect | Verify `PARTITION BY` / `ORDER BY` in generated window spec |
| **String interpolation in expressions** | Translated literally; dynamic field names not supported | Refactor to explicit column references |

### Output Targets

| Scenario | Generated Output | Manual Steps |
|---|---|---|
| **Excel output (`.xlsx`)** | `# TODO: manual conversion required` | Use `pandas` `.to_excel()` with Databricks file download, or export as CSV |
| **Local / UNC output paths** | `# WARNING: local/network path detected` | Change output path to DBFS (`/dbfs/...`) or Unity Catalog Volume (`/Volumes/...`) |
| **Database / ODBC output** | Basic `df.write` with warning | Configure JDBC sink or Delta table as replacement |

### Tools Not Yet Supported

| Alteryx Tool | Status | Recommended Databricks Alternative |
|---|---|---|
| **Predictive tools** (Linear Reg, Decision Tree, etc.) | Not converted — `# TODO` block emitted | Use MLflow + scikit-learn / Spark MLlib |
| **Spatial tools** (Spatial Match, Make Points, etc.) | Not converted | Use `sedona` / `mosaic` libraries |
| **Reporting / Layout tools** (Report Header, Table, etc.) | Not converted | Use Databricks Lakeview dashboards |
| **Publish to Tableau Server** | Not converted | Use Databricks Partner Connect (Tableau) |
| **Salesforce Connector** | Not converted | Use `databricks-sdk` + Salesforce REST API or Fivetran |
| **Snowflake Connector** | Not converted | Use Databricks native Snowflake connector |
| **PowerBI Connector** | Not converted | Use Power BI Databricks connector (partner integration) |
| **Google connectors** (Analytics, BigQuery, Sheets) | Not converted | Use Databricks Partner Connect or direct SDK |
| **Email tools** | Not converted | Use Databricks Workflows notification actions |
| **Directory / File Browse** | Not converted | Use `dbutils.fs.ls()` |
| **Random Records macro** (`RandomRecords.yxmc`) | Converted to `df.sample()` | Review sample fraction / seed |

---

## Expression Function Mapping

The expression engine translates Alteryx formula functions to PySpark equivalents:

| Alteryx Function | PySpark Equivalent |
|---|---|
| `IF ... THEN ... ELSEIF ... ELSE ... ENDIF` | `F.when(..., ...).when(..., ...).otherwise(...)` |
| `IIF(cond, t, f)` | `F.when(cond, t).otherwise(f)` |
| `IsNull(x)` | `x.isNull()` |
| `IsEmpty(x)` | `x.isNull() \| (x == "")` |
| `Null()` | `F.lit(None)` |
| `ToString(x, fmt)` | `F.format_string(fmt, x)` |
| `ToNumber(x)` | `x.cast("double")` |
| `ToDateTime(x)` | `F.to_timestamp(x)` |
| `DateTimeToday()` | `F.current_date()` |
| `DateTimeNow()` | `F.current_timestamp()` |
| `DateTimeFormat(dt, fmt)` | `F.date_format(dt, fmt)` |
| `DateTimeParse(s, fmt)` | `F.to_timestamp(s, fmt)` |
| `DateTimeAdd(dt, n, unit)` | `F.date_add(dt, n)` / `F.add_months(dt, n)` |
| `DateTimeDiff(dt1, dt2, unit)` | `F.datediff(dt1, dt2)` / `F.months_between(...)` |
| `Left(s, n)` | `F.substring(s, 1, n)` |
| `Right(s, n)` | `F.expr(f"right({s}, {n})")` |
| `Mid(s, start, len)` | `F.substring(s, start, len)` |
| `Length(s)` | `F.length(s)` |
| `Trim(s)` | `F.trim(s)` |
| `LTrim(s)` / `RTrim(s)` | `F.ltrim(s)` / `F.rtrim(s)` |
| `Uppercase(s)` / `Lowercase(s)` | `F.upper(s)` / `F.lower(s)` |
| `Contains(s, sub)` | `s.contains(sub)` |
| `StartsWith(s, pre)` | `s.startswith(pre)` |
| `EndsWith(s, suf)` | `s.endswith(suf)` |
| `Replace(s, find, repl)` | `F.regexp_replace(s, find, repl)` |
| `RegexReplace(s, pat, repl)` | `F.regexp_replace(s, pat, repl)` |
| `RegexMatch(s, pat)` | `s.rlike(pat)` |
| `Abs(x)` | `F.abs(x)` |
| `Ceil(x)` / `Floor(x)` | `F.ceil(x)` / `F.floor(x)` |
| `Round(x, n)` | `F.round(x, n)` |
| `Sqrt(x)` | `F.sqrt(x)` |
| `Pow(x, n)` | `F.pow(x, n)` |
| `Log(x)` / `Log10(x)` | `F.log(x)` / `F.log10(x)` |
| `Switch(val, default, v1, r1, ...)` | `F.when(val==v1, r1).when(...).otherwise(default)` |
| `[Row-1:Field]` | `F.lag(F.col("Field"), 1).over(window)` |
| `[Row+1:Field]` | `F.lead(F.col("Field"), 1).over(window)` |

---

## Confidence Scores

The converter attaches a `conversion_confidence` score (0–1) to each node:

| Score | Meaning |
|---|---|
| **1.0** | Fully converted, no known issues |
| **0.8–0.9** | Converted with minor caveats (e.g. `.yxdb` format, approximated semantics) |
| **0.7** | Database connection detected — SQL preserved but source needs remapping |
| **0.5** | Expression partially translated — `# TODO` block emitted |
| **0.0** | Tool not supported — passthrough skeleton only |

---

## Post-Conversion Checklist

After running `a2d convert`:

1. **Search for `# TODO`** in the generated notebook — each block describes what needs manual attention.
2. **Search for `# WARNING`** — flags local paths and network shares that are unreachable from Databricks.
3. **Search for `PLACEHOLDER`** — marks expressions or DataFrames that were not converted and will cause runtime errors if left unchanged.
4. **Replace ODBC `spark.sql()`** blocks with Unity Catalog table references (`catalog.schema.table`) or configure JDBC connections via Databricks secrets.
5. **Upload local files** referenced in `# WARNING: local/network path detected` blocks to DBFS or Unity Catalog Volumes.
6. **Test each DataFrame** with `.show(5)` or `.count()` before wiring up downstream nodes.
