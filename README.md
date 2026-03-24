# alteryx2databricks (a2d)

**Automatically convert Alteryx workflows into runnable Databricks code — no Alteryx license required.**

Upload a `.yxmd` file. Get back a PySpark notebook, Delta Live Tables pipeline, or SQL script ready to run in Databricks.

---

## Table of Contents

1. [What is this?](#what-is-this)
2. [What can it convert?](#what-can-it-convert)
3. [What still needs manual work?](#what-still-needs-manual-work)
4. [Getting Started](#getting-started)
5. [Recent Improvements](#recent-improvements)
6. [How it works](#how-it-works)
7. [CLI Reference](#cli-reference)
8. [Development](#development)
9. [License](#license)

---

## What is this?

Large organizations use Alteryx to build data pipelines visually. Moving those pipelines to Databricks usually means rewriting them from scratch — which takes months.

**a2d** reads your Alteryx `.yxmd` workflow files and automatically generates equivalent Databricks code:

- **What you save:** weeks of manual rewriting per workflow
- **What you get:** PySpark notebooks, Delta Live Tables pipelines, Databricks SQL, and Workflow JSON — all from a single command or web upload
- **What it handles:** 33+ Alteryx tools, 80+ formula functions, database connections, expressions, joins, aggregations, and more

> **You do not need Alteryx installed** to run this tool.

---

## What can it convert?

The tool automatically converts the following Alteryx tools into equivalent Databricks / PySpark code:

### Reading and Writing Data

| Alteryx Tool | What it becomes in Databricks |
|---|---|
| Input Data (CSV, Parquet, JSON, Avro) | `spark.read.format(...).load(path)` |
| Input Data (database / ODBC query) | `spark.sql("""SELECT ...""")` with a TODO to map the connection |
| Output Data | `df.write.format(...).save(path)` |
| Text Input (inline data) | `spark.createDataFrame([...])` |
| Browse | `display(df)` |
| Dynamic Input (ModifySQL) | A Python loop that runs one parameterized SQL query per input row |

### Preparing Data

| Alteryx Tool | What it becomes in Databricks |
|---|---|
| Select (rename/drop columns) | `df.drop(...)` / `df.withColumnRenamed(...)` |
| Filter | `df.filter(condition)` — True/False outputs split into two DataFrames |
| Formula | `df.withColumn("field", expression)` |
| Multi-Field Formula | Multiple `withColumn` calls in one step |
| Multi-Row Formula | Window functions (`F.lag`, `F.lead`) |
| Sort | `df.orderBy(...)` |
| Sample (first N / random / percent) | `df.limit(n)` or `df.sample(fraction)` |
| Unique / Deduplicate | `df.dropDuplicates(key_fields)` |
| Data Cleansing | Trim, null handling, case conversion |
| Record ID | `F.monotonically_increasing_id()` |
| Imputation | Missing value fill logic |

### Combining Data

| Alteryx Tool | What it becomes in Databricks |
|---|---|
| Join (inner/left/right/full) | `df_left.join(df_right, condition, how=...)` |
| Union | `df1.unionByName(df2, allowMissingColumns=True)` |
| Append Fields | Cross join |
| Find Replace | Lookup-based replacement |

### Transforming Data

| Alteryx Tool | What it becomes in Databricks |
|---|---|
| Summarize (group + aggregate) | `df.groupBy(...).agg(F.sum(), F.avg(), ...)` |
| Cross Tab (Pivot) | `df.groupBy(...).pivot(...).agg(...)` |
| Transpose (Unpivot) | `stack()` expression |
| Running Total | Window function with cumulative sum |
| Tile / Quantile binning | `F.ntile(n).over(window)` |

### Parsing

| Alteryx Tool | What it becomes in Databricks |
|---|---|
| RegEx (match / replace / parse) | `F.rlike(...)` / `F.regexp_replace(...)` |
| Text to Columns | `F.split(col, delimiter)` |
| DateTime parse/format | `F.to_timestamp(...)` / `F.date_format(...)` |
| JSON Parse | `F.get_json_object(...)` |

### Formula Functions

The expression engine translates **80+ Alteryx formula functions** to PySpark equivalents, including:

| Category | Examples |
|---|---|
| String (24) | `Contains`, `Left`, `Right`, `Trim`, `Replace`, `RegexMatch`, `Substring` |
| Math (21) | `Abs`, `Round`, `Ceil`, `Floor`, `Sqrt`, `Pow`, `Log`, `Mod`, `Rand` |
| Date/Time (15) | `DateTimeNow`, `DateTimeAdd`, `DateTimeDiff`, `DateTimeFormat`, `DateTimeParse` |
| Conversion (9) | `ToNumber`, `ToInteger`, `ToString`, `ToDate`, `ToDateTime` |
| Test / Null (8) | `IsNull`, `IsEmpty`, `IsNumber`, `Coalesce`, `IIF`, `Null`, `IfNull` |
| Conditional | `IF/THEN/ELSEIF/ELSE/ENDIF`, `IIF`, `Switch` |

---

## What still needs manual work?

Some Alteryx features cannot be automatically converted. The tool will always flag these clearly with a `# TODO` comment in the generated code — it will never silently skip them or produce broken code.

| Category | What you'll need to do |
|---|---|
| **Database connections** (ODBC/DSN) | The SQL is preserved; you replace the connection string with a Unity Catalog table name or Databricks JDBC URL |
| **Excel files** (`.xlsx` / `.xls`) | Upload the file to DBFS or a Unity Catalog Volume, then replace the placeholder with a proper read call |
| **Local / network paths** (`\\server\share\...`) | Upload files to cloud storage; the tool flags every occurrence with a `# WARNING` comment |
| **Predictive / ML tools** | Use MLflow + scikit-learn or Spark MLlib |
| **Spatial tools** | Use Sedona or Mosaic libraries |
| **Reporting / Layout tools** | Use Databricks Lakeview dashboards |
| **Email / Publish tools** | Use Databricks Workflows notification actions |
| **R Tool** | Rewrite R code in Python/PySpark |
| **Iterative macros** | Require manual rewrite as Databricks Workflows |
| **Custom third-party Alteryx tools** | Require bespoke conversion |

> **Tip:** After converting, search the output file for `# TODO` and `# WARNING` — every item that needs attention is marked there.

---

## Getting Started

Choose the option that matches your comfort level:

### Option A: Web UI — recommended if you're not a developer

No command line needed. Just install Python, run two commands, and use your browser.

**Prerequisites:** Python 3.10+ ([download](https://www.python.org/downloads/))

**Windows (PowerShell):**
```powershell
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# If step above fails with a "scripts disabled" error, run this once first:
# Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# 2. Install and launch
pip install ".[streamlit]"
python -m streamlit run streamlit_app.py
```

**Mac / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ".[streamlit]"
python -m streamlit run streamlit_app.py
```

Then open **http://localhost:8501** in your browser. Upload a `.yxmd` file and download the generated code.

---

### Option B: Command Line

For users comfortable with a terminal.

```bash
# Install (CLI only — no web UI)
pip install "."

# Convert a single workflow
python -m a2d.cli convert my_workflow.yxmd -o output/

# Convert to Delta Live Tables instead of PySpark
python -m a2d.cli convert my_workflow.yxmd -o output/ -f dlt

# Convert all workflows in a folder
python -m a2d.cli convert workflows/ -o output/

# Generate a migration readiness report
python -m a2d.cli analyze workflows/ -o report/
```

After conversion, the CLI automatically validates the generated Python syntax and prints:
```
  Written: output/my_workflow.py
  ✓ Syntax OK: my_workflow.py
```

> **Windows note:** Use `python -m a2d.cli` (not just `a2d`) to avoid PATH issues.

---

### Option C: Advanced deployment

<details>
<summary><strong>Docker</strong></summary>

```bash
docker build -t a2d .
docker run -p 8000:8000 a2d
```
Open http://localhost:8000.
</details>

<details>
<summary><strong>React Web UI (requires Node.js 18+)</strong></summary>

```bash
pip install ".[server]"
cd frontend && npm install && npm run build && cd ..
PYTHONPATH=src:. uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload
```

Open http://localhost:8000. Includes DAG visualization, batch WebSocket progress, conversion history, and a tool support matrix.
</details>

<details>
<summary><strong>Databricks Apps</strong></summary>

```bash
# Sync files to your workspace
databricks workspace import-dir src /Workspace/Users/<you>/a2d/src --overwrite
databricks workspace import-dir server /Workspace/Users/<you>/a2d/server --overwrite
databricks workspace import-dir frontend/dist /Workspace/Users/<you>/a2d/frontend/dist --overwrite
databricks workspace import-dir demo /Workspace/Users/<you>/a2d/demo --overwrite

for f in app.yaml pyproject.toml requirements.txt; do
  databricks workspace import "/Workspace/Users/<you>/a2d/$f" --file "$f" --format AUTO --overwrite
done

# Create and deploy
databricks apps create a2d --description "Alteryx to Databricks Migration Accelerator"
databricks apps deploy a2d --source-code-path /Workspace/Users/<you>/a2d
```

**Environment variables** (all optional):

| Variable | Default | Description |
|---|---|---|
| `A2D_CORS_ORIGINS` | `["http://localhost:5173"]` | Allowed CORS origins |
| `A2D_MAX_UPLOAD_SIZE_BYTES` | `52428800` (50 MB) | Max upload file size |
| `A2D_MAX_BATCH_FILES` | `50` | Max files per batch |
| `A2D_LOG_LEVEL` | `info` | Logging level |
| `A2D_DATABASE_URL` | `""` (disabled) | PostgreSQL URL for conversion history |
| `PORT` | `8000` | Server port |
</details>

---

## Recent Improvements

### v1.3 — Runtime correctness fixes (AppendFields, Formula literals, Sample, TextToColumns)

| # | What changed | Plain English | Technical detail |
|---|---|---|---|
| 20 | AppendFields no longer crashes at runtime | Workflows using AppendFields now generate correct code — previously the target DataFrame variable was always `MISSING_TARGET`, causing a `NameError` the moment the notebook ran | Alteryx XML uses `Connection="Targets"` (plural) for the target anchor; generator was looking up `"Target"` (singular). Fixed the lookup fallback chain in `_generate_AppendFieldsNode` |
| 21 | Formula integer constants wrapped in `F.lit()` | A formula like `500` or `1` used as a constant field value now generates `F.lit(500)` instead of a bare `500`, which PySpark accepts as a valid column expression | Number literal detection added in `_generate_FormulaNode` and `_generate_MultiFieldFormulaNode` after translation — bare number regex `^-?\d+(\.\d+)?$` wraps the result in `F.lit()`; translator unchanged so function args like `F.round(col, 2)` are unaffected |
| 22 | Sample tool no longer clashes with PySpark `min`/`max` | Random sampling code no longer uses Python's `min()`/`max()` builtins, which can be shadowed by PySpark's aggregation functions when using `from pyspark.sql.functions import *` | Replaced `min(1.0, n * 2 / max(1, df.count()))` with explicit Python conditionals using `_count` and `_frac` variables |
| 23 | TextToColumns uses a single `withColumns({...})` call | Splitting a column into N parts now emits one plan node instead of N+2 separate `withColumn` reassignments | `_split_N` expression is indexed directly (e.g. `_split_79[0]`) inside `withColumns({...})`; the intermediate `_split_arr` column and the trailing `drop()` are eliminated entirely |

---

### v1.2.1 — Spark best-practice code quality (withColumns, chaining, no-op passthrough)

| # | What changed | Plain English | Technical detail |
|---|---|---|---|
| 15 | MultiFieldFormula uses `withColumns({...})` | Applying the same expression across multiple fields now emits one Catalyst plan node instead of N separate `withColumn` reassignments | Same pattern as `DataCleansingNode` — all field expressions collected into a dict and emitted as a single `withColumns({...})` call (PySpark 3.3+ / DBR 11.3+) |
| 16 | Select node uses a single chained expression | Column renames and drops are chained on one line: `(inp.withColumnRenamed(...).drop(...))` | Eliminates `df_out = inp` init line + repeated `df_out = df_out.withColumnRenamed(...)` reassignments |
| 17 | Join post-ops chained and drops batched | Post-join renames and drops are part of one fluent chain; all drops are batched into a single `.drop("a", "b")` call | `_generate_JoinNode` now collects all post-ops and emits a single parenthesised chain expression |
| 18 | Join with no parseable keys flagged with TODO | When join keys can't be extracted from the workflow, a `# TODO` comment appears before `F.lit(True)` — previously the cross-join was silent | `no_keys_warning` flag triggers a comment + warning entry in the conversion report |
| 19 | AutoField is a true no-op | AutoField (automatic type-sizing) no longer emits `df_N = df_M` — it passes the input variable directly to downstream nodes | `_generate_AutoFieldNode` returns `{"Output": inp}` with a comment; no variable assignment emitted |

---

### v1.2 — Dynamic inputs, cleaner code, auto-validation

| # | What changed | Plain English | Technical detail |
|---|---|---|---|
| 9 | DynamicInput (ModifySQL) | Workflows that loop over a SQL query — running it once per row with different parameters — now generate a correct Python `for` loop that substitutes values and calls `spark.sql()` each time, then unions the results | `DynamicInputNode` IR extended with `mode`, `template_query`, `template_connection`, `modifications`; converter extracts `InputConfiguration/Configuration/File` + `Modifications/Modify` list; generator emits per-row loop with `.replace()` substitutions |
| 10 | No more redundant DataFrame chains | Tools that don't convert (e.g. BlockUntilDone, Email) no longer create `df3 = df2`, `df4 = df3` chains in the output — downstream code references the original DataFrame directly, producing shorter and cleaner notebooks | `_unsupported_passthrough` now returns `{"Output": inp}` instead of emitting `df_N = df_M`; `_generate_WorkflowControlNode` applies the same pattern |
| 11 | Automatic syntax validation | Every time you convert a workflow, the CLI immediately tells you whether the output is syntactically valid Python — no need to run a separate check | `_write_output` in `cli.py` now calls `SyntaxValidator` on each `.py` file; Streamlit shows an inline green/red badge per file |
| 12 | No-op Select nodes pass through directly | A Select node that only reorders columns (no renames, no drops) no longer emits `df_7 = df_6` — it contributes zero lines to the output | `_generate_SelectNode` returns `{"Output": inp}` immediately when `renames` and `drops` are both empty |
| 13 | Formula and MultiFieldFormula: no dead init line | Both generators no longer open with `df_out = df_in` before the first `.withColumn()` — the first expression chains directly from the input variable | Formula: `src = inp if not lines else out_var` tracks the chain source; MultiFieldFormula matched |
| 14 | Fan-out caching | Nodes whose output feeds two or more downstream steps now emit `.cache()` automatically | After node code is generated, `len(dag.get_successors(node_id)) >= 2` triggers a `.cache()` line |

---

### v1.1 — Database connections, expressions, and file handling

| # | What changed | Plain English | Technical detail |
|---|---|---|---|
| 1 | Database queries fully preserved | ODBC/DSN database inputs now include the complete SQL query in the output, not just the connection name | `InputDataConverter` splits `odbc:DSN=...|||SELECT ...` on `\|\|\|` and routes to `spark.sql("""...""")` generator path |
| 2 | Multi-line annotations | Workflow canvas notes that span multiple lines now appear correctly as multi-line comments in the output | `_generate_CommentNode` uses `splitlines()` + `join` to prefix every line with `#` |
| 3 | Date arithmetic | `DateTimeAdd` now generates correct PySpark date functions instead of broken placeholder code | `__DATEADD__` sentinel in expression engine dispatches to `_translate_dateadd_pyspark`, which emits `F.date_add()` / `F.add_months()` for common units |
| 4 | `Null()` expressions | Using `Null()` in a formula no longer crashes the conversion | Tokenizer now checks if `NULL`/`TRUE`/`FALSE` is followed by `(` before treating it as a keyword; if so, it's tokenized as a `FUNCTION` |
| 5 | Safe placeholders for untranslatable expressions | When a filter or formula expression can't be converted, the output now contains a clear `# TODO` block and a safe `F.lit(True)` or `F.lit(None)` placeholder — instead of invalid code that would crash at runtime | `_generate_FilterNode` and `_generate_FormulaNode` catch `BaseTranslationError` / `ParserError` and emit structured fallback with the original expression preserved as a comment |
| 6 | Excel files handled correctly | `.xlsx`/`.xls` inputs now produce a clear TODO comment instead of silently failing or generating code with a broken path | `_detect_format` strips `\|\|\|` sheet-selector suffix before `splitext`; `_generate_ReadNode` branches on `raw_fmt in ("xlsx", "xls")` before `_map_file_format` has a chance to rename it |
| 7 | Alteryx built-in macros recognised | The `RandomRecords` built-in macro (used when limiting records) is now correctly detected and converted to `df.sample()` | `_MACRO_TOOL_MAP` in `node_parser.py` maps `randomrecords.yxmc` → `("Sample", "preparation")`; `SampleConverter` updated to handle the `Value` list config format |
| 8 | Windows network path warnings | Paths pointing to network drives (`\\server\share\...`) now include a `# WARNING` comment explaining the file must be uploaded to cloud storage first, and backslashes are escaped so the generated code is always syntactically valid | `path.replace("\\", "\\\\")` applied before embedding in string literals; UNC detection triggers warning comment block in both `_generate_ReadNode` and `_generate_WriteNode` |

---

## How it works

> This section is for the technically curious. You don't need to read it to use the tool.

### Architecture

Every conversion goes through two phases:

```
  .yxmd file
       │
       ▼
  ┌──────────┐     ┌──────────────────┐     ┌─────────────┐     ┌──────────────┐
  │  Parser  │────▶│  Converter + IR  │────▶│  Generator  │────▶│ Output files │
  │ XML→DAG  │     │ Tool-specific    │     │ Format-     │     │ .py / .sql / │
  │          │     │ ParsedNode→      │     │ specific    │     │ .json        │
  └──────────┘     │ IRNode           │     └─────────────┘     └──────────────┘
                   └──────────────────┘
```

**Phase 1 — Parse:** The `.yxmd` XML is read and turned into a typed `WorkflowDAG` of `ParsedNode` objects. Each Alteryx plugin name is mapped to a human-readable tool type via `PLUGIN_NAME_MAP`.

**Phase 2 — Convert → Generate:** Each node is converted to a typed IR node (e.g. `FilterNode`, `JoinNode`) by a tool-specific converter in `ConverterRegistry`. Expression strings are tokenized, parsed into an AST, and translated to PySpark or SQL. The IR DAG is then walked in topological order by the target generator (PySpark, DLT, SQL, or Workflow JSON).

### Expression Engine

The expression engine is a full recursive-descent parser. It handles:
- All standard operators (`+`, `-`, `*`, `/`, `%`, `==`, `!=`, `<`, `>`, `AND`, `OR`, `NOT`)
- Field references (`[FieldName]`)
- Row references (`[Row-1:Field]`, `[Row+1:Field]`) → translated to `F.lag` / `F.lead` window functions
- `IF/THEN/ELSEIF/ELSE/ENDIF` → `F.when(...).when(...).otherwise(...)`
- `IN(val, list...)` → `.isin(...)`
- Nested function calls (80+ functions — see [What can it convert?](#what-can-it-convert))
- `Switch` → chained `F.when` expressions

When an expression can't be translated, a `# TODO` placeholder is emitted with the original Alteryx expression preserved as a comment.

---

## CLI Reference

### `python -m a2d.cli convert`

| Option | Default | Description |
|---|---|---|
| `INPUT_PATH` (required) | — | Path to `.yxmd` file or directory |
| `-o`, `--output-dir` | `./a2d-output` | Output directory |
| `-f`, `--format` | `pyspark` | Output format: `pyspark`, `dlt`, `sql` |
| `--catalog` | `main` | Unity Catalog name |
| `--schema` | `default` | Schema name |
| `--no-orchestration` | `False` | Skip Workflow JSON generation |
| `--comments` | `False` | Include explanatory comments in generated code |
| `--verbose-unsupported` | `False` | Emit detailed TODO stubs for unsupported nodes |
| `-v`, `--verbose` | `False` | Enable verbose logging |

### `python -m a2d.cli analyze`

| Option | Default | Description |
|---|---|---|
| `INPUT_PATH` (required) | — | Path to `.yxmd` file or directory |
| `-o`, `--output-dir` | `./a2d-report` | Report output directory |
| `--format` | `html` | Report format: `html`, `json`, `both` |

### `python -m a2d.cli validate`

| Option | Default | Description |
|---|---|---|
| `GENERATED_CODE` (required) | — | Path to generated `.py` file to validate |

### `python -m a2d.cli list-tools`

| Option | Default | Description |
|---|---|---|
| `-s`, `--supported` | `False` | Show only supported tools |

---

## Development

### Setup

```bash
# Install all dev dependencies (pytest, mypy, ruff, etc.)
make dev
# or: pip install -e ".[all]"
```

### Commands

```bash
make test        # Run all tests
make test-cov    # Run tests with coverage report
make lint        # Lint with ruff
make format      # Format with ruff
make typecheck   # Type check with mypy
make all         # Lint + typecheck + test
make serve       # Start dev server with hot-reload
make frontend    # Build React frontend (npm install + npm run build)
make clean       # Remove build artifacts
```

### Adding a New Tool Converter

1. Create a file in `src/a2d/converters/<category>/`
2. Add an IR node class in `src/a2d/ir/nodes.py` if needed
3. Implement a converter extending `ToolConverter` with `@ConverterRegistry.register`
4. Add the plugin name mapping in `src/a2d/parser/schema.py` → `PLUGIN_NAME_MAP`
5. Add a visitor method in each generator (`pyspark.py`, `dlt.py`, `sql.py`)
6. Add a unit test in `tests/unit/converters/`

### Project Structure

```
src/a2d/
  cli.py                   # Typer CLI entry point
  config.py                # Configuration dataclasses
  pipeline.py              # Orchestration: Parse → Convert → Generate
  parser/                  # .yxmd XML parsing
  ir/                      # Typed IR nodes + WorkflowDAG
  converters/              # Tool-specific ParsedNode → IRNode converters
  expressions/             # Expression engine (tokenizer, parser, AST, translators)
  generators/              # Code generators (PySpark, DLT, SQL, Workflow JSON)
  analyzer/                # Complexity scoring, coverage analysis, reports
  validation/              # Syntax and schema validation

server/                    # FastAPI backend (REST + WebSocket)
  main.py                  # App entry point
  routers/                 # REST endpoints (convert, analyze, history, tools, validate)
  services/                # Business logic
  websocket/               # Real-time batch progress

frontend/                  # React 19 + TypeScript + Tailwind 4
  src/
    routes/                # Pages (convert, analyze, history, tools, validate)
    components/            # UI components (workflow graph, code viewer, etc.)
    stores/                # Zustand state management
    lib/                   # API client, utilities
  dist/                    # Pre-built assets (committed for Databricks App deployment)

demo/                      # Sample .yxmd workflows for testing
tests/                     # pytest test suite (434 tests, 70%+ coverage)
docs/                      # Architecture, expression reference, migration playbook,
                           # conversion mapping (Alteryx → Databricks tool map + known limitations)
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
