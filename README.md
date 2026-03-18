# alteryx2databricks (a2d)

**Automated Alteryx-to-Databricks migration accelerator** -- parse `.yxmd` workflows and generate PySpark notebooks, Delta Live Tables pipelines, Databricks SQL, and Workflow JSON orchestration.

---

## Overview

Large enterprises rely on Alteryx for visual data preparation, but as organizations move analytics workloads to the Lakehouse, manual re-implementation of hundreds of workflows becomes a bottleneck. **a2d** automates the heavy lifting.

The tool parses Alteryx `.yxmd` XML files, builds a typed intermediate representation (IR), and generates idiomatic Databricks code in multiple output formats -- all without requiring an Alteryx license to run.

### Architecture

```
  .yxmd file(s)
       |
       v
  +-----------+     +-----------+     +-------------+     +----------------+
  |   Parser  | --> |    IR     | --> |  Generator  | --> | Output Files   |
  |  (XML ->  |     | (Typed   |     | (PySpark /  |     | (.py / .sql /  |
  |  Schema)  |     |  DAG)    |     |  DLT / SQL) |     |  .json)        |
  +-----------+     +-----------+     +-------------+     +----------------+
       |                 |
       v                 v
  +-----------+     +-----------+
  | Converter |     | Analyzer  |
  | Registry  |     | (Report)  |
  +-----------+     +-----------+
```

**Phase 1 -- Parse:** XML is parsed into `ParsedWorkflow` objects with `ParsedNode` and `ParsedConnection` structures. Each Alteryx plugin name is mapped to a human-readable tool type via `PLUGIN_NAME_MAP`.

**Phase 2 -- Convert & Generate:** Each parsed node is converted to a typed IR node (e.g., `FilterNode`, `JoinNode`) via the `ConverterRegistry`. Expression strings are tokenized, parsed into an AST, and translated to PySpark or Spark SQL. The IR DAG is then walked in topological order by the target generator.

---

## Features

- **33+ supported Alteryx tools** across 6 categories (IO, Preparation, Join, Parse, Transform, Developer)
- **80+ expression function translations** -- string, math, date/time, conversion, test/null functions
- **4 output formats** -- PySpark notebooks, Delta Live Tables, Databricks SQL (CTEs), Workflow JSON
- **Web UI** -- full-featured React frontend with DAG visualization, batch processing, download, and history
- **Migration readiness reports** -- HTML and JSON reports with complexity scoring and coverage analysis
- **Batch processing** -- convert or analyze entire directories of `.yxmd` files with real-time progress
- **Post-migration validation** -- syntax validation of generated code
- **Orchestration generation** -- Databricks Workflow JSON for Jobs API or Terraform import
- **Expression engine** -- full recursive-descent parser handles IF/THEN/ELSE, IN, nested functions, field refs, row refs, and all operators

---

## Quick Start

### Prerequisites

- Python 3.10+
- **Node.js 18+** — only needed for the React web UI (Option 3 below); not required for CLI or Streamlit use

### Installation

**Download the ZIP from GitHub** (click *Code → Download ZIP*), unzip it, and open a terminal in the project folder.

| What you want to run | Install command |
|---|---|
| CLI only (`python -m a2d.cli convert`) | `pip install "."` |
| Streamlit web UI | `pip install ".[streamlit]"` |
| React web UI | `pip install ".[server]"` + Node.js 18+ |

> **Tip:** Use a virtual environment to keep dependencies isolated — see the Windows and Mac/Linux sections below.

---

### Windows setup (step-by-step)

Open **PowerShell** in the project folder:

```powershell
# 1. Create a virtual environment
python -m venv .venv

# 2. Activate it
.venv\Scripts\Activate.ps1
```

> If step 2 fails with *"running scripts is disabled"*, run this once and retry:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```
> Alternatively, use **Command Prompt** (`cmd.exe`) instead:
> ```cmd
> .venv\Scripts\activate.bat
> ```

```powershell
# 3. Install (choose based on what you want to run)
pip install "."              # CLI only
pip install ".[streamlit]"   # Streamlit web UI

# 4a. Convert a workflow (CLI)
python -m a2d.cli convert "C:\Users\YourName\Downloads\workflow.yxmd" -o "C:\Users\YourName\Downloads\output"

# 4b. Launch the Streamlit web UI
python -m streamlit run streamlit_app.py
```

> **Note on paths (Windows):** Use native Windows paths (`C:\Users\...`) or forward-slash equivalents (`C:/Users/...`). Paths starting with `/C/Users/...` are Git Bash syntax — they only work inside Git Bash, not PowerShell or Command Prompt.

---

### Mac / Linux setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install "."             # CLI only
# pip install ".[streamlit]"  # add this instead for the Streamlit web UI
```

---

### Option 1: CLI Usage

The CLI is invoked via `python -m a2d.cli` (works on all platforms including Windows, bypassing any PATH issues):

```bash
# Convert a single workflow to PySpark (default)
python -m a2d.cli convert workflow.yxmd -o output/

# Convert to Delta Live Tables
python -m a2d.cli convert workflow.yxmd -o output/ -f dlt

# Convert to SQL
python -m a2d.cli convert workflow.yxmd -o output/ -f sql

# Convert all workflows in a directory
python -m a2d.cli convert workflows/ -o output/

# Include explanatory comments in generated code (off by default)
python -m a2d.cli convert workflow.yxmd -o output/ --comments

# Show detailed stubs for unsupported nodes
python -m a2d.cli convert workflow.yxmd -o output/ --verbose-unsupported

# Generate migration readiness report
python -m a2d.cli analyze workflows/ -o report/

# List all supported Alteryx tools
python -m a2d.cli list-tools

# Validate generated code syntax
python -m a2d.cli validate output/workflow.py
```

> **`a2d` shorthand:** After installation, `a2d convert workflow.yxmd` also works if pip's Scripts directory is on your PATH. On Windows this sometimes requires adding `%APPDATA%\Python\Scripts` to the PATH manually — using `python -m a2d.cli` is more reliable.

### Option 2: Streamlit Web UI (Recommended for non-developers)

A pure-Python web UI with no Node.js required. See [README_STREAMLIT.md](README_STREAMLIT.md) for full setup instructions including Windows-specific steps.

```bash
pip install ".[streamlit]"
python -m streamlit run streamlit_app.py
```

Open `http://localhost:8501` in your browser.

### Option 3: React Web UI (Local)

Requires Node.js 18+.

```bash
pip install ".[server]"

# Build the frontend once
cd frontend && npm install && npm run build && cd ..

# Start the API server with hot-reload (serves React frontend from frontend/dist/)
PYTHONPATH=src:. uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload

# Or use Make
make serve
```

Open http://localhost:8000 in your browser. The web UI provides:

- **Convert** -- upload `.yxmd` files and see generated code with syntax highlighting
- **Batch Convert** -- upload multiple workflows with real-time WebSocket progress
- **Analyze** -- migration readiness reports with complexity scoring
- **DAG Visualization** -- interactive workflow graph with React Flow
- **History** -- browse past conversions (local storage or PostgreSQL)
- **Download** -- ZIP download of all generated files
- **Tool Matrix** -- visual overview of supported Alteryx tools
- **Validate** -- check generated Python code for syntax errors

### Option 4: Docker

```bash
# Build the image (multi-stage: frontend build + Python runtime)
docker build -t a2d .

# Run the container
docker run -p 8000:8000 a2d
```

Open http://localhost:8000.

### Option 5: Databricks Apps

Deploy as a managed Databricks App:

```bash
# 1. Sync the project to your Databricks workspace
databricks workspace import-dir src /Workspace/Users/<you>/a2d/src --overwrite
databricks workspace import-dir server /Workspace/Users/<you>/a2d/server --overwrite
databricks workspace import-dir frontend/dist /Workspace/Users/<you>/a2d/frontend/dist --overwrite
databricks workspace import-dir demo /Workspace/Users/<you>/a2d/demo --overwrite

# Import root config files
for f in app.yaml pyproject.toml requirements.txt; do
  databricks workspace import "/Workspace/Users/<you>/a2d/$f" --file "$f" --format AUTO --overwrite
done

# 2. Create and deploy the app
databricks apps create a2d --description "Alteryx to Databricks Migration Accelerator"
databricks apps deploy a2d --source-code-path /Workspace/Users/<you>/a2d
```

The `app.yaml` configures the runtime command, CORS, and `PYTHONPATH`. The Databricks Apps runtime installs dependencies from `requirements.txt` automatically.

**Environment variables** (all optional, prefix `A2D_`):

| Variable | Default | Description |
|----------|---------|-------------|
| `A2D_CORS_ORIGINS` | `["http://localhost:5173"]` | Allowed CORS origins |
| `A2D_MAX_UPLOAD_SIZE_BYTES` | `52428800` (50 MB) | Max upload file size |
| `A2D_MAX_BATCH_FILES` | `50` | Max files per batch |
| `A2D_LOG_LEVEL` | `info` | Logging level |
| `A2D_DATABASE_URL` | `""` (disabled) | PostgreSQL URL for conversion history |
| `PORT` | `8000` | Server port |

---

## CLI Reference

### `python -m a2d.cli convert`

| Option | Default | Description |
|--------|---------|-------------|
| `INPUT_PATH` (required) | -- | Path to `.yxmd` file or directory |
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
|--------|---------|-------------|
| `INPUT_PATH` (required) | -- | Path to `.yxmd` file or directory |
| `-o`, `--output-dir` | `./a2d-report` | Report output directory |
| `--format` | `html` | Report format: `html`, `json`, `both` |

### `python -m a2d.cli list-tools`

| Option | Default | Description |
|--------|---------|-------------|
| `-s`, `--supported` | `False` | Show only supported tools |

### `python -m a2d.cli validate`

| Option | Default | Description |
|--------|---------|-------------|
| `GENERATED_CODE` (required) | -- | Path to generated `.py` file |

---

## Supported Alteryx Tools

### IO (6 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| Input Data | File and database sources; maps to `spark.read` or `spark.table` |
| Output Data | File and database destinations; maps to `df.write` |
| Text Input | Inline data becomes `spark.createDataFrame` |
| Browse | Maps to `display()` |
| Cloud Storage | S3/Azure/GCS connectors |
| Publish to Tableau | Converts to Delta table write (recommended Databricks-Tableau pattern) |

### Preparation (14 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| Select | Column rename, drop, retype |
| Filter | Full expression translation; produces True/False outputs |
| Formula | Multi-formula support with full expression engine |
| Sort | Multi-field, ascending/descending |
| Sample | First N, percentage, random |
| Unique | Deduplicate with key fields; Unique/Duplicate outputs |
| RecordID | Sequential ID via `monotonically_increasing_id` |
| MultiRow Formula | Window functions with `lag`/`lead` |
| MultiField Formula | Apply same expression across multiple columns |
| Data Cleansing | Trim, null handling, case modification |
| Generate Rows | Placeholder with `spark.range` |
| AutoField | Passthrough (no-op in Spark) |
| Imputation | Missing value imputation |
| Arrange | Column reordering |

### Join (5 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| Join | Inner/left/right/full; produces Join/Left/Right outputs |
| Union | `unionByName` with `allowMissingColumns` |
| Find Replace | Lookup-based replacement |
| Append Fields | Cross join |
| Join Multiple | Chained joins on shared keys |

### Parse (7 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| RegEx | Parse, match, replace, tokenize modes |
| Text to Columns | Split to columns or rows |
| DateTime | Parse and format modes |
| JSON Parse | `get_json_object` based |
| XML Parse | XML element extraction |
| Field Summary | Statistical profiling |

### Transform (8 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| Summarize | GroupBy + 15 aggregation types |
| CrossTab (Pivot) | `groupBy().pivot().agg()` |
| Transpose (Unpivot) | `stack()` based |
| Running Total | Window functions |
| Count Records | Single-row count DataFrame |
| Tile | Quantile/percentile binning |
| Weighted Average | Weighted aggregation |

### Developer (12 tools)

| Alteryx Tool | Notes |
|-------------|-------|
| Python Tool | Original code preserved as comments |
| Download | Placeholder with TODO |
| Run Command | Placeholder with TODO |
| Dynamic Input/Output | Dynamic source/destination mapping |
| Widget tools | TextBox, Chart, Report, Email — scaffolding generated |

---

## Expression Engine

The expression engine translates 80+ Alteryx functions:

| Category | Count | Examples |
|----------|-------|---------|
| String | 24 | `Contains`, `Left`, `Right`, `Substring`, `Replace`, `REGEX_Match`, `Trim` |
| Math | 21 | `ABS`, `CEIL`, `FLOOR`, `Round`, `POW`, `SQRT`, `LOG`, `Mod`, `RAND` |
| DateTime | 15 | `DateTimeNow`, `DateTimeAdd`, `DateTimeDiff`, `DateTimeFormat`, `DateTimeParse` |
| Conversion | 9 | `ToNumber`, `ToInteger`, `ToString`, `ToDate`, `ToDateTime` |
| Test / Null | 8 | `IsNull`, `IsEmpty`, `IsNumber`, `Coalesce`, `IIF`, `IFNULL` |
| Scalar | 3 | `Min`, `Max`, `Null` |

The engine also handles field references (`[FieldName]`), row references (`[Row-1:Field]`), arithmetic/comparison/logical operators, `IF/THEN/ELSEIF/ELSE/ENDIF`, and `IN` expressions.

---

## Development

### Setup

```bash
# Install all dependencies (includes dev tools: pytest, mypy, ruff, etc.)
make dev

# Or manually
pip install -e ".[all]"
```

> The `[all]` extra includes server, streamlit, validation, and dev dependencies.
> For CLI-only use, `pip install "."` is sufficient.

### Commands

```bash
make test          # Run tests
make test-cov      # Run tests with coverage report
make lint          # Lint with ruff
make format        # Format with ruff
make typecheck     # Type check with mypy
make clean         # Remove build artifacts
make all           # Lint + typecheck + test
make frontend      # Build frontend (npm install + npm run build)
make serve         # Start dev server with hot-reload
```

### Adding a New Tool Converter

1. Create a file in the appropriate `src/a2d/converters/<category>/` directory
2. Add an IR node class in `src/a2d/ir/nodes.py` if needed
3. Implement a converter extending `ToolConverter` with `@ConverterRegistry.register`
4. Add the plugin name mapping in `src/a2d/parser/schema.py` `PLUGIN_NAME_MAP`
5. Add a visitor method in each generator (`pyspark.py`, `dlt.py`, `sql.py`)
6. Add a unit test in `tests/unit/converters/`

### Project Structure

```
src/a2d/
  cli.py                    # Typer CLI
  config.py                 # Configuration dataclasses
  pipeline.py               # Main orchestration: Parse -> Convert -> Generate
  parser/                   # .yxmd XML parsing
  ir/                       # Typed IR nodes + WorkflowDAG
  converters/               # Tool-specific ParsedNode -> IRNode converters
  expressions/              # Expression engine (tokenizer, parser, AST, translators)
  generators/               # Code generators (PySpark, DLT, SQL, Workflow JSON)
  analyzer/                 # Complexity scoring, coverage analysis, reports
  validation/               # Syntax and schema validation

server/                     # FastAPI backend (API + WebSocket)
  main.py                   # App entry point with lifespan management
  settings.py               # Environment-based configuration
  routers/                  # REST endpoints (convert, analyze, history, tools, validate)
  services/                 # Business logic (conversion, batch, history)
  websocket/                # WebSocket for real-time batch progress

frontend/                   # React 19 + TypeScript + Tailwind 4
  src/
    routes/                 # Pages (convert, analyze, history, tools, validate, etc.)
    components/             # UI components (workflow graph, code blocks, etc.)
    stores/                 # Zustand state management
    lib/                    # API client, utilities
  dist/                     # Pre-built assets (committed for Databricks App deployment)

demo/                       # Sample .yxmd workflows for testing
tests/                      # pytest test suite
```

---

## What's NOT Covered

| Category | Details |
|----------|---------|
| **Spatial tools** | Buffer, SpatialMatch, Distance, Geocoder -- consider Sedona/H3 |
| **Predictive / ML tools** | Decision Tree, Logistic Regression, etc. -- use MLflow / Spark MLlib |
| **Macros** | Standard, batch, and iterative macros detected but not expanded |
| **R Tool** | R code blocks require manual rewrite |
| **Reporting tools** | Table, Layout, Render -- use Databricks dashboards |
| **Server orchestration** | Alteryx Server schedules -- map to Databricks Workflows |
| **Interface / Analytic App tools** | CheckBox, DropDown, etc. -- use Databricks widgets |
| **Custom SDK tools** | Third-party Alteryx tools require bespoke conversion |

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
