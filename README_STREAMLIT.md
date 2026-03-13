# Running a2d with the Streamlit UI (no NodeJS required)

This guide is for environments where NodeJS is not available. The Streamlit UI is a pure-Python alternative to the React web interface.

## Prerequisites

- Python 3.10 or higher
- `pip` (standard Python package manager)

No NodeJS, no npm, no build tools.

## Installation

```bash
# Clone the branch
git clone -b feature/streamlit-ui https://github.com/eduardohl/alteryx-to-databricks-converter.git
cd alteryx-to-databricks-converter

# Install with the streamlit extra
pip install ".[streamlit]"
```

> **Tip:** Use a virtual environment to keep dependencies isolated:
> ```bash
> python -m venv .venv
> source .venv/bin/activate   # Windows: .venv\Scripts\activate
> pip install ".[streamlit]"
> ```

## Running the app

```bash
streamlit run streamlit_app.py
```

Streamlit will print a local URL (default `http://localhost:8501`) — open it in your browser.

## Features

| Tab | What it does |
|-----|-------------|
| **Convert** | Upload one or more `.yxmd` files, choose output format (PySpark / DLT / SQL), set Unity Catalog and schema, then download generated code files or a ZIP |
| **Analyze** | Upload `.yxmd` files to assess migration readiness — coverage %, complexity score, priority, and effort estimate. Download HTML or JSON reports |
| **Tools** | Browse the full Alteryx tool support matrix showing which tools are converted and how |

## Output formats

| Format | Best for |
|--------|---------|
| `pyspark` | Interactive notebooks, exploratory data work |
| `dlt` | Production Delta Live Tables pipelines |
| `sql` | Databricks SQL analysts |

## Troubleshooting

**`ModuleNotFoundError: No module named 'a2d'`**
Make sure you ran `pip install ".[streamlit]"` from the project root directory.

**Port already in use**
Run on a different port: `streamlit run streamlit_app.py --server.port 8502`

**Streamlit opens a browser I don't want**
Disable auto-open: `streamlit run streamlit_app.py --server.headless true`
Then navigate to the printed URL manually.
