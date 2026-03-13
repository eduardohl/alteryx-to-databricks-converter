# Running a2d with the Streamlit UI (no NodeJS required)

This guide is for environments where NodeJS is not available. The Streamlit UI is a pure-Python alternative to the React web interface.

## Prerequisites

- Python 3.10 or higher
- `pip` (standard Python package manager)

No NodeJS, no npm, no build tools.

## Installation

Download the ZIP from GitHub and unzip it, then open a terminal in the project folder.

```bash
pip install ".[streamlit]"
```

> **Tip:** Use a virtual environment to keep dependencies isolated — see the Windows and Mac/Linux sections below.

## Running the app

Use the module invocation (works on all platforms, including Windows):

```bash
python -m streamlit run streamlit_app.py
```

Streamlit will print a local URL (default `http://localhost:8501`) — open it in your browser.

> `streamlit run streamlit_app.py` (without `python -m`) also works on Mac/Linux, but on
> Windows it can fail with *"streamlit is not recognized"* because pip's Scripts folder is
> not always on PATH. `python -m streamlit run` bypasses this and always works.

---

## Windows setup (step-by-step)

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
> Alternatively, use **Command Prompt** (`cmd.exe`) instead of PowerShell:
> ```cmd
> .venv\Scripts\activate.bat
> ```

```powershell
# 3. Install dependencies
pip install ".[streamlit]"

# 4. Launch the app
python -m streamlit run streamlit_app.py
```

---

## Mac / Linux setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ".[streamlit]"
python -m streamlit run streamlit_app.py
```

---

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

---

## Troubleshooting

**`'streamlit' is not recognized as the name of a cmdlet` (Windows)**
Use `python -m streamlit run streamlit_app.py` instead of `streamlit run streamlit_app.py`.

**`ModuleNotFoundError: No module named 'a2d'`**
Make sure you ran `pip install ".[streamlit]"` from the project root directory (where `pyproject.toml` lives).

**Port already in use**
Run on a different port: `python -m streamlit run streamlit_app.py --server.port 8502`

**Streamlit opens a browser automatically and you don't want it to**
`python -m streamlit run streamlit_app.py --server.headless true` — then open the printed URL manually.
