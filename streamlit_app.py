"""Streamlit UI for the Alteryx-to-Databricks migration accelerator (a2d).

Pure-Python alternative to the React web UI — no NodeJS required.

Usage:
    pip install ".[streamlit]"
    streamlit run streamlit_app.py
"""

from __future__ import annotations

import io
import tempfile
import zipfile
from pathlib import Path

import streamlit as st

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="a2d – Alteryx to Databricks",
    page_icon="⚡",
    layout="wide",
)

# ── helpers ───────────────────────────────────────────────────────────────────

def _run_conversion(
    file_bytes: bytes,
    filename: str,
    output_format: str,
    catalog: str,
    schema: str,
    include_orchestration: bool,
) -> dict:
    """Run conversion pipeline on uploaded bytes, return result dict."""
    from a2d.config import ConversionConfig, OutputFormat
    from a2d.pipeline import ConversionPipeline

    fmt_map = {"pyspark": OutputFormat.PYSPARK, "dlt": OutputFormat.DLT, "sql": OutputFormat.SQL}
    fmt = fmt_map.get(output_format, OutputFormat.PYSPARK)

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / filename
        file_path.write_bytes(file_bytes)

        config = ConversionConfig(
            input_path=file_path,
            output_format=fmt,
            generate_orchestration=include_orchestration,
            catalog_name=catalog,
            schema_name=schema,
            include_comments=True,
        )
        pipeline = ConversionPipeline(config)
        result = pipeline.convert(file_path)

    files = [
        {"filename": f.filename, "content": f.content, "file_type": f.file_type}
        for f in result.output.files
    ]
    return {
        "workflow_name": Path(filename).stem,
        "files": files,
        "stats": result.output.stats,
        "warnings": result.warnings,
        "node_count": result.dag.node_count,
        "edge_count": result.dag.edge_count,
    }


def _run_analysis(file_bytes: bytes, filename: str) -> object:
    """Run migration readiness analysis, return WorkflowAnalysis."""
    from a2d.analyzer.batch import BatchAnalyzer

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / filename
        file_path.write_bytes(file_bytes)
        analyzer = BatchAnalyzer()
        results = analyzer.analyze_files([file_path])

    return results[0] if results else None


def _make_zip(files: list[dict]) -> bytes:
    """Pack a list of {filename, content} dicts into a ZIP and return the bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.writestr(f["filename"], f["content"])
    return buf.getvalue()


def _lang_for(file_type: str) -> str:
    return {"python": "python", "sql": "sql", "json": "json"}.get(file_type.lower(), "text")


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("⚡ a2d")
    st.caption("Alteryx → Databricks migration accelerator")
    st.divider()
    st.markdown(
        "Upload `.yxmd` workflow files to convert them to PySpark, "
        "Delta Live Tables, or Databricks SQL."
    )
    st.divider()
    try:
        from a2d.__about__ import __version__
        st.caption(f"a2d v{__version__}")
    except ImportError:
        pass

# ── tabs ──────────────────────────────────────────────────────────────────────

tab_convert, tab_analyze, tab_tools = st.tabs(["Convert", "Analyze", "Tools"])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 – CONVERT
# ─────────────────────────────────────────────────────────────────────────────

with tab_convert:
    st.header("Convert Alteryx Workflows")

    col_upload, col_opts = st.columns([1, 1], gap="large")

    with col_upload:
        uploaded_files = st.file_uploader(
            "Upload .yxmd file(s)",
            type=["yxmd"],
            accept_multiple_files=True,
            help="Select one or more Alteryx workflow files to convert.",
        )

    with col_opts:
        output_format = st.selectbox(
            "Output format",
            options=["pyspark", "dlt", "sql"],
            format_func=lambda x: {
                "pyspark": "PySpark (DataFrame API)",
                "dlt": "Delta Live Tables",
                "sql": "Databricks SQL",
            }[x],
        )
        col_cat, col_schema = st.columns(2)
        with col_cat:
            catalog = st.text_input("Unity Catalog", value="main")
        with col_schema:
            schema = st.text_input("Schema", value="default")
        include_orchestration = st.checkbox("Include Workflow JSON", value=True)

    if uploaded_files:
        if st.button("Convert", type="primary", use_container_width=True):
            all_results = []
            errors = []

            prog = st.progress(0, text="Converting…")
            for i, uf in enumerate(uploaded_files):
                prog.progress((i) / len(uploaded_files), text=f"Converting {uf.name}…")
                try:
                    result = _run_conversion(
                        uf.read(),
                        uf.name,
                        output_format,
                        catalog,
                        schema,
                        include_orchestration,
                    )
                    all_results.append(result)
                except Exception as exc:
                    errors.append((uf.name, str(exc)))
            prog.progress(1.0, text="Done")

            st.session_state["convert_results"] = all_results
            st.session_state["convert_errors"] = errors

    # Render stored results (persists across reruns)
    results = st.session_state.get("convert_results", [])
    errors = st.session_state.get("convert_errors", [])

    for err_name, err_msg in errors:
        st.error(f"**{err_name}**: {err_msg}")

    for result in results:
        wf_name = result["workflow_name"]
        files = result["files"]
        stats = result["stats"]
        warnings = result["warnings"]

        with st.expander(f"**{wf_name}** — {len(files)} file(s) generated", expanded=True):
            # Summary metrics
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Nodes", result["node_count"])
            mc2.metric("Edges", result["edge_count"])
            total = stats.get("total_nodes", result["node_count"])
            supported = stats.get("supported_nodes", total)
            pct = round(supported / total * 100) if total else 0
            mc3.metric("Coverage", f"{pct}%")
            mc4.metric("Warnings", len(warnings))

            if warnings:
                with st.expander(f"Warnings ({len(warnings)})"):
                    for w in warnings:
                        st.warning(w)

            # File tabs
            if files:
                # Syntax validation for Python files
                from a2d.validation.syntax_validator import SyntaxValidator
                _validator = SyntaxValidator()
                for f in files:
                    if f["file_type"] == "python":
                        _vr = _validator.validate_string(f["content"], filename=f["filename"])
                        if not _vr.is_valid:
                            for _err in _vr.errors:
                                st.error(f"✗ Syntax error in {f['filename']}: {_err}")

                file_tabs = st.tabs([f["filename"] for f in files])
                for ftab, f in zip(file_tabs, files):
                    with ftab:
                        st.code(f["content"], language=_lang_for(f["file_type"]))
                        st.download_button(
                            label=f"Download {f['filename']}",
                            data=f["content"],
                            file_name=f["filename"],
                            mime="text/plain",
                            key=f"dl_{wf_name}_{f['filename']}",
                        )

                # ZIP download for all files in this workflow
                zip_bytes = _make_zip(files)
                st.download_button(
                    label=f"Download all as {wf_name}.zip",
                    data=zip_bytes,
                    file_name=f"{wf_name}.zip",
                    mime="application/zip",
                    key=f"zip_{wf_name}",
                )

    # Combined ZIP if multiple workflows
    if len(results) > 1:
        st.divider()
        all_files = [f for r in results for f in r["files"]]
        combined_zip = _make_zip(all_files)
        st.download_button(
            label="Download all workflows as a single ZIP",
            data=combined_zip,
            file_name="a2d_output.zip",
            mime="application/zip",
            use_container_width=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 – ANALYZE
# ─────────────────────────────────────────────────────────────────────────────

with tab_analyze:
    st.header("Migration Readiness Analysis")
    st.caption(
        "Upload one or more .yxmd files to assess complexity, tool coverage, "
        "migration priority, and estimated effort."
    )

    analyze_files = st.file_uploader(
        "Upload .yxmd file(s)",
        type=["yxmd"],
        accept_multiple_files=True,
        key="analyze_upload",
    )

    if analyze_files:
        if st.button("Analyze", type="primary", use_container_width=True):
            analyses = []
            errors = []
            prog = st.progress(0, text="Analyzing…")
            for i, uf in enumerate(analyze_files):
                prog.progress(i / len(analyze_files), text=f"Analyzing {uf.name}…")
                try:
                    a = _run_analysis(uf.read(), uf.name)
                    if a:
                        analyses.append(a)
                except Exception as exc:
                    errors.append((uf.name, str(exc)))
            prog.progress(1.0, text="Done")
            st.session_state["analyze_results"] = analyses
            st.session_state["analyze_errors"] = errors

    analyses = st.session_state.get("analyze_results", [])
    a_errors = st.session_state.get("analyze_errors", [])

    for err_name, err_msg in a_errors:
        st.error(f"**{err_name}**: {err_msg}")

    if analyses:
        # Aggregate summary
        n = len(analyses)
        avg_cov = sum(a.coverage.coverage_percentage for a in analyses) / n
        avg_cpx = sum(a.complexity.total_score for a in analyses) / n
        total_nodes = sum(a.node_count for a in analyses)

        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("Workflows", n)
        sm2.metric("Total Nodes", total_nodes)
        sm3.metric("Avg Coverage", f"{avg_cov:.1f}%")
        sm4.metric("Avg Complexity", f"{avg_cpx:.1f}/100")

        st.divider()

        # Per-workflow table
        import pandas as pd

        rows = []
        for a in analyses:
            rows.append({
                "Workflow": a.workflow_name,
                "Nodes": a.node_count,
                "Connections": a.connection_count,
                "Coverage (%)": round(a.coverage.coverage_percentage, 1),
                "Complexity": round(a.complexity.total_score, 1),
                "Level": a.complexity.level,
                "Priority": a.migration_priority,
                "Effort": a.estimated_effort,
                "Unsupported Tools": ", ".join(sorted(a.coverage.unsupported_types)) or "—",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()

        # Tool frequency
        from collections import Counter
        tool_counter: Counter = Counter()
        for a in analyses:
            for tool_type, count in a.coverage.per_tool_counts.items():
                tool_counter[tool_type] += count

        if tool_counter:
            st.subheader("Tool Frequency")
            tf_rows = [{"Tool": t, "Count": c} for t, c in tool_counter.most_common(20)]
            st.dataframe(pd.DataFrame(tf_rows), use_container_width=True, hide_index=True)

        # HTML report download
        from a2d.analyzer.report import ReportGenerator
        with tempfile.TemporaryDirectory() as tmpdir:
            html_path = Path(tmpdir) / "migration_report.html"
            json_path = Path(tmpdir) / "migration_report.json"
            rg = ReportGenerator()
            rg.generate_html(analyses, html_path)
            rg.generate_json(analyses, json_path)
            html_bytes = html_path.read_bytes()
            json_bytes = json_path.read_bytes()

        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "Download HTML Report",
                data=html_bytes,
                file_name="migration_report.html",
                mime="text/html",
                use_container_width=True,
            )
        with dl2:
            st.download_button(
                "Download JSON Report",
                data=json_bytes,
                file_name="migration_report.json",
                mime="application/json",
                use_container_width=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 – TOOLS
# ─────────────────────────────────────────────────────────────────────────────

with tab_tools:
    st.header("Alteryx Tool Support Matrix")

    show_supported_only = st.checkbox("Show supported tools only")

    try:
        from a2d.converters.registry import ConverterRegistry
        from a2d.parser.schema import PLUGIN_NAME_MAP, TOOL_METADATA

        supported_set = ConverterRegistry.supported_tools()

        rows = []
        seen: set[str] = set()
        for _plugin, (tool_type, category) in sorted(
            PLUGIN_NAME_MAP.items(), key=lambda x: (x[1][1], x[1][0])
        ):
            if tool_type in seen:
                continue
            seen.add(tool_type)
            is_supported = tool_type in supported_set
            if show_supported_only and not is_supported:
                continue
            meta = TOOL_METADATA.get(tool_type)
            rows.append({
                "Tool Type": tool_type,
                "Category": category,
                "Status": "Supported" if is_supported else "Unsupported",
                "Method": meta.conversion_method if meta else "-",
                "Description": meta.short_description if meta else "-",
            })

        import pandas as pd
        df = pd.DataFrame(rows)

        # Colour the Status column
        def _colour_status(val: str) -> str:
            return "color: green; font-weight: bold" if val == "Supported" else "color: orange"

        st.dataframe(
            df.style.map(_colour_status, subset=["Status"]),
            use_container_width=True,
            hide_index=True,
        )

        n_supported = len(supported_set)
        n_total = len(seen)
        st.caption(f"{n_supported} of {n_total} unique tool types supported")

    except Exception as exc:
        st.error(f"Could not load tool registry: {exc}")
