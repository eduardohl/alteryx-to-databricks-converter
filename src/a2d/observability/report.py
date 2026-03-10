"""Outcome report generation for batch conversion results."""

from __future__ import annotations

import html
import json
from pathlib import Path

from a2d.observability.batch import BatchConversionResult
from a2d.observability.errors import ErrorKind, ErrorSeverity


class OutcomeReportGenerator:
    """Generate outcome reports in JSON, JSONL, and HTML formats."""

    def generate_json(self, result: BatchConversionResult, output_path: Path) -> None:
        """Write a comprehensive JSON report."""
        report = {
            "batch_metrics": result.batch_metrics.to_dict(),
            "file_results": [
                {
                    "file_path": fr.file_path,
                    "workflow_name": fr.workflow_name,
                    "success": fr.success,
                    "metrics": fr.metrics.to_dict(),
                    "errors": [e.to_dict() for e in fr.errors],
                }
                for fr in result.file_results
            ],
            "error_summary": self._error_summary(result),
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, default=str))

    def generate_jsonl(self, result: BatchConversionResult, output_path: Path) -> None:
        """Write a streaming JSONL report (one record per line)."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []

        # Batch start record
        lines.append(
            json.dumps(
                {"type": "batch_start", "data": result.batch_metrics.to_dict()},
                default=str,
            )
        )

        # Per-file records
        for fr in result.file_results:
            lines.append(
                json.dumps(
                    {
                        "type": "file_result",
                        "data": {
                            "file_path": fr.file_path,
                            "workflow_name": fr.workflow_name,
                            "success": fr.success,
                            "metrics": fr.metrics.to_dict(),
                        },
                    },
                    default=str,
                )
            )

        # Error records
        for fr in result.file_results:
            for err in fr.errors:
                lines.append(
                    json.dumps({"type": "error", "data": err.to_dict()}, default=str)
                )

        output_path.write_text("\n".join(lines) + "\n")

    def generate_html(self, result: BatchConversionResult, output_path: Path) -> None:
        """Write an HTML outcome report with status table and error breakdown."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        bm = result.batch_metrics

        # Build file results table rows
        rows = ""
        for fr in result.file_results:
            status_class = "success" if fr.success else "failure"
            status_icon = "&#9989;" if fr.success else "&#10060;"
            error_count = sum(1 for e in fr.errors if e.severity == ErrorSeverity.ERROR)
            warning_count = sum(1 for e in fr.errors if e.severity == ErrorSeverity.WARNING)
            rows += (
                f"<tr class='{status_class}'>"
                f"<td>{status_icon}</td>"
                f"<td>{html.escape(fr.workflow_name)}</td>"
                f"<td>{fr.metrics.coverage_percentage:.0f}%</td>"
                f"<td>{error_count}</td>"
                f"<td>{warning_count}</td>"
                f"<td>{fr.metrics.duration_seconds:.2f}s</td>"
                f"</tr>"
            )

        # Error breakdown by kind
        errors_by_kind = result.errors_by_kind()
        error_breakdown_rows = ""
        for kind in ErrorKind:
            count = len(errors_by_kind.get(kind, []))
            if count > 0:
                error_breakdown_rows += f"<tr><td>{kind.value}</td><td>{count}</td></tr>"

        html_content = f"""<!DOCTYPE html>
<html>
<head>
<title>a2d Batch Conversion Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; }}
h1 {{ color: #1b3a57; }}
table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #f2f2f2; }}
tr.success {{ background: #f0fff0; }}
tr.failure {{ background: #fff0f0; }}
.metrics {{ display: flex; gap: 2rem; margin: 1rem 0; }}
.metric {{ background: #f8f9fb; border-radius: 8px; padding: 1rem 1.5rem; text-align: center; }}
.metric .value {{ font-size: 1.5rem; font-weight: bold; }}
.metric .label {{ font-size: 0.85rem; color: #666; }}
</style>
</head>
<body>
<h1>a2d Batch Conversion Report</h1>

<div class="metrics">
<div class="metric"><div class="value">{bm.total_files}</div><div class="label">Total Files</div></div>
<div class="metric"><div class="value">{bm.successful_files}</div><div class="label">Successful</div></div>
<div class="metric"><div class="value">{bm.partial_files}</div><div class="label">Partial</div></div>
<div class="metric"><div class="value">{bm.failed_files}</div><div class="label">Failed</div></div>
<div class="metric"><div class="value">{bm.duration_seconds:.2f}s</div><div class="label">Duration</div></div>
<div class="metric"><div class="value">{bm.avg_coverage_percentage:.0f}%</div><div class="label">Avg Coverage</div></div>
</div>

<h2>File Results</h2>
<table>
<thead><tr><th>Status</th><th>Workflow</th><th>Coverage</th><th>Errors</th><th>Warnings</th><th>Duration</th></tr></thead>
<tbody>{rows}</tbody>
</table>

<h2>Error Breakdown by Stage</h2>
<table>
<thead><tr><th>Stage</th><th>Count</th></tr></thead>
<tbody>{error_breakdown_rows if error_breakdown_rows else "<tr><td colspan='2'>No errors</td></tr>"}</tbody>
</table>

</body>
</html>"""

        output_path.write_text(html_content)

    def _error_summary(self, result: BatchConversionResult) -> dict:
        """Build an error summary dict for the JSON report."""
        by_kind: dict[str, int] = {}
        by_severity: dict[str, int] = {}

        for fr in result.file_results:
            for err in fr.errors:
                by_kind[err.kind.value] = by_kind.get(err.kind.value, 0) + 1
                by_severity[err.severity.value] = by_severity.get(err.severity.value, 0) + 1

        return {
            "by_kind": by_kind,
            "by_severity": by_severity,
            "total": sum(by_severity.values()),
        }
