"""CLI entry point for the Alteryx-to-Databricks migration tool."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from a2d.__about__ import __version__
from a2d.config import ConversionConfig, OutputFormat
from a2d.utils.logging import setup_logging

app = typer.Typer(
    name="a2d",
    help="Alteryx to Databricks migration accelerator",
    add_completion=True,
    no_args_is_help=True,
)
console = Console()


@app.command()
def convert(
    input_path: Path = typer.Argument(..., help="Path to .yxmd file or directory of .yxmd files"),
    output_dir: Path = typer.Option("./a2d-output", "--output-dir", "-o", help="Output directory"),
    format: str = typer.Option("pyspark", "--format", "-f", help="Output format: pyspark, dlt, sql"),
    catalog: str = typer.Option("main", help="Unity Catalog name"),
    schema: str = typer.Option("default", help="Schema name"),
    no_orchestration: bool = typer.Option(False, help="Skip workflow JSON generation"),
    batch: bool = typer.Option(False, "--batch", "-b", help="Enable batch mode with structured error tracking"),
    report_format: str = typer.Option("json", "--report-format", help="Batch report format: json, jsonl, html, all"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Convert Alteryx workflows to Databricks code."""
    setup_logging(verbose)

    output_format = OutputFormat(format)
    config = ConversionConfig(
        input_path=input_path,
        output_dir=output_dir,
        output_format=output_format,
        catalog_name=catalog,
        schema_name=schema,
        generate_orchestration=not no_orchestration,
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    if batch and input_path.is_dir():
        _run_batch_conversion(config, input_path, output_dir, report_format)
    elif input_path.is_file():
        from a2d.pipeline import ConversionPipeline

        pipeline = ConversionPipeline(config)
        result = pipeline.convert(input_path)
        _write_output(result.output, output_dir)
        _print_conversion_summary(result, input_path)
    elif input_path.is_dir():
        from a2d.pipeline import ConversionPipeline

        pipeline = ConversionPipeline(config)
        results = pipeline.convert_batch(input_path)
        for result in results:
            name = Path(result.parsed_workflow.file_path).stem
            subdir = output_dir / name
            subdir.mkdir(parents=True, exist_ok=True)
            _write_output(result.output, subdir)
        console.print(f"\n[bold green]Converted {len(results)} workflows[/bold green] to {output_dir}")
    else:
        console.print(f"[red]Error: {input_path} not found[/red]")
        raise typer.Exit(code=1)


@app.command()
def analyze(
    input_path: Path = typer.Argument(..., help="Path to .yxmd file or directory"),
    output_dir: Path = typer.Option("./a2d-report", "--output-dir", "-o", help="Report output directory"),
    format: str = typer.Option("html", help="Report format: html, json, both"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Analyze Alteryx workflows and generate migration readiness report."""
    setup_logging(verbose)

    from a2d.analyzer.batch import BatchAnalyzer
    from a2d.analyzer.report import ReportGenerator

    output_dir.mkdir(parents=True, exist_ok=True)

    analyzer = BatchAnalyzer()
    if input_path.is_file():
        results = analyzer.analyze_files([input_path])
    elif input_path.is_dir():
        files = sorted(input_path.glob("**/*.yxmd"))
        results = analyzer.analyze_files(files)
    else:
        console.print(f"[red]Error: {input_path} not found[/red]")
        raise typer.Exit(code=1)

    report_gen = ReportGenerator()
    if format in ("html", "both"):
        report_gen.generate_html(results, output_dir / "migration_report.html")
    if format in ("json", "both"):
        report_gen.generate_json(results, output_dir / "migration_report.json")

    console.print(f"\n[bold green]Report generated[/bold green] at {output_dir}")


@app.command()
def validate(
    generated_code: Path = typer.Argument(..., help="Generated .py file to validate"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Validate generated code syntax."""
    setup_logging(verbose)

    from a2d.validation.syntax_validator import SyntaxValidator

    validator = SyntaxValidator()
    result = validator.validate_file(generated_code)

    if result.is_valid:
        console.print(f"[bold green]Valid[/bold green]: {generated_code}")
    else:
        console.print(f"[bold red]Invalid[/bold red]: {generated_code}")
        for error in result.errors:
            console.print(f"  - {error}")
        raise typer.Exit(code=1)


@app.command(name="list-tools")
def list_tools(
    supported_only: bool = typer.Option(False, "--supported", "-s", help="Show only supported tools"),
) -> None:
    """List all known Alteryx tools and their conversion status."""
    from a2d.converters.registry import ConverterRegistry
    from a2d.parser.schema import PLUGIN_NAME_MAP, TOOL_METADATA

    supported = ConverterRegistry.supported_tools()

    table = Table(title="Alteryx Tool Support Matrix")
    table.add_column("Tool Type", style="cyan")
    table.add_column("Category", style="magenta")
    table.add_column("Status", style="green")
    table.add_column("Method", style="blue")
    table.add_column("Description", style="white", max_width=55)

    seen: set[str] = set()
    for _plugin, (tool_type, category) in sorted(PLUGIN_NAME_MAP.items(), key=lambda x: (x[1][1], x[1][0])):
        if tool_type in seen:
            continue
        seen.add(tool_type)
        is_supported = tool_type in supported
        if supported_only and not is_supported:
            continue
        status = "[green]Supported[/green]" if is_supported else "[yellow]Unsupported[/yellow]"
        meta = TOOL_METADATA.get(tool_type)
        method = meta.conversion_method if meta else "-"
        desc = meta.short_description if meta else "-"
        table.add_row(tool_type, category, status, method, desc)

    console.print(table)
    console.print(
        f"\n{len(supported)} of {len(set(t for t, _ in PLUGIN_NAME_MAP.values()))} unique tool types supported"
    )


@app.command()
def version() -> None:
    """Show version."""
    console.print(f"a2d v{__version__}")


def _write_output(output, output_dir: Path) -> None:
    """Write all generated files to the output directory."""
    for f in output.files:
        path = output_dir / f.filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f.content)
        console.print(f"  Written: {path}")


def _run_batch_conversion(
    config: ConversionConfig, input_path: Path, output_dir: Path, report_format: str
) -> None:
    """Run batch conversion with structured error tracking and outcome reports."""
    from rich.progress import Progress, SpinnerColumn, TextColumn

    from a2d.observability.batch import BatchOrchestrator
    from a2d.observability.report import OutcomeReportGenerator

    file_paths = sorted(input_path.glob("**/*.yxmd"))
    if not file_paths:
        console.print(f"[yellow]No .yxmd files found in {input_path}[/yellow]")
        return

    orchestrator = BatchOrchestrator(config)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Converting...", total=len(file_paths))

        def on_progress(current: int, total: int, filename: str) -> None:
            progress.update(task, completed=current, description=f"Converting {filename}")

        batch_result = orchestrator.convert_batch(file_paths, progress_callback=on_progress)

    # Write output files for successful conversions
    for fr in batch_result.file_results:
        if fr.success and fr.conversion_result:
            name = fr.workflow_name
            subdir = output_dir / name
            subdir.mkdir(parents=True, exist_ok=True)
            _write_output(fr.conversion_result.output, subdir)

    # Generate outcome reports
    report_gen = OutcomeReportGenerator()
    if report_format in ("json", "all"):
        report_gen.generate_json(batch_result, output_dir / "batch_report.json")
    if report_format in ("jsonl", "all"):
        report_gen.generate_jsonl(batch_result, output_dir / "batch_report.jsonl")
    if report_format in ("html", "all"):
        report_gen.generate_html(batch_result, output_dir / "batch_report.html")

    _print_batch_summary(batch_result, output_dir)


def _print_batch_summary(result, output_dir: Path) -> None:
    """Print a summary table of batch conversion results."""
    from a2d.observability.batch import BatchConversionResult

    assert isinstance(result, BatchConversionResult)
    bm = result.batch_metrics

    console.print("\n[bold]Batch Conversion Summary[/bold]")

    summary_table = Table(show_header=False, box=None)
    summary_table.add_column("Key", style="cyan")
    summary_table.add_column("Value")
    summary_table.add_row("Total files", str(bm.total_files))
    summary_table.add_row("Successful", f"[green]{bm.successful_files}[/green]")
    summary_table.add_row("Partial", f"[yellow]{bm.partial_files}[/yellow]")
    summary_table.add_row("Failed", f"[red]{bm.failed_files}[/red]")
    summary_table.add_row("Duration", f"{bm.duration_seconds:.2f}s")
    summary_table.add_row("Avg coverage", f"{bm.avg_coverage_percentage:.0f}%")
    summary_table.add_row("Total errors", str(bm.total_errors))
    summary_table.add_row("Total warnings", str(bm.total_warnings))
    console.print(summary_table)

    # Per-file results table
    if result.file_results:
        file_table = Table(title="Per-File Results")
        file_table.add_column("Workflow", style="cyan")
        file_table.add_column("Status")
        file_table.add_column("Coverage", justify="right")
        file_table.add_column("Errors", justify="right")
        file_table.add_column("Duration", justify="right")

        for fr in result.file_results:
            status = "[green]OK[/green]" if fr.success else "[red]FAIL[/red]"
            if fr.success and fr.errors:
                status = "[yellow]PARTIAL[/yellow]"
            error_count = sum(1 for e in fr.errors if e.severity.value == "error")
            file_table.add_row(
                fr.workflow_name,
                status,
                f"{fr.metrics.coverage_percentage:.0f}%",
                str(error_count),
                f"{fr.metrics.duration_seconds:.2f}s",
            )
        console.print(file_table)

    console.print(f"\n[bold green]Output written to {output_dir}[/bold green]")


def _print_conversion_summary(result, input_path: Path) -> None:
    """Print a summary of the conversion result."""
    dag = result.dag
    output = result.output

    console.print(f"\n[bold]Conversion Summary for {input_path.name}[/bold]")
    console.print(f"  Nodes: {dag.node_count}")
    console.print(f"  Edges: {dag.edge_count}")
    console.print(f"  Files generated: {len(output.files)}")

    stats = output.stats
    if "supported_nodes" in stats:
        total = stats.get("total_nodes", 0)
        supported = stats.get("supported_nodes", 0)
        pct = (supported / total * 100) if total > 0 else 0
        console.print(f"  Coverage: {supported}/{total} nodes ({pct:.0f}%)")

    if result.warnings:
        console.print(f"\n  [yellow]Warnings ({len(result.warnings)}):[/yellow]")
        for w in result.warnings[:10]:
            console.print(f"    - {w}")
        if len(result.warnings) > 10:
            console.print(f"    ... and {len(result.warnings) - 10} more")


if __name__ == "__main__":
    app()
