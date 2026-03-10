# Alteryx-to-Databricks Migration Accelerator (a2d)

## Project Overview
Production-grade Python CLI tool that parses Alteryx .yxmd workflow files and generates equivalent PySpark notebooks, Delta Live Tables pipelines, Databricks SQL, and Workflow JSON.

## Architecture
- Two-phase: Parse → IR (intermediate representation) → Generate
- Converters: ParsedNode → IRNode (tool-specific, target-agnostic)
- Generators: IRNode → Code (format-specific, tool-agnostic)
- Expression engine: Alteryx expressions → PySpark/SQL via tokenizer → AST → translator

## Key Commands
- `make dev` - Install with all dev dependencies
- `make test` - Run all tests
- `make lint` - Lint with ruff
- `make typecheck` - Type check with mypy
- `make all` - Lint + typecheck + test
- `a2d convert <path>` - Convert workflow(s)
- `a2d analyze <path>` - Analyze and report
- `a2d list-tools` - Show supported tools

## Code Conventions
- Python 3.10+, type hints on all public functions
- dataclasses (not attrs/pydantic) for data models
- `@ConverterRegistry.register` decorator for new converters
- Jinja2 templates in `src/a2d/generators/templates/`
- Tests mirror source structure under `tests/unit/`
- Fixtures in `tests/fixtures/`

## Adding a New Tool Converter
1. Create file in appropriate `src/a2d/converters/<category>/` directory
2. Add IR node class in `src/a2d/ir/nodes.py` if needed
3. Implement converter extending `ToolConverter` with `@ConverterRegistry.register`
4. Add plugin name mapping in `src/a2d/parser/schema.py` PLUGIN_NAME_MAP
5. Add visitor method in generators
6. Add unit test in `tests/unit/converters/`

## Dependencies
- lxml: XML parsing
- networkx: DAG graph
- jinja2: Code generation templates
- typer + rich: CLI
- sqlglot: SQL dialect handling
- pytest: Testing
