"""Tests for the CLI entry point."""

from __future__ import annotations

from typer.testing import CliRunner

from a2d.__about__ import __version__
from a2d.cli import app

runner = CliRunner()


class TestVersionCommand:
    def test_version_output(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestListToolsCommand:
    def test_list_tools_runs(self):
        result = runner.invoke(app, ["list-tools"])
        assert result.exit_code == 0
        assert "Tool Type" in result.output

    def test_list_tools_supported_only(self):
        result = runner.invoke(app, ["list-tools", "--supported"])
        assert result.exit_code == 0
        assert "Supported" in result.output


class TestConvertCommand:
    def test_convert_missing_file(self):
        result = runner.invoke(app, ["convert", "/nonexistent/path.yxmd"])
        assert result.exit_code != 0
