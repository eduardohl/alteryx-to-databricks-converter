"""Shared test fixtures for a2d tests."""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WORKFLOWS_DIR = FIXTURES_DIR / "workflows"
EXPECTED_DIR = FIXTURES_DIR / "expected_outputs"
EXPRESSIONS_DIR = FIXTURES_DIR / "expressions"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def workflows_dir() -> Path:
    return WORKFLOWS_DIR


@pytest.fixture
def complex_pipeline_path() -> Path:
    return WORKFLOWS_DIR / "complex_pipeline.yxmd"
