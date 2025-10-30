"""Tests for deterministic column ordering across pipeline configs."""

from pathlib import Path

import yaml

from bioetl.config import load_config

FALLBACK_COLUMNS_PATH = Path("configs/includes/fallback_columns.yaml")
FALLBACK_COLUMNS_RAW = yaml.safe_load(FALLBACK_COLUMNS_PATH.read_text())
if not isinstance(FALLBACK_COLUMNS_RAW, list):  # pragma: no cover - guard clause
    raise AssertionError("Fallback columns include must define a list")

FALLBACK_COLUMNS: list[str] = list(FALLBACK_COLUMNS_RAW)


def _load_pipeline_config(name: str):
    return load_config(Path(f"configs/pipelines/{name}.yaml"))


def test_fallback_columns_tail_activity():
    config = _load_pipeline_config("activity")
    column_order = list(config.determinism.column_order)
    assert all(isinstance(column, str) for column in column_order)
    assert column_order[-len(FALLBACK_COLUMNS) :] == FALLBACK_COLUMNS


def test_fallback_columns_tail_assay():
    config = _load_pipeline_config("assay")
    column_order = list(config.determinism.column_order)
    assert all(isinstance(column, str) for column in column_order)
    assert column_order[-len(FALLBACK_COLUMNS) :] == FALLBACK_COLUMNS


def test_fallback_columns_tail_document():
    config = _load_pipeline_config("document")
    column_order = list(config.determinism.column_order)
    assert all(isinstance(column, str) for column in column_order)
    assert column_order[-len(FALLBACK_COLUMNS) :] == FALLBACK_COLUMNS
