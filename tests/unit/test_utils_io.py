"""Unit tests for input loading helpers."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.utils.io import load_input_frame


def _build_config(tmp_path: Path) -> PipelineConfig:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    cache_dir = tmp_path / "cache"
    input_root.mkdir()
    output_root.mkdir()
    cache_dir.mkdir()

    return PipelineConfig.model_validate(
        {
            "version": 1,
            "pipeline": {"name": "test", "entity": "unit"},
            "http": {},
            "cache": {"enabled": False, "directory": str(cache_dir), "ttl": 0},
            "paths": {
                "input_root": str(input_root),
                "output_root": str(output_root),
            },
            "determinism": {},
        }
    )


def test_load_input_frame_returns_empty_for_missing_file(tmp_path: Path) -> None:
    """Ensure the helper yields an empty frame with the requested columns."""

    config = _build_config(tmp_path)
    frame = load_input_frame(config, Path("missing.csv"), expected_columns=["id"])

    assert frame.empty
    assert list(frame.columns) == ["id"]


def test_load_input_frame_applies_limit(tmp_path: Path) -> None:
    """The helper should honour runtime limits when reading CSV files."""

    config = _build_config(tmp_path)
    source_path = config.paths.input_root / "data.csv"

    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    df.to_csv(source_path, index=False)

    limited = load_input_frame(
        config,
        Path("data.csv"),
        expected_columns=["id", "value"],
        limit=2,
    )

    assert len(limited) == 2
    assert list(limited.columns) == ["id", "value"]
    assert limited["id"].tolist() == [1, 2]
