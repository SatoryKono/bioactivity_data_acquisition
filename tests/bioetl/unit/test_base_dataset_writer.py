from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.core.io import BaseDatasetWriter, RunArtifacts, WriteArtifacts
from tests.support.factories import build_pipeline_config

pytestmark = pytest.mark.unit


def _build_run_artifacts(dataset_path: Path) -> RunArtifacts:
    write_artifacts = WriteArtifacts(dataset=dataset_path)
    return RunArtifacts(
        write=write_artifacts,
        run_directory=dataset_path.parent,
        manifest=None,
        log_file=dataset_path.parent / "run.log",
    )


def test_writer_delegates_to_write_dataset_atomic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dataset_path = tmp_path / "dataset.csv"
    artifacts = _build_run_artifacts(dataset_path)
    config = object()
    writer = BaseDatasetWriter(config=config)  # type: ignore[arg-type]
    frame = pd.DataFrame({"id": [1]})
    prepared = pd.DataFrame({"id": [1], "extra": ["prepared"]})

    prepare_mock = MagicMock(return_value=prepared)
    write_mock = MagicMock()
    monkeypatch.setattr("bioetl.core.io.base_writer.prepare_dataframe", prepare_mock)
    monkeypatch.setattr("bioetl.core.io.base_writer.write_dataset_atomic", write_mock)

    result = writer.write(frame, artifacts)

    prepare_mock.assert_called_once_with(frame, config=config)
    write_mock.assert_called_once_with(prepared, dataset_path, config=config)
    assert result.dataset == dataset_path


def test_writer_accepts_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.csv"
    writer = BaseDatasetWriter(config=object())  # type: ignore[arg-type]
    frame = pd.DataFrame({})

    monkeypatch.setattr(
        "bioetl.core.io.base_writer.prepare_dataframe",
        MagicMock(return_value=frame),
    )
    write_mock = MagicMock()
    monkeypatch.setattr("bioetl.core.io.base_writer.write_dataset_atomic", write_mock)

    writer.write(frame, dataset_path)

    write_mock.assert_called_once_with(frame, dataset_path, config=writer.config)


def test_write_uses_pipeline_config(tmp_path: Path) -> None:
    frame = pd.DataFrame({"id": [2, 1], "value": ["b", "a"]})
    dataset_path = tmp_path / "dataset.csv"
    config = build_pipeline_config(tmp_path)
    config.infrastructure.determinism.column_order = ("value", "id")
    config.infrastructure.determinism.sort.by = ["id"]
    writer = BaseDatasetWriter(config=config)

    result = writer.write(frame, dataset_path)

    loaded = pd.read_csv(result.dataset)
    assert list(loaded.columns)[:2] == ["value", "id"]
    assert loaded.to_dict(orient="list") == {"value": ["a", "b"], "id": [1, 2]}
