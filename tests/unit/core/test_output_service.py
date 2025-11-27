import pandas as pd
import pytest

from bioetl.core.io.output_service import PipelineOutputService
from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult


class DummyResultWriter:
    def __init__(self) -> None:
        self.output_dir = None
        self.called_with_atomic = False
        self.called_with_write = False

    def write_dataset_atomic(self, df, artifacts, *, format="csv"):
        self.called_with_atomic = True
        assert format == "csv"
        return WriteResult(rows=len(df), artifacts=artifacts.write_artifacts or WriteArtifacts())

    def write(self, df, artifacts, *, run_stem, output_dir):  # pragma: no cover - fallback
        self.called_with_write = True
        return WriteResult(rows=len(df), artifacts=artifacts)


@pytest.fixture()
def sample_options() -> StageExecutionOptions:
    return StageExecutionOptions(run_tag=None, mode=None)


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    return pd.DataFrame({"a": [1]})


def test_sets_output_dir(tmp_path, sample_frame, sample_options):
    writer = DummyResultWriter()
    artifacts = WriteArtifacts(data_path=tmp_path / "data.csv")
    service = PipelineOutputService({"io": {"writer": writer}})

    result = service.save(sample_frame, artifacts, sample_options)

    assert writer.output_dir == tmp_path
    assert result.rows == 1


def test_prefers_atomic_writer(tmp_path, sample_frame, sample_options):
    writer = DummyResultWriter()
    artifacts = WriteArtifacts(data_path=tmp_path / "data.csv")
    service = PipelineOutputService({"io": {"writer": writer}})

    service.save(sample_frame, artifacts, sample_options)

    assert writer.called_with_atomic is True
    assert writer.called_with_write is False


def test_raises_when_writer_missing(tmp_path):
    service = PipelineOutputService({})

    with pytest.raises(ValueError):
        service.resolve_writer(tmp_path)


def test_emits_qc_artifact(monkeypatch, tmp_path, sample_frame, sample_options):
    writer = DummyResultWriter()
    artifacts = WriteArtifacts(data_path=tmp_path / "data.csv")
    service = PipelineOutputService({"io": {"writer": writer}})
    qc_called: dict[str, bool] = {"flag": False}

    def _fake_emit(df, run_artifacts):
        qc_called["flag"] = True
        assert run_artifacts.output_dir == tmp_path
        return {}

    monkeypatch.setattr(
        "bioetl.core.io.output_service.emit_qc_artifact",
        _fake_emit,
    )

    service.save(sample_frame, artifacts, sample_options)

    assert qc_called["flag"] is True
