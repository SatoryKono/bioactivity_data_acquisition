import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import pandas as pd

from bioetl.core.io.output_service import PipelineOutputService
from bioetl.core.io.artifacts import WriteArtifacts, RunArtifacts


@pytest.fixture
def mock_writer():
    writer = Mock()
    writer.write_dataset_atomic.return_value = Mock(success=True)
    writer.write.return_value = Mock(success=True)
    return writer


@pytest.fixture
def mock_logger():
    return Mock()


def test_resolve_writer_sets_output_dir(mock_writer):
    output_dir = Path("/tmp/output")
    config = {"io": {"writer": mock_writer}}
    service = PipelineOutputService(config=config)

    resolved = service.resolve_writer(output_dir)

    assert resolved is mock_writer
    assert mock_writer.output_dir == output_dir


def test_resolve_writer_returns_none_if_missing():
    service = PipelineOutputService(config={})
    assert service.resolve_writer(Path("/tmp")) is None


def test_save_calls_write_atomic_and_qc(mock_writer, mock_logger):
    df = pd.DataFrame({"col": [1]})
    artifacts = WriteArtifacts(data_path=Path("/tmp/data.csv"))
    output_dir = Path("/tmp/output")

    config = {"io": {"writer": mock_writer}}
    service = PipelineOutputService(config=config, logger=mock_logger)

    with patch("bioetl.core.io.output.emit_qc_artifact") as mock_emit_qc:
        service.save(df, artifacts, output_dir)

        mock_writer.write_dataset_atomic.assert_called_once()
        args, kwargs = mock_writer.write_dataset_atomic.call_args
        assert args[0] is df
        assert isinstance(args[1], RunArtifacts)
        assert kwargs["format"] == "csv"

        mock_emit_qc.assert_called_once()


def test_save_calls_write_fallback(mock_logger):
    mock_writer = Mock()
    del mock_writer.write_dataset_atomic  # Ensure it doesn't have this method
    mock_writer.write.return_value = Mock(success=True)

    df = pd.DataFrame({"col": [1]})
    artifacts = WriteArtifacts(data_path=Path("/tmp/data.csv"))
    output_dir = Path("/tmp/output")

    config = {"io": {"writer": mock_writer}}
    service = PipelineOutputService(config=config, logger=mock_logger)

    with patch("bioetl.core.io.output.emit_qc_artifact"):
        service.save(df, artifacts, output_dir)

        mock_writer.write.assert_called_once()


def test_save_raises_if_no_writer():
    service = PipelineOutputService(config={})
    with pytest.raises(RuntimeError, match="No unified writer configured"):
        service.save(pd.DataFrame(), WriteArtifacts(None), Path("/tmp"))


def test_save_handles_qc_error(mock_writer, mock_logger):
    df = pd.DataFrame()
    artifacts = WriteArtifacts(data_path=Path("/tmp/data.csv"))
    output_dir = Path("/tmp/output")
    config = {"io": {"writer": mock_writer}}
    service = PipelineOutputService(config=config, logger=mock_logger)

    with patch(
        "bioetl.core.io.output.emit_qc_artifact",
        side_effect=ValueError("QC error"),
    ):
        service.save(df, artifacts, output_dir)

        # Should not raise
        mock_logger.debug.assert_called_with("emit_qc_failed", exc_info=True)
