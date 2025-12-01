"""Test Output Service."""
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts
from bioetl.core.io.output_service import PipelineOutputService


@pytest.fixture(name="mock_writer")
def fixture_mock_writer() -> Mock:
    """Create a mock writer with write_dataset_atomic and write methods."""
    writer = Mock()
    writer.write_dataset_atomic.return_value = Mock(success=True)
    writer.write.return_value = Mock(success=True)
    return writer


@pytest.fixture(name="mock_logger")
def fixture_mock_logger() -> Mock:
    """Create a mock logger."""
    return Mock()


def test_resolve_writer_sets_output_dir(mock_writer: Mock) -> None:
    """Test that resolve_writer sets the output directory on the writer."""
    output_dir = Path("/tmp/output")
    config = {"io": {"writer": mock_writer}}
    service = PipelineOutputService(config=config)

    resolved = service.resolve_writer(output_dir)

    assert resolved is mock_writer
    assert mock_writer.output_dir == output_dir


def test_resolve_writer_returns_none_if_missing() -> None:
    """Test that resolve_writer returns None if no writer is configured."""
    service = PipelineOutputService(config={})
    assert service.resolve_writer(Path("/tmp")) is None


def test_save_calls_write_atomic_and_qc(mock_writer: Mock, mock_logger: Mock) -> None:
    """Test that save calls write_dataset_atomic and emits QC artifacts."""
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


def test_save_calls_write_fallback(mock_logger: Mock) -> None:
    """Test save falls back to write if write_dataset_atomic is missing."""
    fallback_writer = Mock()
    # Ensure it doesn't have this method
    del fallback_writer.write_dataset_atomic
    fallback_writer.write.return_value = Mock(success=True)

    df = pd.DataFrame({"col": [1]})
    artifacts = WriteArtifacts(data_path=Path("/tmp/data.csv"))
    output_dir = Path("/tmp/output")

    config = {"io": {"writer": fallback_writer}}
    service = PipelineOutputService(config=config, logger=mock_logger)

    with patch("bioetl.core.io.output.emit_qc_artifact"):
        service.save(df, artifacts, output_dir)

        fallback_writer.write.assert_called_once()


def test_save_raises_if_no_writer() -> None:
    """Test that save raises RuntimeError if no writer is configured."""
    service = PipelineOutputService(config={})
    with pytest.raises(RuntimeError, match="No unified writer configured"):
        service.save(pd.DataFrame(), WriteArtifacts(None), Path("/tmp"))


def test_save_handles_qc_error(mock_writer: Mock, mock_logger: Mock) -> None:
    """Test that save logs an error if QC emission fails."""
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
