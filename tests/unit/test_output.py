"""Unit tests for output utilities."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.config.models import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSerializationConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSortingConfig,
    HTTPClientConfig,
    HTTPConfig,
    MaterializationConfig,
    PipelineMetadata,
    RetryConfig,
    ValidationConfig,
)
from bioetl.core.output import (
    DeterministicWriteArtifacts,
    ensure_hash_columns,
    prepare_dataframe,
    serialise_metadata,
    write_dataset_atomic,
    write_frame_like,
    write_yaml_atomic,
)


@pytest.fixture
def output_config(tmp_path: Path) -> PipelineConfig:
    """Sample PipelineConfig for output testing."""
    return PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="test_pipeline",
            version="1.0.0",
            description="Test pipeline",
        ),
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
        ),
        materialization=MaterializationConfig(root=str(tmp_path)),
        determinism=DeterminismConfig(  # type: ignore[call-arg]
            sort=DeterminismSortingConfig(by=["id"], ascending=[True]),
            hashing=DeterminismHashingConfig(
                business_key_fields=("id",),
                row_fields=("id", "value"),
            ),
            column_order=(),  # Requires schema_out to be set
            serialization=DeterminismSerializationConfig(
                booleans=("true", "false"),
                nan_rep="",
                csv=DeterminismSerializationCSVConfig(
                    separator=",",
                    na_rep="",
                    quoting="minimal",
                ),
            ),
            float_precision=6,
        ),
        validation=ValidationConfig(
            strict=True,
            coerce=True,
            schema_out="bioetl.schemas.activity:ActivitySchema",  # Required for column_order
        ),
    )


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [3, 1, 2],
            "value": [30.0, 10.0, 20.0],
            "name": ["C", "A", "B"],
        }
    )


@pytest.mark.unit
class TestOutput:
    """Test suite for output utilities."""

    def test_prepare_dataframe_sort(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame) -> None:
        """Test preparing dataframe with sorting."""
        result = prepare_dataframe(sample_dataframe, config=output_config)

        assert result["id"].tolist() == [1, 2, 3]
        assert result["value"].tolist() == [10.0, 20.0, 30.0]

    def test_prepare_dataframe_column_order(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame) -> None:
        """Test preparing dataframe with column order."""
        # Set column_order and schema_out
        output_config.determinism.column_order = ("id", "value")
        output_config.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"

        result = prepare_dataframe(sample_dataframe, config=output_config)

        # Check that id and value come first
        assert result.columns.tolist()[:2] == ["id", "value"]

    def test_prepare_dataframe_empty(self, output_config: PipelineConfig) -> None:
        """Test preparing empty dataframe."""
        df = pd.DataFrame()
        result = prepare_dataframe(df, config=output_config)

        assert result.empty

    def test_prepare_dataframe_missing_sort_column(self, output_config: PipelineConfig) -> None:
        """Test preparing dataframe with missing sort column raises error."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        output_config.determinism.sort.by = ["id"]

        with pytest.raises(KeyError, match="missing from dataframe"):
            prepare_dataframe(df, config=output_config)

    def test_prepare_dataframe_missing_column_order(self, output_config: PipelineConfig) -> None:
        """Test preparing dataframe with missing column order raises error."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        output_config.determinism.column_order = ("id",)

        with pytest.raises(ValueError, match="missing columns"):
            prepare_dataframe(df, config=output_config)

    def test_ensure_hash_columns_basic(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame) -> None:
        """Test ensuring hash columns are added."""
        result = ensure_hash_columns(sample_dataframe, config=output_config)

        assert "hash_row" in result.columns
        assert "hash_business_key" in result.columns
        assert len(result["hash_row"]) == 3
        assert len(result["hash_business_key"]) == 3

    def test_ensure_hash_columns_existing(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame) -> None:
        """Test ensuring hash columns when they already exist."""
        sample_dataframe["hash_row"] = "existing"
        result = ensure_hash_columns(sample_dataframe, config=output_config)

        assert result["hash_row"].tolist() == ["existing"] * 3

    def test_ensure_hash_columns_no_business_key(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame) -> None:
        """Test ensuring hash columns without business key."""
        output_config.determinism.hashing.business_key_fields = ()
        result = ensure_hash_columns(sample_dataframe, config=output_config)

        assert "hash_row" in result.columns
        assert "hash_business_key" not in result.columns

    def test_ensure_hash_columns_missing_field(self, output_config: PipelineConfig) -> None:
        """Test ensuring hash columns with missing field raises error."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        output_config.determinism.hashing.row_fields = ("id",)

        with pytest.raises(KeyError, match="is missing from dataframe"):
            ensure_hash_columns(df, config=output_config)

    def test_write_dataset_atomic(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path) -> None:
        """Test writing dataset atomically."""
        output_path = tmp_path / "output.csv"
        write_dataset_atomic(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        assert not output_path.with_suffix(output_path.suffix + ".tmp").exists()

        # Verify content
        loaded = pd.read_csv(output_path)  # type: ignore[unknown-member]
        assert len(loaded) == 3

    def test_write_dataset_atomic_creates_directory(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path) -> None:
        """Test writing dataset creates directory if needed."""
        output_path = tmp_path / "subdir" / "output.csv"
        write_dataset_atomic(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_yaml_atomic(self, tmp_path: Path) -> None:
        """Test writing YAML atomically."""
        output_path = tmp_path / "output.yaml"
        payload = {"key": "value", "number": 123}

        write_yaml_atomic(payload, output_path)

        assert output_path.exists()
        assert not output_path.with_suffix(output_path.suffix + ".tmp").exists()

        # Verify content
        import yaml

        with output_path.open("r") as f:
            loaded = yaml.safe_load(f)
        assert loaded == payload

    def test_write_yaml_atomic_creates_directory(self, tmp_path: Path) -> None:
        """Test writing YAML creates directory if needed."""
        output_path = tmp_path / "subdir" / "output.yaml"
        payload = {"key": "value"}

        write_yaml_atomic(payload, output_path)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_frame_like_dataframe(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path) -> None:
        """Test writing frame-like with DataFrame."""
        output_path = tmp_path / "output.csv"
        write_frame_like(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        loaded = pd.read_csv(output_path)  # type: ignore[unknown-member]
        assert len(loaded) == 3

    def test_write_frame_like_dict(self, output_config: PipelineConfig, tmp_path: Path) -> None:
        """Test writing frame-like with dict."""
        output_path = tmp_path / "output.csv"
        data = {"id": 1, "value": 10.0, "name": "test"}
        write_frame_like(data, output_path, config=output_config)

        assert output_path.exists()
        loaded = pd.read_csv(output_path)  # type: ignore[unknown-member]
        assert len(loaded) == 1
        assert loaded["id"].iloc[0] == 1

    def test_serialise_metadata_basic(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path) -> None:
        """Test serializing metadata."""
        dataset_path = tmp_path / "output.csv"
        result = serialise_metadata(
            sample_dataframe,
            config=output_config,
            run_id="test-run-123",
            pipeline_code="test_pipeline",
            dataset_path=dataset_path,
            stage_durations_ms={"extract": 100.0, "transform": 50.0},
        )

        assert isinstance(result, dict)
        assert "row_count" in result
        assert result["row_count"] == 3
        assert result["pipeline"] == "test_pipeline"
        assert result["run_id"] == "test-run-123"

    def test_serialise_metadata_with_hashes(self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path) -> None:
        """Test serializing metadata with hash columns."""
        df_with_hashes = ensure_hash_columns(sample_dataframe, config=output_config)
        dataset_path = tmp_path / "output.csv"
        result = serialise_metadata(
            df_with_hashes,
            config=output_config,
            run_id="test-run-123",
            pipeline_code="test_pipeline",
            dataset_path=dataset_path,
            stage_durations_ms={},
        )

        assert "row_count" in result
        assert "hash_row" in result.get("hashing", {}) or "hash_business_key" in result.get("hashing", {})

    def test_deterministic_write_artifacts(self) -> None:
        """Test DeterministicWriteArtifacts dataclass."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        metadata = {"row_count": 3}

        artifacts = DeterministicWriteArtifacts(dataframe=df, metadata=metadata)

        assert artifacts.dataframe.equals(df)
        assert artifacts.metadata == metadata

