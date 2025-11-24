"""Unit tests for deterministic domain helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from infrastructure.config.models.base import PipelineMetadata
from infrastructure.config.models.determinism import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSerializationConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSortingConfig,
)
from infrastructure.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from infrastructure.config.models.models import (
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
)
from infrastructure.config.models.paths import MaterializationConfig
from infrastructure.config.models.validation import ValidationConfig
from infrastructure.io.determinism import (
    DeterministicWriteArtifacts,
    build_write_artifacts,
    ensure_hash_columns,
    prepare_dataframe,
    serialise_metadata,
)


@pytest.fixture
def output_config(tmp_path: Path) -> PipelineConfig:
    """Sample PipelineConfig for output testing."""
    infrastructure = PipelineInfrastructureConfig(
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
        ),
        materialization=MaterializationConfig(root=str(tmp_path)),
        determinism=DeterminismConfig(
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
    )
    domain = PipelineDomainConfig(
        validation=ValidationConfig(
            strict=True,
            coerce=True,
            schema_out="infrastructure.schemas.chembl_activity_schema:ActivitySchema",  # Required for column_order
        ),
    )
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(
            name="test_pipeline",
            version="1.0.0",
            description="Test pipeline",
        ),
        domain=domain,
        infrastructure=infrastructure,
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
class TestDeterminism:
    """Test suite for deterministic helpers."""

    def test_prepare_dataframe_sort(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame
    ) -> None:
        """Test preparing dataframe with sorting."""
        result = prepare_dataframe(sample_dataframe, config=output_config)

        assert result["id"].tolist() == [1, 2, 3]
        assert result["value"].tolist() == [10.0, 20.0, 30.0]

    def test_prepare_dataframe_column_order(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame
    ) -> None:
        """Test preparing dataframe with column order."""
        output_config.determinism.column_order = ("id", "value")
        output_config.validation.schema_out = "infrastructure.schemas.chembl_activity_schema:ActivitySchema"

        result = prepare_dataframe(sample_dataframe, config=output_config)

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

    def test_ensure_hash_columns_basic(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame
    ) -> None:
        """Test ensuring hash columns are added."""
        result = ensure_hash_columns(sample_dataframe, config=output_config)

        assert "hash_row" in result.columns
        assert "hash_business_key" in result.columns
        assert len(result["hash_row"]) == 3
        assert len(result["hash_business_key"]) == 3

    def test_ensure_hash_columns_existing(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame
    ) -> None:
        """Test ensuring hash columns when they already exist."""
        sample_dataframe["hash_row"] = "existing"
        result = ensure_hash_columns(sample_dataframe, config=output_config)

        assert result["hash_row"].tolist() == ["existing"] * 3

    def test_ensure_hash_columns_no_business_key(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame
    ) -> None:
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

    def test_serialise_metadata_basic(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
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

    def test_serialise_metadata_with_hashes(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
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
        assert "hash_row" in result.get("hashing", {}) or "hash_business_key" in result.get(
            "hashing", {}
        )

    def test_build_write_artifacts(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
        """Ensure build_write_artifacts prepares dataframe and metadata."""
        artifacts = build_write_artifacts(
            sample_dataframe,
            config=output_config,
            run_id="run-1",
            pipeline_code="test-pipeline",
            dataset_path=tmp_path / "data.csv",
            stage_durations_ms={"extract": 12.5},
        )

        assert isinstance(artifacts, DeterministicWriteArtifacts)
        assert not artifacts.dataframe.empty
        assert artifacts.metadata["pipeline"] == "test-pipeline"

    def test_deterministic_write_artifacts(self) -> None:
        """Test DeterministicWriteArtifacts dataclass."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        metadata = {"row_count": 3}

        artifacts = DeterministicWriteArtifacts(dataframe=df, metadata=metadata)

        assert artifacts.dataframe.equals(df)
        assert artifacts.metadata == metadata
