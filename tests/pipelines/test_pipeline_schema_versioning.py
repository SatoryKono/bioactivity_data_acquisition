"""Tests for schema version enforcement inside PipelineBase."""

from __future__ import annotations

from typing import Callable

import pandas as pd
import pandera as pa
import pytest
from pandera import Column

from bioetl.config.models.http import HTTPClientConfig, HTTPConfig
from bioetl.config.models.models import (
    MaterializationConfig,
    PipelineConfig,
    PipelineMetadata,
    ValidationConfig,
)
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import SchemaDescriptor, SchemaRegistry
from bioetl.schemas.versioning import (
    SchemaMigration,
    SchemaMigrationRegistry,
    SchemaVersionMismatchError,
)


class MinimalPipeline(PipelineBase):
    """Test double implementing abstract methods."""

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover - unused
        return pd.DataFrame()

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        return pd.DataFrame()

    def extract_by_ids(self, ids: list[str]) -> pd.DataFrame:  # pragma: no cover - unused
        return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
        return df


@pytest.fixture
def schema_registry_setup(monkeypatch: pytest.MonkeyPatch) -> tuple[str, SchemaMigrationRegistry]:
    """Provide isolated schema/migration registries for tests."""

    schema_registry = SchemaRegistry()
    migration_registry = SchemaMigrationRegistry()
    monkeypatch.setattr("bioetl.schemas.SCHEMA_REGISTRY", schema_registry)
    monkeypatch.setattr("bioetl.schemas.versioning.SCHEMA_MIGRATION_REGISTRY", migration_registry)
    monkeypatch.setattr("bioetl.pipelines.base.SCHEMA_MIGRATION_REGISTRY", migration_registry)

    schema_identifier = "tests.schemas.Versioned"
    schema = pa.DataFrameSchema(
        {
            "value_new": Column(pa.String, nullable=False),
            "load_meta_id": Column(pa.String, nullable=False),
            "hash_row": Column(pa.String, nullable=False),
            "hash_business_key": Column(pa.String, nullable=False),
        },
        strict=True,
        name="VersionedSchema",
    )
    descriptor = SchemaDescriptor.from_components(
        identifier=schema_identifier,
        schema=schema,
        version="2.0.0",
        column_order=("value_new", "load_meta_id", "hash_row", "hash_business_key"),
        business_key_fields=("value_new",),
        required_fields=("value_new", "load_meta_id", "hash_row", "hash_business_key"),
        row_hash_fields=("value_new", "load_meta_id"),
    )
    schema_registry.register(descriptor)
    return schema_identifier, migration_registry


def _build_pipeline_config(
    *,
    schema_identifier: str,
    schema_out_version: str | None,
    allow_migration: bool,
    tmp_path_factory: Callable[[], str],
) -> PipelineConfig:
    """Helper to construct PipelineConfig objects for tests."""

    return PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="test_pipeline",
            version="1.0.0",
        ),
        http=HTTPConfig(default=HTTPClientConfig()),
        materialization=MaterializationConfig(root=str(tmp_path_factory())),
        validation=ValidationConfig(
            schema_out=schema_identifier,
            schema_out_version=schema_out_version,
            allow_schema_migration=allow_migration,
            max_schema_migration_hops=5,
        ),
    )


def test_validate_applies_schema_migration(
    schema_registry_setup: tuple[str, SchemaMigrationRegistry],
    tmp_path,
) -> None:
    schema_identifier, migration_registry = schema_registry_setup

    def _rename_column(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["value_new"] = result.pop("value_old").astype("string")
        return result

    migration_registry.register(
        SchemaMigration(
            schema_identifier=schema_identifier,
            from_version="1.0.0",
            to_version="2.0.0",
            transform_fn=_rename_column,
            description="Rename value column",
        )
    )

    config = _build_pipeline_config(
        schema_identifier=schema_identifier,
        schema_out_version="1.0.0",
        allow_migration=True,
        tmp_path_factory=lambda: tmp_path,
    )
    pipeline = MinimalPipeline(config, run_id="run-1")

    df = pd.DataFrame(
        {
            "value_old": pd.Series(["alpha", "beta"], dtype="string"),
            "load_meta_id": pd.Series(["m1", "m2"], dtype="string"),
            "hash_row": pd.Series(["r1", "r2"], dtype="string"),
            "hash_business_key": pd.Series(["bk1", "bk2"], dtype="string"),
        }
    )

    validated = pipeline.validate(df)
    assert list(validated.columns) == [
        "value_new",
        "load_meta_id",
        "hash_row",
        "hash_business_key",
    ]
    assert validated["value_new"].tolist() == ["alpha", "beta"]
    summary = pipeline._validation_summary or {}
    assert summary.get("migrations_applied") == 1
    assert summary.get("migrated_from_version") == "1.0.0"
    assert pipeline._validation_schema_version == "2.0.0"


def test_validate_raises_when_migration_disabled(
    schema_registry_setup: tuple[str, SchemaMigrationRegistry],
    tmp_path,
) -> None:
    schema_identifier, migration_registry = schema_registry_setup

    migration_registry.register(
        SchemaMigration(
            schema_identifier=schema_identifier,
            from_version="1.0.0",
            to_version="2.0.0",
            transform_fn=lambda df: df,
            description="noop",
        )
    )

    config = _build_pipeline_config(
        schema_identifier=schema_identifier,
        schema_out_version="1.0.0",
        allow_migration=False,
        tmp_path_factory=lambda: tmp_path,
    )
    pipeline = MinimalPipeline(config, run_id="run-2")
    df = pd.DataFrame(
        {
            "value_old": pd.Series(["alpha"], dtype="string"),
            "load_meta_id": pd.Series(["m1"], dtype="string"),
            "hash_row": pd.Series(["r1"], dtype="string"),
            "hash_business_key": pd.Series(["bk1"], dtype="string"),
        }
    )

    with pytest.raises(SchemaVersionMismatchError):
        pipeline.validate(df)


def test_run_schema_validation_uses_schema_in_version(
    schema_registry_setup: tuple[str, SchemaMigrationRegistry],
    tmp_path,
) -> None:
    schema_identifier, migration_registry = schema_registry_setup

    def _rename_column(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["value_new"] = result.pop("value_old").astype("string")
        return result

    migration_registry.register(
        SchemaMigration(
            schema_identifier=schema_identifier,
            from_version="1.0.0",
            to_version="2.0.0",
            transform_fn=_rename_column,
            description="Rename value column",
        )
    )

    config = PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="test_pipeline",
            version="1.0.0",
        ),
        http=HTTPConfig(default=HTTPClientConfig()),
        materialization=MaterializationConfig(root=str(tmp_path)),
        validation=ValidationConfig(
            schema_out=schema_identifier,
            schema_out_version="2.0.0",
            schema_in=schema_identifier,
            schema_in_version="1.0.0",
            allow_schema_migration=True,
            max_schema_migration_hops=5,
        ),
    )
    pipeline = MinimalPipeline(config, run_id="run-3")

    df = pd.DataFrame(
        {
            "value_old": pd.Series(["alpha", "beta"], dtype="string"),
            "load_meta_id": pd.Series(["m1", "m2"], dtype="string"),
            "hash_row": pd.Series(["r1", "r2"], dtype="string"),
            "hash_business_key": pd.Series(["bk1", "bk2"], dtype="string"),
        }
    )

    validated = pipeline.run_schema_validation(
        df,
        schema_identifier,
        dataset_name="input",
    )
    assert list(validated.columns) == [
        "value_new",
        "load_meta_id",
        "hash_row",
        "hash_business_key",
    ]
