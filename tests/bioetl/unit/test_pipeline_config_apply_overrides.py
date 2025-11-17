"""Unit tests exercising PipelineConfig.apply_overrides behavior."""

from __future__ import annotations

from pathlib import Path

from bioetl.config.environment import EnvironmentSettings
from bioetl.config.models.models import PipelineConfig
from bioetl.core.runtime.cli_pipeline_runner import (
    PipelineCommandOptions,
    PipelineConfigFactory,
)


def test_apply_overrides_updates_nested_sections(
    pipeline_config_fixture: PipelineConfig,
    tmp_path: Path,
) -> None:
    base_config = pipeline_config_fixture.model_copy(deep=True)
    overrides = {
        "cli": {"dry_run": True, "limit": 50},
        "materialization": {"root": str(tmp_path / "custom")},
    }

    updated = base_config.apply_overrides(overrides)

    assert updated is not base_config
    assert base_config.cli.dry_run is False
    assert updated.cli.dry_run is True
    assert updated.cli.limit == 50
    assert updated.materialization.root == str(tmp_path / "custom")


def test_pipeline_config_factory_applies_cli_and_materialization_overrides(
    pipeline_config_fixture: PipelineConfig,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "pipeline.yml"
    config_path.write_text("pipeline: test")
    golden_path = tmp_path / "golden.csv"
    input_file = tmp_path / "input.csv"

    def _load_config(**_: object) -> PipelineConfig:
        return pipeline_config_fixture.model_copy(deep=True)

    factory = PipelineConfigFactory(
        environment_loader=EnvironmentSettings,
        environment_runtime_applier=lambda _: None,
        config_loader=_load_config,
    )

    options = PipelineCommandOptions(
        config_path=config_path,
        output_dir=tmp_path,
        dry_run=True,
        verbose=True,
        set_overrides={"activity.limit": "100"},
        sample=5,
        limit=10,
        extended=True,
        fail_on_schema_drift=False,
        validate_columns=False,
        golden=golden_path,
        input_file=input_file,
    )

    result = factory.create(options)

    assert result.cli.dry_run is True
    assert result.cli.verbose is True
    assert result.cli.limit == 10
    assert result.cli.sample == 5
    assert result.cli.extended is True
    assert result.cli.fail_on_schema_drift is False
    assert result.cli.validate_columns is False
    assert result.cli.golden == str(golden_path)
    assert result.cli.input_file == str(input_file)
    assert result.cli.set_overrides == {"activity.limit": "100"}
    assert result.validation.strict is False
    assert result.materialization.root == str(tmp_path)
