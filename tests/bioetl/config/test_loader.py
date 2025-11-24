from __future__ import annotations

from pathlib import Path

import pytest
from bioetl.config.loader import ConfigValidationError, load_pipeline_config


def test_load_pipeline_config_success() -> None:
    config_path = Path("configs/pipelines/activity/activity_chembl.yaml")
    config = load_pipeline_config(config_path)

    assert config.pipeline.name == "activity_chembl"
    assert config.pipeline.version


def test_load_pipeline_config_validation_error(tmp_path: Path) -> None:
    invalid_config = tmp_path / "invalid.yaml"
    invalid_config.write_text("pipeline: null\n", encoding="utf-8")

    with pytest.raises(ConfigValidationError) as exc_info:
        load_pipeline_config(invalid_config)

    assert "Validation failed" in str(exc_info.value)
