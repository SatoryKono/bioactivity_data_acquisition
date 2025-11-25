from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.config.loader import load_config
from bioetl.config.models import PipelineConfig


def test_load_config_merges_layers_and_env(tmp_path: Path):
    profile = tmp_path / "profile.yaml"
    profile.write_text("section:\n  from_profile: true\n", encoding="utf-8")

    included = tmp_path / "included.yaml"
    included.write_text("inner: 5\n", encoding="utf-8")

    main = tmp_path / "pipeline.yaml"
    main.write_text(
        """
extends: profile.yaml
section:
  from_pipeline: yes
  nested: !include included.yaml
""",
        encoding="utf-8",
    )

    cli_overrides = {"section.cli": "override"}
    env_vars = {"BIOETL__SECTION__FROM_ENV": "42"}

    config = load_config(
        main,
        profiles=[],
        cli_overrides=cli_overrides,
        env=env_vars,
    )

    assert isinstance(config, PipelineConfig)
    payload = config.model_dump()
    assert payload["section"]["from_profile"] is True
    assert payload["section"]["from_pipeline"] is True
    assert payload["section"]["nested"]["inner"] == 5
    assert payload["section"]["cli"] == "override"
    assert payload["section"]["from_env"] == 42


def test_load_config_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")


def test_load_config_rejects_invalid_yaml(tmp_path: Path):
    path = tmp_path / "invalid.yaml"
    path.write_text("- just\n- a list", encoding="utf-8")
    with pytest.raises(TypeError):
        load_config(path)
