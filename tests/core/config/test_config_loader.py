from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

from bioetl.core.config.config_resolver import FileConfigResolver, SecretProviderABC
from bioetl.core.config.loader import load_config
from bioetl.core.config.models import PipelineConfig


def test_load_config_merges_layers_and_env(tmp_path: Path) -> None:
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


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")


def test_load_config_rejects_invalid_yaml(tmp_path: Path) -> None:
    path = tmp_path / "invalid.yaml"
    path.write_text("- just\n- a list", encoding="utf-8")
    with pytest.raises(TypeError):
        load_config(path)


def test_file_config_resolver_merges_profile_argument(tmp_path: Path) -> None:
    profile = tmp_path / "profile.yaml"
    profile.write_text("section:\n  from_profile: true\n", encoding="utf-8")

    main = tmp_path / "pipeline.yaml"
    main.write_text("section:\n  from_pipeline: yes\n", encoding="utf-8")

    resolver = FileConfigResolver(main)
    config = resolver.resolve(profile=profile)

    payload = config.model_dump()
    assert payload["section"]["from_profile"] is True
    assert payload["section"]["from_pipeline"] is True


def test_file_config_resolver_applies_cli_overrides(tmp_path: Path) -> None:
    main = tmp_path / "pipeline.yaml"
    main.write_text("section:\n  value: base\n", encoding="utf-8")

    resolver = FileConfigResolver(main)
    config = resolver.resolve(overrides={"section.value": "from_cli"})

    assert config.model_dump()["section"]["value"] == "from_cli"


class _DummySecretProvider(SecretProviderABC):
    def __init__(self, secrets: dict[str, str]) -> None:
        self._secrets = secrets

    def get_secret(self, name: str) -> str:
        return self._secrets[name]

    def get_variable(self, name: str) -> str:
        return self._secrets[name]

    def iter_variables(self) -> Mapping[str, str]:
        return dict(self._secrets)


def test_file_config_resolver_injects_env_and_secrets(tmp_path: Path) -> None:
    main = tmp_path / "pipeline.yaml"
    main.write_text(
        """
section:
  number: ${ENV:MY_NUMBER}
  token: ${SECRET:API_TOKEN}
""",
        encoding="utf-8",
    )

    env_vars = {"MY_NUMBER": "123"}
    secrets = _DummySecretProvider({"API_TOKEN": "super-secret"})

    resolver = FileConfigResolver(main, env=env_vars, secret_provider=secrets)
    config = resolver.resolve()

    payload = config.model_dump()
    assert payload["section"]["number"] == 123
    assert payload["section"]["token"] == "super-secret"
