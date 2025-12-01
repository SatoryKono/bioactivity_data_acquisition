from __future__ import annotations

from pathlib import Path

from bioetl.core.config.config_resolver import EnvSecretProvider, FileConfigResolver
from bioetl.core.config.merge_utils import _collect_env_overrides


def test_env_secret_provider_returns_prefixed_variables(tmp_path: Path) -> None:
    provider = EnvSecretProvider(
        {
            "BIOETL__SECTION__VALUE": "5",
            "API_TOKEN": "from-env",
        }
    )

    overrides = _collect_env_overrides(
        provider.iter_variables(), prefixes=("BIOETL__",)
    )

    assert overrides == {"section": {"value": 5}}
    assert provider.get_secret("API_TOKEN") == "from-env"


def test_env_secret_provider_reads_custom_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "custom.env"
    env_file.write_text(
        """
MY_NUMBER=11
API_TOKEN=top-secret
BIOETL__SECTION__FLAG=true
""".strip(),
        encoding="utf-8",
    )

    main = tmp_path / "pipeline.yaml"
    main.write_text(
        """
section:
  value: ${ENV:MY_NUMBER}
  token: ${SECRET:API_TOKEN}
  flag: false
""",
        encoding="utf-8",
    )

    resolver = FileConfigResolver(
        main, env_prefixes=("BIOETL__",), env_file=env_file
    )
    config = resolver.resolve()

    payload = config.model_dump()
    assert payload["section"]["value"] == 11
    assert payload["section"]["token"] == "top-secret"
    assert payload["section"]["flag"] is True
