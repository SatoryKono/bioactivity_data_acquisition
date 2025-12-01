from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.core.config.environment import (
    EnvironmentSettings,
    load_environment_settings,
)


def test_environment_settings_aliases(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        """
PUBMED_TOOL=mytool
CROSSREF_MAILTO=user@example.com
BIOETL_OFFLINE_CHEMBL_CLIENT=1
""",
        encoding="utf-8",
    )
    settings = load_environment_settings(env_file=env_file)
    assert settings.pubmed_tool == "mytool"
    assert settings.crossref_mailto == "user@example.com"
    assert settings.offline_chembl_client is True


def test_environment_settings_validates_email() -> None:
    with pytest.raises(ValueError):
        EnvironmentSettings(crossref_mailto="invalid")


def test_environment_settings_normalizes_email_and_env() -> None:
    settings = EnvironmentSettings(
        crossref_mailto="  user@example.com  ", bioetl_env="  PROD  "
    )

    assert settings.crossref_mailto == "user@example.com"
    assert settings.bioetl_env == "prod"


@pytest.mark.parametrize(
    "raw_value,expected",
    [
        ("1", True),
        ("false", False),
        (" yes ", True),
        ("off", False),
    ],
)
def test_environment_settings_coerces_boolean(raw_value: str, expected: bool) -> None:
    settings = EnvironmentSettings(offline_chembl_client=raw_value)

    assert settings.offline_chembl_client is expected


def test_environment_settings_rejects_invalid_email() -> None:
    with pytest.raises(ValueError):
        EnvironmentSettings(pubmed_email="no-at-symbol")
