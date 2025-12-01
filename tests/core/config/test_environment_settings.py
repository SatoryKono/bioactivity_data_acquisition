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
