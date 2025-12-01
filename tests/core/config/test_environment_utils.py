from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.core.config.environment_utils import (
    DefaultEnvironmentProvider,
    coerce_bool,
    normalize_env_name,
    normalize_tool,
    parse_env_file,
    resolve_vocab_store,
    validate_email,
)


def test_parse_env_file_reads_key_values(tmp_path: Path) -> None:
    env_file = tmp_path / "custom.env"
    env_file.write_text("API_KEY=secret\n# comment\nEMPTY=   \n", encoding="utf-8")

    values = parse_env_file(env_file)

    assert values == {"API_KEY": "secret", "EMPTY": ""}


def test_normalization_helpers_handle_blanks(tmp_path: Path) -> None:
    assert normalize_env_name("  PROD  ") == "prod"
    assert normalize_env_name("   ") is None
    assert normalize_tool(" tool ") == "tool"
    vocab_path = resolve_vocab_store(tmp_path / "vocab.db")
    assert vocab_path is not None and vocab_path.is_absolute()


@pytest.mark.parametrize(
    "value,expected",
    [("true", True), ("0", False), (1, True), ("Off", False)],
)
def test_coerce_bool_matches_expected(value: object, expected: bool) -> None:
    assert coerce_bool(value) is expected


def test_validate_email_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        validate_email("not-an-email")

    assert validate_email("  user@example.com  ") == "user@example.com"


def test_default_environment_provider_uses_loader(tmp_path: Path) -> None:
    calls: list[Path | None] = []

    def loader(env_file: Path | None = None):  # type: ignore[override]
        calls.append(env_file)
        class Dummy:
            model_fields: dict[str, object] = {}
        return Dummy()

    provider = DefaultEnvironmentProvider(loader)
    env_file = tmp_path / "stub.env"
    env_file.write_text("NAME=value", encoding="utf-8")

    provider.load_environment_settings(env_file=env_file)
    assert calls == [env_file]

    loaded = provider.load_env_file_values(env_file)
    assert loaded == {"NAME": "value"}
