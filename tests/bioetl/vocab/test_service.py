import yaml

from domain.vocab.service import (
    VOCAB_STORE_ENV_VAR,
    refresh_vocab_cache,
    required_vocab_ids,
)


def _write_vocab(directory, name: str, values: list[str]) -> None:
    payload = {"values": [{"id": value, "status": "active"} for value in values]}
    path = directory / f"{name}.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def test_refresh_vocab_cache_invalidates_store(monkeypatch, tmp_path):
    vocab_dir = tmp_path / "dictionaries"
    vocab_dir.mkdir()
    _write_vocab(vocab_dir, "status", ["legacy"])

    monkeypatch.setenv(VOCAB_STORE_ENV_VAR, str(vocab_dir))
    refresh_vocab_cache()
    assert required_vocab_ids("status") == {"legacy"}

    _write_vocab(vocab_dir, "status", ["legacy", "current"])
    assert required_vocab_ids("status") == {"legacy"}

    refresh_vocab_cache()
    assert required_vocab_ids("status") == {"legacy", "current"}


def test_missing_dictionary_raises_error(monkeypatch, tmp_path):
    vocab_dir = tmp_path / "dictionaries"
    vocab_dir.mkdir()
    _write_vocab(vocab_dir, "present", ["ok"])

    monkeypatch.setenv(VOCAB_STORE_ENV_VAR, str(vocab_dir))
    refresh_vocab_cache()

    message = "Unable to load vocabulary 'missing'"
    try:
        required_vocab_ids("missing")
    except RuntimeError as exc:
        assert message in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("required_vocab_ids did not raise for missing vocabulary")
