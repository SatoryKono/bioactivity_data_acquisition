"""Тесты для build_vocab_store."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

import pytest

from bioetl.etl.vocab_store import VocabStoreError
from bioetl.tools import build_vocab_store as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def bind(self, **_: Any) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


@pytest.fixture(autouse=True)
def patch_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())


def test_extract_release_enforces_consistency() -> None:
    meta_a: Mapping[str, Any] = {"chembl_release": "33"}
    meta_b: Mapping[str, Any] = {"chembl_release": "33"}
    first = module._extract_release(meta_a, name="a", current=None)
    assert first == "33"
    assert module._extract_release(meta_b, name="b", current=first) == "33"

    with pytest.raises(VocabStoreError):
        module._extract_release({"chembl_release": "32"}, name="c", current="33")


def test_atomic_write_yaml_creates_file(tmp_path: Path) -> None:
    target = tmp_path / "vocab.yaml"
    module._atomic_write_yaml({"key": "value"}, target)
    assert target.exists()
    payload = target.read_text(encoding="utf-8")
    assert "key: value" in payload


def test_build_vocab_store_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    output_path = tmp_path / "out.yaml"

    vocab_payload = {
        "dictionary_a": {
            "meta": {"chembl_release": "33"},
            "items": [{"id": 1}],
        },
        "dictionary_b": {
            "meta": {"chembl_release": "33"},
            "items": [{"id": 2}],
        },
    }

    monkeypatch.setattr(module, "_utc_timestamp", lambda: "2025-01-01T00:00:00Z")
    monkeypatch.setattr(module, "clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr(module, "load_vocab_store", lambda path: vocab_payload)

    result = module.build_vocab_store(src=src_dir, output=output_path)

    assert result == output_path.resolve()
    assert output_path.exists()
    content = yaml.safe_load(output_path.read_text(encoding="utf-8"))
    assert content["meta"]["chembl_release"] == "33"
    assert content["meta"]["built_at"] == "2025-01-01T00:00:00Z"


def test_build_vocab_store_missing_release(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    output_path = tmp_path / "out.yaml"

    payload = {
        "dictionary_a": {"items": [{"id": 1}]},
    }

    monkeypatch.setattr(module, "clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr(module, "load_vocab_store", lambda path: payload)

    with pytest.raises(VocabStoreError):
        module.build_vocab_store(src=src_dir, output=output_path)

