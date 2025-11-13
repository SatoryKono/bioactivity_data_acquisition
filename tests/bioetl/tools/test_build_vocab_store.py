"""Тесты для утилиты сборки словаря ChEMBL."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from bioetl.etl.vocab_store import VocabStoreError
from bioetl.tools.build_vocab_store import build_vocab_store


class _LoggerStub:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, str]]] = []

    def info(self, event: str, **context: str) -> None:
        self.records.append((event, context))


@pytest.fixture
def logger_stub(monkeypatch: pytest.MonkeyPatch) -> _LoggerStub:
    """Подменяем UnifiedLogger на заглушку."""

    logger = _LoggerStub()
    monkeypatch.setattr("bioetl.tools.build_vocab_store.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.UnifiedLogger.get", lambda *_: logger)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.clear_vocab_store_cache", lambda: None)
    return logger


@pytest.mark.unit
def test_build_vocab_store_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, logger_stub: _LoggerStub
) -> None:
    """Собираем словарь и проверяем артефакт и логирование."""

    store: dict[str, Any] = {
        "meta": {"source": "tests"},
        "dictionary_a": {
            "meta": {"chembl_release": "34"},
            "values": [{"id": "A1"}],
        },
        "dictionary_b": {
            "values": [{"id": "B1"}],
        },
    }

    monkeypatch.setattr("bioetl.tools.build_vocab_store.read_vocab_store", lambda *_: store)

    output_path = tmp_path / "aggregated.yaml"
    result_path = build_vocab_store(tmp_path / "vocab", output_path)

    payload = yaml.safe_load(result_path.read_text(encoding="utf-8"))

    assert result_path == output_path.resolve()
    assert payload["meta"]["chembl_release"] == "34"
    assert "dictionary_a" in payload
    assert payload["dictionary_a"]["values"][0]["id"] == "A1"
    assert payload["meta"]["built_at"].endswith("Z")
    assert logger_stub.records[0][0] == "vocab_store_built"
    assert logger_stub.records[0][1]["output"] == str(output_path.resolve())


@pytest.mark.unit
def test_build_vocab_store_inconsistent_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибка при несовпадающих релизах ChEMBL."""

    store = {
        "dictionary_a": {"meta": {"chembl_release": "34"}, "values": []},
        "dictionary_b": {"meta": {"chembl_release": "35"}, "values": []},
    }
    monkeypatch.setattr("bioetl.tools.build_vocab_store.read_vocab_store", lambda *_: store)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr(
        "bioetl.tools.build_vocab_store.UnifiedLogger.get",
        lambda *_: SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    with pytest.raises(VocabStoreError, match="Inconsistent chembl_release"):
        build_vocab_store(Path("src"), Path("out"))


@pytest.mark.unit
def test_build_vocab_store_invalid_meta_section(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибка при неверном типе meta-секции словаря."""

    store = {"dictionary_a": {"meta": ["not-a-mapping"], "values": []}}
    monkeypatch.setattr("bioetl.tools.build_vocab_store.read_vocab_store", lambda *_: store)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr(
        "bioetl.tools.build_vocab_store.UnifiedLogger.get",
        lambda *_: SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    with pytest.raises(VocabStoreError, match="meta section must be a mapping"):
        build_vocab_store(Path("src"), Path("out"))


@pytest.mark.unit
def test_build_vocab_store_handles_empty_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибка при отсутствии словарей."""

    store: dict[str, Any] = {}
    monkeypatch.setattr("bioetl.tools.build_vocab_store.read_vocab_store", lambda *_: store)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr("bioetl.tools.build_vocab_store.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr(
        "bioetl.tools.build_vocab_store.UnifiedLogger.get",
        lambda *_: SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    with pytest.raises(VocabStoreError, match="No dictionaries found"):
        build_vocab_store(Path("src"), Path("out"))

