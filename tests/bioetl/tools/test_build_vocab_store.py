from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import build_vocab_store


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()


def test_extract_release_validation() -> None:
    assert build_vocab_store._extract_release(None, name="demo", current=None) is None
    meta = {"chembl_release": "v1"}
    assert build_vocab_store._extract_release(meta, name="demo", current=None) == "v1"
    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store._extract_release({"chembl_release": 1}, name="demo", current="v2")


def test_build_vocab_store_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    output_path = tmp_path / "out" / "store.yaml"

    payload = {
        "meta": {"chembl_release": "v1"},
        "activity": {"meta": {"chembl_release": "v1"}, "values": [], "other": 1},
    }

    def fake_load_vocab_store(path: Path) -> dict[str, Any]:
        assert path == src_dir.resolve()
        return payload

    monkeypatch.setattr(build_vocab_store, "load_vocab_store", fake_load_vocab_store)
    monkeypatch.setattr(build_vocab_store, "UnifiedLogger", DummyUnifiedLogger)

    result_path = build_vocab_store.build_vocab_store(src_dir, output_path)
    assert result_path == output_path.resolve()
    data = result_path.read_text(encoding="utf-8")
    assert "chembl_release" in data

