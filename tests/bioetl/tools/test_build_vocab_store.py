from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

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
    assert "built_at" in data


def test_utc_timestamp_format(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    class DummyDateTime:
        @staticmethod
        def now(tz: timezone) -> datetime:
            assert tz is timezone.utc
            return fixed

    monkeypatch.setattr(build_vocab_store, "datetime", DummyDateTime)
    value = build_vocab_store._utc_timestamp()
    assert value == "2024-01-02T03:04:05Z"


def test_atomic_write_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "payload.yaml"
    payload = {"key": "value"}
    called: list[Path] = []

    def fake_fsync(fd: int) -> None:
        called.append(target)

    monkeypatch.setattr(build_vocab_store.os, "fsync", fake_fsync)
    build_vocab_store._atomic_write_yaml(payload, target)
    assert target.exists()
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == payload
    assert called


def test_extract_release_requires_mapping() -> None:
    invalid_meta: Any = ["not-a-mapping"]
    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store._extract_release(
            invalid_meta,
            name="bad",
            current=None,
        )


def test_extract_release_inconsistent() -> None:
    meta = {"chembl_release": "v2"}
    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store._extract_release(meta, name="demo", current="v1")


def test_build_vocab_store_rejects_missing_dictionaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src = tmp_path / "src"
    src.mkdir()
    target = tmp_path / "out.yaml"

    monkeypatch.setattr(build_vocab_store, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(build_vocab_store, "clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr(
        build_vocab_store,
        "load_vocab_store",
        lambda _: {"meta": {"chembl_release": "v1"}},
    )

    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store.build_vocab_store(src, target)


def test_build_vocab_store_validates_block_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src = tmp_path / "src"
    src.mkdir()
    target = tmp_path / "out.yaml"

    monkeypatch.setattr(build_vocab_store, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(build_vocab_store, "clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr(
        build_vocab_store,
        "load_vocab_store",
        lambda _: {
            "meta": {"chembl_release": "v1"},
            "activity": ["invalid"],
        },
    )

    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store.build_vocab_store(src, target)


def test_build_vocab_store_requires_chembl_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src = tmp_path / "src"
    src.mkdir()
    target = tmp_path / "out.yaml"

    monkeypatch.setattr(build_vocab_store, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(build_vocab_store, "clear_vocab_store_cache", lambda: None)
    monkeypatch.setattr(
        build_vocab_store,
        "load_vocab_store",
        lambda _: {"activity": {"values": []}},
    )

    with pytest.raises(build_vocab_store.VocabStoreError):
        build_vocab_store.build_vocab_store(src, target)

