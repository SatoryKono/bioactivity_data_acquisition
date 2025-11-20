from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from bioetl.schemas.common import default_schema_path, load_schema, refresh_vocabulary


class DummyResponse:
    def __init__(self, status_code: int, content: bytes = b"", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def test_default_schema_path_with_custom_base(tmp_path: Path) -> None:
    base = tmp_path / "configs"
    result = default_schema_path("dictionaries", base_path=base)
    assert result == (base / "dictionaries").resolve()


@pytest.mark.parametrize("use_name", [True, False])
def test_load_schema_from_path(tmp_path: Path, use_name: bool) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir(parents=True)
    schema_file = schema_dir / "test.yaml"
    schema_file.write_text("name: example\nvalue: 1\n", encoding="utf-8")

    target: str | Path = "test.yaml" if use_name else schema_file
    loaded = load_schema(target, base_path=schema_dir)
    assert loaded == {"name": "example", "value": 1}


def test_refresh_vocabulary_uses_etag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cache_file = tmp_path / "cache.yaml"
    cache_file.write_bytes(b"old")
    etag_file = cache_file.with_suffix(cache_file.suffix + ".etag")
    etag_file.write_text("etag-value", encoding="utf-8")

    captured: dict[str, Any] = {}

    def fake_get(url: str, *, headers: dict[str, str] | None = None, timeout: int) -> DummyResponse:  # type: ignore[override]
        captured.update(headers or {})
        return DummyResponse(status_code=304)

    monkeypatch.setattr("bioetl.schemas.common.requests.get", fake_get)

    refreshed = refresh_vocabulary(cache_file, "https://example.com/vocab.yaml")
    assert refreshed is False
    assert cache_file.read_bytes() == b"old"
    assert captured["If-None-Match"] == "etag-value"


def test_refresh_vocabulary_downloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cache_file = tmp_path / "cache.yaml"
    captured_headers: dict[str, Any] = {}

    def fake_get(url: str, *, headers: dict[str, str] | None = None, timeout: int) -> DummyResponse:  # type: ignore[override]
        captured_headers.update(headers or {})
        return DummyResponse(status_code=200, content=b"payload", headers={"ETag": "fresh"})

    monkeypatch.setattr("bioetl.schemas.common.requests.get", fake_get)

    refreshed = refresh_vocabulary(cache_file, "https://example.com/vocab.yaml")
    assert refreshed is True
    assert cache_file.read_bytes() == b"payload"
    assert cache_file.with_suffix(cache_file.suffix + ".etag").read_text(encoding="utf-8") == "fresh"
    assert captured_headers == {}
