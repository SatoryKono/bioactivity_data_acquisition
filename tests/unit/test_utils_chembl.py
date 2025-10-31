"""Tests for the ChEMBL utility helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.utils.chembl import ChemblRelease, _resolve_release_name, fetch_chembl_release


class _StubClient:
    """Simple stub matching the :class:`SupportsRequestJson` protocol."""

    def __init__(self, payload: Mapping[str, Any]):
        self.payload = payload
        self.calls: list[str] = []

    def request_json(self, url: str, /, **_: Any) -> Mapping[str, Any]:
        self.calls.append(url)
        return self.payload


def test_fetch_chembl_release_with_client():
    """The helper should reuse the provided client's ``request_json`` method."""

    payload = {"chembl_db_version": "ChEMBL_X", "chembl_release_date": "2024-01-01"}
    client = _StubClient(payload)

    release = fetch_chembl_release(client)

    assert release == ChemblRelease(version="ChEMBL_X", status=payload)
    assert client.calls == ["/status.json"]


def test_fetch_chembl_release_with_url(monkeypatch):
    """Passing a string should trigger a direct HTTP request."""

    captured: dict[str, Any] = {}

    class _Response:
        def __init__(self, data: Mapping[str, Any]):
            self._data = data

        def raise_for_status(self) -> None:  # pragma: no cover - always succeeds in test
            return None

        def json(self) -> Mapping[str, Any]:
            return self._data

    def _fake_get(url: str, timeout: int):  # type: ignore[override]
        captured["url"] = url
        captured["timeout"] = timeout
        return _Response({"chembl_db_version": "ChEMBL_Y"})

    monkeypatch.setattr("bioetl.utils.chembl.requests.get", _fake_get)

    release = fetch_chembl_release("https://example.org/chembl/api")

    assert release.version == "ChEMBL_Y"
    assert release.status == {"chembl_db_version": "ChEMBL_Y"}
    assert captured["url"].endswith("/status.json")
    assert captured["timeout"] == 30


def test_resolve_release_name_prefers_known_keys():
    """Alternative key names should still yield a release identifier."""

    payload = {
        "chembl_db_release": "",
        "chembl_release": "  ChEMBL_36  ",
        "version": "ChEMBL_OLD",
    }

    assert _resolve_release_name(payload) == "ChEMBL_36"


def test_resolve_release_name_handles_nested_payloads():
    """Nested objects or non-string values should be coerced to strings."""

    payload = {
        "chembl_release": {"name": ""},
        "version": 37,
    }

    assert _resolve_release_name(payload) == "37"


def test_fetch_release_uses_fallback_keys():
    """``fetch_chembl_release`` should use any supported key name."""

    payload = {"version": " ChEMBL_ALT "}
    client = _StubClient(payload)

    release = fetch_chembl_release(client)

    assert release.version == "ChEMBL_ALT"
    assert release.status is payload
