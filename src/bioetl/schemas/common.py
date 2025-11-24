"""Shared helpers for resolving schema resources and vocabulary cache updates."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import requests
import yaml

DEFAULT_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "configs"


def default_schema_path(
    entity: str, base_path: str | Path | None = None
) -> Path:
    """Return absolute path to the requested entity under the configuration tree."""

    root = Path(base_path) if base_path is not None else DEFAULT_CONFIG_ROOT
    return root.expanduser().resolve() / entity


def _resolve_schema_path(
    path_or_name: str | Path, base_path: str | Path | None
) -> Path:
    candidate = Path(path_or_name)
    if not candidate.is_absolute():
        base = (
            Path(base_path) if base_path is not None else DEFAULT_CONFIG_ROOT
        )
        candidate = base / candidate
    return candidate.expanduser().resolve()


def load_schema(
    path_or_name: str | Path, base_path: str | Path | None = None
) -> dict[str, Any]:
    """Load a YAML schema definition from the provided path or relative name."""

    resolved = _resolve_schema_path(path_or_name, base_path)
    try:
        payload = resolved.read_text(encoding="utf-8")
    except FileNotFoundError as exc:  # pragma: no cover - defensive guard
        raise FileNotFoundError(f"Schema file not found: {resolved}") from exc

    loaded = yaml.safe_load(payload)
    return loaded or {}


def refresh_vocabulary(
    cache_path: str | Path,
    source_url: str,
    *,
    force: bool = False,
) -> bool:
    """Update a cached vocabulary file returning ``True`` when refreshed."""

    cache_file = Path(cache_path).expanduser().resolve()
    etag_file = cache_file.with_suffix(cache_file.suffix + ".etag")
    headers: dict[str, str] = {}

    if cache_file.exists() and not force:
        if etag_file.exists():
            etag = etag_file.read_text(encoding="utf-8").strip()
            if etag:
                headers["If-None-Match"] = etag
        else:
            last_modified = datetime.fromtimestamp(
                cache_file.stat().st_mtime, tz=timezone.utc
            )
            headers["If-Modified-Since"] = format_datetime(
                last_modified, usegmt=True
            )

    response = requests.get(source_url, headers=headers, timeout=30)
    if response.status_code == 304:
        return False

    response.raise_for_status()

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(response.content)

    etag_header = response.headers.get("ETag")
    if etag_header:
        etag_file.write_text(etag_header, encoding="utf-8")
    elif etag_file.exists():
        etag_file.unlink()

    return True


__all__ = [
    "DEFAULT_CONFIG_ROOT",
    "default_schema_path",
    "load_schema",
    "refresh_vocabulary",
]
