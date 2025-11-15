"""Shared helper utilities for BioETL tooling workflows."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final

__all__ = [
    "get_project_root",
    "hash_file",
]

_DEFAULT_CHUNK_SIZE: Final[int] = 128 * 1024


def get_project_root() -> Path:
    """Return the absolute repository root."""

    return Path(__file__).resolve().parents[3]


def hash_file(path: str | Path, *, algorithm: str = "sha256") -> str:
    """Return hexadecimal digest for ``path`` using ``algorithm``."""

    file_path = Path(path)
    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:  # pragma: no cover - invalid algorithm usage
        msg = f"Unsupported hash algorithm: {algorithm}"
        raise ValueError(msg) from exc

    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_DEFAULT_CHUNK_SIZE), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
