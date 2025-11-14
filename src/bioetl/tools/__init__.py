"""Shared helper utilities for BioETL tooling workflows."""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "get_project_root",
]


def get_project_root() -> Path:
    """Return the absolute repository root."""

    return Path(__file__).resolve().parents[3]
