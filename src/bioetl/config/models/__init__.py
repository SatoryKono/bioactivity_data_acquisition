"""Namespace package for configuration models."""

from __future__ import annotations

from typing import NoReturn

__all__: list[str] = []


def __getattr__(name: str) -> NoReturn:
    msg = (
        "Top-level names were removed from 'bioetl.config.models'. "
        "Import from 'bioetl.config.models.models' or "
        "'bioetl.config.models.policies' instead."
    )
    raise AttributeError(msg)
