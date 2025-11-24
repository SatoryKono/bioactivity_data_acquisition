"""Namespace package for configuration models."""

from __future__ import annotations

from typing import NoReturn

__all__: list[str] = []


def __getattr__(name: str) -> NoReturn:
    msg = (
        "Top-level names were removed from 'infrastructure.config.models'. "
        "Import from 'infrastructure.config.models.models' or "
        "'infrastructure.config.models.policies' instead."
    )
    raise AttributeError(msg)
