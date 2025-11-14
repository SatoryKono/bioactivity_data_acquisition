"""Compatibility shims for legacy base source models."""
from __future__ import annotations

from typing import Generic, TypeVar

from bioetl.config.models.source import SourceConfig, SourceParameters

ParametersT = TypeVar("ParametersT", bound=SourceParameters)


class BaseSourceParameters(SourceParameters):
    """Deprecated alias of :class:`bioetl.config.models.source.SourceParameters`."""


class BaseSourceConfig(SourceConfig, Generic[ParametersT]):
    """Deprecated alias of :class:`bioetl.config.models.source.SourceConfig`."""

