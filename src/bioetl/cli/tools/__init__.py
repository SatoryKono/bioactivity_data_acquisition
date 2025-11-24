"""Совместимость с устаревшим пространством ``bioetl.cli.tools``."""

from __future__ import annotations

from ._logic import LEGACY_TOOL_MAP, load_tool_module

__all__ = ["LEGACY_TOOL_MAP", "load_tool_module"]
