"""Stub for structlog.processors module."""

from __future__ import annotations

from typing import Any

def format_exc_info(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...

class JSONRenderer:
    def __init__(self, **kwargs: Any) -> None: ...

class UnicodeDecoder:
    def __init__(self, **kwargs: Any) -> None: ...

class StackInfoRenderer:
    def __init__(self, **kwargs: Any) -> None: ...
