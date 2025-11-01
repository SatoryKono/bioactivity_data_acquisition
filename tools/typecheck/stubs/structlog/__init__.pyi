from __future__ import annotations

from typing import Any

from . import contextvars, dev, processors, stdlib

__all__ = ["contextvars", "dev", "processors", "stdlib", "get_logger", "configure"]

def get_logger(name: str) -> Any: ...
def configure(**kwargs: Any) -> None: ...
