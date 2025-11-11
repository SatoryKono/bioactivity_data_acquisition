from __future__ import annotations

from typing import Any

from .stdlib import BoundLogger

def configure(*, processors: Any = ..., **kwargs: Any) -> None: ...

def get_logger(name: str | None = ...) -> BoundLogger: ...

contextvars: Any
processors: Any
stdlib: Any

