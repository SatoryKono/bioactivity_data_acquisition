from __future__ import annotations

from typing import Any

from . import contextvars, dev, processors, stdlib
from .stdlib import BoundLogger

__all__ = ["contextvars", "dev", "processors", "stdlib", "get_logger", "configure", "BoundLogger"]

def get_logger(name: str) -> BoundLogger: ...
def configure(**kwargs: Any) -> None: ...
