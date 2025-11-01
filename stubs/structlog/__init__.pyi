from __future__ import annotations

from typing import Any

__all__: list[str]

def get_logger(name: str) -> BoundLogger: ...
def configure(**kwargs: Any) -> None: ...

class BoundLogger:
    def info(self, *args: Any, **kwargs: Any) -> None: ...
    def error(self, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, *args: Any, **kwargs: Any) -> None: ...
    def debug(self, *args: Any, **kwargs: Any) -> None: ...

# Submodules
contextvars: Any
stdlib: Any
dev: Any
processors: Any