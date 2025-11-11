from __future__ import annotations

from typing import Any


def bind_contextvars(**kwargs: Any) -> None: ...


def clear_contextvars() -> None: ...


def get_contextvars() -> dict[str, Any]: ...


def unbind_contextvars(*keys: str) -> None: ...

