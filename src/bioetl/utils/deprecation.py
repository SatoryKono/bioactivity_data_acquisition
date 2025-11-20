"""Утилиты для пометки объектов как устаревающих."""
from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, Optional, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def deprecated(
    *,
    reason: str,
    category: type[Warning] = DeprecationWarning,
    version: Optional[str] = None,
    alternative: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Декоратор для пометки функций и методов как устаревших.

    Пример:
        @deprecated(reason="Перенесено в новый модуль", version="1.2.0")
        def old_helper(...):
            ...
    """

    def decorator(func: F) -> F:
        message_parts = [f"{func.__name__} is deprecated: {reason}."]
        if version:
            message_parts.append(f"Effective version: {version}.")
        if alternative:
            message_parts.append(f"Use {alternative} instead.")
        message = " ".join(message_parts)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            warnings.warn(message, category=category, stacklevel=2)
            return func(*args, **kwargs)

        existing_doc = wrapper.__doc__ or ""
        deprecation_note = f"\n\nDeprecated: {reason}."
        if alternative:
            deprecation_note += f" Use {alternative} instead."
        wrapper.__doc__ = (existing_doc + deprecation_note).strip()
        return wrapper  # type: ignore[return-value]

    return decorator
