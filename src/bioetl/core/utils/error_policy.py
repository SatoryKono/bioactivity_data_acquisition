from __future__ import annotations

from collections.abc import Callable

from .interfaces import ErrorAction, ErrorPolicyABC


class ExceptionErrorPolicy(ErrorPolicyABC):
    """Простая стратегия выбора действия на основании типа исключения."""

    def __init__(
        self,
        *,
        retry_on: tuple[type[Exception], ...] | None = None,
        skip_on: tuple[type[Exception], ...] | None = None,
        fallback: ErrorAction = ErrorAction.FAIL,
        predicate: Callable[[Exception], ErrorAction] | None = None,
    ) -> None:
        self._retry_on = retry_on or ()
        self._skip_on = skip_on or ()
        self._fallback = fallback
        self._predicate = predicate

    def decide(self, error: Exception) -> ErrorAction:
        if self._predicate:
            return self._predicate(error)

        if isinstance(error, self._retry_on):
            return ErrorAction.RETRY
        if isinstance(error, self._skip_on):
            return ErrorAction.SKIP
        return self._fallback

