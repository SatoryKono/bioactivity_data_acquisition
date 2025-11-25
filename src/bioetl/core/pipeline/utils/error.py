from __future__ import annotations

from typing import Iterable, Mapping, Type

from .interfaces import ErrorAction, ErrorPolicyABC


class DefaultErrorPolicy(ErrorPolicyABC):
    """Решает, что делать с ошибками, по таблице соответствий."""

    def __init__(
        self,
        mapping: Mapping[Type[BaseException], ErrorAction] | None = None,
        default: ErrorAction = ErrorAction.FAIL,
        retry_exceptions: Iterable[Type[BaseException]] | None = None,
    ) -> None:
        self.mapping = dict(mapping or {})
        if retry_exceptions:
            for exc in retry_exceptions:
                self.mapping.setdefault(exc, ErrorAction.RETRY)
        self.default = default

    def decide(self, error: Exception) -> ErrorAction:
        for exc_type, action in self.mapping.items():
            if isinstance(error, exc_type):
                return action
        return self.default
