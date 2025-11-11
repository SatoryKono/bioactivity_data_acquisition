from __future__ import annotations

from enum import Enum
from typing import Any, Iterable, Sequence


class CallsiteParameter(Enum):
    PATHNAME = "pathname"
    LINENO = "lineno"
    FUNC_NAME = "func_name"


class TimeStamper:
    def __init__(self, *, fmt: str, utc: bool, key: str) -> None: ...

    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class EventRenamer:
    def __init__(self, to: str) -> None: ...

    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class CallsiteParameterAdder:
    def __init__(
        self,
        *,
        parameters: Iterable[CallsiteParameter],
        additional_ignores: Sequence[str] | None = None,
    ) -> None: ...

    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class StackInfoRenderer:
    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class ExceptionPrettyPrinter:
    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class UnicodeDecoder:
    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> dict[str, Any]: ...


class JSONRenderer:
    def __init__(self, *, sort_keys: bool, ensure_ascii: bool) -> None: ...

    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> str: ...


class KeyValueRenderer:
    def __init__(
        self,
        *,
        key_order: Sequence[str] | None,
        sort_keys: bool,
        drop_missing: bool,
    ) -> None: ...

    def __call__(self, logger: Any, name: str, event_dict: dict[str, Any]) -> str: ...

