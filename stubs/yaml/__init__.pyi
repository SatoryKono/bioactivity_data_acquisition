"""Minimal PyYAML stub definitions used for type checking."""

from collections.abc import Iterable
from typing import IO, Any

class Loader: ...


def safe_load(stream: str | bytes | IO[str] | IO[bytes]) -> Any: ...


def safe_load_all(stream: str | bytes | IO[str] | IO[bytes]) -> Iterable[Any]: ...


def dump(
    data: Any,
    stream: IO[str] | None = ...,
    *,
    default_flow_style: bool | None = ...,
    sort_keys: bool | None = ...,
) -> str | None: ...


def safe_dump(
    data: Any,
    stream: IO[str] | None = ...,
    *,
    default_flow_style: bool | None = ...,
    sort_keys: bool | None = ...,
) -> str | None: ...
