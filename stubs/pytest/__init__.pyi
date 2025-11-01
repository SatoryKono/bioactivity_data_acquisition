"""Type stubs for pytest."""

from typing import Any
from collections.abc import Callable

def fixture(
    fixture_function: Callable[..., Any] | None = ...,
    *,
    scope: str = ...,
    params: Any = ...,
    autouse: bool = ...,
    ids: Any = ...,
    name: str | None = ...,
) -> Any: ...

class FixtureRequest:
    param: Any = ...

