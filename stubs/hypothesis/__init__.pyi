"""Type stubs for hypothesis."""

from typing import Any, TypeVar

T = TypeVar("T")

def given(*given_arguments: Any, **given_kwargs: Any) -> Any: ...

def settings(*args: Any, **kwargs: Any) -> Any: ...

