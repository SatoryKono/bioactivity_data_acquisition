"""Minimal Typer stub for project type checking."""

from __future__ import annotations

from typing import Any, Callable

class Exit(Exception):  # noqa: N818
    def __init__(self, *, code: int | None = ...) -> None: ...

class BadParameter(Exception):  # noqa: N818
    def __init__(self, message: str, *, param_hint: str | None = ...) -> None: ...

class Typer:
    def __init__(
        self,
        *,
        name: str | None = ...,
        help: str | None = ...,
        add_help_option: bool = ...,
        no_args_is_help: bool = ...,
        add_completion: bool = ...,
    ) -> None: ...
    def callback(self, *args: Any, **kwargs: Any) -> Callable[..., Any]: ...
    def command(self, *args: Any, **kwargs: Any) -> Callable[..., Any]: ...
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

def echo(*args: Any, **kwargs: Any) -> None: ...
def Option(*args: Any, **kwargs: Any) -> Any: ...  # noqa: N802 - mimic Typer API casing

__all__ = ["Typer", "Option", "echo", "Exit", "BadParameter"]
