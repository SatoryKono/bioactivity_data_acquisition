"""Type stubs for typer."""

from collections.abc import Callable
from typing import Any

class BadParameter(Exception):
    """Exception raised for bad parameter values."""
    def __init__(self, message: str) -> None: ...

class Exit(Exception):
    """Exception for exiting the application."""
    def __init__(self, code: int = 0) -> None: ...

class TyperInstance:
    def command(
        self,
        name: str | None = ...,
        *,
        cls: type[Any] | None = ...,
        help: str | None = ...,
        epilog: str | None = ...,
        short_help: str | None = ...,
        options_metavar: str = ...,
        add_help_option: bool = ...,
        no_args_is_help: bool = ...,
        hidden: bool = ...,
        deprecated: bool = ...,
    ) -> Callable[[Callable[..., Any]], Any]: ...
    
    def callback(
        self,
        name: str | None = ...,
        *,
        invoke_without_command: bool = ...,
        no_args_is_help: bool = ...,
        help: str | None = ...,
        epilog: str | None = ...,
        **kwargs: Any,
    ) -> Callable[[Callable[..., Any]], Any]: ...
    
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

class Typer(TyperInstance):
    """Typer application class."""
    def __init__(
        self,
        name: str | None = ...,
        add_completion: bool = ...,
        *,
        invoke_without_command: bool = ...,
        no_args_is_help: bool = ...,
        rich_markup_mode: str | None = ...,
        help: str | None = ...,
        epilog: str | None = ...,
        **kwargs: Any,
    ) -> None: ...

def run(func: Callable[..., Any] | None = ...) -> None: ...

def Option(
    default: Any = ...,
    *param_decls: str,
    help: str | None = ...,
    metavar: str | None = ...,
    show_default: bool = ...,
    prompt: bool | str | None = ...,
    confirmation_prompt: bool = ...,
    hide_input: bool = ...,
    **kwargs: Any,
) -> Any: ...

def echo(message: str = ..., *, err: bool = ...) -> None: ...

def secho(
    message: str = ...,
    *,
    fg: str | None = ...,
    bg: str | None = ...,
    bold: bool = ...,
    dim: bool = ...,
    underline: bool = ...,
    overline: bool = ...,
    italic: bool = ...,
    blink: bool = ...,
    reverse: bool = ...,
    strikethrough: bool = ...,
    err: bool = ...,
) -> None: ...

class _Colors:
    RED: str = ...
    GREEN: str = ...
    YELLOW: str = ...
    BLUE: str = ...
    MAGENTA: str = ...
    CYAN: str = ...
    WHITE: str = ...
    BLACK: str = ...
    RESET: str = ...

colors: _Colors = ...

app: TyperInstance = ...

