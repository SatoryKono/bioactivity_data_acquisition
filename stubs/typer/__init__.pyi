"""Type stubs for typer."""

from collections.abc import Callable
from typing import Any

def Typer(
    name: str | None = ...,
    add_completion: bool = ...,
    *,
    invoke_without_command: bool = ...,
    no_args_is_help: bool = ...,
    rich_markup_mode: str | None = ...,
    help: str | None = ...,
    epilog: str | None = ...,
    **kwargs: Any,
) -> Any: ...

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

def run(func: Callable[..., Any] | None = ...) -> None: ...

app: TyperInstance = ...

