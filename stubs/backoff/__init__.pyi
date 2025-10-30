from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")

def constant(interval: float) -> Any: ...

def on_exception(
    *,
    wait_gen: Any,
    interval: float | int | None = ...,  # accepts our usage with interval=0
    exception: Any,
    max_tries: int | None = ...,
    giveup: Callable[[Exception], bool] | None = ...,
    on_backoff: Callable[[dict[str, Any]], None] | None = ...,
    on_giveup: Callable[[dict[str, Any]], None] | None = ...,
    logger: Any | None = ...,
) -> Callable[[Callable[P, T]], Callable[P, T]]: ...


