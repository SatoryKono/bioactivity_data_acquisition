"""Minimal pytest type stubs used by tests."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, ContextManager, Protocol, TypeVar, overload

_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., Any])
_T_co = TypeVar("_T_co", covariant=True)


class _MarkDecorator(Protocol):
    @overload
    def __call__(self, func: _F, /) -> _F: ...

    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

    def with_args(self, *args: Any, **kwargs: Any) -> _MarkDecorator: ...

    def __getattr__(self, name: str) -> _MarkDecorator: ...


class _Markers(Protocol):
    def __getattr__(self, name: str) -> _MarkDecorator: ...


mark: _Markers


class MonkeyPatch:
    def setattr(
        self,
        target: Any,
        name: str | None = ...,
        value: Any = ...,
        *,
        raising: bool = ...,
    ) -> None: ...

    def setitem(
        self,
        dictionary: dict[Any, Any],
        name: Any,
        value: Any,
    ) -> None: ...

    def setenv(self, name: str, value: str, *, prepend: bool = ...) -> None: ...

    def undo(self) -> None: ...


@overload
def fixture(function: _F, /) -> _F: ...


@overload
def fixture(
    function: None = ...,
    /,
    *,
    scope: str | None = ...,
    params: Iterable[Any] | None = ...,
    autouse: bool = ...,
    ids: Iterable[Any] | None = ...,
    name: str | None = ...,
) -> Callable[[_F], _F]: ...


class RaisesContextManager(Protocol[_T_co]):
    def __enter__(self) -> Any: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool: ...


@overload
def raises(
    expected_exception: type[BaseException] | tuple[type[BaseException], ...],
    match: str | None = ...,
    *,
    message: str | None = ...,
) -> RaisesContextManager[Any]: ...


@overload
def raises(
    expected_exception: type[BaseException] | tuple[type[BaseException], ...],
    func: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any: ...


def fail(message: str, *, pytrace: bool = ...) -> None: ...


def skip(reason: str) -> None: ...


def param(*values: Any, **kwargs: Any) -> Any: ...


def approx(actual: Any, *, rel: float | None = ..., abs: float | None = ...) -> Any: ...


class LogCaptureFixture(Protocol):
    records: list[Any]
    text: str

    def at_level(
        self,
        level: int | str,
        *,
        logger: str | None = ...,
    ) -> ContextManager[Any]: ...

    def clear(self) -> None: ...

