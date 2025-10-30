from __future__ import annotations

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, Type

__all__ = [
    "BaseModel",
    "ConfigDict",
    "Field",
    "field_validator",
    "model_validator",
    "PrivateAttr",
]

P = ParamSpec("P")
T = TypeVar("T")
ModelT = TypeVar("ModelT", bound="BaseModel")

ConfigDict = dict[str, Any]

class BaseModel:
    model_config: ConfigDict

    def __init__(self, **data: Any) -> None: ...

    def model_dump(
        self,
        *,
        mode: str | None = ...,
        exclude: Any = ...,
        include: Any = ...,
    ) -> dict[str, Any]: ...

    def model_dump_json(self, *, indent: int | None = ...) -> str: ...

    @classmethod
    def model_validate(cls: Type[ModelT], obj: Any) -> ModelT: ...

def Field(
    default: Any = ...,
    *,
    default_factory: Callable[[], Any] | None = ...,
    description: str | None = ...,
    ge: int | float | None = ...,
    gt: int | float | None = ...,
    le: int | float | None = ...,
    lt: int | float | None = ...,
    max_length: int | None = ...,
    min_length: int | None = ...,
) -> Any: ...


def field_validator(
    __field: str,
    *fields: str,
    mode: str | None = ...,
) -> Callable[[Callable[P, T]], Callable[P, T]]: ...


def model_validator(
    *,
    mode: str | None = ...,
) -> Callable[[Callable[P, T]], Callable[P, T]]: ...


def PrivateAttr(default: Any = ...) -> Any: ...
