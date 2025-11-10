"""Общие утилиты для работы с YAML-конфигурациями."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, cast, overload

import yaml
from yaml.nodes import ScalarNode

IncludeResolver = Callable[[str, Path], Path]
TypeErrorMessageFactory = Callable[[Any, str], str]
InvalidKeyMessageFactory = Callable[[list[Any], str], str]

TypeErrorMessage = str | TypeErrorMessageFactory | None
InvalidKeyMessage = str | InvalidKeyMessageFactory | None


def load_yaml_document(
    path: str | Path,
    *,
    loader_cls: type[yaml.SafeLoader] = yaml.SafeLoader,
    preprocess: Callable[[str], str] | None = None,
    include_resolver: IncludeResolver | None = None,
) -> Any:
    """Загрузить YAML-документ, опционально поддерживая ``!include``."""

    resolved_path = Path(path).expanduser().resolve()

    loader_type = cast(type[yaml.SafeLoader], type("Loader", (loader_cls,), {}))

    if include_resolver is not None:

        def construct_include(loader: yaml.SafeLoader, node: ScalarNode) -> Any:
            filename = loader.construct_scalar(node)
            include_path = include_resolver(filename, resolved_path.parent)
            return load_yaml_document(
                include_path,
                loader_cls=loader_cls,
                preprocess=preprocess,
                include_resolver=include_resolver,
            )

        loader_type.add_constructor("!include", construct_include)

    with resolved_path.open("r", encoding="utf-8") as handle:
        text = handle.read()

    if preprocess is not None:
        text = preprocess(text)

    return yaml.load(text, Loader=loader_type)


@overload
def ensure_mapping(
    value: Any,
    *,
    context: str,
    exception_type: type[Exception] = ...,
    require_string_keys: bool = ...,
    coerce_dict: Literal[False],
    type_error_message: TypeErrorMessage = ...,
    invalid_key_message: InvalidKeyMessage = ...,
) -> Mapping[str, Any]:
    ...


@overload
def ensure_mapping(
    value: Any,
    *,
    context: str,
    exception_type: type[Exception] = TypeError,
    require_string_keys: bool = True,
    coerce_dict: Literal[True] = True,
    type_error_message: TypeErrorMessage = None,
    invalid_key_message: InvalidKeyMessage = None,
) -> dict[str, Any]:
    ...


@overload
def ensure_mapping(
    value: Any,
    *,
    context: str,
    exception_type: type[Exception] = TypeError,
    require_string_keys: bool = True,
    coerce_dict: bool,
    type_error_message: TypeErrorMessage = None,
    invalid_key_message: InvalidKeyMessage = None,
) -> Mapping[str, Any] | dict[str, Any]:
    ...


def ensure_mapping(
    value: Any,
    *,
    context: str,
    exception_type: type[Exception] = TypeError,
    require_string_keys: bool = True,
    coerce_dict: bool = True,
    type_error_message: TypeErrorMessage = None,
    invalid_key_message: InvalidKeyMessage = None,
) -> Mapping[str, Any] | dict[str, Any]:
    """Убедиться, что объект является отображением с ожидаемыми ключами."""

    if not isinstance(value, Mapping):
        message = _resolve_type_error_message(value, context, type_error_message)
        raise exception_type(message)

    mapping = cast(Mapping[Any, Any], value)

    if require_string_keys:
        invalid_keys = [key for key in mapping if not isinstance(key, str)]
        if invalid_keys:
            message = _resolve_invalid_key_message(invalid_keys, context, invalid_key_message)
            raise exception_type(message)

    if not coerce_dict:
        if require_string_keys:
            return cast(Mapping[str, Any], mapping)
        return mapping

    if require_string_keys:
        normalized = {str(key): item for key, item in mapping.items()}
    else:
        normalized = dict(mapping.items())

    return normalized


def _resolve_type_error_message(
    value: Any,
    context: str,
    message: TypeErrorMessage,
) -> str:
    if message is None:
        return f"Expected mapping for {context}, got {type(value)!r}"
    if callable(message):
        return message(value, context)
    return message


def _resolve_invalid_key_message(
    invalid_keys: list[Any],
    context: str,
    message: InvalidKeyMessage,
) -> str:
    if message is None:
        keys_repr = ", ".join(map(repr, invalid_keys))
        return f"Expected string keys for {context}, invalid keys: {keys_repr}"
    if callable(message):
        return message(invalid_keys, context)
    return message


__all__ = ["ensure_mapping", "load_yaml_document"]

