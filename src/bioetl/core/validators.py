"""Базовые валидаторы контейнеров и JSON-подобных структур.

Функции в модуле предназначены для повторного использования на «нижнем» уровне
кода. Они не зависят от доменных сущностей и следуют строгим требованиям
детерминизма проекта.

Основные принципы реализации:

* «Итерируемость» определяется вызовом :func:`iter`. Проверка через
  :class:`collections.abc.Iterable` недостаточна, потому что некоторые объекты
  поддерживают только ``__getitem__`` и при этом остаются итерируемыми.
* Строки и байтовые последовательности по умолчанию исключаются из проверки
  итерируемости, чтобы избежать сюрпризов при валидации коллекций.
* JSON-подобные словари допускают только стандартные литералы JSON, списки и
  словари с ключами-строками; рекурсивные структуры обрабатываются с защитой
  от циклических ссылок.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Callable, cast

JSON_PRIMITIVE_TYPES: tuple[type[Any], ...] = (str, int, float, bool, type(None))

Predicate = Callable[[Any], bool]


def is_iterable(obj: Any, /, *, exclude_str: bool = True) -> bool:
    """Проверить, что объект итерируем, опираясь на вызов ``iter(obj)``.

    ``isinstance(obj, Iterable)`` не считается достаточным критерием: объекты,
    реализующие только ``__getitem__``, могут не зарегистрироваться в ABC, но
    при этом успешно поддерживают итерацию. Чтобы учесть такие случаи, функция
    пытается вызвать :func:`iter`.

    Parameters
    ----------
    obj:
        Проверяемый объект.
    exclude_str:
        Исключить ли строковые и байтовые типы из позитивного результата.

    Returns
    -------
    bool
        ``True`` — объект итерируем; ``False`` — иначе.
    """

    if exclude_str and isinstance(obj, (str, bytes, bytearray)):
        return False

    try:
        iter(obj)
    except TypeError:
        return False
    return True


def assert_iterable(
    obj: Any,
    /,
    *,
    exclude_str: bool = True,
    argument_name: str = "value",
) -> Iterable[Any]:
    """Убедиться, что аргумент итерируем; иначе выбросить :class:`TypeError`."""

    if not is_iterable(obj, exclude_str=exclude_str):
        kind = "итерируемым"
        if exclude_str:
            kind = "итерируемым (исключая строки и байты)"
        msg = f"{argument_name} должен быть {kind}, получено {type(obj)!r}"
        raise TypeError(msg)
    return cast(Iterable[Any], obj)


def is_list_of(obj: Any, predicate: Predicate) -> bool:
    """Проверить, что объект — список, чьи элементы удовлетворяют предикату."""

    if not isinstance(obj, list):
        return False
    return all(predicate(element) for element in obj)


def assert_list_of(
    obj: Any,
    predicate: Predicate,
    *,
    argument_name: str = "value",
    predicate_name: str | None = None,
) -> list[Any]:
    """Убедиться, что аргумент — список, элементы которого проходят проверку."""

    if not isinstance(obj, list):
        msg = f"{argument_name} должен быть list, получено {type(obj)!r}"
        raise TypeError(msg)

    violations: list[int] = []
    for index, element in enumerate(obj):
        if not predicate(element):
            violations.append(index)

    if violations:
        predicate_hint = f" предикат {predicate_name}" if predicate_name else ""
        msg = (
            f"{argument_name} содержит элементы, не удовлетворяющие{predicate_hint}: "
            f"индексы (indices {violations})"
        )
        raise ValueError(msg)

    return obj


def is_json_mapping(obj: Any) -> bool:
    """Проверить, что объект — JSON-подобный словарь с ключами-строками."""

    return _is_json_mapping_internal(obj, seen_ids=set())


def assert_json_mapping(
    obj: Any,
    *,
    argument_name: str = "value",
) -> Mapping[str, Any]:
    """Убедиться, что объект — корректный JSON-подобный словарь."""

    if not is_json_mapping(obj):
        msg = f"{argument_name} должен быть JSON-совместимым словарём, получено {type(obj)!r}"
        raise TypeError(msg)
    mapping = cast(Mapping[str, Any], obj)
    return mapping


def _is_json_mapping_internal(obj: Any, *, seen_ids: set[int]) -> bool:
    if not isinstance(obj, Mapping):
        return False

    obj_id = id(obj)
    if obj_id in seen_ids:
        return False
    seen_ids.add(obj_id)

    for key, value in obj.items():
        if not isinstance(key, str):
            return False
        if not _is_json_value(value, seen_ids=seen_ids):
            return False
    seen_ids.remove(obj_id)
    return True


def _is_json_value(value: Any, *, seen_ids: set[int]) -> bool:
    if isinstance(value, JSON_PRIMITIVE_TYPES):
        return True

    if isinstance(value, list):
        return all(_is_json_value(element, seen_ids=seen_ids) for element in value)

    if isinstance(value, Mapping):
        return _is_json_mapping_internal(value, seen_ids=seen_ids)

    return False


__all__ = [
    "JSON_PRIMITIVE_TYPES",
    "assert_iterable",
    "assert_json_mapping",
    "assert_list_of",
    "is_iterable",
    "is_json_mapping",
    "is_list_of",
]

