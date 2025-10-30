"""Утилиты для канонизации JSON."""

import json
import math
from collections.abc import Callable
from typing import Any

from bioetl.normalizers.constants import NA_STRINGS
from bioetl.normalizers.helpers import _is_na


def canonical_json(value: Any, sort_keys: bool = True, ensure_ascii: bool = True) -> str | None:
    """Сериализует значение в канонический JSON.

    Детерминированная сериализация с сортировкой ключей и компактным форматом.

    Args:
        value: Значение для сериализации
        sort_keys: Сортировать ключи для детерминизма
        ensure_ascii: Экранировать не-ASCII символы (совместимо с json.dumps)

    Returns:
        JSON строка или None для пустых значений
    """
    if value in (None, ""):
        return None
    try:
        return json.dumps(
            value,
            sort_keys=sort_keys,
            separators=(",", ":"),
            ensure_ascii=ensure_ascii,
        )
    except (TypeError, ValueError):
        return None


def normalize_json_list(
    raw: Any,
    sort_fn: Callable[[dict[str, Any]], Any] | None = None,
) -> tuple[str | None, list[dict[str, Any]]]:
    """Нормализует и канонизирует список словарей.

    Парсит JSON, нормализует строки и числа, сортирует записи детерминированно.

    Args:
        raw: Сырые данные (строка JSON или уже распарсенный объект)
        sort_fn: Функция для сортировки записей (по умолчанию по name/type)

    Returns:
        Кортеж (канонический JSON, список нормализованных записей)
    """
    if _is_na(raw):
        return None, []

    parsed: Any = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None, []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None, []

    if isinstance(parsed, dict):
        parsed = [parsed]

    if not isinstance(parsed, list):
        return None, []

    normalized_records: list[dict[str, Any]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue

        normalized_entry: dict[str, Any] = {}
        for key, value in entry.items():
            if isinstance(value, str):
                text_value = value.strip()
                if not text_value or text_value.lower() in NA_STRINGS:
                    normalized_entry[key] = None
                else:
                    # Попытка парсинга как число
                    try:
                        numeric = float(text_value)
                        if not math.isnan(numeric):
                            normalized_entry[key] = numeric
                        else:
                            normalized_entry[key] = text_value
                    except (TypeError, ValueError):
                        normalized_entry[key] = text_value
            elif isinstance(value, int | float):
                if isinstance(value, float) and math.isnan(value):
                    normalized_entry[key] = None
                else:
                    normalized_entry[key] = value
            elif isinstance(value, bool) or value is None:
                normalized_entry[key] = value
            else:
                normalized_entry[key] = value

        if normalized_entry:
            normalized_records.append(dict(sorted(normalized_entry.items())))

    if not normalized_records:
        return None, []

    # Сортировка записей
    if sort_fn is not None:
        normalized_records.sort(key=sort_fn)
    else:
        # Дефолтная сортировка по name/type/property_name
        normalized_records.sort(
            key=lambda item: (
                str(
                    item.get("name")
                    or item.get("type")
                    or item.get("property_name")
                    or ""
                ).lower(),
                json.dumps(item, ensure_ascii=False, sort_keys=True),
            )
        )

    canonical = json.dumps(normalized_records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return canonical, normalized_records
