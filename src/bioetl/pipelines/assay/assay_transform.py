"""Transform utilities for ChEMBL assay pipeline array serialization."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, cast

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = [
    "header_rows_serialize",
    "serialize_array_fields",
    "validate_assay_parameters_truv",
]


def escape_delims(s: str) -> str:
    r"""Escape pipe and slash delimiters in string values.

    Parameters
    ----------
    s:
        Input string to escape.

    Returns
    -------
    str:
        String with escaped delimiters: `|` → `\|`, `/` → `\/`, `\` → `\\`.
    """
    return s.replace("\\", "\\\\").replace("|", "\\|").replace("/", "\\/")


def header_rows_serialize(items: Any) -> str:
    """Serialize array-of-objects to header+rows format.

    Format: `header/row1/row2/...` where:
    - Header: `k1|k2|...` (ordered list of keys)
    - Row: `v1|v2|...` (values for each key, empty string if missing)

    Parameters
    ----------
    items:
        List of dicts, None, or empty list.

    Returns
    -------
    str:
        Serialized string in header+rows format, or empty string for None/empty.

    Examples
    --------
    >>> header_rows_serialize([{"a": "A", "b": "B"}])
    'a|b/A|B'
    >>> header_rows_serialize([{"a": "A1"}, {"a": "A2", "b": "B2"}])
    'a|b/A1|/A2|B2'
    >>> header_rows_serialize([])
    ''
    >>> header_rows_serialize(None)
    ''
    >>> header_rows_serialize([{"x": "A|B", "y": "C/D"}])
    'x|y/A\\|B|C\\/D'
    """
    if items is None:
        return ""

    if not isinstance(items, list):
        # Non-list value: JSON serialize and escape delimiters
        json_str = json.dumps(items, ensure_ascii=False, sort_keys=True)
        return escape_delims(json_str)

    # Type narrowing: items is now list[Any]
    typed_items: list[Any] = cast(list[Any], items)

    if not typed_items:
        return ""

    # Gather keys deterministically:
    # 1. Preserve order from first item
    # 2. Append unseen keys from other items in alphabetical order
    ordered_keys: list[str] = []
    seen_set: set[str] = set()

    # First pass: collect keys from first item in order
    if len(typed_items) > 0 and isinstance(typed_items[0], dict):
        first_item: dict[str, Any] = cast(dict[str, Any], typed_items[0])
        for key in first_item.keys():
            if key not in seen_set:
                ordered_keys.append(key)
                seen_set.add(key)

    # Second pass: collect remaining keys from other items, then sort alphabetically
    remaining_keys: set[str] = set()
    for item in typed_items[1:]:
        if isinstance(item, dict):
            remaining_item: dict[str, Any] = cast(dict[str, Any], item)
            for key in remaining_item.keys():
                if key not in seen_set:
                    remaining_keys.add(key)
                    seen_set.add(key)

    # Append remaining keys in alphabetical order
    ordered_keys.extend(sorted(remaining_keys))

    # Build header
    header = "|".join(ordered_keys)

    # Build rows
    rows: list[str] = []
    for item in typed_items:
        if not isinstance(item, dict):
            # Fallback: JSON serialize non-dict item
            json_str = json.dumps(item, ensure_ascii=False, sort_keys=True)
            rows.append(escape_delims(json_str))
            continue

        # Extract values for each key
        item_dict: dict[str, Any] = cast(dict[str, Any], item)
        values: list[str] = []
        for key in ordered_keys:
            value: Any | None = item_dict.get(key)
            if value is None:
                values.append("")
            elif isinstance(value, (list, dict)):
                # Nested structure: JSON serialize and escape
                json_str = json.dumps(value, ensure_ascii=False, sort_keys=True)
                values.append(escape_delims(json_str))
            else:
                # Scalar value: convert to string and escape
                values.append(escape_delims(str(value)))

        rows.append("|".join(values))

    # Join header and rows
    if not rows:
        return ""

    return header + "/" + "/".join(rows)


def serialize_array_fields(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Serialize array-of-object fields to header+rows format.

    Parameters
    ----------
    df:
        DataFrame to transform.
    columns:
        List of column names to serialize.

    Returns
    -------
    pd.DataFrame:
        DataFrame with specified columns serialized to strings.
    """
    df = df.copy()

    for col in columns:
        if col in df.columns:
            df[col] = df[col].map(header_rows_serialize)

    return df


def validate_assay_parameters_truv(
    df: pd.DataFrame,
    column: str = "assay_parameters",
    fail_fast: bool = True,
) -> pd.DataFrame:
    """Валидировать TRUV-инварианты для assay_parameters.

    Проверяет следующие инварианты:
    - value IS NOT NULL XOR text_value IS NOT NULL (не оба одновременно не NULL)
    - standard_value IS NOT NULL XOR standard_text_value IS NOT NULL
    - active ∈ {0, 1, NULL}
    - relation ∈ {'=', '<', '≤', '>', '≥', '~', NULL} (с предупреждением для нестандартных)

    Parameters
    ----------
    df:
        DataFrame с колонкой assay_parameters (JSON-строка с массивом параметров).
    column:
        Имя колонки с параметрами (по умолчанию "assay_parameters").
    fail_fast:
        Если True, выбрасывает ValueError при нарушении инвариантов.
        Если False, логирует предупреждения и продолжает.

    Returns
    -------
    pd.DataFrame:
        Исходный DataFrame (валидация не изменяет данные).

    Raises
    ------
    ValueError:
        Если fail_fast=True и обнаружено нарушение инвариантов.

    Notes
    -----
    - Валидация выполняется на этапе transform для fail-fast подхода
    - Стандартные операторы relation: '=', '<', '≤', '>', '≥', '~'
    - Нестандартные операторы логируются как предупреждения, но не блокируют выполнение
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_transform")

    if column not in df.columns:
        log.debug("truv_validation_skipped_missing_column", column=column)
        return df

    # Стандартные операторы relation
    STANDARD_RELATIONS = {"=", "<", "≤", ">", "≥", "~"}

    errors: list[str] = []
    warnings: list[str] = []

    for idx, row in df.iterrows():
        params_str = row.get(column)
        if pd.isna(params_str) or params_str is None or params_str == "":
            continue

        try:
            # Парсим JSON-строку
            if isinstance(params_str, str):
                params_list = json.loads(params_str)
            else:
                # Если уже список (не сериализован)
                params_list = params_str
        except (json.JSONDecodeError, TypeError) as exc:
            errors.append(
                f"Row {idx}: Invalid JSON in {column}: {exc}",
            )
            continue

        if not isinstance(params_list, list):
            errors.append(
                f"Row {idx}: {column} must be a JSON array, got {type(params_list).__name__}",
            )
            continue

        # Валидируем каждый параметр
        for param_idx, param in enumerate(params_list):
            if not isinstance(param, dict):
                errors.append(
                    f"Row {idx}, param {param_idx}: Parameter must be a dict, got {type(param).__name__}",
                )
                continue

            # Проверка TRUV-инварианта: value XOR text_value
            value = param.get("value")
            text_value = param.get("text_value")
            # value считается NULL если None, NaN или пустая строка
            value_is_null = (
                value is None
                or (isinstance(value, float) and pd.isna(value))
                or (isinstance(value, str) and value.strip() == "")
            )
            # text_value считается NULL если None, NaN или пустая строка
            text_value_is_null = (
                text_value is None
                or (isinstance(text_value, float) and pd.isna(text_value))
                or (isinstance(text_value, str) and text_value.strip() == "")
            )

            if not value_is_null and not text_value_is_null:
                errors.append(
                    f"Row {idx}, param {param_idx}: Both 'value' and 'text_value' are not NULL "
                    f"(value={value}, text_value={text_value}). TRUV invariant violation: "
                    "value and text_value must be mutually exclusive.",
                )

            # Проверка standard_TRUV-инварианта: standard_value XOR standard_text_value
            standard_value = param.get("standard_value")
            standard_text_value = param.get("standard_text_value")
            # standard_value считается NULL если None, NaN или пустая строка
            standard_value_is_null = (
                standard_value is None
                or (isinstance(standard_value, float) and pd.isna(standard_value))
                or (isinstance(standard_value, str) and standard_value.strip() == "")
            )
            # standard_text_value считается NULL если None, NaN или пустая строка
            standard_text_value_is_null = (
                standard_text_value is None
                or (isinstance(standard_text_value, float) and pd.isna(standard_text_value))
                or (
                    isinstance(standard_text_value, str)
                    and standard_text_value.strip() == ""
                )
            )

            if not standard_value_is_null and not standard_text_value_is_null:
                errors.append(
                    f"Row {idx}, param {param_idx}: Both 'standard_value' and 'standard_text_value' "
                    f"are not NULL (standard_value={standard_value}, "
                    f"standard_text_value={standard_text_value}). TRUV invariant violation: "
                    "standard_value and standard_text_value must be mutually exclusive.",
                )

            # Проверка active ∈ {0, 1, NULL}
            active = param.get("active")
            if active is not None:
                if isinstance(active, bool):
                    # Преобразуем bool в int для проверки
                    active_int = 1 if active else 0
                elif isinstance(active, (int, float)):
                    active_int = int(active)
                elif isinstance(active, str):
                    try:
                        active_int = int(active)
                    except ValueError:
                        errors.append(
                            f"Row {idx}, param {param_idx}: Invalid 'active' value: {active!r}. "
                            "Must be 0, 1, or NULL.",
                        )
                        continue
                else:
                    errors.append(
                        f"Row {idx}, param {param_idx}: Invalid 'active' type: {type(active).__name__}. "
                        "Must be 0, 1, or NULL.",
                    )
                    continue

                if active_int not in {0, 1}:
                    errors.append(
                        f"Row {idx}, param {param_idx}: Invalid 'active' value: {active_int}. "
                        "Must be 0, 1, or NULL.",
                    )

            # Проверка relation ∈ {'=', '<', '≤', '>', '≥', '~', NULL}
            relation = param.get("relation")
            if relation is not None and not (
                isinstance(relation, float) and pd.isna(relation)
            ):
                relation_str = str(relation).strip()
                if relation_str and relation_str not in STANDARD_RELATIONS:
                    warnings.append(
                        f"Row {idx}, param {param_idx}: Non-standard 'relation' value: {relation_str!r}. "
                        f"Standard operators: {', '.join(sorted(STANDARD_RELATIONS))}.",
                    )

    # Логируем предупреждения
    if warnings:
        for warning in warnings:
            log.warning("truv_validation_warning", message=warning)

    # Обрабатываем ошибки
    if errors:
        error_msg = f"TRUV validation failed for {column}:\n" + "\n".join(errors)
        if fail_fast:
            log.error("truv_validation_failed", error_count=len(errors))
            raise ValueError(error_msg)
        else:
            for error in errors:
                log.warning("truv_validation_error", message=error)

    if not errors and not warnings:
        log.debug("truv_validation_passed", rows_checked=len(df))

    return df

