"""Вспомогательные функции для новых Chembl пайплайнов."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd


def safe_cast(
    value: Any, target_type: type, default: Any | None = None
) -> Any:
    """Безопасное приведение типов с возвратом ``default`` при ошибке."""

    try:
        return target_type(value)
    except (TypeError, ValueError):
        return default


def build_dataframe(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Детерминированно построить ``DataFrame`` из записей."""

    materialized = list(records)
    if not materialized:
        return pd.DataFrame()

    # Фиксируем порядок столбцов по первому элементу, затем дополняем новыми при необходимости.
    columns: list[str] = list(materialized[0].keys())
    for record in materialized[1:]:
        for key in record.keys():
            if key not in columns:
                columns.append(key)

    df = pd.DataFrame(materialized)
    return df.loc[:, columns]
