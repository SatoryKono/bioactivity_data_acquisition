"""Mixin-классы для нормализации, обогащения и валидации."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import pandas as pd


class NormalizationMixin:
    """Набор утилит для нормализации записей."""

    def normalize_records(
        self,
        records: Sequence[Mapping[str, Any]],
        field_mappings: Mapping[str, str | Callable[[Mapping[str, Any]], Any]],
        value_normalizers: Mapping[str, Callable[[Any], Any]] | None = None,
        filters: Iterable[Callable[[Mapping[str, Any]], bool]] | None = None,
    ) -> list[dict[str, Any]]:
        """Применить отображение полей, фильтры и нормализацию значений."""

        filtered_records = list(records)
        if filters:
            for predicate in filters:
                filtered_records = [
                    record for record in filtered_records if predicate(record)
                ]

        value_normalizers = value_normalizers or {}
        normalized: list[dict[str, Any]] = []
        for record in filtered_records:
            row: dict[str, Any] = {}
            for target_field, source in field_mappings.items():
                value = (
                    source(record) if callable(source) else record.get(source)
                )
                normalizer = value_normalizers.get(target_field)
                row[target_field] = normalizer(value) if normalizer else value
            normalized.append(row)

        return normalized


class EnrichmentMixin:
    """Базовые хелперы для последовательного обогащения записей."""

    def enrich_records(
        self,
        records: Sequence[Mapping[str, Any]],
        enrichment_rules: Iterable[
            Callable[[Mapping[str, Any]], Mapping[str, Any]]
        ],
    ) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for record in records:
            enriched_record: Mapping[str, Any] = record
            for rule in enrichment_rules:
                enriched_record = rule(enriched_record)
            enriched.append(dict(enriched_record))
        return enriched


class ValidationMixin:
    """Простая проверка схемы DataFrame."""

    def validate_dataframe(
        self,
        df: pd.DataFrame,
        schema: (
            Mapping[str, Callable[[pd.Series], pd.Series | bool]] | None
        ) = None,
    ) -> None:
        if schema is None:
            return

        missing = [
            column for column in schema.keys() if column not in df.columns
        ]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        for column, validator in schema.items():
            result = validator(df[column])
            if isinstance(result, pd.Series):
                if not result.all():
                    raise ValueError(
                        f"Validation failed for column '{column}'"
                    )
            elif not result:
                raise ValueError(f"Validation failed for column '{column}'")
