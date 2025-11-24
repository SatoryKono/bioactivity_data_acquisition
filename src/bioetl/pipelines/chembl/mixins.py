"""Mixin-классы для нормализации, обогащения и валидации."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
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

        missing = [column for column in schema if column not in df.columns]
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


@dataclass(frozen=True)
class FieldMappingRule:
    """Declarative description of a single field mapping rule.

    The rule is later converted into the arguments expected by
    ``NormalizationMixin.normalize_records``.
    """

    source: str | None
    aliases: tuple[str, ...] = ()
    required: bool = False
    drop: bool = False
    value_normalizer: Callable[[Any], Any] | None = None
    id_type: str | None = None


class ChemblFieldMappingMixin:
    """Helpers to build normalization rules from ``FieldMappingRule`` specs.

    Pipelines can define a mapping from target column names to
    :class:`FieldMappingRule` and call
    :meth:`build_normalization_rules_from_spec` to obtain a
    ``field_mappings``/``value_normalizers`` payload compatible with
    :class:`NormalizationMixin`.
    """

    def build_normalization_rules_from_spec(
        self,
        spec: Mapping[str, FieldMappingRule],
    ) -> Mapping[str, Any]:
        """Return NormalizationMixin rules constructed from ``spec``.

        The result is a mapping containing at least ``field_mappings`` and,
        when value normalizers are present, ``value_normalizers``. Rules
        marked with ``drop=True`` are ignored.
        """

        field_mappings: dict[str, Callable[[Mapping[str, Any]], Any] | str]
        field_mappings = {}
        value_normalizers: dict[str, Callable[[Any], Any]]
        value_normalizers = {}

        for target, rule in spec.items():
            if rule.drop:
                continue
            field_mappings[target] = self._build_field_mapping_value(rule)
            if rule.value_normalizer is not None:
                value_normalizers[target] = rule.value_normalizer

        if value_normalizers:
            return {
                "field_mappings": field_mappings,
                "value_normalizers": value_normalizers,
            }
        return {"field_mappings": field_mappings}

    def _build_field_mapping_value(
        self,
        rule: FieldMappingRule,
    ) -> Callable[[Mapping[str, Any]], Any] | str:
        """Build a value for ``field_mappings`` from a single rule.

        For simple rules without aliases or ``id_type`` the ``source`` name
        is returned directly so that ``NormalizationMixin`` can use
        ``record.get(source)``. When aliases or ``id_type`` are present a
        small resolver callable is constructed instead.
        """

        if rule.id_type is not None:
            # Import lazily to avoid circular imports.
            from bioetl.pipelines.chembl import id_aliases

            if rule.id_type == "assay":
                return id_aliases.resolve_assay_chembl_id
            if rule.id_type == "testitem":
                return id_aliases.resolve_testitem_chembl_id
            if rule.id_type == "target":
                return id_aliases.resolve_target_chembl_id

        if not rule.aliases and rule.source is not None:
            # Simple one-to-one mapping without aliases.
            return rule.source

        def _resolver(
            record: Mapping[str, Any],
            *,
            _rule: FieldMappingRule = rule,
        ) -> Any:
            if _rule.source is not None:
                value = record.get(_rule.source)
                if value is not None:
                    return value
            for name in _rule.aliases:
                value = record.get(name)
                if value is not None:
                    return value
            return None

        return _resolver
