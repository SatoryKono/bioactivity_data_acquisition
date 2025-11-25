from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from hashlib import sha256
from typing import Iterable, Mapping, MutableMapping, Sequence

from .interfaces import (
    BusinessKeyDeriverABC,
    DeduplicatorABC,
    HasherABC,
    LookupEnricherABC,
    MergeStrategyABC,
    SideInputProviderABC,
    TransformerABC,
)


class NormalizationTransformer(TransformerABC):
    """Базовая нормализация записей.

    * Нормализует названия полей: обрезает пробелы и по умолчанию приводит к lower-case.
    * Очищает строковые значения от ведущих/конечных пробелов.
    * Преобразует пустые строки в ``None`` для унификации отсутствующих значений.
    """

    def __init__(
        self,
        *,
        field_renames: Mapping[str, str] | None = None,
        lowercase_keys: bool = True,
        trim_strings: bool = True,
        empty_as_none: bool = True,
    ) -> None:
        self._field_renames = {k: v for k, v in (field_renames or {}).items()}
        self._lowercase_keys = lowercase_keys
        self._trim_strings = trim_strings
        self._empty_as_none = empty_as_none

    def transform(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        normalized: list[Mapping[str, object]] = []
        for record in records:
            new_record: dict[str, object | None] = {}
            for raw_key, raw_value in record.items():
                key = self._field_renames.get(raw_key, raw_key)
                if isinstance(key, str):
                    key = key.strip()
                    if self._lowercase_keys:
                        key = key.lower()
                value = raw_value
                if isinstance(value, str) and self._trim_strings:
                    value = value.strip()
                if self._empty_as_none and value == "":
                    value = None
                new_record[str(key)] = value
            normalized.append(new_record)
        return normalized


class CleanupTransformer(TransformerABC):
    """Очищает записи после нормализации.

    * Удаляет записи, в которых отсутствуют обязательные поля.
    * Удаляет поля со значением ``None`` из списка ``drop_null_fields``.
    * Может отбрасывать полностью пустые записи.
    """

    def __init__(
        self,
        *,
        required_fields: Sequence[str] | None = None,
        drop_null_fields: Sequence[str] | None = None,
        drop_empty_records: bool = True,
    ) -> None:
        self._required_fields = set(required_fields or [])
        self._drop_null_fields = set(drop_null_fields or [])
        self._drop_empty_records = drop_empty_records

    def transform(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        cleaned: list[Mapping[str, object]] = []
        for record in records:
            if any(record.get(field) in {None, ""} for field in self._required_fields):
                continue
            new_record = {
                key: value
                for key, value in record.items()
                if not (key in self._drop_null_fields and value is None)
            }
            if self._drop_empty_records and all(value in {None, ""} for value in new_record.values()):
                continue
            cleaned.append(new_record)
        return cleaned


class StaticSideInputProvider(SideInputProviderABC):
    """Возвращает предопределённый набор вспомогательных данных."""

    def __init__(self, name: str, payload: Mapping[str, object]) -> None:
        self._name = name
        self._payload = dict(payload)

    def load(self) -> Mapping[str, object]:
        return {self._name: self._payload}


class MappingLookupEnricher(LookupEnricherABC):
    """Обогащение записей по словарю, полученному из сайд-инпутов."""

    def __init__(
        self,
        *,
        lookup_name: str,
        source_field: str,
        target_field: str | None = None,
        default: object | None = None,
    ) -> None:
        self._lookup_name = lookup_name
        self._source_field = source_field
        self._target_field = target_field
        self._default = default
        self._side_inputs: Mapping[str, object] = {}

    def bind_side_inputs(self, side_inputs: Mapping[str, object]) -> None:
        self._side_inputs = side_inputs

    def enrich(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        lookup_table = self._side_inputs.get(self._lookup_name, {})
        if not isinstance(lookup_table, Mapping):
            return list(records)

        enriched: list[Mapping[str, object]] = []
        for record in records:
            key = record.get(self._source_field)
            enrichment = lookup_table.get(key, self._default)
            if enrichment is None:
                enriched.append(dict(record))
                continue

            if isinstance(enrichment, Mapping) and self._target_field is None:
                merged = {**record, **enrichment}
            else:
                merged = dict(record)
                merged[self._target_field or f"{self._lookup_name}_value"] = enrichment
            enriched.append(merged)
        return enriched


class SHA256RecordHasher(HasherABC):
    """Генерирует стабильный 64-символьный SHA-256 хеш."""

    def hash(self, record: Mapping[str, object]) -> str:
        serialized = "|".join(
            f"{key}={self._normalize_value(record.get(key))}"
            for key in sorted(record.keys())
        )
        return sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_value(value: object | None) -> str:
        if value is None:
            return "<null>"
        if isinstance(value, (int, float, bool)):
            return str(value)
        return str(value).strip()


class SHA256BusinessKeyDeriver(BusinessKeyDeriverABC):
    """Формирует бизнес-ключ из набора полей."""

    def __init__(
        self,
        key_fields: Sequence[str],
        *,
        target_field: str = "business_key",
        hasher: HasherABC | None = None,
    ) -> None:
        self._key_fields = tuple(key_fields)
        self._target_field = target_field
        self._hasher = hasher or SHA256RecordHasher()

    def derive(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        derived: list[Mapping[str, object]] = []
        for record in records:
            key_record = {field: record.get(field) for field in self._key_fields}
            business_key = self._hasher.hash(key_record)
            if len(business_key) != 64:
                raise ValueError("Business key must be a 64-character SHA-256 hex digest")
            new_record = dict(record)
            new_record[self._target_field] = business_key
            derived.append(new_record)
        return derived


class ColumnHashingTransformer(TransformerABC):
    """Генерация хэш-колонок для выбранных полей."""

    def __init__(
        self,
        columns: Sequence[str],
        *,
        suffix: str = "_hash",
        hasher: HasherABC | None = None,
    ) -> None:
        self._columns = tuple(columns)
        self._suffix = suffix
        self._hasher = hasher or SHA256RecordHasher()

    def transform(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        transformed: list[Mapping[str, object]] = []
        for record in records:
            new_record = dict(record)
            for column in self._columns:
                hash_value = self._hasher.hash({column: record.get(column)})
                new_record[f"{column}{self._suffix}"] = hash_value
            transformed.append(new_record)
        return transformed


class BusinessKeyDeduplicator(DeduplicatorABC):
    """Убирает дубликаты на основе бизнес-ключа, сохраняя порядок первых вхождений."""

    def __init__(self, *, business_key_field: str = "business_key") -> None:
        self._business_key_field = business_key_field

    def deduplicate(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        seen: OrderedDict[str, Mapping[str, object]] = OrderedDict()
        preserved: list[Mapping[str, object]] = []
        for record in records:
            business_key = record.get(self._business_key_field)
            if business_key is None:
                preserved.append(record)
                continue
            if business_key not in seen:
                seen[business_key] = record
        return preserved + list(seen.values())


class PreferPrimaryMergeStrategy(MergeStrategyABC):
    """Объединяет записи по бизнес-ключу, отдавая приоритет primary."""

    def __init__(self, *, business_key_field: str = "business_key") -> None:
        self._business_key_field = business_key_field

    def merge(
        self,
        primary: Iterable[Mapping[str, object]],
        secondary: Iterable[Mapping[str, object]],
    ) -> Iterable[Mapping[str, object]]:
        secondary_by_key: MutableMapping[str, Mapping[str, object]] = {}
        for record in secondary:
            business_key = record.get(self._business_key_field)
            if business_key:
                secondary_by_key[str(business_key)] = record

        merged: list[Mapping[str, object]] = []
        for record in primary:
            business_key = record.get(self._business_key_field)
            if business_key is None:
                merged.append(record)
                continue
            secondary_record = secondary_by_key.pop(str(business_key), {})
            combined = {**secondary_record, **record}
            merged.append(combined)

        merged.extend(secondary_by_key.values())
        return merged


@dataclass
class ProcessingChain:
    """Композиция операций трансформации и обогащения."""

    transformers: Sequence[TransformerABC] = ()
    side_input_providers: Sequence[SideInputProviderABC] = ()
    enricher: LookupEnricherABC | None = None
    business_key_deriver: BusinessKeyDeriverABC | None = None
    deduplicator: DeduplicatorABC | None = None
    merge_strategy: MergeStrategyABC | None = None

    def run(
        self,
        primary_records: Sequence[Mapping[str, object]],
        *,
        secondary_records: Sequence[Mapping[str, object]] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        records: Sequence[Mapping[str, object]] = list(primary_records)

        for transformer in self.transformers:
            records = transformer.transform(records)

        side_inputs: dict[str, object] = {}
        for provider in self.side_input_providers:
            side_inputs.update(provider.load())

        if self.enricher:
            if side_inputs and hasattr(self.enricher, "bind_side_inputs"):
                getattr(self.enricher, "bind_side_inputs")(side_inputs)
            records = self.enricher.enrich(records)

        if self.business_key_deriver:
            records = self.business_key_deriver.derive(records)

        if secondary_records is not None and self.merge_strategy:
            records = tuple(self.merge_strategy.merge(records, secondary_records))

        if self.deduplicator:
            records = self.deduplicator.deduplicate(records)

        return records
