"""Реализации базовых шагов трансформации и обогащения.

Модуль предоставляет конвейер обработки записей с этапами нормализации,
cleanup, обогащения по side-input и вычисления бизнес-ключей. Реализация
опирается на простые словари, чтобы можно было покрыть функциональность
юнит-тестами без внешних зависимостей.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, MutableMapping, Sequence

from .interfaces import (
    BusinessKeyDeriverABC,
    DeduplicatorABC,
    HasherABC,
    LookupEnricherABC,
    MergeStrategyABC,
    SideInputProviderABC,
    TransformerABC,
)


Record = Mapping[str, object]
RecordMutable = MutableMapping[str, object]
Records = Sequence[Record]


class NormalizationTransformer(TransformerABC):
    """Нормализует ключи и строковые значения в записях."""

    def __init__(
        self,
        *,
        field_map: Mapping[str, str] | None = None,
        lowercase_keys: bool = True,
        strip_strings: bool = True,
        lowercase_strings: bool = False,
    ) -> None:
        self._field_map = {**(field_map or {})}
        self._lowercase_keys = lowercase_keys
        self._strip_strings = strip_strings
        self._lowercase_strings = lowercase_strings

    def _normalize_key(self, key: str) -> str:
        mapped = self._field_map.get(key, key)
        return mapped.lower() if self._lowercase_keys else mapped

    def _normalize_value(self, value: object) -> object:
        if isinstance(value, str):
            value = value.strip() if self._strip_strings else value
            return value.lower() if self._lowercase_strings else value
        return value

    def transform(self, records: Records) -> Records:
        normalized: list[dict[str, object]] = []
        for record in records:
            new_record: dict[str, object] = {}
            for key, value in record.items():
                new_key = self._normalize_key(key)
                new_record[new_key] = self._normalize_value(value)
            normalized.append(new_record)
        return normalized


class CleanupTransformer(TransformerABC):
    """Удаляет записи с незаполненными обязательными полями и заполняет дефолты."""

    def __init__(
        self,
        *,
        required_fields: Sequence[str] | None = None,
        defaults: Mapping[str, object] | None = None,
    ) -> None:
        self._required_fields = set(required_fields or ())
        self._defaults = {**(defaults or {})}

    def transform(self, records: Records) -> Records:
        cleaned: list[dict[str, object]] = []
        for record in records:
            if any(record.get(field) in (None, "") for field in self._required_fields):
                continue
            merged = {**self._defaults, **record}
            cleaned.append(merged)
        return cleaned


class StaticSideInputProvider(SideInputProviderABC):
    """Возвращает заранее переданные side-input данные."""

    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = {**payload}

    def load(self) -> Mapping[str, object]:
        return {**self._payload}


class CompositeSideInputProvider(SideInputProviderABC):
    """Комбинирует несколько провайдеров в один словарь."""

    def __init__(self, providers: Sequence[SideInputProviderABC]) -> None:
        self._providers = list(providers)

    def load(self) -> Mapping[str, object]:
        merged: dict[str, object] = {}
        for provider in self._providers:
            for key, value in provider.load().items():
                if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key] = {**merged[key], **value}
                else:
                    merged[key] = value
        return merged


class SimpleLookupEnricher(LookupEnricherABC):
    """Обогащает записи по заранее подготовленному справочнику."""

    def __init__(
        self,
        *,
        lookup: Mapping[str, Mapping[str, object]],
        record_key: str,
        merge_to_field: str | None = None,
    ) -> None:
        self._lookup = lookup
        self._record_key = record_key
        self._merge_to_field = merge_to_field

    def enrich(self, records: Records) -> Records:
        enriched: list[dict[str, object]] = []
        for record in records:
            key = record.get(self._record_key)
            extra = self._lookup.get(str(key)) if key is not None else None
            new_record = {**record}
            if extra:
                if self._merge_to_field:
                    new_record[self._merge_to_field] = extra
                else:
                    new_record.update(extra)
            enriched.append(new_record)
        return enriched


class SHA256BusinessKeyDeriver(BusinessKeyDeriverABC):
    """Строит стабильный 64-символьный хэш на основе выбранных полей."""

    def __init__(
        self,
        *,
        fields: Sequence[str],
        field_name: str = "business_key",
        canonicalize_strings: bool = True,
    ) -> None:
        self._fields = tuple(fields)
        self._field_name = field_name
        self._canonicalize_strings = canonicalize_strings

    def _canonical_value(self, value: object) -> object:
        if self._canonicalize_strings and isinstance(value, str):
            return value.strip().lower()
        return value

    def _serialize(self, record: Record) -> str:
        payload = {field: self._canonical_value(record.get(field)) for field in self._fields}
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def derive(self, records: Records) -> Records:
        output: list[dict[str, object]] = []
        for record in records:
            serialized = self._serialize(record)
            digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
            new_record = {**record, self._field_name: digest}
            output.append(new_record)
        return output


class ColumnHasher(HasherABC):
    """Генерирует хэш для набора колонок."""

    def __init__(self, *, fields: Sequence[str]) -> None:
        self._fields = tuple(fields)

    def hash(self, record: Record) -> str:
        serialized = json.dumps({field: record.get(field) for field in self._fields}, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class HashingTransformer(TransformerABC):
    """Добавляет колонку с хэшем, используя переданный Hasher."""

    def __init__(self, *, hasher: HasherABC, target_field: str = "row_hash") -> None:
        self._hasher = hasher
        self._target_field = target_field

    def transform(self, records: Records) -> Records:
        transformed: list[dict[str, object]] = []
        for record in records:
            digest = self._hasher.hash(record)
            transformed.append({**record, self._target_field: digest})
        return transformed


class BusinessKeyDeduplicator(DeduplicatorABC):
    """Удаляет дубликаты на основе бизнес-ключа."""

    def __init__(self, *, business_key_field: str = "business_key", keep: str = "first") -> None:
        if keep not in {"first", "last"}:
            raise ValueError("keep должен быть 'first' или 'last'")
        self._business_key_field = business_key_field
        self._keep = keep

    def deduplicate(self, records: Records) -> Records:
        buffer: dict[str, dict[str, object]] = {}
        for record in records:
            key = str(record.get(self._business_key_field))
            if key not in buffer or self._keep == "last":
                buffer[key] = {**record}
        ordered = list(buffer.values())
        if self._keep == "last":
            return ordered
        # keep=="first" сохраняет порядок первой встречи
        seen: set[str] = set()
        result: list[dict[str, object]] = []
        for record in records:
            key = str(record.get(self._business_key_field))
            if key in seen:
                continue
            seen.add(key)
            result.append(buffer[key])
        return result


class MergeByBusinessKey(MergeStrategyABC):
    """Объединяет коллекции, опираясь на бизнес-ключ."""

    def __init__(
        self,
        *,
        business_key_field: str = "business_key",
        merge_func: Callable[[RecordMutable, Record], RecordMutable] | None = None,
    ) -> None:
        self._business_key_field = business_key_field
        self._merge_func = merge_func

    def merge(
        self, primary: Iterable[Record], secondary: Iterable[Record]
    ) -> Iterable[Record]:
        merged: dict[str, dict[str, object]] = {}
        for record in primary:
            key = str(record.get(self._business_key_field))
            merged[key] = {**record}
        for record in secondary:
            key = str(record.get(self._business_key_field))
            if key in merged and self._merge_func:
                merged[key] = self._merge_func(merged[key], record)
            elif key not in merged:
                merged[key] = {**record}
        return list(merged.values())


@dataclass
class ProcessingChain:
    """Конвейер обработки записей."""

    normalizers: Sequence[TransformerABC]
    cleanup: TransformerABC
    side_input_provider: SideInputProviderABC
    enricher: LookupEnricherABC
    business_key_deriver: BusinessKeyDeriverABC
    deduplicator: DeduplicatorABC
    hash_generator: TransformerABC | None = None

    def run(self, records: Records) -> Records:
        current: Records = records
        for transformer in self.normalizers:
            current = transformer.transform(current)
        current = self.cleanup.transform(current)
        side_inputs = self.side_input_provider.load()
        if side_inputs:
            current = self.enricher.enrich(current)
        current = self.business_key_deriver.derive(current)
        current = self.deduplicator.deduplicate(current)
        if self.hash_generator:
            current = self.hash_generator.transform(current)
        return current
