# Промпт: Создать pagination/, merge/, schema/ для Crossref

## Контекст задачи

**Проблема:** Crossref источник имеет только базовую структуру (`client/`, `request/`, `parser/`, `normalizer/`, `output/`, `pipeline.py`) без опциональных модулей согласно MODULE_RULES.md.

**Статус:** ⚠️ **ЧАСТИЧНО РЕАЛИЗОВАНО** (базовая структура есть, опциональные модули отсутствуют)

**Ссылки:**

- `refactoring/MODULE_RULES.md` (строки 24-28): Опциональные подпапки (SHOULD): `schema/`, `merge/`, `pagination/`
- `refactoring/AUDIT_REPORT_2025.md` (строка 1074): "Crossref: ✅ Базовая структура, ⚠️ Нет pagination/, merge/, schema/"
- `refactoring/DATA_SOURCES.md` (строки 157-176): Требования к Crossref источнику

## Текущая структура

```
src/bioetl/sources/crossref/
├── __init__.py
├── client/
│   └── __init__.py
├── request/
│   └── __init__.py
├── parser/
│   └── __init__.py
├── normalizer/
│   └── __init__.py
├── output/
│   └── __init__.py
└── pipeline.py
```

**Отсутствует:**

- `pagination/` - стратегии пагинации для Crossref API
- `merge/` - политика объединения с document pipeline
- `schema/` - Pandera схемы для валидации Crossref данных

## Требования из документации

### Crossref API специфика

**Источник:** `docs/requirements/09-document-chembl-extraction.md` (строки 1104-1286)

**Pagination:**

- Crossref поддерживает cursor-based pagination для больших списков
- API возвращает `next-cursor` в заголовках или в JSON ответе
- Поддерживает фильтры и фасеты
- Rate limit: ~50 запросов/секунду (Public Pool) или выше с mailto (Polite Pool)

**Merge Policy:**

- Join по `doi_clean` (нормализованный DOI)
- Приоритет Crossref для библиографических метаданных
- Используется в document pipeline для обогащения ChEMBL документов

**Schema:**

- Документы Crossref включают: DOI, title, authors, journal, year, ISSN, publisher, subject, doc_type
- Нормализация: DOI, ORCID, ISSN (print vs electronic), даты публикации

## Реализация

### 1. Pagination (`src/bioetl/sources/crossref/pagination/`)

**Файл:** `src/bioetl/sources/crossref/pagination/__init__.py`

```python
"""Pagination helpers for the Crossref REST API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["CursorPaginator"]

PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]

@dataclass(slots=True)
class CursorPaginator:
    """Cursor-based paginator for Crossref API.

    Crossref supports cursor pagination via 'cursor' query parameter
    and returns 'next-cursor' in response for continuation.
    """

    client: UnifiedAPIClient
    page_size: int = 100

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "DOI",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages for Crossref API using cursor pagination.

        Args:
            path: API endpoint path (e.g., '/works')
            unique_key: Key to use for duplicate detection (default: 'DOI')
            params: Base query parameters (filters, facets, etc.)
            parser: Function to extract items from API response

        Returns:
            List of unique items (by unique_key)

        Note:
            Crossref API uses 'cursor' parameter for pagination.
            Response contains 'message' with 'items' array and
            'next-cursor' for continuation (if available).
        """
        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None

        while True:
            query: dict[str, Any] = dict(params or {})
            query["rows"] = self.page_size
            if cursor:
                query["cursor"] = cursor

            payload = self.client.request_json(path, params=query)

            if not isinstance(payload, dict) or "message" not in payload:
                break

            message = payload["message"]

            if parser is None:
                # Default parser for Crossref works endpoint
                items = message.get("items", [])
            else:
                items = list(parser(message))

            filtered: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, dict):
                    continue

                # Extract unique key value
                key_value = item.get(unique_key)
                if key_value is None:
                    # Skip items without unique key
                    continue

                key_str = str(key_value).strip()
                if key_str and key_str not in seen:
                    seen.add(key_str)
                    filtered.append(dict(item))

            if not filtered:
                break

            collected.extend(filtered)

            # Check for next cursor
            next_cursor = message.get("next-cursor")
            if not next_cursor or len(filtered) < self.page_size:
                break

            cursor = next_cursor

        return collected
```

**Пример использования:**
```python
from bioetl.sources.crossref.pagination import CursorPaginator
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

client = UnifiedAPIClient(APIConfig(base_url="https://api.crossref.org"))
paginator = CursorPaginator(client, page_size=100)

def parse_works(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract works from Crossref message."""
    return message.get("items", [])

works = paginator.fetch_all(
    "/works",
    params={"filter": "has-abstract:true", "facet": "published:*"},
    parser=parse_works,
    unique_key="DOI",
)
```

### 2. Merge (`src/bioetl/sources/crossref/merge/`)

**Файл:** `src/bioetl/sources/crossref/merge/__init__.py`

```python
"""Merge policy for Crossref enrichment data."""

from __future__ import annotations

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["merge_crossref_with_base", "CROSSREF_MERGE_KEYS"]

logger = UnifiedLogger.get(__name__)

CROSSREF_MERGE_KEYS = {
    "primary": "doi_clean",  # Normalized DOI
    "fallback": ["crossref_doi"],  # Fallback keys if doi_clean missing
}

def merge_crossref_with_base(
    base_df: pd.DataFrame,
    crossref_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge Crossref enrichment data with base document dataframe.

    Args:
        base_df: Base dataframe (e.g., ChEMBL documents)
        crossref_df: Crossref enrichment dataframe
        base_doi_column: Column name in base_df with DOI for joining
        conflict_detection: Whether to detect DOI conflicts

    Returns:
        Merged dataframe with Crossref data prefixed as 'crossref_*'

    Strategy:

        - Join on normalized DOI (doi_clean)
        - Prefix all Crossref columns with 'crossref_'
        - Preserve source metadata for conflict detection
        - Prefer Crossref values for bibliographic metadata

    """
    if crossref_df.empty:
        return base_df.copy()

    # Ensure doi_clean exists in crossref_df
    if "doi_clean" not in crossref_df.columns:
        if "crossref_doi" in crossref_df.columns:
            crossref_df["doi_clean"] = crossref_df["crossref_doi"]
        else:
            logger.warning("crossref_no_doi_column", columns=list(crossref_df.columns))
            return base_df.copy()

    # Prefix Crossref columns (except doi_clean which is used for joining)
    crossref_prefixed = crossref_df.copy()
    columns_to_prefix = [col for col in crossref_prefixed.columns if col != "doi_clean"]
    rename_map = {col: f"crossref_{col}" for col in columns_to_prefix}
    crossref_prefixed = crossref_prefixed.rename(columns=rename_map)

    # Ensure crossref_doi exists for joining
    if "crossref_doi_clean" in crossref_prefixed.columns:
        crossref_prefixed["crossref_doi"] = crossref_prefixed["crossref_doi_clean"]
    elif "doi_clean" in crossref_prefixed.columns:
        crossref_prefixed["crossref_doi"] = crossref_prefixed["doi_clean"]

    # Join on DOI
    if base_doi_column not in base_df.columns:
        logger.warning("base_no_doi_column", base_columns=list(base_df.columns))
        return base_df.copy()

    merged_df = base_df.merge(
        crossref_prefixed,
        left_on=base_doi_column,
        right_on="crossref_doi",
        how="left",
        suffixes=("", "_crossref"),
    )

    # Detect conflicts if requested
    if conflict_detection:
        merged_df = _detect_crossref_conflicts(merged_df, base_doi_column)

    return merged_df

def _detect_crossref_conflicts(
    merged_df: pd.DataFrame,
    base_doi_column: str,
) -> pd.DataFrame:
    """Detect DOI conflicts between base and Crossref.

    Adds 'conflict_crossref_doi' column indicating when base DOI
    differs from Crossref DOI (should not happen for successful matches).
    """
    if "conflict_crossref_doi" in merged_df.columns:
        return merged_df

    merged_df["conflict_crossref_doi"] = False

    if base_doi_column not in merged_df.columns or "crossref_doi" not in merged_df.columns:
        return merged_df

    base_mask = merged_df[base_doi_column].notna()
    crossref_mask = merged_df["crossref_doi"].notna()
    both_present = base_mask & crossref_mask

    if both_present.any():
        base_values = merged_df.loc[both_present, base_doi_column].astype(str).str.strip()
        crossref_values = merged_df.loc[both_present, "crossref_doi"].astype(str).str.strip()

        conflicts = base_values != crossref_values
        merged_df.loc[both_present, "conflict_crossref_doi"] = conflicts

    return merged_df
```

**Интеграция с document/merge/policy.py:**
Обновить `src/bioetl/sources/document/merge/policy.py` для использования Crossref merge:

```python

# Добавить импорт

from bioetl.sources.crossref.merge import merge_crossref_with_base, CROSSREF_MERGE_KEYS

# Использовать в merge_with_precedence

if crossref_df is not None and not crossref_df.empty:
    merged_df = merge_crossref_with_base(
        merged_df,
        crossref_df,
        base_doi_column="chembl_doi",
    )
```

### 3. Schema (`src/bioetl/sources/crossref/schema/`)

**Файл:** `src/bioetl/sources/crossref/schema/__init__.py`

```python
"""Pandera schemas for Crossref data validation."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = [
    "CrossrefRawSchema",
    "CrossrefNormalizedSchema",
]

class CrossrefRawSchema(BaseSchema):
    """Schema for raw Crossref API response data.

    Validates structure of Crossref 'message' object before normalization.
    """

    DOI: Series[str] = pa.Field(nullable=False)
    title: Series[str] = pa.Field(nullable=True)
    container_title: Series[str] = pa.Field(nullable=True)  # Journal name
    short_container_title: Series[str] = pa.Field(nullable=True)
    author: Series[object] = pa.Field(nullable=True)  # List of author dicts
    published_print: Series[object] = pa.Field(nullable=True)
    published_online: Series[object] = pa.Field(nullable=True)
    issued: Series[object] = pa.Field(nullable=True)
    created: Series[object] = pa.Field(nullable=True)
    volume: Series[str] = pa.Field(nullable=True)
    issue: Series[str] = pa.Field(nullable=True)
    page: Series[str] = pa.Field(nullable=True)
    ISSN: Series[object] = pa.Field(nullable=True)  # List of ISSN strings
    issn_type: Series[object] = pa.Field(nullable=True)  # List of ISSN type dicts
    publisher: Series[str] = pa.Field(nullable=True)
    subject: Series[object] = pa.Field(nullable=True)  # List of subject strings
    type: Series[str] = pa.Field(nullable=True)  # Document type

    class Config:
        strict = True
        coerce = True
        ordered = False

class CrossrefNormalizedSchema(BaseSchema):
    """Schema for normalized Crossref enrichment data.

    Validates output of CrossrefAdapter.normalize_record() and
    integration with document pipeline.
    """

    # Business key
    doi_clean: Series[str] = pa.Field(nullable=False)
    crossref_doi: Series[str] = pa.Field(nullable=True)

    # Bibliographic metadata
    crossref_title: Series[str] = pa.Field(nullable=True)
    crossref_journal: Series[str] = pa.Field(nullable=True)
    crossref_authors: Series[str] = pa.Field(nullable=True)
    crossref_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    crossref_month: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    crossref_day: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Publication details
    crossref_volume: Series[str] = pa.Field(nullable=True)
    crossref_issue: Series[str] = pa.Field(nullable=True)
    crossref_first_page: Series[str] = pa.Field(nullable=True)

    # Identifiers
    crossref_issn_print: Series[str] = pa.Field(nullable=True)
    crossref_issn_electronic: Series[str] = pa.Field(nullable=True)
    crossref_orcid: Series[str] = pa.Field(nullable=True)

    # Additional metadata
    crossref_publisher: Series[str] = pa.Field(nullable=True)
    crossref_subject: Series[str] = pa.Field(nullable=True)
    crossref_doc_type: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "doi_clean",
        "crossref_doi",
        "crossref_title",
        "crossref_journal",
        "crossref_authors",
        "crossref_year",
        "crossref_month",
        "crossref_day",
        "crossref_volume",
        "crossref_issue",
        "crossref_first_page",
        "crossref_issn_print",
        "crossref_issn_electronic",
        "crossref_orcid",
        "crossref_publisher",
        "crossref_subject",
        "crossref_doc_type",
    ]
```

## Тесты

### Pagination тесты

**Файл:** `tests/sources/crossref/test_pagination.py`

```python
"""Tests for Crossref pagination."""

import pytest
from unittest.mock import MagicMock

from bioetl.sources.crossref.pagination import CursorPaginator
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

def test_cursor_paginator_fetch_all():
    """Test cursor pagination with multiple pages."""
    client = MagicMock(spec=UnifiedAPIClient)

    # First page response
    client.request_json.side_effect = [
        {
            "message": {
                "items": [{"DOI": f"10.1000/test.{i}"} for i in range(100)],
                "next-cursor": "abc123",
            }
        },
        # Second page response
        {
            "message": {
                "items": [{"DOI": f"10.1000/test.{i}"} for i in range(100, 150)],
                "next-cursor": None,  # Last page
            }
        },
    ]

    paginator = CursorPaginator(client, page_size=100)

    def parse_works(message):
        return message.get("items", [])

    works = paginator.fetch_all(
        "/works",
        params={"filter": "has-abstract:true"},
        parser=parse_works,
    )

    assert len(works) == 150
    assert all("DOI" in work for work in works)
    assert client.request_json.call_count == 2
```

### Merge тесты

**Файл:** `tests/sources/crossref/test_merge.py`

```python
"""Tests for Crossref merge policy."""

import pandas as pd

from bioetl.sources.crossref.merge import merge_crossref_with_base

def test_merge_crossref_with_base():
    """Test merging Crossref data with base dataframe."""
    base_df = pd.DataFrame({
        "chembl_doi": ["10.1000/test.1", "10.1000/test.2"],
        "chembl_title": ["Title 1", "Title 2"],
    })

    crossref_df = pd.DataFrame({
        "doi_clean": ["10.1000/test.1"],
        "crossref_title": ["Crossref Title 1"],
        "crossref_journal": ["Journal 1"],
    })

    merged = merge_crossref_with_base(
        base_df,
        crossref_df,
        base_doi_column="chembl_doi",
    )

    assert "crossref_title" in merged.columns
    assert merged.loc[0, "crossref_title"] == "Crossref Title 1"
    assert pd.isna(merged.loc[1, "crossref_title"])
```

### Schema тесты

**Файл:** `tests/sources/crossref/test_schema.py`

```python
"""Tests for Crossref schemas."""

import pandas as pd
import pytest

from bioetl.sources.crossref.schema import (
    CrossrefRawSchema,
    CrossrefNormalizedSchema,
)

def test_crossref_raw_schema():
    """Test validation of raw Crossref data."""
    df = pd.DataFrame({
        "DOI": ["10.1000/test.1"],
        "title": ["Test Title"],
        "container_title": ["Test Journal"],
    })

    schema = CrossrefRawSchema()
    validated = schema.validate(df)
    assert len(validated) == 1

def test_crossref_normalized_schema():
    """Test validation of normalized Crossref data."""
    df = pd.DataFrame({
        "doi_clean": ["10.1000/test.1"],
        "crossref_doi": ["10.1000/test.1"],
        "crossref_title": ["Test Title"],
    })

    schema = CrossrefNormalizedSchema()
    validated = schema.validate(df)
    assert len(validated) == 1
```

## Критерии завершения

- ✅ Создан `src/bioetl/sources/crossref/pagination/` с `CursorPaginator`
- ✅ Создан `src/bioetl/sources/crossref/merge/` с merge политикой
- ✅ Создан `src/bioetl/sources/crossref/schema/` с Pandera схемами
- ✅ Все модули имеют `__all__` и docstrings
- ✅ Добавлены unit тесты для каждого модуля
- ✅ Интеграция с `document/merge/policy.py` обновлена
- ✅ Документация обновлена (если требуется)

## Примечания

- Crossref API использует cursor-based pagination через параметр `cursor`
- Merge политика: join по `doi_clean` (нормализованный DOI)
- Схемы валидируют структуру до и после нормализации
- Все модули опциональны (SHOULD) согласно MODULE_RULES.md
