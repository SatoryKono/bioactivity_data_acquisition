# Промпт: Создать pagination/, merge/, schema/ для OpenAlex

## Контекст задачи

**Проблема:** OpenAlex источник имеет только базовую структуру без опциональных модулей согласно MODULE_RULES.md.

**Статус:** ⚠️ **ЧАСТИЧНО РЕАЛИЗОВАНО** (базовая структура есть, опциональные модули отсутствуют)

**Ссылки:**

- `refactoring/MODULE_RULES.md` (строки 24-28): Опциональные подпапки (SHOULD): `schema/`, `merge/`, `pagination/`
- `refactoring/AUDIT_REPORT_2025.md` (строка 1075): "OpenAlex: ✅ Базовая структура, ⚠️ Нет pagination/, merge/, schema/"
- `refactoring/DATA_SOURCES.md` (строки 199-217): Требования к OpenAlex источнику

## Текущая структура

```
src/bioetl/sources/openalex/
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

- `pagination/` - стратегии пагинации для OpenAlex API
- `merge/` - политика объединения с document pipeline
- `schema/` - Pandera схемы для валидации OpenAlex данных

## Требования из документации

### OpenAlex API специфика

**Источник:** `docs/requirements/09-document-chembl-extraction.md` (строки 1287-1456)

**Pagination:**

- OpenAlex использует cursor-based pagination через параметр `cursor` в URL
- API возвращает `meta.next_cursor` в ответе для продолжения
- Rate limit: "polite pool" с указанием email в заголовках (более высокие лимиты)
- Поддерживает фильтры и сортировку

**Merge Policy:**

- Join по `doi_clean` (нормализованный DOI) с fallback на OpenAlex ID
- Приоритет OpenAlex для библиографических метаданных
- Используется в document pipeline для обогащения ChEMBL документов

**Schema:**

- Документы OpenAlex включают: OpenAlex ID, DOI, title, authors, journal, year, open_access, concepts, venue
- Нормализация: OpenAlex ID, DOI, PMID (если есть), даты публикации, ISSN, OA статус, concepts (топ-3)

## Реализация

### 1. Pagination (`src/bioetl/sources/openalex/pagination/`)

**Файл:** `src/bioetl/sources/openalex/pagination/__init__.py`

```python
"""Pagination helpers for the OpenAlex API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["CursorPaginator"]

PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]

@dataclass(slots=True)
class CursorPaginator:
    """Cursor-based paginator for OpenAlex API.

    OpenAlex supports cursor pagination via 'cursor' query parameter
    and returns 'meta.next_cursor' in response for continuation.

    Note:
        OpenAlex recommends using email in User-Agent header for
        "polite pool" access with higher rate limits.
    """

    client: UnifiedAPIClient
    page_size: int = 100

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "id",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages for OpenAlex API using cursor pagination.

        Args:
            path: API endpoint path (e.g., '/works')
            unique_key: Key to use for duplicate detection (default: 'id')
            params: Base query parameters (filters, sort, select, etc.)
            parser: Function to extract items from API response

        Returns:
            List of unique items (by unique_key)

        Note:
            OpenAlex API uses 'cursor' parameter for pagination.
            Response contains 'results' array with items and
            'meta.next_cursor' for continuation (if available).
        """
        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None

        while True:
            query: dict[str, Any] = dict(params or {})
            query["per_page"] = self.page_size
            if cursor:
                query["cursor"] = cursor

            payload = self.client.request_json(path, params=query)

            if not isinstance(payload, dict):
                break

            # Default parser for OpenAlex works endpoint
            if parser is None:
                items = payload.get("results", [])
            else:
                items = list(parser(payload))

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
            meta = payload.get("meta", {})
            next_cursor = meta.get("next_cursor")

            if not next_cursor or len(filtered) < self.page_size:
                break

            cursor = next_cursor

        return collected
```

**Пример использования:**
```python
from bioetl.sources.openalex.pagination import CursorPaginator
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

client = UnifiedAPIClient(APIConfig(base_url="https://api.openalex.org"))
paginator = CursorPaginator(client, page_size=100)

def parse_works(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract works from OpenAlex response."""
    return response.get("results", [])

works = paginator.fetch_all(
    "/works",
    params={
        "filter": "has_doi:true,is_oa:true",
        "sort": "cited_by_count:desc",
        "select": "id,title,doi,authorships,publication_date",
    },
    parser=parse_works,
    unique_key="id",
)
```

### 2. Merge (`src/bioetl/sources/openalex/merge/`)

**Файл:** `src/bioetl/sources/openalex/merge/__init__.py`

```python
"""Merge policy for OpenAlex enrichment data."""

from __future__ import annotations

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["merge_openalex_with_base", "OPENALEX_MERGE_KEYS"]

logger = UnifiedLogger.get(__name__)

OPENALEX_MERGE_KEYS = {
    "primary": "doi_clean",  # Normalized DOI
    "fallback": ["openalex_doi", "openalex_id"],  # Fallback keys
}

def merge_openalex_with_base(
    base_df: pd.DataFrame,
    openalex_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    base_pmid_column: str | None = "chembl_pmid",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge OpenAlex enrichment data with base document dataframe.

    Args:
        base_df: Base dataframe (e.g., ChEMBL documents)
        openalex_df: OpenAlex enrichment dataframe
        base_doi_column: Column name in base_df with DOI for joining
        base_pmid_column: Optional column name in base_df with PMID for fallback join
        conflict_detection: Whether to detect DOI/PMID conflicts

    Returns:
        Merged dataframe with OpenAlex data prefixed as 'openalex_*'

    Strategy:

        - Primary join on normalized DOI (doi_clean)
        - Fallback join on OpenAlex ID if DOI missing
        - Prefix all OpenAlex columns with 'openalex_'
        - Preserve source metadata for conflict detection
        - Prefer OpenAlex values for bibliographic metadata

    """
    if openalex_df.empty:
        return base_df.copy()

    # Ensure doi_clean exists in openalex_df
    if "doi_clean" not in openalex_df.columns:
        if "openalex_doi" in openalex_df.columns:
            openalex_df["doi_clean"] = openalex_df["openalex_doi"]
        elif "openalex_id" in openalex_df.columns:
            # Fallback to OpenAlex ID if DOI missing
            openalex_df["doi_clean"] = openalex_df["openalex_id"]
        else:
            logger.warning("openalex_no_doi_column", columns=list(openalex_df.columns))
            return base_df.copy()

    # Prefix OpenAlex columns (except doi_clean which is used for joining)
    openalex_prefixed = openalex_df.copy()
    columns_to_prefix = [col for col in openalex_prefixed.columns if col not in ("doi_clean", "openalex_id")]
    rename_map = {col: f"openalex_{col}" for col in columns_to_prefix}
    openalex_prefixed = openalex_prefixed.rename(columns=rename_map)

    # Ensure openalex_doi exists for joining
    if "openalex_doi_clean" in openalex_prefixed.columns:
        openalex_prefixed["openalex_doi"] = openalex_prefixed["openalex_doi_clean"]
    elif "doi_clean" in openalex_prefixed.columns:
        openalex_prefixed["openalex_doi"] = openalex_prefixed["doi_clean"]

    # Primary join on DOI
    merged_df = base_df.copy()

    if base_doi_column in base_df.columns:
        merged_df = base_df.merge(
            openalex_prefixed,
            left_on=base_doi_column,
            right_on="openalex_doi",
            how="left",
            suffixes=("", "_openalex"),
        )
    elif base_pmid_column and base_pmid_column in base_df.columns:
        # Fallback join on PMID if DOI missing
        if "openalex_pmid" in openalex_prefixed.columns:
            # Normalize PMID to integer
            base_df_normalized = base_df.copy()
            base_df_normalized[base_pmid_column] = pd.to_numeric(
                base_df_normalized[base_pmid_column],
                errors="coerce",
            ).astype("Int64")

            openalex_prefixed["openalex_pmid"] = pd.to_numeric(
                openalex_prefixed["openalex_pmid"],
                errors="coerce",
            ).astype("Int64")

            merged_df = base_df_normalized.merge(
                openalex_prefixed,
                left_on=base_pmid_column,
                right_on="openalex_pmid",
                how="left",
                suffixes=("", "_openalex"),
            )
        else:
            logger.warning("openalex_no_pmid_for_fallback")
            return base_df.copy()
    else:
        logger.warning("openalex_no_join_keys", base_columns=list(base_df.columns))
        return base_df.copy()

    # Detect conflicts if requested
    if conflict_detection:
        merged_df = _detect_openalex_conflicts(merged_df, base_doi_column, base_pmid_column)

    return merged_df

def _detect_openalex_conflicts(
    merged_df: pd.DataFrame,
    base_doi_column: str | None,
    base_pmid_column: str | None,
) -> pd.DataFrame:
    """Detect DOI/PMID conflicts between base and OpenAlex."""
    if "conflict_openalex_doi" in merged_df.columns:
        return merged_df

    merged_df["conflict_openalex_doi"] = False

    if base_doi_column and base_doi_column in merged_df.columns and "openalex_doi" in merged_df.columns:
        base_mask = merged_df[base_doi_column].notna()
        openalex_mask = merged_df["openalex_doi"].notna()
        both_present = base_mask & openalex_mask

        if both_present.any():
            base_values = merged_df.loc[both_present, base_doi_column].astype(str).str.strip()
            openalex_values = merged_df.loc[both_present, "openalex_doi"].astype(str).str.strip()
            conflicts = base_values != openalex_values
            merged_df.loc[both_present, "conflict_openalex_doi"] = conflicts

    return merged_df
```

**Интеграция с document/merge/policy.py:**
Обновить `src/bioetl/sources/document/merge/policy.py` для использования OpenAlex merge аналогично Crossref.

### 3. Schema (`src/bioetl/sources/openalex/schema/`)

**Файл:** `src/bioetl/sources/openalex/schema/__init__.py`

```python
"""Pandera schemas for OpenAlex data validation."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = [
    "OpenAlexRawSchema",
    "OpenAlexNormalizedSchema",
]

class OpenAlexRawSchema(BaseSchema):
    """Schema for raw OpenAlex API response data.

    Validates structure of OpenAlex 'results' items before normalization.
    """

    id: Series[str] = pa.Field(nullable=False)  # OpenAlex ID (URL)
    title: Series[str] = pa.Field(nullable=True)
    doi: Series[str] = pa.Field(nullable=True)
    authorships: Series[object] = pa.Field(nullable=True)  # List of author dicts
    primary_location: Series[object] = pa.Field(nullable=True)  # Venue/location dict
    publication_date: Series[str] = pa.Field(nullable=True)
    publication_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    type: Series[str] = pa.Field(nullable=True)
    language: Series[str] = pa.Field(nullable=True)
    open_access: Series[object] = pa.Field(nullable=True)  # OA status dict
    concepts: Series[object] = pa.Field(nullable=True)  # List of concept dicts
    ids: Series[object] = pa.Field(nullable=True)  # External IDs dict
    abstract: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

class OpenAlexNormalizedSchema(BaseSchema):
    """Schema for normalized OpenAlex enrichment data.

    Validates output of OpenAlexAdapter.normalize_record() and
    integration with document pipeline.
    """

    # Business keys
    doi_clean: Series[str] = pa.Field(nullable=True)
    openalex_doi: Series[str] = pa.Field(nullable=True)
    openalex_id: Series[str] = pa.Field(nullable=True)
    openalex_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Bibliographic metadata
    openalex_title: Series[str] = pa.Field(nullable=True)
    openalex_journal: Series[str] = pa.Field(nullable=True)
    openalex_authors: Series[str] = pa.Field(nullable=True)  # Semicolon-separated
    openalex_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    openalex_month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    openalex_day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)

    # Publication details
    openalex_publication_date: Series[str] = pa.Field(nullable=True)
    openalex_type: Series[str] = pa.Field(nullable=True)
    openalex_language: Series[str] = pa.Field(nullable=True)

    # Open Access
    openalex_is_oa: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    openalex_oa_status: Series[str] = pa.Field(nullable=True)
    openalex_oa_url: Series[str] = pa.Field(nullable=True)

    # Identifiers
    openalex_issn: Series[str] = pa.Field(nullable=True)

    # Concepts (top 3)
    openalex_concepts_top3: Series[object] = pa.Field(nullable=True)  # List of concept names
    openalex_landing_page: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "doi_clean",
        "openalex_doi",
        "openalex_id",
        "openalex_pmid",
        "openalex_title",
        "openalex_journal",
        "openalex_authors",
        "openalex_year",
        "openalex_month",
        "openalex_day",
        "openalex_publication_date",
        "openalex_type",
        "openalex_language",
        "openalex_is_oa",
        "openalex_oa_status",
        "openalex_oa_url",
        "openalex_issn",
        "openalex_concepts_top3",
        "openalex_landing_page",
    ]
```

## Тесты

### Pagination тесты

**Файл:** `tests/sources/openalex/test_pagination.py`

```python
"""Tests for OpenAlex pagination."""

import pytest
from unittest.mock import MagicMock

from bioetl.sources.openalex.pagination import CursorPaginator
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

def test_cursor_paginator_fetch_all():
    """Test cursor pagination with multiple pages."""
    client = MagicMock(spec=UnifiedAPIClient)

    # First page response
    client.request_json.side_effect = [
        {
            "results": [{"id": f"https://openalex.org/W{i}"} for i in range(100)],
            "meta": {"next_cursor": "abc123"},
        },
        # Second page response
        {
            "results": [{"id": f"https://openalex.org/W{i}"} for i in range(100, 150)],
            "meta": {"next_cursor": None},  # Last page
        },
    ]

    paginator = CursorPaginator(client, page_size=100)

    works = paginator.fetch_all(
        "/works",
        params={"filter": "has_doi:true"},
    )

    assert len(works) == 150
    assert all("id" in work for work in works)
    assert client.request_json.call_count == 2
```

### Merge тесты

**Файл:** `tests/sources/openalex/test_merge.py`

```python
"""Tests for OpenAlex merge policy."""

import pandas as pd

from bioetl.sources.openalex.merge import merge_openalex_with_base

def test_merge_openalex_with_base():
    """Test merging OpenAlex data with base dataframe."""
    base_df = pd.DataFrame({
        "chembl_doi": ["10.1000/test.1", "10.1000/test.2"],
        "chembl_title": ["Title 1", "Title 2"],
    })

    openalex_df = pd.DataFrame({
        "doi_clean": ["10.1000/test.1"],
        "openalex_title": ["OpenAlex Title 1"],
        "openalex_journal": ["Journal 1"],
        "openalex_is_oa": [True],
    })

    merged = merge_openalex_with_base(
        base_df,
        openalex_df,
        base_doi_column="chembl_doi",
    )

    assert "openalex_title" in merged.columns
    assert merged.loc[0, "openalex_title"] == "OpenAlex Title 1"
    assert pd.isna(merged.loc[1, "openalex_title"])
```

### Schema тесты

**Файл:** `tests/sources/openalex/test_schema.py`

```python
"""Tests for OpenAlex schemas."""

import pandas as pd
import pytest

from bioetl.sources.openalex.schema import (
    OpenAlexRawSchema,
    OpenAlexNormalizedSchema,
)

def test_openalex_raw_schema():
    """Test validation of raw OpenAlex data."""
    df = pd.DataFrame({
        "id": ["https://openalex.org/W1"],
        "title": ["Test Title"],
        "doi": ["https://doi.org/10.1000/test.1"],
    })

    schema = OpenAlexRawSchema()
    validated = schema.validate(df)
    assert len(validated) == 1

def test_openalex_normalized_schema():
    """Test validation of normalized OpenAlex data."""
    df = pd.DataFrame({
        "doi_clean": ["10.1000/test.1"],
        "openalex_doi": ["10.1000/test.1"],
        "openalex_title": ["Test Title"],
        "openalex_is_oa": [True],
    })

    schema = OpenAlexNormalizedSchema()
    validated = schema.validate(df)
    assert len(validated) == 1
```

## Критерии завершения

- ✅ Создан `src/bioetl/sources/openalex/pagination/` с `CursorPaginator`
- ✅ Создан `src/bioetl/sources/openalex/merge/` с merge политикой
- ✅ Создан `src/bioetl/sources/openalex/schema/` с Pandera схемами
- ✅ Все модули имеют `__all__` и docstrings
- ✅ Добавлены unit тесты для каждого модуля
- ✅ Интеграция с `document/merge/policy.py` обновлена
- ✅ Документация обновлена (если требуется)

## Примечания

- OpenAlex API использует cursor-based pagination через параметр `cursor`
- Merge политика: primary join на `doi_clean`, fallback на OpenAlex ID или PMID
- Схемы валидируют структуру до и после нормализации
- OpenAlex-specific поля: concepts_top3, open_access (is_oa, oa_status, oa_url)
- Рекомендуется использовать email в User-Agent для "polite pool" доступа
