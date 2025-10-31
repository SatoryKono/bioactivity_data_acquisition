# Промпт: Создать pagination/, merge/, schema/ для Semantic Scholar

## Контекст задачи

**Проблема:** Semantic Scholar источник имеет только базовую структуру без опциональных модулей согласно MODULE_RULES.md.

**Статус:** ⚠️ **ЧАСТИЧНО РЕАЛИЗОВАНО** (базовая структура есть, опциональные модули отсутствуют)

**Ссылки:**
- `refactoring/MODULE_RULES.md` (строки 24-28): Опциональные подпапки (SHOULD): `schema/`, `merge/`, `pagination/`
- `refactoring/AUDIT_REPORT_2025.md` (строка 1077): "Semantic Scholar: ✅ Базовая структура, ⚠️ Нет pagination/, merge/, schema/"
- `refactoring/DATA_SOURCES.md` (строки 218-237): Требования к Semantic Scholar источнику

## Текущая структура

```
src/bioetl/sources/semantic_scholar/
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
- `pagination/` - опциональная стратегия для search endpoint
- `merge/` - политика объединения с document pipeline
- `schema/` - Pandera схемы для валидации Semantic Scholar данных

## Требования из документации

### Semantic Scholar API специфика

**Источник:** `src/bioetl/adapters/semantic_scholar.py`

**Pagination:**
- Semantic Scholar API не имеет традиционной пагинации для paper endpoint
- Использует ID-based поиск (DOI, PMID, ArXiv ID)
- Search endpoint поддерживает limit/offset (опционально)
- Rate limit: низкий без API key (1 req/1.25s), выше с API key

**Merge Policy:**
- Join по `doi_clean` и `pmid` (pubmed_id) с fallback на title matching
- Приоритет Semantic Scholar для citation metrics и fields of study
- Используется в document pipeline для обогащения ChEMBL документов

**Schema:**
- Документы Semantic Scholar включают: paperId, externalIds (DOI, PMID), title, abstract, venue, year, citation_count, influential_citations, fields_of_study, isOpenAccess
- Нормализация: DOI, PMID, paperId, citation metrics, publication types

## Реализация

### 1. Pagination (`src/bioetl/sources/semantic_scholar/pagination/`)

**Файл:** `src/bioetl/sources/semantic_scholar/pagination/__init__.py`

```python
"""Pagination helpers for the Semantic Scholar API.

Note:
    Semantic Scholar API primarily uses ID-based retrieval.
    Pagination is only available for the search endpoint.
    This module provides optional pagination support for search queries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["OffsetPaginator"]


@dataclass(slots=True)
class OffsetPaginator:
    """Offset-based paginator for Semantic Scholar search endpoint.
    
    Note:
        Semantic Scholar search endpoint supports limit/offset pagination.
        Maximum limit is typically 1000 per request.
        This paginator is optional as most use cases use ID-based retrieval.
    """

    client: UnifiedAPIClient
    page_size: int = 100  # Max typically 1000

    def fetch_all(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages for Semantic Scholar search endpoint.
        
        Args:
            path: API endpoint path (e.g., '/paper/search')
            params: Base query parameters (query, fields, etc.)
            max_items: Maximum number of items to fetch (None = all)
        
        Returns:
            List of items from search results
        
        Note:
            Semantic Scholar search uses offset/limit pagination.
            Response contains 'data' array with items.
            Stops when 'data' is empty or max_items reached.
        """
        collected: list[dict[str, Any]] = []
        offset = 0

        while True:
            query: dict[str, Any] = dict(params or {})
            query["limit"] = self.page_size
            query["offset"] = offset

            payload = self.client.request_json(path, params=query)
            
            if not isinstance(payload, dict):
                break

            items = payload.get("data", [])
            
            if not items:
                break

            collected.extend([dict(item) for item in items if isinstance(item, dict)])

            # Check if we've reached max_items
            if max_items and len(collected) >= max_items:
                collected = collected[:max_items]
                break

            # Check if there are more items
            if len(items) < self.page_size:
                break

            offset += self.page_size

        return collected
```

**Примечание:** Pagination модуль опционален, так как Semantic Scholar в основном использует ID-based retrieval. Может быть полезен для search endpoint.

### 2. Merge (`src/bioetl/sources/semantic_scholar/merge/`)

**Файл:** `src/bioetl/sources/semantic_scholar/merge/__init__.py`

```python
"""Merge policy for Semantic Scholar enrichment data."""

from __future__ import annotations

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["merge_semantic_scholar_with_base", "SEMANTIC_SCHOLAR_MERGE_KEYS"]


logger = UnifiedLogger.get(__name__)


SEMANTIC_SCHOLAR_MERGE_KEYS = {
    "primary": "doi_clean",  # Normalized DOI
    "fallback": ["semantic_scholar_doi", "semantic_scholar_pmid", "semantic_scholar_paper_id"],
}


def merge_semantic_scholar_with_base(
    base_df: pd.DataFrame,
    semantic_scholar_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    base_pmid_column: str | None = "chembl_pmid",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge Semantic Scholar enrichment data with base document dataframe.
    
    Args:
        base_df: Base dataframe (e.g., ChEMBL documents)
        semantic_scholar_df: Semantic Scholar enrichment dataframe
        base_doi_column: Column name in base_df with DOI for joining
        base_pmid_column: Optional column name in base_df with PMID for joining
        conflict_detection: Whether to detect DOI/PMID conflicts
    
    Returns:
        Merged dataframe with Semantic Scholar data prefixed as 'semantic_scholar_*'
    
    Strategy:
        - Primary join on normalized DOI (doi_clean)
        - Fallback join on PMID (pubmed_id) if DOI missing
        - Fallback join on paper ID if both missing (rare)
        - Prefix all Semantic Scholar columns with 'semantic_scholar_'
        - Preserve source metadata for conflict detection
        - Prefer Semantic Scholar values for citation metrics
    """
    if semantic_scholar_df.empty:
        return base_df.copy()

    # Ensure doi_clean exists in semantic_scholar_df
    if "doi_clean" not in semantic_scholar_df.columns:
        if "semantic_scholar_doi" in semantic_scholar_df.columns:
            semantic_scholar_df["doi_clean"] = semantic_scholar_df["semantic_scholar_doi"]
        elif "semantic_scholar_paper_id" in semantic_scholar_df.columns:
            # Fallback to paper ID if DOI missing (not recommended)
            logger.warning("semantic_scholar_using_paper_id_fallback")
            semantic_scholar_df["doi_clean"] = semantic_scholar_df["semantic_scholar_paper_id"]
        else:
            logger.warning("semantic_scholar_no_doi_column", columns=list(semantic_scholar_df.columns))
            return base_df.copy()

    # Prefix Semantic Scholar columns (except doi_clean which is used for joining)
    ss_prefixed = semantic_scholar_df.copy()
    columns_to_prefix = [col for col in ss_prefixed.columns if col not in ("doi_clean", "paper_id")]
    rename_map = {col: f"semantic_scholar_{col}" for col in columns_to_prefix}
    ss_prefixed = ss_prefixed.rename(columns=rename_map)

    # Ensure semantic_scholar_doi exists for joining
    if "semantic_scholar_doi_clean" in ss_prefixed.columns:
        ss_prefixed["semantic_scholar_doi"] = ss_prefixed["semantic_scholar_doi_clean"]
    elif "doi_clean" in ss_prefixed.columns:
        ss_prefixed["semantic_scholar_doi"] = ss_prefixed["doi_clean"]

    # Primary join on DOI
    merged_df = base_df.copy()
    
    if base_doi_column in base_df.columns:
        merged_df = base_df.merge(
            ss_prefixed,
            left_on=base_doi_column,
            right_on="semantic_scholar_doi",
            how="left",
            suffixes=("", "_ss"),
        )
    elif base_pmid_column and base_pmid_column in base_df.columns:
        # Fallback join on PMID if DOI missing
        if "semantic_scholar_pubmed_id" in ss_prefixed.columns:
            # Normalize PMID to integer
            base_df_normalized = base_df.copy()
            base_df_normalized[base_pmid_column] = pd.to_numeric(
                base_df_normalized[base_pmid_column],
                errors="coerce",
            ).astype("Int64")
            
            ss_prefixed["semantic_scholar_pmid"] = pd.to_numeric(
                ss_prefixed["semantic_scholar_pubmed_id"],
                errors="coerce",
            ).astype("Int64")
            
            merged_df = base_df_normalized.merge(
                ss_prefixed,
                left_on=base_pmid_column,
                right_on="semantic_scholar_pmid",
                how="left",
                suffixes=("", "_ss"),
            )
        else:
            logger.warning("semantic_scholar_no_pmid_for_fallback")
            return base_df.copy()
    else:
        logger.warning("semantic_scholar_no_join_keys", base_columns=list(base_df.columns))
        return base_df.copy()

    # Detect conflicts if requested
    if conflict_detection:
        merged_df = _detect_semantic_scholar_conflicts(merged_df, base_doi_column, base_pmid_column)

    return merged_df


def _detect_semantic_scholar_conflicts(
    merged_df: pd.DataFrame,
    base_doi_column: str | None,
    base_pmid_column: str | None,
) -> pd.DataFrame:
    """Detect DOI/PMID conflicts between base and Semantic Scholar."""
    if "conflict_semantic_scholar_doi" in merged_df.columns:
        return merged_df

    merged_df["conflict_semantic_scholar_doi"] = False

    if base_doi_column and base_doi_column in merged_df.columns and "semantic_scholar_doi" in merged_df.columns:
        base_mask = merged_df[base_doi_column].notna()
        ss_mask = merged_df["semantic_scholar_doi"].notna()
        both_present = base_mask & ss_mask

        if both_present.any():
            base_values = merged_df.loc[both_present, base_doi_column].astype(str).str.strip()
            ss_values = merged_df.loc[both_present, "semantic_scholar_doi"].astype(str).str.strip()
            conflicts = base_values != ss_values
            merged_df.loc[both_present, "conflict_semantic_scholar_doi"] = conflicts

    return merged_df
```

### 3. Schema (`src/bioetl/sources/semantic_scholar/schema/`)

**Файл:** `src/bioetl/sources/semantic_scholar/schema/__init__.py`

```python
"""Pandera schemas for Semantic Scholar data validation."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = [
    "SemanticScholarRawSchema",
    "SemanticScholarNormalizedSchema",
]


class SemanticScholarRawSchema(BaseSchema):
    """Schema for raw Semantic Scholar API response data.
    
    Validates structure of Semantic Scholar paper objects before normalization.
    """

    paperId: Series[str] = pa.Field(nullable=False)
    title: Series[str] = pa.Field(nullable=True)
    abstract: Series[str] = pa.Field(nullable=True)
    venue: Series[str] = pa.Field(nullable=True)  # Journal name
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    publicationDate: Series[str] = pa.Field(nullable=True)
    externalIds: Series[object] = pa.Field(nullable=True)  # Dict with DOI, PMID, etc.
    authors: Series[object] = pa.Field(nullable=True)  # List of author dicts
    citationCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    influentialCitationCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    referenceCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    isOpenAccess: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    publicationTypes: Series[object] = pa.Field(nullable=True)  # List of publication types
    fieldsOfStudy: Series[object] = pa.Field(nullable=True)  # List of field names

    class Config:
        strict = True
        coerce = True
        ordered = False


class SemanticScholarNormalizedSchema(BaseSchema):
    """Schema for normalized Semantic Scholar enrichment data.
    
    Validates output of SemanticScholarAdapter.normalize_record() and
    integration with document pipeline.
    """

    # Business keys
    doi_clean: Series[str] = pa.Field(nullable=True)
    semantic_scholar_doi: Series[str] = pa.Field(nullable=True)
    semantic_scholar_paper_id: Series[str] = pa.Field(nullable=True)
    semantic_scholar_pubmed_id: Series[str] = pa.Field(nullable=True)
    semantic_scholar_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Bibliographic metadata
    semantic_scholar_title: Series[str] = pa.Field(nullable=True)
    semantic_scholar_abstract: Series[str] = pa.Field(nullable=True)
    semantic_scholar_journal: Series[str] = pa.Field(nullable=True)
    semantic_scholar_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    semantic_scholar_publication_date: Series[str] = pa.Field(nullable=True)

    # Citation metrics (unique to Semantic Scholar)
    semantic_scholar_citation_count: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    semantic_scholar_influential_citations: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    semantic_scholar_reference_count: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Authors and metadata
    semantic_scholar_authors: Series[str] = pa.Field(nullable=True)  # Semicolon-separated

    # Open Access
    semantic_scholar_is_oa: Series[pd.BooleanDtype] = pa.Field(nullable=True)

    # Publication metadata
    semantic_scholar_publication_types: Series[object] = pa.Field(nullable=True)  # List
    semantic_scholar_doc_type: Series[str] = pa.Field(nullable=True)  # Semicolon-separated
    semantic_scholar_fields_of_study: Series[object] = pa.Field(nullable=True)  # List

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "doi_clean",
        "semantic_scholar_doi",
        "semantic_scholar_paper_id",
        "semantic_scholar_pubmed_id",
        "semantic_scholar_pmid",
        "semantic_scholar_title",
        "semantic_scholar_abstract",
        "semantic_scholar_journal",
        "semantic_scholar_year",
        "semantic_scholar_publication_date",
        "semantic_scholar_citation_count",
        "semantic_scholar_influential_citations",
        "semantic_scholar_reference_count",
        "semantic_scholar_authors",
        "semantic_scholar_is_oa",
        "semantic_scholar_publication_types",
        "semantic_scholar_doc_type",
        "semantic_scholar_fields_of_study",
    ]
```

## Тесты

Создать тесты аналогично Crossref/PubMed:
- `tests/sources/semantic_scholar/test_pagination.py`
- `tests/sources/semantic_scholar/test_merge.py`
- `tests/sources/semantic_scholar/test_schema.py`

## Критерии завершения

- ✅ Создан `src/bioetl/sources/semantic_scholar/pagination/` с `OffsetPaginator` (опционально)
- ✅ Создан `src/bioetl/sources/semantic_scholar/merge/` с merge политикой
- ✅ Создан `src/bioetl/sources/semantic_scholar/schema/` с Pandera схемами
- ✅ Все модули имеют `__all__` и docstrings
- ✅ Добавлены unit тесты для каждого модуля
- ✅ Интеграция с `document/merge/policy.py` обновлена

## Примечания

- Semantic Scholar API в основном использует ID-based retrieval, pagination опционален
- Merge политика: primary join на `doi_clean`, fallback на PMID (pubmed_id)
- Схемы валидируют структуру до и после нормализации
- Semantic Scholar-specific поля: citation_count, influential_citations, fields_of_study
- API key рекомендуется для production использования (более высокие rate limits)

