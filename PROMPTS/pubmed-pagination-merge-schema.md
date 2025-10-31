# Промпт: Создать pagination/, merge/, schema/ для PubMed

## Контекст задачи

**Проблема:** PubMed источник имеет только базовую структуру без опциональных модулей согласно MODULE_RULES.md.

**Статус:** ⚠️ **ЧАСТИЧНО РЕАЛИЗОВАНО** (базовая структура есть, опциональные модули отсутствуют)

**Ссылки:**
- `refactoring/MODULE_RULES.md` (строки 24-28): Опциональные подпапки
- `refactoring/AUDIT_REPORT_2025.md` (строка 1076): "PubMed: ⚠️ Нет pagination/, merge/, schema/"
- `refactoring/DATA_SOURCES.md` (строки 178-197): Требования к PubMed источнику

## Текущая структура

```
src/bioetl/sources/pubmed/
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

## Требования из документации

### PubMed E-utilities специфика

**Источник:** `docs/requirements/09-document-chembl-extraction.md` (строки 178-1102)

**Pagination:**
- PubMed использует двухэтапный процесс: `esearch` → `efetch`
- `esearch` возвращает WebEnv и QueryKey для batch получения
- Нет традиционной пагинации; используется WebEnv/QueryKey для больших наборов
- Batch size ограничен (до 10,000 IDs за раз для efetch)

**Merge Policy:**
- Join по `pmid` (PubMed ID)
- Приоритет PubMed для библиографических метаданных (title, abstract, journal)
- Fallback на DOI для случаев когда PMID отсутствует

**Schema:**
- Документы PubMed включают: PMID, title, abstract, authors, journal, year, volume, issue, pages, MeSH terms, chemicals
- Нормализация: PMID (integer), DOI, ISSN, даты публикации, MeSH descriptors

## Реализация

### 1. Pagination (`src/bioetl/sources/pubmed/pagination/`)

**Файл:** `src/bioetl/sources/pubmed/pagination/__init__.py`

```python
"""Pagination helpers for PubMed E-utilities API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["WebEnvPaginator"]


@dataclass(slots=True)
class WebEnvPaginator:
    """Paginator for PubMed E-utilities using WebEnv/QueryKey pattern.
    
    PubMed uses a two-step process:
    1. esearch: Get WebEnv and QueryKey for a search query
    2. efetch: Fetch records using WebEnv/QueryKey in batches
    
    This paginator handles the WebEnv/QueryKey lifecycle.
    """

    client: UnifiedAPIClient
    batch_size: int = 200  # PubMed recommended batch size

    def fetch_all(
        self,
        search_params: Mapping[str, Any],
        *,
        fetch_params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all records for a PubMed search using WebEnv/QueryKey.
        
        Args:
            search_params: Parameters for esearch (query, db='pubmed', retmax, etc.)
            fetch_params: Parameters for efetch (retmode, rettype, etc.)
        
        Returns:
            List of PubMed records
        
        Process:
            1. Call esearch to get WebEnv and QueryKey
            2. Call efetch in batches using WebEnv/QueryKey
            3. Parse and return all records
        """
        from bioetl.sources.pubmed.parser import parse_esearch_response, parse_efetch_response
        
        # Step 1: esearch to get WebEnv and QueryKey
        esearch_path = "/esearch.fcgi"
        esearch_params = dict(search_params)
        esearch_params.setdefault("db", "pubmed")
        esearch_params.setdefault("retmax", 10000)  # Max records per query
        esearch_params.setdefault("usehistory", "y")  # Enable WebEnv
        
        esearch_response = self.client.request_json(esearch_path, params=esearch_params)
        web_env, query_key, total_count = parse_esearch_response(esearch_response)
        
        if not web_env or not query_key:
            return []
        
        # Step 2: efetch in batches
        all_records = []
        fetch_defaults = dict(fetch_params or {})
        fetch_defaults.setdefault("db", "pubmed")
        fetch_defaults.setdefault("retmode", "xml")
        fetch_defaults.setdefault("rettype", "abstract")
        
        offset = 0
        while offset < total_count:
            fetch_params_batch = dict(fetch_defaults)
            fetch_params_batch["WebEnv"] = web_env
            fetch_params_batch["query_key"] = query_key
            fetch_params_batch["retstart"] = offset
            fetch_params_batch["retmax"] = min(self.batch_size, total_count - offset)
            
            efetch_path = "/efetch.fcgi"
            efetch_response = self.client.request_text(efetch_path, params=fetch_params_batch)
            
            # Parse XML response
            records = parse_efetch_response(efetch_response)
            if not records:
                break
            
            all_records.extend(records)
            offset += len(records)
            
            if len(records) < self.batch_size:
                break
        
        return all_records
```

**Зависимость:** Требуется helper в `parser/` для парсинга esearch и efetch ответов.

### 2. Merge (`src/bioetl/sources/pubmed/merge/`)

**Файл:** `src/bioetl/sources/pubmed/merge/__init__.py`

```python
"""Merge policy for PubMed enrichment data."""

from __future__ import annotations

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["merge_pubmed_with_base", "PUBMED_MERGE_KEYS"]


logger = UnifiedLogger.get(__name__)


PUBMED_MERGE_KEYS = {
    "primary": "pmid",  # PubMed ID (integer)
    "fallback": ["pubmed_pmid", "doi_clean"],  # Fallback keys
}


def merge_pubmed_with_base(
    base_df: pd.DataFrame,
    pubmed_df: pd.DataFrame,
    *,
    base_pmid_column: str = "chembl_pmid",
    base_doi_column: str | None = "chembl_doi",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge PubMed enrichment data with base document dataframe.
    
    Args:
        base_df: Base dataframe (e.g., ChEMBL documents)
        pubmed_df: PubMed enrichment dataframe
        base_pmid_column: Column name in base_df with PMID for joining
        base_doi_column: Optional column name in base_df with DOI for fallback join
        conflict_detection: Whether to detect PMID/DOI conflicts
    
    Returns:
        Merged dataframe with PubMed data prefixed as 'pubmed_*'
    
    Strategy:
        - Primary join on PMID (integer)
        - Fallback join on DOI if PMID missing
        - Prefix all PubMed columns with 'pubmed_'
        - Preserve source metadata for conflict detection
    """
    if pubmed_df.empty:
        return base_df.copy()

    # Ensure pmid exists in pubmed_df
    if "pmid" not in pubmed_df.columns:
        if "pubmed_pmid" in pubmed_df.columns:
            pubmed_df["pmid"] = pd.to_numeric(pubmed_df["pubmed_pmid"], errors="coerce")
        else:
            logger.warning("pubmed_no_pmid_column", columns=list(pubmed_df.columns))
            return base_df.copy()

    # Normalize pmid to integer
    pubmed_df["pmid"] = pd.to_numeric(pubmed_df["pmid"], errors="coerce").astype("Int64")

    # Prefix PubMed columns (except pmid which is used for joining)
    pubmed_prefixed = pubmed_df.copy()
    columns_to_prefix = [col for col in pubmed_prefixed.columns if col not in ("pmid", "doi_clean")]
    rename_map = {col: f"pubmed_{col}" for col in columns_to_prefix}
    pubmed_prefixed = pubmed_prefixed.rename(columns=rename_map)

    # Ensure pubmed_pmid exists for joining
    if "pubmed_pmid" not in pubmed_prefixed.columns:
        pubmed_prefixed["pubmed_pmid"] = pubmed_prefixed["pmid"]

    # Primary join on PMID
    merged_df = base_df.copy()
    
    if base_pmid_column in base_df.columns:
        # Normalize base PMID to integer
        base_df_normalized = base_df.copy()
        base_df_normalized[base_pmid_column] = pd.to_numeric(
            base_df_normalized[base_pmid_column],
            errors="coerce",
        ).astype("Int64")
        
        merged_df = base_df_normalized.merge(
            pubmed_prefixed,
            left_on=base_pmid_column,
            right_on="pmid",
            how="left",
            suffixes=("", "_pubmed"),
        )
    elif base_doi_column and base_doi_column in base_df.columns:
        # Fallback join on DOI
        if "doi_clean" in pubmed_prefixed.columns:
            merged_df = base_df.merge(
                pubmed_prefixed,
                left_on=base_doi_column,
                right_on="doi_clean",
                how="left",
                suffixes=("", "_pubmed"),
            )
        else:
            logger.warning("pubmed_no_doi_for_fallback")
            return base_df.copy()
    else:
        logger.warning("pubmed_no_join_keys", base_columns=list(base_df.columns))
        return base_df.copy()

    # Detect conflicts if requested
    if conflict_detection:
        merged_df = _detect_pubmed_conflicts(merged_df, base_pmid_column, base_doi_column)

    return merged_df


def _detect_pubmed_conflicts(
    merged_df: pd.DataFrame,
    base_pmid_column: str | None,
    base_doi_column: str | None,
) -> pd.DataFrame:
    """Detect PMID/DOI conflicts between base and PubMed."""
    if "conflict_pubmed_pmid" in merged_df.columns:
        return merged_df

    merged_df["conflict_pubmed_pmid"] = False

    if base_pmid_column and base_pmid_column in merged_df.columns and "pubmed_pmid" in merged_df.columns:
        base_mask = merged_df[base_pmid_column].notna()
        pubmed_mask = merged_df["pubmed_pmid"].notna()
        both_present = base_mask & pubmed_mask

        if both_present.any():
            base_values = merged_df.loc[both_present, base_pmid_column].astype("Int64")
            pubmed_values = merged_df.loc[both_present, "pubmed_pmid"].astype("Int64")
            conflicts = base_values != pubmed_values
            merged_df.loc[both_present, "conflict_pubmed_pmid"] = conflicts

    return merged_df
```

### 3. Schema (`src/bioetl/sources/pubmed/schema/`)

**Файл:** `src/bioetl/sources/pubmed/schema/__init__.py`

```python
"""Pandera schemas for PubMed data validation."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = [
    "PubMedRawSchema",
    "PubMedNormalizedSchema",
]


class PubMedRawSchema(BaseSchema):
    """Schema for raw PubMed XML parsed data.
    
    Validates structure of parsed PubMed article elements.
    """

    pmid: Series[pd.Int64Dtype] = pa.Field(nullable=False, ge=1)
    title: Series[str] = pa.Field(nullable=True)
    abstract: Series[str] = pa.Field(nullable=True)
    journal: Series[str] = pa.Field(nullable=True)
    journal_abbrev: Series[str] = pa.Field(nullable=True)
    volume: Series[str] = pa.Field(nullable=True)
    issue: Series[str] = pa.Field(nullable=True)
    first_page: Series[str] = pa.Field(nullable=True)
    last_page: Series[str] = pa.Field(nullable=True)
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)
    issn: Series[object] = pa.Field(nullable=True)  # List of ISSN strings
    doi: Series[str] = pa.Field(nullable=True)
    authors: Series[object] = pa.Field(nullable=True)  # List of author dicts
    mesh_terms: Series[object] = pa.Field(nullable=True)  # List of MeSH descriptors
    chemicals: Series[object] = pa.Field(nullable=True)  # List of chemical substances

    class Config:
        strict = True
        coerce = True
        ordered = False


class PubMedNormalizedSchema(BaseSchema):
    """Schema for normalized PubMed enrichment data."""

    # Business keys
    pmid: Series[pd.Int64Dtype] = pa.Field(nullable=False, ge=1)
    pubmed_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)
    doi_clean: Series[str] = pa.Field(nullable=True)

    # Bibliographic metadata
    pubmed_article_title: Series[str] = pa.Field(nullable=True)
    pubmed_abstract: Series[str] = pa.Field(nullable=True)
    pubmed_journal: Series[str] = pa.Field(nullable=True)
    pubmed_journal_abbrev: Series[str] = pa.Field(nullable=True)
    pubmed_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    pubmed_month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    pubmed_day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)

    # Publication details
    pubmed_volume: Series[str] = pa.Field(nullable=True)
    pubmed_issue: Series[str] = pa.Field(nullable=True)
    pubmed_first_page: Series[str] = pa.Field(nullable=True)
    pubmed_last_page: Series[str] = pa.Field(nullable=True)

    # Identifiers
    pubmed_doi: Series[str] = pa.Field(nullable=True)
    pubmed_issn_print: Series[str] = pa.Field(nullable=True)
    pubmed_issn_electronic: Series[str] = pa.Field(nullable=True)

    # Authors and metadata
    pubmed_authors: Series[str] = pa.Field(nullable=True)  # Semicolon-separated
    pubmed_mesh_descriptors: Series[str] = pa.Field(nullable=True)  # Semicolon-separated
    pubmed_chemical_list: Series[str] = pa.Field(nullable=True)  # Semicolon-separated

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "pmid",
        "pubmed_pmid",
        "doi_clean",
        "pubmed_article_title",
        "pubmed_abstract",
        "pubmed_journal",
        "pubmed_journal_abbrev",
        "pubmed_year",
        "pubmed_month",
        "pubmed_day",
        "pubmed_volume",
        "pubmed_issue",
        "pubmed_first_page",
        "pubmed_last_page",
        "pubmed_doi",
        "pubmed_issn_print",
        "pubmed_issn_electronic",
        "pubmed_authors",
        "pubmed_mesh_descriptors",
        "pubmed_chemical_list",
    ]
```

## Тесты

Создать тесты аналогично Crossref:
- `tests/sources/pubmed/test_pagination.py`
- `tests/sources/pubmed/test_merge.py`
- `tests/sources/pubmed/test_schema.py`

## Критерии завершения

- ✅ Создан `src/bioetl/sources/pubmed/pagination/` с `WebEnvPaginator`
- ✅ Создан `src/bioetl/sources/pubmed/merge/` с merge политикой
- ✅ Создан `src/bioetl/sources/pubmed/schema/` с Pandera схемами
- ✅ Все модули имеют `__all__` и docstrings
- ✅ Добавлены unit тесты для каждого модуля
- ✅ Интеграция с `document/merge/policy.py` обновлена

## Примечания

- PubMed использует WebEnv/QueryKey pattern вместо традиционной пагинации
- Merge политика: primary join на PMID, fallback на DOI
- Схемы валидируют структуру до и после нормализации
- PMID должен быть integer (Int64) для корректного join

