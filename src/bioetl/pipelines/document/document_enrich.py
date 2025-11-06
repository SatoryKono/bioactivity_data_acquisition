"""Enrichment functions for Document pipeline."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from bioetl.clients import ChemblClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["enrich_with_document_terms", "aggregate_terms", "_escape_pipe"]


def _escape_pipe(s: str) -> str:
    """Escape pipe and backslash delimiters in string values.

    Parameters
    ----------
    s:
        Input string to escape.

    Returns
    -------
    str:
        String with escaped delimiters: `|` → `\|`, `\` → `\\`.
    """
    if not isinstance(s, str):
        s = str(s) if s is not None else ""
    return s.replace("\\", "\\\\").replace("|", "\\|")


def aggregate_terms(
    rows: Iterable[dict[str, Any]],
    sort: str = "weight_desc",
) -> dict[str, dict[str, str]]:
    """Aggregate document terms by document_chembl_id.

    Parameters
    ----------
    rows:
        Iterable of document_term records, each with 'document_chembl_id', 'term', 'weight'.
    sort:
        Sort order: 'weight_desc' (default) sorts by weight descending, None preserves order.

    Returns
    -------
    dict[str, dict[str, str]]:
        Dictionary keyed by document_chembl_id -> {'term': 't1|t2|...', 'weight': 'w1|w2|...'}.
        Terms and weights are serialized with "|" separator, order is synchronized.
    """
    bucket: dict[str, list[tuple[str, Any]]] = defaultdict(list)

    for r in rows:
        did = r.get("document_chembl_id")
        if not did:
            continue

        term_value = r.get("term")
        weight_value = r.get("weight")

        # Convert term to string, handle None
        term_str = str(term_value) if term_value is not None else ""

        # Keep weight as-is (can be number, string, or None)
        bucket[did].append((term_str, weight_value))

    result: dict[str, dict[str, str]] = {}

    for did, items in bucket.items():
        # Sort by weight (descending) if sort='weight_desc'
        if sort == "weight_desc":
            items.sort(
                key=lambda x: (
                    float(x[1]) if x[1] not in (None, "") and _is_numeric(x[1]) else float("-inf")
                ),
                reverse=True,
            )

        # Escape terms and join with "|"
        terms_list: list[str] = []
        weights_list: list[str] = []

        for term, weight in items:
            # Escape pipe in term
            escaped_term = _escape_pipe(term or "")
            terms_list.append(escaped_term)

            # Convert weight to string, handle None/empty
            if weight in (None, ""):
                weights_list.append("")
            else:
                weights_list.append(str(weight))

        result[did] = {
            "term": "|".join(terms_list),
            "weight": "|".join(weights_list),
        }

    return result


def _is_numeric(value: Any) -> bool:
    """Check if value can be converted to float."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def enrich_with_document_terms(
    df_docs: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Обогатить DataFrame документов полями из document_term.

    Parameters
    ----------
    df_docs:
        DataFrame с данными документов, должен содержать document_chembl_id.
    client:
        ChemblClient для запросов к ChEMBL API.
    cfg:
        Конфигурация обогащения из config.chembl.document.enrich.document_term.

    Returns
    -------
    pd.DataFrame:
        Обогащенный DataFrame с добавленными колонками:
        - term (nullable string, pipe-separated terms)
        - weight (nullable string, pipe-separated weights)
    """
    log = UnifiedLogger.get(__name__).bind(component="document_enrichment")

    if df_docs.empty:
        log.debug("enrichment_skipped_empty_dataframe")
        df_docs = df_docs.copy()
        df_docs["term"] = ""
        df_docs["weight"] = ""
        return df_docs

    # Проверка наличия необходимых колонок
    required_cols = ["document_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_docs.columns]
    if missing_cols:
        log.warning(
            "enrichment_skipped_missing_columns",
            missing_columns=missing_cols,
        )
        df_docs = df_docs.copy()
        df_docs["term"] = ""
        df_docs["weight"] = ""
        return df_docs

    # Собрать уникальные document_chembl_id, dropna
    doc_ids: list[str] = []
    for _, row in df_docs.iterrows():
        doc_id = row.get("document_chembl_id")

        # Пропускаем NaN/None значения
        if pd.isna(doc_id) or doc_id is None:
            continue

        # Преобразуем в строку
        doc_id_str = str(doc_id).strip()

        if doc_id_str:
            doc_ids.append(doc_id_str)

    if not doc_ids:
        log.debug("enrichment_skipped_no_valid_ids")
        df_docs = df_docs.copy()
        df_docs["term"] = ""
        df_docs["weight"] = ""
        return df_docs

    # Получить конфигурацию
    fields = cfg.get("select_fields", ["document_chembl_id", "term", "weight"])
    page_limit = cfg.get("page_limit", 1000)
    sort = cfg.get("sort", "weight_desc")

    # Вызвать client.fetch_document_terms_by_ids
    log.info("enrichment_fetching_terms", ids_count=len(set(doc_ids)))
    records_dict = client.fetch_document_terms_by_ids(
        ids=doc_ids,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Преобразовать dict[doc_id, list[records]] в плоский список для aggregate_terms
    all_records: list[dict[str, Any]] = []
    for doc_id, records_list in records_dict.items():
        all_records.extend(records_list)

    # Агрегировать термины
    agg_result = aggregate_terms(all_records, sort=sort)

    # Создать DataFrame для join
    enrichment_data: list[dict[str, Any]] = []
    for doc_id, term_weight in agg_result.items():
        enrichment_data.append({
            "document_chembl_id": doc_id,
            "term": term_weight["term"],
            "weight": term_weight["weight"],
        })

    if not enrichment_data:
        log.debug("enrichment_no_records_found")
        df_docs = df_docs.copy()
        df_docs["term"] = ""
        df_docs["weight"] = ""
        return df_docs

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join обратно к df_docs на document_chembl_id
    # Сохраняем исходный порядок строк через индекс
    original_index = df_docs.index.copy()
    df_result = df_docs.merge(
        df_enrich,
        on=["document_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
    for col in ["term", "weight"]:
        if col not in df_result.columns:
            df_result[col] = ""
        else:
            # Заполнить NaN/None пустыми строками
            df_result[col] = df_result[col].fillna("").astype(str)

    # Восстановить исходный порядок
    df_result = df_result.reindex(original_index)

    # Нормализовать типы
    df_result["term"] = df_result["term"].astype("string")
    df_result["weight"] = df_result["weight"].astype("string")

    log.info(
        "enrichment_completed",
        rows_enriched=df_result.shape[0],
        documents_with_terms=len(agg_result),
    )
    return df_result

