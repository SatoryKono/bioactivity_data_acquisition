#!/usr/bin/env python3
"""
Диагностический скрипт для тестирования OpenAlex API.

Проверяет доступность OpenAlex API для документов из входного файла,
сравнивает результаты запросов по DOI и PMID.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent))

from library.clients.openalex import OpenAlexClient
from library.config import APIClientConfig, RateLimitSettings, RetrySettings


def setup_logging() -> None:
    """Настройка логирования."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('openalex_diagnostic.log')
        ]
    )


def create_openalex_client() -> OpenAlexClient:
    """Создает клиент OpenAlex для диагностики."""
    config = APIClientConfig(
        name="openalex",
        base_url="https://api.openalex.org",
        headers={
            "User-Agent": "bioactivity-data-acquisition/0.1.0 (mailto:your-email@example.com)",
            "Accept": "application/json"
        },
        timeout=45.0,
        retries=RetrySettings(total=3, backoff_multiplier=2.0, backoff_max=60.0),
        rate_limit=RateLimitSettings(max_calls=10, period=1.0),
    )
    return OpenAlexClient(config)


def test_document_by_doi(client: OpenAlexClient, doi: str) -> dict[str, Any]:
    """Тестирует получение документа по DOI."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Testing DOI: {doi}")
    try:
        result = client.fetch_by_doi(doi)
        
        # Анализируем результат
        analysis = {
            "doi": doi,
            "success": result.get("openalex_title") is not None,
            "title": result.get("openalex_title"),
            "pmid": result.get("openalex_pmid"),
            "year": result.get("openalex_year"),
            "journal": result.get("openalex_journal"),
            "has_abstract": bool(result.get("openalex_abstract")),
            "has_authors": bool(result.get("openalex_authors")),
            "error": result.get("openalex_error"),
            "fields_count": len([k for k, v in result.items() if v is not None and not k.startswith("openalex_")])
        }
        
        logger.info(f"DOI {doi} result: success={analysis['success']}, title_length={len(analysis['title']) if analysis['title'] else 0}")
        return analysis
        
    except Exception as e:
        logger.error(f"DOI {doi} failed: {e}")
        return {
            "doi": doi,
            "success": False,
            "error": str(e),
            "title": None,
            "pmid": None,
            "year": None,
            "journal": None,
            "has_abstract": False,
            "has_authors": False,
            "fields_count": 0
        }


def test_document_by_pmid(client: OpenAlexClient, pmid: str) -> dict[str, Any]:
    """Тестирует получение документа по PMID."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Testing PMID: {pmid}")
    try:
        result = client.fetch_by_pmid(pmid)
        
        # Анализируем результат
        analysis = {
            "pmid": pmid,
            "success": result.get("openalex_title") is not None,
            "title": result.get("openalex_title"),
            "doi": result.get("openalex_doi"),
            "year": result.get("openalex_year"),
            "journal": result.get("openalex_journal"),
            "has_abstract": bool(result.get("openalex_abstract")),
            "has_authors": bool(result.get("openalex_authors")),
            "error": result.get("openalex_error"),
            "fields_count": len([k for k, v in result.items() if v is not None and not k.startswith("openalex_")])
        }
        
        logger.info(f"PMID {pmid} result: success={analysis['success']}, title_length={len(analysis['title']) if analysis['title'] else 0}")
        return analysis
        
    except Exception as e:
        logger.error(f"PMID {pmid} failed: {e}")
        return {
            "pmid": pmid,
            "success": False,
            "error": str(e),
            "title": None,
            "doi": None,
            "year": None,
            "journal": None,
            "has_abstract": False,
            "has_authors": False,
            "fields_count": 0
        }


def compare_results(doi_result: dict[str, Any], pmid_result: dict[str, Any]) -> dict[str, Any]:
    """Сравнивает результаты запросов по DOI и PMID."""
    comparison = {
        "doi_success": doi_result["success"],
        "pmid_success": pmid_result["success"],
        "both_success": doi_result["success"] and pmid_result["success"],
        "title_match": (doi_result.get("title") == pmid_result.get("title") and 
                       doi_result.get("title") is not None),
        "year_match": (doi_result.get("year") == pmid_result.get("year") and 
                      doi_result.get("year") is not None),
        "journal_match": (doi_result.get("journal") == pmid_result.get("journal") and 
                         doi_result.get("journal") is not None),
        "doi_fields": doi_result["fields_count"],
        "pmid_fields": pmid_result["fields_count"],
        "doi_error": doi_result.get("error"),
        "pmid_error": pmid_result.get("error")
    }
    
    return comparison


def main():
    """Основная функция диагностики."""
    parser = argparse.ArgumentParser(description="Диагностика OpenAlex API")
    parser.add_argument("--input", required=True, help="Путь к входному CSV файлу")
    parser.add_argument("--limit", type=int, default=10, help="Количество документов для тестирования")
    parser.add_argument("--output", default="openalex_diagnostic_results.csv", help="Путь к выходному файлу")
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Загружаем данные
    try:
        df = pd.read_csv(args.input)
        logger.info(f"Loaded {len(df)} documents from {args.input}")
    except Exception as e:
        logger.error(f"Failed to load input file: {e}")
        return 1
    
    # Ограничиваем количество документов
    df = df.head(args.limit)
    logger.info(f"Testing {len(df)} documents")
    
    # Создаем клиент
    client = create_openalex_client()
    
    # Результаты диагностики
    results = []
    
    for idx, row in df.iterrows():
        logger.info(f"Processing document {idx + 1}/{len(df)}: {row.get('document_chembl_id', 'unknown')}")
        
        doi_result = None
        pmid_result = None
        
        # Тестируем DOI если есть
        if pd.notna(row.get("doi")) and str(row["doi"]).strip():
            doi_result = test_document_by_doi(client, str(row["doi"]).strip())
        
        # Тестируем PMID если есть
        if pd.notna(row.get("pubmed_id")) and str(row["pubmed_id"]).strip():
            pmid_result = test_document_by_pmid(client, str(row["pubmed_id"]).strip())
        
        # Сравниваем результаты
        if doi_result and pmid_result:
            comparison = compare_results(doi_result, pmid_result)
        else:
            comparison = {
                "doi_success": doi_result["success"] if doi_result else False,
                "pmid_success": pmid_result["success"] if pmid_result else False,
                "both_success": False,
                "title_match": False,
                "year_match": False,
                "journal_match": False,
                "doi_fields": doi_result["fields_count"] if doi_result else 0,
                "pmid_fields": pmid_result["fields_count"] if pmid_result else 0,
                "doi_error": doi_result.get("error") if doi_result else "No DOI",
                "pmid_error": pmid_result.get("error") if pmid_result else "No PMID"
            }
        
        # Сохраняем результат
        result = {
            "document_chembl_id": row.get("document_chembl_id"),
            "doi": row.get("doi"),
            "pubmed_id": row.get("pubmed_id"),
            **comparison
        }
        
        if doi_result:
            result.update({f"doi_{k}": v for k, v in doi_result.items() if k not in ["doi"]})
        
        if pmid_result:
            result.update({f"pmid_{k}": v for k, v in pmid_result.items() if k not in ["pmid"]})
        
        results.append(result)
    
    # Сохраняем результаты
    results_df = pd.DataFrame(results)
    results_df.to_csv(args.output, index=False)
    
    # Выводим статистику
    logger.info("=== DIAGNOSTIC SUMMARY ===")
    logger.info(f"Total documents tested: {len(results)}")
    logger.info(f"DOI success rate: {results_df['doi_success'].mean():.2%}")
    logger.info(f"PMID success rate: {results_df['pmid_success'].mean():.2%}")
    logger.info(f"Both success rate: {results_df['both_success'].mean():.2%}")
    logger.info(f"Title match rate: {results_df['title_match'].mean():.2%}")
    logger.info(f"Average DOI fields: {results_df['doi_fields'].mean():.1f}")
    logger.info(f"Average PMID fields: {results_df['pmid_fields'].mean():.1f}")
    
    # Анализ ошибок
    doi_errors = results_df[results_df['doi_error'].notna()]['doi_error'].value_counts()
    pmid_errors = results_df[results_df['pmid_error'].notna()]['pmid_error'].value_counts()
    
    if not doi_errors.empty:
        logger.info("Top DOI errors:")
        for error, count in doi_errors.head(5).items():
            logger.info(f"  {error}: {count}")
    
    if not pmid_errors.empty:
        logger.info("Top PMID errors:")
        for error, count in pmid_errors.head(5).items():
            logger.info(f"  {error}: {count}")
    
    logger.info(f"Results saved to: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
