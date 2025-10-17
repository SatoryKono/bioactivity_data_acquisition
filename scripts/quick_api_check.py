#!/usr/bin/env python3
"""Быстрая проверка конкретного API."""

import argparse
import sys
import time
from pathlib import Path

import requests

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.logging_setup import get_logger

logger = get_logger(__name__)


def check_api(name: str, url: str, params: dict = None, headers: dict = None):
    """Быстрая проверка API."""
    logger.info(f"Проверка {name}...")
    logger.info(f"URL: {url}")
    
    if params:
        logger.info(f"Параметры: {params}")
    if headers:
        logger.info(f"Заголовки: {headers}")
    
    try:
        start_time = time.time()
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response_time = time.time() - start_time
        
        logger.info("\nРезультат:")
        logger.info(f"  Статус: {response.status_code}")
        logger.info(f"  Время ответа: {response_time:.3f}с")
        
        # Проверяем заголовки rate limit
        rate_limit_headers = []
        for header in response.headers:
            if 'rate' in header.lower() or 'limit' in header.lower():
                rate_limit_headers.append(f"{header}: {response.headers[header]}")
        
        if rate_limit_headers:
            logger.info("  Rate Limit заголовки:")
            for header in rate_limit_headers:
                logger.info(f"    {header}")
        
        if response.status_code == 200:
            logger.info("  Статус: OK")
        elif response.status_code == 429:
            logger.info("  Статус: RATE LIMITED")
        elif response.status_code in [401, 403]:
            logger.info("  Статус: AUTH REQUIRED")
        else:
            logger.error("  Статус: ERROR")
            logger.info(f"  Ответ: {response.text[:200]}...")
            
    except Exception as e:
        logger.error(f"  Ошибка: {e}")


def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(description="Быстрая проверка API")
    parser.add_argument("api", choices=["chembl", "pubmed", "crossref", "openalex", "semantic"], 
                       help="API для проверки")
    
    args = parser.parse_args()
    
    # Определяем параметры для каждого API
    apis = {
        "chembl": {
            "name": "chembl",
            "url": "https://www.ebi.ac.uk/chembl/api/data/activity",
            "params": {"limit": 1},
            "headers": {"Accept": "application/json"}
        },
        "pubmed": {
            "name": "PubMed",
            "url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            "params": {"db": "pubmed", "term": "cancer", "retmax": 1, "retmode": "json"},
            "headers": {"Accept": "application/json"}
        },
        "crossref": {
            "name": "Crossref",
            "url": "https://api.crossref.org/works",
            "params": {"rows": 1},
            "headers": {"Accept": "application/json"}
        },
        "openalex": {
            "name": "OpenAlex",
            "url": "https://api.openalex.org/works",
            "params": {"per-page": 1},
            "headers": {"Accept": "application/json"}
        },
        "semantic": {
            "name": "Semantic Scholar",
            "url": "https://api.semanticscholar.org/graph/v1/paper/search",
            "params": {"query": "machine learning", "limit": 1},
            "headers": {"Accept": "application/json"}
        }
    }
    
    api_config = apis[args.api]
    check_api(**api_config)


if __name__ == "__main__":
    main()
