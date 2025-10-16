#!/usr/bin/env python3
"""Быстрая проверка конкретного API."""

import argparse
import time
import requests


def check_api(name: str, url: str, params: dict = None, headers: dict = None):
    """Быстрая проверка API."""
    print(f"Проверка {name}...")
    print(f"URL: {url}")
    
    if params:
        print(f"Параметры: {params}")
    if headers:
        print(f"Заголовки: {headers}")
    
    try:
        start_time = time.time()
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response_time = time.time() - start_time
        
        print("\nРезультат:")
        print(f"  Статус: {response.status_code}")
        print(f"  Время ответа: {response_time:.3f}с")
        
        # Проверяем заголовки rate limit
        rate_limit_headers = []
        for header in response.headers:
            if 'rate' in header.lower() or 'limit' in header.lower():
                rate_limit_headers.append(f"{header}: {response.headers[header]}")
        
        if rate_limit_headers:
            print("  Rate Limit заголовки:")
            for header in rate_limit_headers:
                print(f"    {header}")
        
        if response.status_code == 200:
            print("  Статус: OK")
        elif response.status_code == 429:
            print("  Статус: RATE LIMITED")
        elif response.status_code in [401, 403]:
            print("  Статус: AUTH REQUIRED")
        else:
            print("  Статус: ERROR")
            print(f"  Ответ: {response.text[:200]}...")
            
    except Exception as e:
        print(f"  Ошибка: {e}")


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
