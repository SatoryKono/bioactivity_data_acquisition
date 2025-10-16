#!/usr/bin/env python3
"""Скрипт для отладки API клиентов и проверки их работы."""

import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.documents.config import load_document_config
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient

def test_chembl_client():
    """Тестируем ChEMBL клиент."""
    print("=== ТЕСТИРОВАНИЕ ChEMBL КЛИЕНТА ===")
    
    # config_path = Path("configs/config.yaml")  # Не используется
    # doc_config = load_document_config(config_path)  # Не используется
    
    # Создаем клиент как в пайплайне
    from library.config import APIClientConfig, RetrySettings
    
    api_config = APIClientConfig(
        name="chembl",
        base_url="https://www.ebi.ac.uk/chembl/api/data",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=60.0,
        retries=RetrySettings(total=5, backoff_multiplier=2.0),
    )
    
    client = ChEMBLClient(api_config, timeout=60.0)
    
    # Тестируем на первом документе
    test_doc_id = "CHEMBL1121421"
    print(f"Тестируем ChEMBL с doc_id: {test_doc_id}")
    
    try:
        result = client.fetch_by_doc_id(test_doc_id)
        print(f"Результат ChEMBL: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка ChEMBL: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

def test_crossref_client():
    """Тестируем Crossref клиент."""
    print("\n=== ТЕСТИРОВАНИЕ CROSSREF КЛИЕНТА ===")
    
    config_path = Path("configs/config.yaml")
    doc_config = load_document_config(config_path)
    crossref_config = doc_config.api_clients["crossref"]
    
    client = CrossrefClient(crossref_config)
    
    # Тестируем на первом DOI
    test_doi = "10.1021/jm00178a007"
    print(f"Тестируем Crossref с DOI: {test_doi}")
    
    try:
        result = client.fetch_by_doi(test_doi)
        print(f"Результат Crossref: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка Crossref: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

def test_openalex_client():
    """Тестируем OpenAlex клиент."""
    print("\n=== ТЕСТИРОВАНИЕ OPENALEX КЛИЕНТА ===")
    
    config_path = Path("configs/config.yaml")
    doc_config = load_document_config(config_path)
    openalex_config = doc_config.api_clients["openalex"]
    
    client = OpenAlexClient(openalex_config)
    
    # Тестируем на первом DOI
    test_doi = "10.1021/jm00178a007"
    print(f"Тестируем OpenAlex с DOI: {test_doi}")
    
    try:
        result = client.fetch_by_doi(test_doi)
        print(f"Результат OpenAlex: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка OpenAlex: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

def test_pubmed_client():
    """Тестируем PubMed клиент."""
    print("\n=== ТЕСТИРОВАНИЕ PUBMED КЛИЕНТА ===")
    
    config_path = Path("configs/config.yaml")
    doc_config = load_document_config(config_path)
    pubmed_config = doc_config.api_clients["pubmed"]
    
    client = PubMedClient(pubmed_config)
    
    # Тестируем на первом PMID
    test_pmid = "6991692"
    print(f"Тестируем PubMed с PMID: {test_pmid}")
    
    try:
        result = client.fetch_by_pmid(test_pmid)
        print(f"Результат PubMed: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка PubMed: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

def test_semantic_scholar_client():
    """Тестируем Semantic Scholar клиент."""
    print("\n=== ТЕСТИРОВАНИЕ SEMANTIC SCHOLAR КЛИЕНТА ===")
    
    config_path = Path("configs/config.yaml")
    doc_config = load_document_config(config_path)
    scholar_config = doc_config.api_clients["semantic_scholar"]
    
    client = SemanticScholarClient(scholar_config)
    
    # Тестируем на первом PMID
    test_pmid = "6991692"
    print(f"Тестируем Semantic Scholar с PMID: {test_pmid}")
    
    try:
        result = client.fetch_by_pmid(test_pmid)
        print(f"Результат Semantic Scholar: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка Semantic Scholar: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

def main():
    """Основная функция тестирования."""
    print("ОТЛАДКА API КЛИЕНТОВ")
    print("=" * 50)
    
    test_chembl_client()
    test_crossref_client()
    test_openalex_client()
    test_pubmed_client()
    test_semantic_scholar_client()
    
    print("\n" + "=" * 50)
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")

if __name__ == "__main__":
    main()
