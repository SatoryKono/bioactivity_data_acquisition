#!/usr/bin/env python3
"""Тест для проверки работы API клиентов."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.config import APIClientConfig


def test_openalex():
    """Тест OpenAlex клиента."""
    print("=== Testing OpenAlex Client ===")
    
    config = APIClientConfig(
        name="openalex",
        base_url="https://api.openalex.org",
        headers={"Accept": "application/json"}
    )
    
    client = OpenAlexClient(config)
    
    # Тестируем с PMID из наших данных
    pmid = "17827018"
    print(f"Testing with PMID: {pmid}")
    
    try:
        result = client.fetch_by_pmid(pmid)
        print(f"OpenAlex result: {result}")
        
        if result:
            print("OpenAlex fields found:")
            for key, value in result.items():
                if value is not None:
                    print(f"  {key}: {value}")
        else:
            print("No data returned from OpenAlex")
            
    except Exception as e:
        print(f"OpenAlex error: {e}")

def test_semantic_scholar():
    """Тест Semantic Scholar клиента."""
    print("\n=== Testing Semantic Scholar Client ===")
    
    config = APIClientConfig(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org/graph/v1",
        headers={"Accept": "application/json"}
    )
    
    client = SemanticScholarClient(config)
    
    # Тестируем с PMID из наших данных
    pmid = "17827018"
    print(f"Testing with PMID: {pmid}")
    
    try:
        result = client.fetch_by_pmid(pmid)
        print(f"Semantic Scholar result: {result}")
        
        if result:
            print("Semantic Scholar fields found:")
            for key, value in result.items():
                if value is not None:
                    print(f"  {key}: {value}")
        else:
            print("No data returned from Semantic Scholar")
            
    except Exception as e:
        print(f"Semantic Scholar error: {e}")

def test_crossref():
    """Тест Crossref клиента."""
    print("\n=== Testing Crossref Client ===")
    
    config = APIClientConfig(
        name="crossref",
        base_url="https://api.crossref.org",
        headers={"Accept": "application/json"}
    )
    
    client = CrossrefClient(config)
    
    # Тестируем с DOI из наших данных
    doi = "10.1016/j.bmc.2007.08.038"
    print(f"Testing with DOI: {doi}")
    
    try:
        result = client.fetch_by_doi(doi)
        print(f"Crossref result: {result}")
        
        if result:
            print("Crossref fields found:")
            for key, value in result.items():
                if value is not None:
                    print(f"  {key}: {value}")
        else:
            print("No data returned from Crossref")
            
    except Exception as e:
        print(f"Crossref error: {e}")

if __name__ == "__main__":
    test_openalex()
    test_semantic_scholar()
    test_crossref()
