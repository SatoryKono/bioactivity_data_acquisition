#!/usr/bin/env python3
"""Тест для проверки извлечения данных."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
from library.documents.extract import extract_from_openalex, extract_from_semantic_scholar, extract_from_crossref
from library.clients.openalex import OpenAlexClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.clients.crossref import CrossrefClient
from library.config import APIClientConfig

def test_extraction():
    """Тест извлечения данных."""
    print("=== Testing Data Extraction ===")
    
    # Создаем клиентов
    openalex_config = APIClientConfig(
        name="openalex",
        base_url="https://api.openalex.org",
        headers={"Accept": "application/json"}
    )
    openalex_client = OpenAlexClient(openalex_config)
    
    semantic_scholar_config = APIClientConfig(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org/graph/v1",
        headers={"Accept": "application/json"}
    )
    semantic_scholar_client = SemanticScholarClient(semantic_scholar_config)
    
    crossref_config = APIClientConfig(
        name="crossref",
        base_url="https://api.crossref.org",
        headers={"Accept": "application/json"}
    )
    crossref_client = CrossrefClient(crossref_config)
    
    # Тестовые данные
    pmids = ["17827018", "18578478"]
    dois = ["10.1016/j.bmc.2007.08.038", "10.1021/jm800092x"]
    
    print(f"Testing with PMIDs: {pmids}")
    print(f"Testing with DOIs: {dois}")
    
    # Тест OpenAlex
    print("\n--- Testing OpenAlex ---")
    try:
        openalex_df = extract_from_openalex(openalex_client, pmids, batch_size=50)
        print(f"OpenAlex DataFrame shape: {openalex_df.shape}")
        print(f"OpenAlex columns: {list(openalex_df.columns)}")
        if not openalex_df.empty:
            print("OpenAlex data:")
            for col in openalex_df.columns:
                non_null_count = openalex_df[col].notna().sum()
                if non_null_count > 0:
                    print(f"  {col}: {non_null_count} non-null values")
                    print(f"    Sample: {openalex_df[col].iloc[0] if len(openalex_df) > 0 else 'N/A'}")
    except Exception as e:
        print(f"OpenAlex extraction error: {e}")
    
    # Тест Semantic Scholar
    print("\n--- Testing Semantic Scholar ---")
    try:
        semantic_df = extract_from_semantic_scholar(semantic_scholar_client, pmids, batch_size=100)
        print(f"Semantic Scholar DataFrame shape: {semantic_df.shape}")
        print(f"Semantic Scholar columns: {list(semantic_df.columns)}")
        if not semantic_df.empty:
            print("Semantic Scholar data:")
            for col in semantic_df.columns:
                non_null_count = semantic_df[col].notna().sum()
                if non_null_count > 0:
                    print(f"  {col}: {non_null_count} non-null values")
                    print(f"    Sample: {semantic_df[col].iloc[0] if len(semantic_df) > 0 else 'N/A'}")
    except Exception as e:
        print(f"Semantic Scholar extraction error: {e}")
    
    # Тест Crossref
    print("\n--- Testing Crossref ---")
    try:
        crossref_df = extract_from_crossref(crossref_client, dois, batch_size=100)
        print(f"Crossref DataFrame shape: {crossref_df.shape}")
        print(f"Crossref columns: {list(crossref_df.columns)}")
        if not crossref_df.empty:
            print("Crossref data:")
            for col in crossref_df.columns:
                non_null_count = crossref_df[col].notna().sum()
                if non_null_count > 0:
                    print(f"  {col}: {non_null_count} non-null values")
                    print(f"    Sample: {crossref_df[col].iloc[0] if len(crossref_df) > 0 else 'N/A'}")
    except Exception as e:
        print(f"Crossref extraction error: {e}")

if __name__ == "__main__":
    test_extraction()
