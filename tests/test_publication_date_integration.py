"""Интеграционные тесты для функции publication_date в pipeline."""

import pandas as pd

from library.documents.config import DocumentConfig
from library.documents.pipeline import run_document_etl


class TestPublicationDateIntegration:
    """Интеграционные тесты для publication_date в pipeline."""

    def test_publication_date_in_pipeline(self):
        """Тест, что publication_date добавляется в pipeline."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources": {
                "pubmed": {"enabled": False},  # Отключаем для простоты теста
                "chembl": {"enabled": True},   # Включаем один источник для валидации
                "crossref": {"enabled": False},
                "openalex": {"enabled": False},
                "semantic_scholar": {"enabled": False}
            },
            "runtime": {
                "limit": None,
                "dry_run": True
            },
            "http": {
                "global": {
                    "timeout_sec": 30.0,
                    "retries": {"total": 3, "backoff_multiplier": 1.0},
                    "headers": {}
                }
            },
            "io": {
                "input": {"documents_csv": "test.csv"},
                "output": {"dir": "test_output"}
            },
            "postprocess": {
                "journal_normalization": {"enabled": False},
                "citation_formatting": {"enabled": False},
                "correlation": {"enabled": False}
            },
            "determinism": {
                "column_order": ["document_chembl_id", "doi", "title", "publication_date"]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные с полями PubMed
        test_data = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123", "CHEMBL456", "CHEMBL789"],
            "title": ["Test Title 1", "Test Title 2", "Test Title 3"],
            "doi": ["10.1000/test1", "10.1000/test2", "10.1000/test3"],
            "pubmed_year_completed": [2023, None, 2022],
            "pubmed_month_completed": [5, None, None],
            "pubmed_day_completed": [15, None, None],
            "pubmed_year_revised": [2022, 2021, None],
            "pubmed_month_revised": [3, None, None],
            "pubmed_day_revised": [10, None, None]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что колонка publication_date добавлена
        assert "publication_date" in result.documents.columns
        
        # Проверяем значения publication_date
        expected_dates = ["2023-05-15", "2021-01-01", "2022-01-01"]
        actual_dates = result.documents["publication_date"].tolist()
        
        assert actual_dates == expected_dates

    def test_publication_date_with_empty_pubmed_fields(self):
        """Тест с пустыми полями PubMed."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources": {
                "pubmed": {"enabled": False},
                "chembl": {"enabled": True},   # Включаем один источник для валидации
                "crossref": {"enabled": False},
                "openalex": {"enabled": False},
                "semantic_scholar": {"enabled": False}
            },
            "runtime": {
                "limit": None,
                "dry_run": True
            },
            "http": {
                "global": {
                    "timeout_sec": 30.0,
                    "retries": {"total": 3, "backoff_multiplier": 1.0},
                    "headers": {}
                }
            },
            "io": {
                "input": {"documents_csv": "test.csv"},
                "output": {"dir": "test_output"}
            },
            "postprocess": {
                "journal_normalization": {"enabled": False},
                "citation_formatting": {"enabled": False},
                "correlation": {"enabled": False}
            },
            "determinism": {
                "column_order": ["document_chembl_id", "doi", "title", "publication_date"]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные без полей PubMed
        test_data = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "title": ["Test Title 1", "Test Title 2"],
            "doi": ["10.1000/test1", "10.1000/test2"]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что колонка publication_date добавлена
        assert "publication_date" in result.documents.columns
        
        # Проверяем, что все значения равны "0000-01-01"
        expected_dates = ["0000-01-01", "0000-01-01"]
        actual_dates = result.documents["publication_date"].tolist()
        
        assert actual_dates == expected_dates
