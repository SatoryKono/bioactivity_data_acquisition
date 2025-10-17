"""Интеграционные тесты для функции document_sortorder в pipeline."""

import pandas as pd

from library.documents.config import DocumentConfig
from library.documents.pipeline import run_document_etl


class TestDocumentSortorderIntegration:
    """Интеграционные тесты для document_sortorder в pipeline."""

    def test_document_sortorder_in_pipeline(self):
        """Тест, что document_sortorder добавляется в pipeline."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources":{
                "pubmed":{"enabled":False},  # Отключаем для простоты теста
                "chembl":{"enabled":True},   # Включаем один источник для валидации
                "crossref":{"enabled":False},
                "openalex":{"enabled":False},
                "semantic_scholar":{"enabled":False}
            },
            "runtime":{
                "limit":None,
                "dry_run":True
            },
            "http":{
                "global":{
                    "timeout_sec":30.0,
                    "retries":{"total":3, "backoff_multiplier":1.0},
                    "headers":{}
                }
            },
            "io":{
                "input":{"documents_csv":"test.csv"},
                "output":{"dir":"test_output"}
            },
            "postprocess":{
                "journal_normalization":{"enabled":False},
                "citation_formatting":{"enabled":False},
                "correlation":{"enabled":False}
            },
            "determinism":{
                "column_order":["document_chembl_id", "doi", "title", "publication_date", "document_sortorder"]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные с полями для document_sortorder
        test_data = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456", "CHEMBL789"],
            "title":["Test Title 1", "Test Title 2", "Test Title 3"],
            "doi":["10.1000/test1", "10.1000/test2", "10.1000/test3"],
            "pubmed_issn":["1234-5678", "9876-5432", "1111-2222"],
            "pubmed_year_completed":[2023, None, 2022],
            "pubmed_month_completed":[5, None, None],
            "pubmed_day_completed":[15, None, None],
            "pubmed_year_revised":[2022, 2021, None],
            "pubmed_month_revised":[3, None, None],
            "pubmed_day_revised":[10, None, None],
            "index":[123, 456, 5]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что колонка document_sortorder добавлена
        assert "document_sortorder" in result.documents.columns
        
        # Проверяем значения document_sortorder
        expected_sortorders = [
            "1234-5678:2023-05-15:000123",
            "9876-5432:2021-01-01:000456", 
            "1111-2222:2022-01-01:000005"
        ]
        actual_sortorders = result.documents["document_sortorder"].tolist()
        
        assert actual_sortorders == expected_sortorders

    def test_document_sortorder_with_empty_fields(self):
        """Тест с пустыми полями для document_sortorder."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources":{
                "pubmed":{"enabled":False},
                "chembl":{"enabled":True},   # Включаем один источник для валидации
                "crossref":{"enabled":False},
                "openalex":{"enabled":False},
                "semantic_scholar":{"enabled":False}
            },
            "runtime":{
                "limit":None,
                "dry_run":True
            },
            "http":{
                "global":{
                    "timeout_sec":30.0,
                    "retries":{"total":3, "backoff_multiplier":1.0},
                    "headers":{}
                }
            },
            "io":{
                "input":{"documents_csv":"test.csv"},
                "output":{"dir":"test_output"}
            },
            "postprocess":{
                "journal_normalization":{"enabled":False},
                "citation_formatting":{"enabled":False},
                "correlation":{"enabled":False}
            },
            "determinism":{
                "column_order":["document_chembl_id", "doi", "title", "publication_date", "document_sortorder"]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные без полей для document_sortorder
        test_data = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456"],
            "title":["Test Title 1", "Test Title 2"],
            "doi":["10.1000/test1", "10.1000/test2"]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что колонка document_sortorder добавлена
        assert "document_sortorder" in result.documents.columns
        
        # Проверяем, что значения содержат правильные индексы (publication_date создается автоматически)
        expected_sortorders = [":0000-01-01:000000", ":0000-01-01:000001"]
        actual_sortorders = result.documents["document_sortorder"].tolist()
        
        assert actual_sortorders == expected_sortorders

    def test_document_sortorder_with_mixed_data_types(self):
        """Тест с различными типами данных для document_sortorder."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources":{
                "pubmed":{"enabled":False},
                "chembl":{"enabled":True},   # Включаем один источник для валидации
                "crossref":{"enabled":False},
                "openalex":{"enabled":False},
                "semantic_scholar":{"enabled":False}
            },
            "runtime":{
                "limit":None,
                "dry_run":True
            },
            "http":{
                "global":{
                    "timeout_sec":30.0,
                    "retries":{"total":3, "backoff_multiplier":1.0},
                    "headers":{}
                }
            },
            "io":{
                "input":{"documents_csv":"test.csv"},
                "output":{"dir":"test_output"}
            },
            "postprocess":{
                "journal_normalization":{"enabled":False},
                "citation_formatting":{"enabled":False},
                "correlation":{"enabled":False}
            },
            "determinism":{
                "column_order":["document_chembl_id", "doi", "title", "publication_date", "document_sortorder"]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные с различными типами данных
        test_data = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456", "CHEMBL789"],
            "title":["Test Title 1", "Test Title 2", "Test Title 3"],
            "doi":["10.1000/test1", "10.1000/test2", "10.1000/test3"],
            "pubmed_issn":["1234-5678", None, ""],
            "pubmed_year_completed":[2023, None, 2022],
            "pubmed_month_completed":[5, None, None],
            "pubmed_day_completed":[15, None, None],
            "pubmed_year_revised":[2022, 2021, None],
            "pubmed_month_revised":[3, None, None],
            "pubmed_day_revised":[10, None, None]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что колонка document_sortorder добавлена
        assert "document_sortorder" in result.documents.columns
        
        # Проверяем значения document_sortorder (index создается автоматически как 0, 1, 2)
        expected_sortorders = [
            "1234-5678:2023-05-15:000000",
            ":2021-01-01:000001",
            ":2022-01-01:000002"
        ]
        actual_sortorders = result.documents["document_sortorder"].tolist()
        
        assert actual_sortorders == expected_sortorders
