#!/usr/bin/env python3
"""Тесты для проверки сохранения метаданных в пайплайне документов."""


import pandas as pd
import yaml

from library.documents.config import DocumentConfig
from library.documents.pipeline import run_document_etl, write_document_outputs


class TestDocumentMetadata:
    """Тесты для метаданных документов."""

    def test_metadata_creation(self):
        """Тест создания метаданных в результате ETL."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources": {
                "pubmed": {"enabled": False},
                "chembl": {"enabled": True},
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
                "sort": {
                    "by": ["document_chembl_id"],
                    "ascending": [True],
                    "na_position": "last"
                },
                "column_order": [
                    "index",
                    "document_chembl_id",
                    "doi",
                    "title",
                    "publication_date",
                    "document_sortorder"
                ]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные
        test_data = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "title": ["Test Title 1", "Test Title 2"],
            "doi": ["10.1000/test1", "10.1000/test2"]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Проверяем, что метаданные созданы
        assert hasattr(result, 'meta'), "Результат должен содержать метаданные"
        assert isinstance(result.meta, dict), "Метаданные должны быть словарем"
        
        # Проверяем основные поля метаданных
        assert 'pipeline_version' in result.meta, "Метаданные должны содержать версию пайплайна"
        assert 'row_count' in result.meta, "Метаданные должны содержать количество строк"
        assert 'enabled_sources' in result.meta, "Метаданные должны содержать включенные источники"
        assert 'extraction_parameters' in result.meta, "Метаданные должны содержать параметры извлечения"
        
        # Проверяем значения
        assert result.meta['pipeline_version'] == "1.0.0"
        assert result.meta['row_count'] == 2
        assert result.meta['enabled_sources'] == ['chembl']
        
        # Проверяем параметры извлечения
        extraction_params = result.meta['extraction_parameters']
        assert extraction_params['total_documents'] == 2
        assert extraction_params['sources_processed'] == 1
        assert extraction_params['correlation_analysis_enabled'] == False
        assert extraction_params['correlation_insights_count'] == 0
        assert extraction_params['chembl_records'] == 2

    def test_metadata_saving(self, tmp_path):
        """Тест сохранения метаданных в файл."""
        # Создаем минимальную конфигурацию
        config_data = {
            "sources": {
                "pubmed": {"enabled": False},
                "chembl": {"enabled": True},
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
                "sort": {
                    "by": ["document_chembl_id"],
                    "ascending": [True],
                    "na_position": "last"
                },
                "column_order": [
                    "index",
                    "document_chembl_id",
                    "doi",
                    "title",
                    "publication_date",
                    "document_sortorder"
                ]
            }
        }
        
        config = DocumentConfig(**config_data)
        
        # Создаем тестовые данные
        test_data = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123"],
            "title": ["Test Title 1"],
            "doi": ["10.1000/test1"]
        })
        
        # Запускаем pipeline
        result = run_document_etl(config, test_data)
        
        # Сохраняем результаты
        outputs = write_document_outputs(result, tmp_path, "test", config)
        
        # Проверяем, что все файлы созданы
        assert "documents" in outputs, "Должен быть создан файл документов"
        assert "qc" in outputs, "Должен быть создан файл QC"
        assert "meta" in outputs, "Должен быть создан файл метаданных"
        
        # Проверяем, что файлы существуют
        assert outputs["documents"].exists(), "Файл документов должен существовать"
        assert outputs["qc"].exists(), "Файл QC должен существовать"
        assert outputs["meta"].exists(), "Файл метаданных должен существовать"
        
        # Проверяем содержимое файла метаданных
        with open(outputs["meta"], encoding='utf-8') as f:
            saved_meta = yaml.safe_load(f)
        
        assert 'file_checksums' in saved_meta, "Сохраненные метаданные должны содержать контрольные суммы"
        assert 'csv' in saved_meta['file_checksums'], "Должна быть контрольная сумма CSV файла"
        assert 'qc' in saved_meta['file_checksums'], "Должна быть контрольная сумма QC файла"
        
        # Проверяем, что контрольные суммы не пустые
        assert len(saved_meta['file_checksums']['csv']) == 64, "Контрольная сумма CSV должна быть SHA256 (64 символа)"
        assert len(saved_meta['file_checksums']['qc']) == 64, "Контрольная сумма QC должна быть SHA256 (64 символа)"
