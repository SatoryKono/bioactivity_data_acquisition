"""Tests for document pipeline configuration usage and validation."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from library.documents.config import DocumentConfig, load_document_config
from library.documents.pipeline import DocumentValidationError, _create_api_client
from tests.schemas.test_column_order_validation import validate_column_order, validate_determinism, validate_pandera_schema


@pytest.fixture()
def custom_headers_config_yaml(tmp_path: Path) -> Path:
    """Create a test configuration with custom headers and settings."""
    config_path = tmp_path / "custom_config.yaml"
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 45.0,
                "retries": {
                    "total": 3,
                    "backoff_multiplier": 1.5
                },
                "headers": {
                    "User-Agent": "custom-user-agent/1.0",
                    "X-Custom-Header": "global-value"
                }
            }
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "http": {
                    "base_url": "https://custom.chembl.api/",
                    "timeout_sec": 90.0,
                    "headers": {
                        "Authorization": "Bearer custom-token",
                        "X-Custom-Header": "chembl-override"
                    },
                    "retries": {
                        "total": 7,
                        "backoff_multiplier": 2.5
                    }
                },
                "rate_limit": {
                    "max_calls": 1,
                    "period": 2.0
                }
            },
            "crossref": {
                "enabled": True,
                "http": {
                    "base_url": "https://custom.crossref.api/",
                    "headers": {
                        "X-API-Key": "custom-crossref-key"
                    }
                }
            },
            "pubmed": {
                "enabled": True,
                "http": {
                    "timeout_sec": 120.0,
                    "headers": {
                        "X-API-Key": "custom-pubmed-key"
                    },
                    "retries": {
                        "total": 10
                    }
                }
            }
        }
    }
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    return config_path


def test_create_api_client_uses_source_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client uses source-specific configuration."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test ChEMBL client creation
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        # Verify client was created
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        
        # Check APIClientConfig was created with correct values
        api_config = call_args[0][0]  # First positional argument
        
        assert api_config.name == "chembl"
        assert str(api_config.base_url) == "https://custom.chembl.api/"
        assert api_config.timeout == 90.0  # Source-specific timeout
        
        # Check headers are merged correctly (default + global + source-specific)
        expected_headers = {
            "Accept": "application/json",  # Default
            "User-Agent": "custom-user-agent/1.0",  # Global overrides default
            "X-Custom-Header": "chembl-override",  # Source overrides global
            "Authorization": "Bearer custom-token"  # Source-specific
        }
        assert api_config.headers == expected_headers
        
        # Check retry settings
        assert api_config.retries.total == 7  # Source-specific
        assert api_config.retries.backoff_multiplier == 2.5  # Source-specific
        
        # Check rate limit settings
        assert api_config.rate_limit is not None
        assert api_config.rate_limit.max_calls == 1
        assert api_config.rate_limit.period == 2.0


def test_create_api_client_fallback_to_global_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client falls back to global config when source config is missing."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test Crossref client creation (uses global timeout, partial headers)
    with patch('library.documents.pipeline.CrossrefClient') as mock_client:
        client = _create_api_client("crossref", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        assert api_config.name == "crossref"
        assert str(api_config.base_url) == "https://custom.crossref.api/"
        assert api_config.timeout == 45.0  # Falls back to global
        
        # Check headers are merged correctly (default + global + source-specific)
        expected_headers = {
            "Accept": "application/json",  # Default
            "User-Agent": "custom-user-agent/1.0",  # Global overrides default
            "X-Custom-Header": "global-value",  # Global value (not overridden)
            "X-API-Key": "custom-crossref-key"  # Source-specific
        }
        assert api_config.headers == expected_headers
        
        # Check retry settings fall back to global
        assert api_config.retries.total == 3  # Global value
        assert api_config.retries.backoff_multiplier == 1.5  # Global value
        
        # No rate limit configured
        assert api_config.rate_limit is None


def test_create_api_client_partial_retry_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client handles partial retry configuration."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test PubMed client creation (partial retry config)
    with patch('library.documents.pipeline.PubMedClient') as mock_client:
        client = _create_api_client("pubmed", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        assert api_config.name == "pubmed"
        assert api_config.timeout == 120.0  # Source-specific timeout
        
        # Check retry settings: source total, global backoff_multiplier
        assert api_config.retries.total == 10  # Source-specific
        assert api_config.retries.backoff_multiplier == 1.5  # Falls back to global


def test_create_api_client_unknown_source() -> None:
    """Test that _create_api_client raises error for unknown source."""
    
    config = DocumentConfig()
    
    with pytest.raises(DocumentValidationError, match="Source 'unknown' not found"):
        _create_api_client("unknown", config)


def test_create_api_client_uses_default_urls_when_not_configured() -> None:
    """Test that _create_api_client uses default URLs when not configured in source."""
    
    config = DocumentConfig()
    
    # Test with a source that has no custom base_url
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Should use default URL from _get_base_url
        assert str(api_config.base_url) == "https://www.ebi.ac.uk/chembl/api/data"
        
        # Should use global timeout, but ChEMBL gets minimum 60 seconds
        assert api_config.timeout == 60.0  # ChEMBL minimum timeout
        
        # Should use default headers (from _get_headers function)
        assert api_config.headers == {"User-Agent": "bioactivity-data-acquisition/0.1.0", "Accept": "application/json"}


def test_create_api_client_chembl_minimum_timeout() -> None:
    """Test that ChEMBL gets minimum 60 second timeout even if configured lower."""
    
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 30.0
            }
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "http": {
                    "timeout_sec": 45.0  # Less than 60, should be increased
                }
            }
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Should be increased to minimum 60 seconds
        assert api_config.timeout == 60.0


def test_create_api_client_all_sources() -> None:
    """Test that _create_api_client works for all supported sources."""
    
    config = DocumentConfig()
    
    sources_and_clients = [
        ("chembl", "ChEMBLClient"),
        ("crossref", "CrossrefClient"),
        ("openalex", "OpenAlexClient"),
        ("pubmed", "PubMedClient"),
        ("semantic_scholar", "SemanticScholarClient")
    ]
    
    for source, client_class in sources_and_clients:
        with patch(f'library.documents.pipeline.{client_class}') as mock_client:
            client = _create_api_client(source, config)
            mock_client.assert_called_once()
            
            # Verify the client was created with APIClientConfig
            call_args = mock_client.call_args
            api_config = call_args[0][0]
            assert api_config.name == source


def test_create_api_client_requests_per_second_rate_limit() -> None:
    """Test that _create_api_client creates RateLimitSettings from requests_per_second format."""
    
    config_data = {
        "sources": {
            "semantic_scholar": {
                "enabled": True,
                "rate_limit": {
                    "requests_per_second": 0.5  # 1 request every 2 seconds
                }
            }
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    
    with patch('library.documents.pipeline.SemanticScholarClient') as mock_client:
        client = _create_api_client("semantic_scholar", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Verify rate limit settings were created correctly
        assert api_config.rate_limit is not None
        assert api_config.rate_limit.max_calls == 1
        assert api_config.rate_limit.period == 2.0  # 1.0 / 0.5 = 2.0


def test_create_api_client_requests_per_second_rate_limit_openalex() -> None:
    """Test that _create_api_client creates RateLimitSettings from requests_per_second for OpenAlex."""
    
    config_data = {
        "sources": {
            "openalex": {
                "enabled": True,
                "rate_limit": {
                    "requests_per_second": 1.0  # 1 request per second
                }
            }
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    
    with patch('library.documents.pipeline.OpenAlexClient') as mock_client:
        client = _create_api_client("openalex", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Verify rate limit settings were created correctly
        assert api_config.rate_limit is not None
        assert api_config.rate_limit.max_calls == 1
        assert api_config.rate_limit.period == 1.0  # 1.0 / 1.0 = 1.0


class TestDocumentPipelineValidation:
    """E2E тесты валидации Document пайплайна."""

    @pytest.fixture
    def document_config(self) -> dict[str, Any]:
        """Конфигурация Document пайплайна."""
        config_path = Path("configs/config_document.yaml")
        if not config_path.exists():
            pytest.skip("Document config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def document_input_data(self) -> pd.DataFrame:
        """Входные данные для Document пайплайна."""
        input_path = Path("data/input/document.csv")
        if not input_path.exists():
            pytest.skip("Document input data not found")
        
        return pd.read_csv(input_path)

    @pytest.fixture
    def temp_output_dir(self) -> Path:
        """Временная директория для выходных файлов."""
        return Path(tempfile.mkdtemp())

    def test_document_pipeline_smoke_test(self, document_config: dict[str, Any], document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Smoke test Document пайплайна с --limit 2."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(2)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн через CLI
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "2",
            "--dry-run"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out after 5 minutes")
        except FileNotFoundError:
            pytest.skip("bioactivity-data-acquisition CLI not found")

    def test_document_pipeline_validation_test(self, document_config: dict[str, Any], document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Validation test Document пайплайна с --limit 10."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(10)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн через CLI
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "10"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем что выходные файлы созданы
            output_files = list(temp_output_dir.glob("*.csv"))
            assert len(output_files) > 0, "No output files created"
            
            # Проверяем основной выходной файл
            main_output = temp_output_dir / "documents.csv"
            if main_output.exists():
                self._validate_document_output(main_output, document_config)
            
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out after 10 minutes")
        except FileNotFoundError:
            pytest.skip("bioactivity-data-acquisition CLI not found")

    def _validate_document_output(self, output_file: Path, config: dict[str, Any]) -> None:
        """Валидация выходного файла Document пайплайна."""
        # Читаем выходные данные
        output_data = pd.read_csv(output_file)
        
        # Проверяем что данные не пустые
        assert len(output_data) > 0, "Output data is empty"
        
        # Проверяем column_order
        column_order = config["determinism"]["column_order"]
        validate_column_order(output_data, column_order)
        
        # Проверяем Pandera схему
        from library.schemas.document_schema import DocumentInputSchema
        try:
            validated_data = validate_pandera_schema(output_data, DocumentInputSchema)
            assert len(validated_data) == len(output_data)
        except Exception as e:
            pytest.fail(f"Document schema validation failed: {e}")
        
        # Проверяем обязательные поля
        required_fields = ["document_chembl_id"]
        for field in required_fields:
            assert field in output_data.columns, f"Required field {field} missing"
            assert not output_data[field].isna().any(), f"Required field {field} contains NULL values"
        
        # Проверяем детерминизм
        validate_determinism(output_file)

    def test_document_pipeline_column_order_consistency(self, document_config: dict[str, Any], document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест консистентности column_order в Document пайплайне."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(5)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "5"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "documents.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Проверяем что порядок колонок соответствует конфигурации
                column_order = document_config["determinism"]["column_order"]
                validate_column_order(output_data, column_order)
                
                # Проверяем что все колонки из конфигурации присутствуют
                expected_columns = []
                for col in column_order:
                    if isinstance(col, str):
                        col_name = col.split('#')[0].strip().strip('"').strip("'")
                        if col_name and col_name != 'index':
                            expected_columns.append(col_name)
                
                missing_columns = set(expected_columns) - set(output_data.columns)
                assert len(missing_columns) == 0, f"Missing columns: {missing_columns}"
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")

    def test_document_pipeline_schema_validation(self, document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест валидации схемы Document пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "3"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "documents.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Валидируем по Pandera схеме
                from library.schemas.document_schema import DocumentInputSchema
                try:
                    validated_data = validate_pandera_schema(output_data, DocumentInputSchema)
                    assert len(validated_data) == len(output_data)
                    
                    # Проверяем типы данных
                    self._validate_document_data_types(validated_data)
                    
                    # Проверяем диапазоны значений
                    self._validate_document_value_ranges(validated_data)
                    
                except Exception as e:
                    pytest.fail(f"Document schema validation failed: {e}")
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")

    def _validate_document_data_types(self, data: pd.DataFrame) -> None:
        """Валидация типов данных в Document."""
        # STRING поля
        string_fields = ["document_chembl_id", "pubmed_id", "doi", "classification"]
        for field in string_fields:
            if field in data.columns:
                assert data[field].dtype == 'object', f"Field {field} should be string type"
        
        # BOOL поля
        bool_fields = ["document_contains_external_links", "is_experimental_doc"]
        for field in bool_fields:
            if field in data.columns:
                assert pd.api.types.is_bool_dtype(data[field]), f"Field {field} should be boolean type"

    def _validate_document_value_ranges(self, data: pd.DataFrame) -> None:
        """Валидация диапазонов значений в Document."""
        # Проверяем DOI паттерн
        if "doi" in data.columns:
            import re
            doi_pattern = re.compile(r'^10\.\d+/[^\s]+$')
            for value in data["doi"].dropna():
                assert doi_pattern.match(str(value)), f"Invalid DOI pattern: {value}"
        
        # Проверяем PMID паттерн
        if "pubmed_id" in data.columns:
            import re
            pmid_pattern = re.compile(r'^\d+$')
            for value in data["pubmed_id"].dropna():
                assert pmid_pattern.match(str(value)), f"Invalid PMID pattern: {value}"
        
        # Проверяем ChEMBL ID паттерн
        if "document_chembl_id" in data.columns:
            import re
            chembl_pattern = re.compile(r'^CHEMBL\d+$')
            for value in data["document_chembl_id"].dropna():
                assert chembl_pattern.match(str(value)), f"Invalid ChEMBL ID pattern: {value}"

    def test_document_pipeline_determinism(self, document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест детерминизма Document пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн дважды
        for run in [1, 2]:
            run_dir = temp_output_dir / f"run_{run}"
            run_dir.mkdir()
            
            cmd = [
                "bioactivity-data-acquisition", "get-document-data",
                "--config", "configs/config_document.yaml",
                "--documents-csv", str(input_file),
                "--output-dir", str(run_dir),
                "--limit", "3"
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                assert result.returncode == 0, f"Pipeline run {run} failed: {result.stderr}"
            except subprocess.TimeoutExpired:
                pytest.fail(f"Pipeline run {run} timed out")
            except FileNotFoundError:
                pytest.skip("CLI not found")
        
        # Сравниваем результаты
        run1_output = temp_output_dir / "run_1" / "documents.csv"
        run2_output = temp_output_dir / "run_2" / "documents.csv"
        
        if run1_output.exists() and run2_output.exists():
            df1 = pd.read_csv(run1_output)
            df2 = pd.read_csv(run2_output)
            
            # Проверяем что результаты идентичны
            pd.testing.assert_frame_equal(df1, df2, check_dtype=False)

    def test_document_pipeline_meta_files(self, document_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест создания meta файлов Document пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = document_input_data.head(5)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_document_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "5"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем meta файлы
            meta_files = list(temp_output_dir.glob("*.yaml")) + list(temp_output_dir.glob("*.json"))
            assert len(meta_files) > 0, "No meta files created"
            
            # Проверяем содержимое meta файла
            for meta_file in meta_files:
                if meta_file.suffix == '.yaml':
                    with open(meta_file, encoding="utf-8") as f:
                        meta_data = yaml.safe_load(f)
                    
                    # Проверяем обязательные поля
                    assert "pipeline_version" in meta_data, "pipeline_version missing from meta"
                    assert "chembl_release" in meta_data, "chembl_release missing from meta"
                    assert "row_count" in meta_data, "row_count missing from meta"
                    assert "checksums" in meta_data, "checksums missing from meta"
                    
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")

    def test_document_pipeline_error_handling(self, temp_output_dir: Path) -> None:
        """Тест обработки ошибок Document пайплайна."""
        pytest.mark.integration
        
        # Создаем невалидный входной файл
        invalid_data = pd.DataFrame({
            "invalid_column": ["value1", "value2"]
        })
        
        input_file = temp_output_dir / "invalid_input.csv"
        invalid_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн с невалидными данными
        cmd = [
            "bioactivity-data-acquisition", "get-document-data",
            "--config", "configs/config_document.yaml",
            "--documents-csv", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "2"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            # Пайплайн должен завершиться с ошибкой
            assert result.returncode != 0, "Pipeline should fail with invalid input"
            assert "error" in result.stderr.lower() or "validation" in result.stderr.lower(), \
                "Pipeline should report validation error"
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")