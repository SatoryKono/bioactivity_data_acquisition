"""Интеграционные тесты для проверки синхронизации унифицированных пайплайнов.

Проверяет, что все пайплайны используют единообразные компоненты и интерфейсы.
"""

import pytest
import pandas as pd
from typing import Any

from library.common.pipeline_base import PipelineBase
from library.common.error_tracking import ErrorTracker, ErrorType
from library.common.metadata import MetadataBuilder
from library.common.qc_profiles import QCValidator, QCProfile
from library.common.postprocess_base import BasePostprocessor
from library.common.writer_base import ETLWriter, ETLResult


class MockConfig:
    """Мок конфигурации для тестирования."""
    
    def __init__(self, entity_type: str):
        self.entity_type = entity_type
        self.pipeline = {"name": entity_type, "version": "2.0.0"}
        self.quality = {
            "profiles": {
                "default": {
                    "name": "default",
                    "description": f"Test profile for {entity_type}",
                    "fail_on_criteria": ["missing_id"],
                    "thresholds": [
                        {
                            "metric": "missing_id_rate",
                            "threshold": 0.1,
                            "fail_on_exceed": True
                        }
                    ],
                    "rules": [
                        {
                            "name": "valid_id",
                            "description": "Test rule",
                            "enabled": True,
                            "parameters": {"pattern": "^TEST\\d+$"}
                        }
                    ]
                }
            }
        }
        self.error_tracking = {
            "enabled": True,
            "log_level": "INFO",
            "max_errors_per_source": 1000
        }
        self.postprocess = {
            "steps": [
                {
                    "name": "test_step",
                    "enabled": True,
                    "parameters": {},
                    "priority": 0
                }
            ]
        }


class MockPipeline(PipelineBase):
    """Мок пайплайна для тестирования."""
    
    def __init__(self, config: MockConfig):
        self.config = config
        super().__init__(config)
        self._initialize_unified_components()
    
    def _setup_clients(self) -> None:
        """Мок настройки клиентов."""
        pass
    
    def _get_entity_type(self) -> str:
        """Получить тип сущности."""
        return self.config.entity_type
    
    def _create_qc_validator(self) -> QCValidator:
        """Создать QC валидатор."""
        profile = QCProfile(**self.config.quality["profiles"]["default"])
        return MockQCValidator(self.config, profile)
    
    def _create_postprocessor(self) -> BasePostprocessor:
        """Создать постпроцессор."""
        return MockPostprocessor(self.config)
    
    def _create_etl_writer(self) -> ETLWriter:
        """Создать ETL writer."""
        return MockETLWriter(self.config, self._get_entity_type())
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Мок извлечения данных."""
        return pd.DataFrame({
            "id": ["TEST001", "TEST002"],
            "name": ["Test 1", "Test 2"],
            "value": [1.0, 2.0]
        })
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Мок нормализации данных."""
        return raw_data.copy()
    
    def validate(self, normalized_data: pd.DataFrame) -> pd.DataFrame:
        """Мок валидации данных."""
        return normalized_data.copy()
    
    def filter_quality(self, validated_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Мок фильтрации по качеству."""
        return validated_data, pd.DataFrame()
    
    def build_qc_report(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """Мок генерации QC отчетов."""
        qc_summary = pd.DataFrame({
            "metric": ["total_records", "valid_records"],
            "value": [len(data), len(data)]
        })
        return qc_summary, None
    
    def _should_build_correlation(self) -> bool:
        """Определить, нужен ли корреляционный анализ."""
        return False
    
    def _build_correlation(self, data: pd.DataFrame) -> tuple[dict[str, Any] | None, dict[str, pd.DataFrame] | None, dict[str, Any] | None]:
        """Мок корреляционного анализа."""
        return None, None, None
    
    def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
        """Мок построения метаданных."""
        return {"total_records": len(data)}


class MockQCValidator(QCValidator):
    """Мок QC валидатора."""
    
    def __init__(self, config: MockConfig, profile: QCProfile):
        super().__init__(profile)
    
    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Мок валидации качества."""
        return {
            "total_records": len(df),
            "valid_records": len(df),
            "missing_id_rate": 0.0
        }


class MockPostprocessor(BasePostprocessor):
    """Мок постпроцессора."""
    
    def apply_steps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Мок применения шагов постобработки."""
        return df.copy()
    
    def merge_sources(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Мок объединения источников."""
        return df.copy()
    
    def deduplicate(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Мок дедупликации."""
        return df.copy()


class MockETLWriter(ETLWriter):
    """Мок ETL writer."""
    
    def get_sort_columns(self) -> list[str]:
        """Получить колонки для сортировки."""
        return ["id"]
    
    def get_column_order(self) -> list[str] | None:
        """Получить порядок колонок."""
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Получить колонки для исключения."""
        return []


class TestUnifiedPipelineSynchronization:
    """Тесты синхронизации унифицированных пайплайнов."""
    
    def test_pipeline_initialization(self):
        """Тест инициализации пайплайна с унифицированными компонентами."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Проверяем, что все компоненты инициализированы
        assert isinstance(pipeline.error_tracker, ErrorTracker)
        assert isinstance(pipeline.metadata_builder, MetadataBuilder)
        assert isinstance(pipeline.qc_validator, QCValidator)
        assert isinstance(pipeline.postprocessor, BasePostprocessor)
        assert isinstance(pipeline.etl_writer, ETLWriter)
        
        # Проверяем, что тип сущности установлен правильно
        assert pipeline.error_tracker.entity_type == "test_entity"
        assert pipeline.metadata_builder.entity_type == "test_entity"
    
    def test_error_tracking_integration(self):
        """Тест интеграции error tracking."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Тестируем отслеживание ошибок
        pipeline._track_extraction_error("test_source", "Test error", {"detail": "test"})
        pipeline._track_validation_error("test_source", "Validation error", "TEST001")
        
        # Проверяем, что ошибки добавлены
        assert pipeline.error_tracker is not None
        assert len(pipeline.error_tracker.errors) == 2
        assert pipeline.error_tracker.errors[0].error_type == "extraction"
        assert pipeline.error_tracker.errors[1].error_type == "validation"
        
        # Проверяем сводку ошибок
        summary = pipeline.error_tracker.get_error_summary()
        assert summary["total_errors"] == 2
        assert summary["errors_by_type"]["extraction"] == 1
        assert summary["errors_by_type"]["validation"] == 1
    
    def test_qc_validation_integration(self):
        """Тест интеграции QC валидации."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Создаем тестовые данные
        test_data = pd.DataFrame({
            "id": ["TEST001", "TEST002"],
            "name": ["Test 1", "Test 2"],
            "value": [1.0, 2.0]
        })
        
        # Тестируем валидацию качества
        quality_results = pipeline._validate_data_quality(test_data)
        
        # Проверяем результаты
        assert "total_records" in quality_results
        assert "valid_records" in quality_results
        assert "missing_id_rate" in quality_results
        assert quality_results["total_records"] == 2
        assert quality_results["valid_records"] == 2
        assert quality_results["missing_id_rate"] == 0.0
    
    def test_postprocessing_integration(self):
        """Тест интеграции постобработки."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Создаем тестовые данные
        test_data = pd.DataFrame({
            "id": ["TEST001", "TEST002"],
            "name": ["Test 1", "Test 2"],
            "value": [1.0, 2.0]
        })
        
        # Тестируем постобработку
        processed_data = pipeline._apply_postprocessing(test_data)
        
        # Проверяем, что данные обработаны
        assert len(processed_data) == len(test_data)
        assert list(processed_data.columns) == list(test_data.columns)
    
    def test_unified_pipeline_execution(self):
        """Тест выполнения унифицированного пайплайна."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Создаем входные данные
        input_data = pd.DataFrame({
            "id": ["TEST001", "TEST002"],
            "name": ["Test 1", "Test 2"]
        })
        
        # Запускаем унифицированный пайплайн
        result = pipeline.run_unified(input_data)
        
        # Проверяем результат
        assert isinstance(result, ETLResult)
        assert len(result.data) == 2
        assert result.accepted_data is not None
        assert result.rejected_data is None or result.rejected_data.empty
        assert result.qc_summary is not None
        assert result.metadata is not None
        assert result.error_tracker is not None
        
        # Проверяем метаданные
        assert result.metadata.pipeline_name == "test_entity"
        assert result.metadata.status == "COMPLETED"
        assert result.metadata.data_stats["total_records"] == 2
    
    def test_error_handling_in_pipeline(self):
        """Тест обработки ошибок в пайплайне."""
        config = MockConfig("test_entity")
        pipeline = MockPipeline(config)
        
        # Создаем пайплайн, который будет падать
        class FailingPipeline(MockPipeline):
            def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
                raise Exception("Test extraction error")
        
        failing_pipeline = FailingPipeline(config)
        
        # Создаем входные данные
        input_data = pd.DataFrame({"id": ["TEST001"]})
        
        # Проверяем, что ошибка обрабатывается
        with pytest.raises(Exception, match="Test extraction error"):
            failing_pipeline.run_unified(input_data)
        
        # Проверяем, что ошибка отслежена
        assert failing_pipeline.error_tracker is not None
        assert len(failing_pipeline.error_tracker.errors) > 0
        assert failing_pipeline.error_tracker.errors[-1].error_type == "load"
    
    def test_multiple_entity_types(self):
        """Тест работы с разными типами сущностей."""
        entity_types = ["documents", "targets", "assays", "activities", "testitems"]
        
        for entity_type in entity_types:
            config = MockConfig(entity_type)
            pipeline = MockPipeline(config)
            
            # Проверяем, что тип сущности установлен правильно
            assert pipeline._get_entity_type() == entity_type
            assert pipeline.error_tracker is not None
            assert pipeline.error_tracker.entity_type == entity_type
            assert pipeline.metadata_builder is not None
            assert pipeline.metadata_builder.entity_type == entity_type
            
            # Проверяем, что пайплайн может быть запущен
            input_data = pd.DataFrame({"id": ["TEST001"]})
            result = pipeline.run_unified(input_data)
            
            assert result.metadata is not None
            assert result.metadata.pipeline_name == entity_type
    
    def test_config_consistency(self):
        """Тест согласованности конфигураций."""
        entity_types = ["documents", "targets", "assays", "activities", "testitems"]
        
        for entity_type in entity_types:
            config = MockConfig(entity_type)
            
            # Проверяем, что конфигурация содержит необходимые секции
            assert hasattr(config, 'quality')
            assert hasattr(config, 'error_tracking')
            assert hasattr(config, 'postprocess')
            
            # Проверяем, что QC профиль настроен
            assert 'default' in config.quality['profiles']
            assert config.quality['profiles']['default']['enabled'] is True
            
            # Проверяем, что error tracking включен
            assert config.error_tracking['enabled'] is True
            
            # Проверяем, что постобработка настроена
            assert 'steps' in config.postprocess
            assert len(config.postprocess['steps']) > 0


if __name__ == "__main__":
    pytest.main([__file__])
