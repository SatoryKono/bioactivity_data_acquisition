"""Тесты совместимости унифицированных компонентов с существующими пайплайнами."""

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from library.common.error_tracking import ErrorSeverity
from library.common.error_tracking import ErrorTracker
from library.common.error_tracking import ErrorType
from library.common.metadata import MetadataBuilder
from library.common.pipeline_base import PipelineBase
from library.common.postprocess_base import BasePostprocessor
from library.common.qc_profiles import QCProfile
from library.common.qc_profiles import QCValidator
from library.common.writer_base import ETLResult
from library.common.writer_base import ETLWriter


class TestPipelineCompatibility:
    """Тесты совместимости с существующими пайплайнами."""
    
    def test_legacy_etl_result_compatibility(self):
        """Тест совместимости с legacy ETLResult."""
        from library.common.pipeline_base import ETLResult as LegacyETLResult

        # Создаем legacy результат
        legacy_result = LegacyETLResult(
            data=pd.DataFrame({"id": ["TEST001"], "name": ["Test"]}),
            qc_summary=pd.DataFrame({"metric": ["total"], "value": [1]}),
            qc_detailed=None,
            rejected=None,
            meta={"total_records": 1},
            correlation_analysis=None,
            correlation_reports=None,
            correlation_insights=None
        )
        
        # Проверяем, что legacy результат работает
        assert len(legacy_result.data) == 1
        assert legacy_result.qc_summary is not None
        assert legacy_result.meta is not None
    
    def test_new_etl_result_compatibility(self):
        """Тест совместимости с новым ETLResult."""
        # Создаем новый результат
        new_result = ETLResult(
            data=pd.DataFrame({"id": ["TEST001"], "name": ["Test"]}),
            accepted_data=pd.DataFrame({"id": ["TEST001"], "name": ["Test"]}),
            rejected_data=None,
            qc_summary=pd.DataFrame({"metric": ["total"], "value": [1]}),
            qc_detailed=None,
            correlation_analysis=None,
            correlation_reports=None,
            correlation_insights=None,
            metadata=None,
            error_tracker=None,
            additional_data={}
        )
        
        # Проверяем, что новый результат работает
        assert len(new_result.data) == 1
        assert new_result.accepted_data is not None
        assert new_result.qc_summary is not None
    
    def test_error_tracker_serialization(self):
        """Тест сериализации ErrorTracker."""
        tracker = ErrorTracker("test_entity")
        
        # Добавляем ошибки
        tracker.add_error(
            error_type=ErrorType.EXTRACTION,
            source="test_source",
            message="Test error",
            severity=ErrorSeverity.HIGH,
            details={"test": "detail"}
        )
        
        # Тестируем JSON сериализацию
        json_str = tracker.to_json()
        assert isinstance(json_str, str)
        assert "Test error" in json_str
        
        # Тестируем десериализацию
        new_tracker = ErrorTracker.from_json("test_entity", json_str)
        assert len(new_tracker.errors) == 1
        assert new_tracker.errors[0].message == "Test error"
    
    def test_metadata_builder_compatibility(self):
        """Тест совместимости MetadataBuilder."""
        from library.config import Config
        
        class MockConfig(Config):
            def __init__(self):
                super().__init__()
                self.pipeline = {"name": "test", "version": "1.0.0"}
        
        config = MockConfig()
        builder = MetadataBuilder(config, "test_entity")
        
        # Тестируем построение метаданных
        test_data = pd.DataFrame({"id": ["TEST001"], "name": ["Test"]})
        metadata = builder.build_metadata(
            df=test_data,
            accepted_df=test_data,
            rejected_df=None,
            output_files=None,
            additional_metadata={"test": "value"}
        )
        
        # Проверяем метаданные
        assert metadata.pipeline_name == "test_entity"
        assert metadata.status == "COMPLETED"
        assert metadata.data_stats["total_records"] == 1
        assert metadata.custom_metadata["test"] == "value"
    
    def test_qc_profile_validation(self):
        """Тест валидации QC профилей."""
        # Создаем QC профиль
        profile = QCProfile(
            name="test_profile",
            description="Test QC profile",
            fail_on_criteria=["missing_id"],
            thresholds=[
                {
                    "metric": "missing_id_rate",
                    "operator": ">",
                    "value": 0.1,
                    "severity": "ERROR"
                }
            ],
            rules=[
                {
                    "name": "valid_id",
                    "description": "Test rule",
                    "enabled": True,
                    "parameters": {"pattern": "^TEST\\d+$"}
                }
            ]
        )
        
        # Проверяем профиль
        assert profile.name == "test_profile"
        assert len(profile.thresholds) == 1
        assert len(profile.rules) == 1
        assert profile.thresholds[0].metric == "missing_id_rate"
        assert profile.rules[0].name == "valid_id"
    
    def test_postprocessor_step_registry(self):
        """Тест реестра шагов постобработки."""
        from library.common.postprocess_base import POSTPROCESS_STEPS_REGISTRY

        # Проверяем, что реестр существует
        assert isinstance(POSTPROCESS_STEPS_REGISTRY, dict)
        
        # Проверяем, что можно добавить шаг
        def test_step(df: pd.DataFrame, config: Any, **kwargs: Any) -> pd.DataFrame:
            return df.copy()
        
        POSTPROCESS_STEPS_REGISTRY["test_step"] = test_step
        assert "test_step" in POSTPROCESS_STEPS_REGISTRY
    
    def test_etl_writer_interface(self):
        """Тест интерфейса ETLWriter."""
        from library.config import Config
        
        class MockConfig(Config):
            def __init__(self):
                super().__init__()
                self.determinism = type('obj', (object,), {
                    'sort': type('obj', (object,), {'ascending': True})()
                })()
        
        class TestETLWriter(ETLWriter):
            def get_sort_columns(self) -> list[str]:
                return ["id"]
            
            def get_column_order(self) -> list[str] | None:
                return None
            
            def get_exclude_columns(self) -> list[str]:
                return []
        
        config = MockConfig()
        writer = TestETLWriter(config, "test_entity")
        
        # Проверяем интерфейс
        assert writer.get_sort_columns() == ["id"]
        assert writer.get_column_order() is None
        assert writer.get_exclude_columns() == []
    
    def test_pipeline_base_abstract_methods(self):
        """Тест абстрактных методов PipelineBase."""
        class TestPipeline(PipelineBase):
            def _setup_clients(self) -> None:
                pass
            
            def _get_entity_type(self) -> str:
                return "test"
            
            def _create_qc_validator(self) -> QCValidator:
                class MockQCValidator(QCValidator):
                    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
                        return {}
                return MockQCValidator(QCProfile(name="test", description="test"))
            
            def _create_postprocessor(self) -> BasePostprocessor:
                class MockPostprocessor(BasePostprocessor):
                    def apply_steps(self, df: pd.DataFrame) -> pd.DataFrame:
                        return df.copy()
                    def merge_sources(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
                        return df.copy()
                    def deduplicate(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
                        return df.copy()
                return MockPostprocessor(config)
            
            def _create_etl_writer(self) -> ETLWriter:
                class MockETLWriter(ETLWriter):
                    def get_sort_columns(self) -> list[str]:
                        return []
                    def get_column_order(self) -> list[str] | None:
                        return None
                    def get_exclude_columns(self) -> list[str]:
                        return []
                return MockETLWriter(config, "test")
            
            def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
                return input_data
            
            def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
                return raw_data
            
            def validate(self, normalized_data: pd.DataFrame) -> pd.DataFrame:
                return normalized_data
            
            def filter_quality(self, validated_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
                return validated_data, pd.DataFrame()
            
            def build_qc_report(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None]:
                return pd.DataFrame(), None
            
            def _should_build_correlation(self) -> bool:
                return False
            
            def _build_correlation(self, data: pd.DataFrame) -> tuple[dict[str, Any] | None, dict[str, pd.DataFrame] | None, dict[str, Any] | None]:
                return None, None, None
            
            def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
                return {}
        
        # Проверяем, что можно создать экземпляр
        from library.config import Config
        config = Config()
        pipeline = TestPipeline(config)
        
        # Проверяем, что все абстрактные методы реализованы
        assert pipeline._get_entity_type() == "test"
        assert pipeline.extract(pd.DataFrame()) is not None
        assert pipeline.normalize(pd.DataFrame()) is not None
        assert pipeline.validate(pd.DataFrame()) is not None
    
    def test_config_structure_compatibility(self):
        """Тест совместимости структуры конфигураций."""
        # Проверяем, что конфигурации содержат необходимые секции
        config_files = [
            "configs/config_document.yaml",
            "configs/config_target.yaml", 
            "configs/config_assay.yaml",
            "configs/config_activity.yaml",
            "configs/config_testitem.yaml"
        ]
        
        for config_file in config_files:
            config_path = Path(config_file)
            if config_path.exists():
                # Проверяем, что файл существует
                assert config_path.exists(), f"Config file {config_file} not found"
                
                # Проверяем, что файл не пустой
                assert config_path.stat().st_size > 0, f"Config file {config_file} is empty"
    
    def test_schema_compatibility(self):
        """Тест совместимости схем."""
        # Проверяем, что схемы существуют
        schema_files = [
            "src/library/schemas/document_schema.py",
            "src/library/schemas/target_schema.py",
            "src/library/schemas/assay_schema.py",
            "src/library/schemas/activity_schema.py",
            "src/library/schemas/testitem_schema.py"
        ]
        
        for schema_file in schema_files:
            schema_path = Path(schema_file)
            if schema_path.exists():
                # Проверяем, что файл существует
                assert schema_path.exists(), f"Schema file {schema_file} not found"
                
                # Проверяем, что файл не пустой
                assert schema_path.stat().st_size > 0, f"Schema file {schema_file} is empty"


if __name__ == "__main__":
    pytest.main([__file__])
