"""Базовые тесты для target пайплайна."""

from unittest.mock import patch

import pandas as pd
import pytest

from library.schemas.target_schema import TargetInputSchema, TargetNormalizedSchema
from library.target import (
    TargetConfig,
    TargetETLResult,
    TargetIOError,
    TargetValidationError,
    load_target_config,
    read_target_input,
    run_target_etl,
    write_target_outputs,
)


class TestTargetConfig:
    """Тесты для конфигурации target пайплайна."""

    def test_default_config_creation(self):
        """Тест создания конфигурации по умолчанию."""
        config = TargetConfig()
        
        # Проверяем, что все источники включены по умолчанию
        assert config.sources["chembl"].enabled is True
        assert config.sources["uniprot"].enabled is True
        assert config.sources["iuphar"].enabled is True
        
        # Проверяем базовые настройки
        assert config.runtime.workers == 4
        assert config.runtime.dry_run is False
        assert config.runtime.dev_mode is False
        assert config.runtime.allow_incomplete_sources is False

    def test_config_validation_all_sources_required(self):
        """Тест валидации конфигурации - все источники обязательны."""
        # Конфигурация с отключенным источником должна вызывать ошибку
        with pytest.raises(ValueError, match="All sources.*must be enabled"):
            TargetConfig(
                sources={
                    "chembl": {"enabled": True},
                    "uniprot": {"enabled": False},  # Отключен
                    "iuphar": {"enabled": True},
                }
            )

    def test_config_dev_mode_allows_incomplete_sources(self):
        """Тест dev режима - позволяет неполные источники."""
        # В dev режиме можно отключить источники
        config = TargetConfig(
            runtime={"dev_mode": True},
            sources={
                "chembl": {"enabled": True},
                "uniprot": {"enabled": False},  # Отключен в dev режиме
                "iuphar": {"enabled": True},
            }
        )
        
        assert config.runtime.dev_mode is True
        assert config.sources["uniprot"].enabled is False

    def test_config_allow_incomplete_sources(self):
        """Тест allow_incomplete_sources флага."""
        config = TargetConfig(
            runtime={"allow_incomplete_sources": True},
            sources={
                "chembl": {"enabled": True},
                "uniprot": {"enabled": False},
                "iuphar": {"enabled": True},
            }
        )
        
        assert config.runtime.allow_incomplete_sources is True
        assert config.sources["uniprot"].enabled is False

    def test_config_load_from_file(self, tmp_path):
        """Тест загрузки конфигурации из файла."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text("""
runtime:
  workers: 8
  dev_mode: true
sources:
  chembl:
    enabled: true
  uniprot:
    enabled: true
  iuphar:
    enabled: true
""")
        
        config = load_target_config(config_file)
        
        assert config.runtime.workers == 8
        assert config.runtime.dev_mode is True
        assert config.sources["chembl"].enabled is True

    def test_config_load_with_overrides(self, tmp_path):
        """Тест загрузки конфигурации с переопределениями."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text("""
runtime:
  workers: 4
sources:
  chembl:
    enabled: true
  uniprot:
    enabled: true
  iuphar:
    enabled: true
""")
        
        overrides = {
            "runtime": {"workers": 16, "limit": 100},
            "sources": {"chembl": {"enabled": False}}
        }
        
        config = load_target_config(config_file, overrides=overrides)
        
        assert config.runtime.workers == 16
        assert config.runtime.limit == 100
        assert config.sources["chembl"].enabled is False


class TestTargetInput:
    """Тесты для чтения входных данных."""

    def test_read_target_input_success(self, tmp_path):
        """Тест успешного чтения входных данных."""
        input_file = tmp_path / "targets.csv"
        input_file.write_text("target_chembl_id\nCHEMBL240\nCHEMBL251\nCHEMBL262")
        
        df = read_target_input(input_file)
        
        assert len(df) == 3
        assert "target_chembl_id" in df.columns
        assert df["target_chembl_id"].tolist() == ["CHEMBL240", "CHEMBL251", "CHEMBL262"]

    def test_read_target_input_file_not_found(self, tmp_path):
        """Тест ошибки при отсутствии файла."""
        input_file = tmp_path / "nonexistent.csv"
        
        with pytest.raises(TargetIOError, match="Input CSV not found"):
            read_target_input(input_file)

    def test_read_target_input_empty_file(self, tmp_path):
        """Тест ошибки при пустом файле."""
        input_file = tmp_path / "empty.csv"
        input_file.write_text("")
        
        with pytest.raises(TargetValidationError, match="Input CSV is empty"):
            read_target_input(input_file)


class TestTargetSchemas:
    """Тесты для Pandera схем."""

    def test_target_input_schema_validation(self):
        """Тест валидации схемы входных данных."""
        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL240", "CHEMBL251", "CHEMBL262"]
        })
        
        # Должно пройти валидацию
        validated_df = TargetInputSchema.validate(df)
        assert len(validated_df) == 3

    def test_target_input_schema_missing_column(self):
        """Тест ошибки валидации при отсутствии обязательной колонки."""
        df = pd.DataFrame({
            "wrong_column": ["CHEMBL240", "CHEMBL251"]
        })
        
        with pytest.raises(Exception):  # Pandera validation error
            TargetInputSchema.validate(df)

    def test_target_normalized_schema_validation(self):
        """Тест валидации схемы нормализованных данных."""
        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL240", "CHEMBL251"],
            "pref_name": ["Target 1", "Target 2"],
            "uniprot_id_primary": ["P12345", "P67890"],
            "iuphar_target_id": ["1", "2"],
        })
        
        # Должно пройти валидацию
        validated_df = TargetNormalizedSchema.validate(df)
        assert len(validated_df) == 2


class TestTargetETL:
    """Тесты для ETL процесса."""

    @patch('library.target.run_target_etl')
    def test_run_target_etl_mock(self, mock_etl):
        """Тест ETL процесса с моком."""
        # Настраиваем мок
        mock_result = TargetETLResult(
            targets=pd.DataFrame({
                "target_chembl_id": ["CHEMBL240"],
                "pref_name": ["Test Target"],
            }),
            qc=pd.DataFrame([{"metric": "row_count", "value": 1}]),
            meta={"pipeline_version": "1.0.0", "row_count": 1},
        )
        mock_etl.return_value = mock_result
        
        config = TargetConfig(runtime={"dev_mode": True})
        input_df = pd.DataFrame({"target_chembl_id": ["CHEMBL240"]})
        
        result = run_target_etl(config, input_frame=input_df)
        
        assert len(result.targets) == 1
        assert result.targets["target_chembl_id"].iloc[0] == "CHEMBL240"
        assert len(result.qc) == 1
        assert result.meta["row_count"] == 1

    def test_run_target_etl_empty_input(self):
        """Тест ETL процесса с пустыми входными данными."""
        config = TargetConfig(runtime={"dev_mode": True})
        input_df = pd.DataFrame({"target_chembl_id": []})
        
        result = run_target_etl(config, input_frame=input_df)
        
        assert len(result.targets) == 0
        assert result.qc["value"].iloc[0] == 0
        assert result.meta["row_count"] == 0

    def test_run_target_etl_missing_input(self):
        """Тест ETL процесса без входных данных."""
        config = TargetConfig(runtime={"dev_mode": True})
        
        with pytest.raises(TargetValidationError, match="Either target_ids or input_frame must be provided"):
            run_target_etl(config)


class TestTargetOutput:
    """Тесты для записи выходных данных."""

    def test_write_target_outputs_success(self, tmp_path):
        """Тест успешной записи выходных данных."""
        result = TargetETLResult(
            targets=pd.DataFrame({
                "target_chembl_id": ["CHEMBL240"],
                "pref_name": ["Test Target"],
            }),
            qc=pd.DataFrame([{"metric": "row_count", "value": 1}]),
            meta={"pipeline_version": "1.0.0", "row_count": 1},
        )
        
        config = TargetConfig()
        output_dir = tmp_path / "output"
        date_tag = "20251020"
        
        outputs = write_target_outputs(result, output_dir, date_tag, config)
        
        # Проверяем, что файлы созданы
        assert "csv" in outputs
        assert "qc" in outputs
        assert "meta" in outputs
        
        # Проверяем существование файлов
        assert outputs["csv"].exists()
        assert outputs["qc"].exists()
        assert outputs["meta"].exists()
        
        # Проверяем содержимое CSV файла
        csv_df = pd.read_csv(outputs["csv"])
        assert len(csv_df) == 1
        assert csv_df["target_chembl_id"].iloc[0] == "CHEMBL240"

    def test_write_target_outputs_with_correlation(self, tmp_path):
        """Тест записи выходных данных с корреляционным анализом."""
        result = TargetETLResult(
            targets=pd.DataFrame({
                "target_chembl_id": ["CHEMBL240", "CHEMBL251"],
                "pref_name": ["Target 1", "Target 2"],
            }),
            qc=pd.DataFrame([{"metric": "row_count", "value": 2}]),
            meta={"pipeline_version": "1.0.0", "row_count": 2},
            correlation_analysis={"test": "analysis"},
            correlation_reports={"test_report": pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})},
            correlation_insights=[{"insight": "test"}],
        )
        
        config = TargetConfig()
        output_dir = tmp_path / "output"
        date_tag = "20251020"
        
        outputs = write_target_outputs(result, output_dir, date_tag, config)
        
        # Проверяем, что корреляционные отчеты созданы
        correlation_files = [name for name in outputs.keys() if name.startswith("correlation_")]
        assert len(correlation_files) > 0


class TestTargetIntegration:
    """Интеграционные тесты для target пайплайна."""

    def test_full_pipeline_mock(self, tmp_path):
        """Тест полного пайплайна с моками."""
        # Создаем входной файл
        input_file = tmp_path / "targets.csv"
        input_file.write_text("target_chembl_id\nCHEMBL240\nCHEMBL251")
        
        # Создаем конфигурацию
        config = TargetConfig(runtime={"dev_mode": True, "limit": 2})
        
        # Мокаем внешние зависимости
        with patch('library.target.get_targets') as mock_get_targets, \
             patch('library.target.enrich_targets_with_uniprot') as mock_uniprot, \
             patch('library.target.enrich_targets_with_iuphar') as mock_iuphar:
            
            # Настраиваем моки
            mock_get_targets.return_value = pd.DataFrame({
                "target_chembl_id": ["CHEMBL240", "CHEMBL251"],
                "pref_name": ["Target 1", "Target 2"],
            })
            
            mock_uniprot.return_value = pd.DataFrame({
                "target_chembl_id": ["CHEMBL240", "CHEMBL251"],
                "pref_name": ["Target 1", "Target 2"],
                "uniprot_id_primary": ["P12345", "P67890"],
            })
            
            mock_iuphar.return_value = pd.DataFrame({
                "target_chembl_id": ["CHEMBL240", "CHEMBL251"],
                "pref_name": ["Target 1", "Target 2"],
                "uniprot_id_primary": ["P12345", "P67890"],
                "iuphar_target_id": ["1", "2"],
            })
            
            # Читаем входные данные
            input_df = read_target_input(input_file)
            
            # Запускаем ETL
            result = run_target_etl(config, input_frame=input_df)
            
            # Проверяем результат
            assert len(result.targets) == 2
            assert "target_chembl_id" in result.targets.columns
            assert "uniprot_id_primary" in result.targets.columns
            assert "iuphar_target_id" in result.targets.columns
            assert len(result.qc) > 0
            assert result.meta["row_count"] == 2


if __name__ == "__main__":
    pytest.main([__file__])
