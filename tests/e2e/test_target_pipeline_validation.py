"""E2E тесты валидации Target пайплайна с реальными данными."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from tests.schemas.test_column_order_validation import validate_column_order, validate_determinism, validate_pandera_schema


class TestTargetPipelineValidation:
    """E2E тесты валидации Target пайплайна."""

    @pytest.fixture
    def target_config(self) -> dict[str, Any]:
        """Конфигурация Target пайплайна."""
        config_path = Path("configs/config_target.yaml")
        if not config_path.exists():
            pytest.skip("Target config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def target_input_data(self) -> pd.DataFrame:
        """Входные данные для Target пайплайна."""
        input_path = Path("data/input/target.csv")
        if not input_path.exists():
            pytest.skip("Target input data not found")
        
        return pd.read_csv(input_path)

    @pytest.fixture
    def temp_output_dir(self) -> Path:
        """Временная директория для выходных файлов."""
        return Path(tempfile.mkdtemp())

    def test_target_pipeline_smoke_test(self, target_config: dict[str, Any], target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Smoke test Target пайплайна с --limit 2."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(2)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн через CLI
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
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

    def test_target_pipeline_validation_test(self, target_config: dict[str, Any], target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Validation test Target пайплайна с --limit 10."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(10)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн через CLI
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
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
            main_output = temp_output_dir / "targets.csv"
            if main_output.exists():
                self._validate_target_output(main_output, target_config)
            
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out after 10 minutes")
        except FileNotFoundError:
            pytest.skip("bioactivity-data-acquisition CLI not found")

    def _validate_target_output(self, output_file: Path, config: dict[str, Any]) -> None:
        """Валидация выходного файла Target пайплайна."""
        # Читаем выходные данные
        output_data = pd.read_csv(output_file)
        
        # Проверяем что данные не пустые
        assert len(output_data) > 0, "Output data is empty"
        
        # Проверяем column_order
        column_order = config["determinism"]["column_order"]
        validate_column_order(output_data, column_order)
        
        # Проверяем Pandera схему
        from library.schemas.target_schema import TargetRawSchema
        try:
            validated_data = validate_pandera_schema(output_data, TargetRawSchema)
            assert len(validated_data) == len(output_data)
        except Exception as e:
            pytest.fail(f"Target schema validation failed: {e}")
        
        # Проверяем обязательные поля
        required_fields = ["target_chembl_id"]
        for field in required_fields:
            assert field in output_data.columns, f"Required field {field} missing"
            assert not output_data[field].isna().any(), f"Required field {field} contains NULL values"
        
        # Проверяем детерминизм
        validate_determinism(output_file)

    def test_target_pipeline_column_order_consistency(self, target_config: dict[str, Any], target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест консистентности column_order в Target пайплайне."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(5)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "5"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "targets.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Проверяем что порядок колонок соответствует конфигурации
                column_order = target_config["determinism"]["column_order"]
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

    def test_target_pipeline_schema_validation(self, target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест валидации схемы Target пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "3"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "targets.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Валидируем по Pandera схеме
                from library.schemas.target_schema import TargetRawSchema
                try:
                    validated_data = validate_pandera_schema(output_data, TargetRawSchema)
                    assert len(validated_data) == len(output_data)
                    
                    # Проверяем типы данных
                    self._validate_target_data_types(validated_data)
                    
                    # Проверяем диапазоны значений
                    self._validate_target_value_ranges(validated_data)
                    
                except Exception as e:
                    pytest.fail(f"Target schema validation failed: {e}")
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")

    def _validate_target_data_types(self, data: pd.DataFrame) -> None:
        """Валидация типов данных в Target."""
        # STRING поля
        string_fields = ["target_chembl_id", "pref_name", "gene", "uniprot_id"]
        for field in string_fields:
            if field in data.columns:
                assert data[field].dtype == 'object', f"Field {field} should be string type"
        
        # INT поля
        int_fields = ["component_id", "tax_id", "sequence_length"]
        for field in int_fields:
            if field in data.columns:
                assert pd.api.types.is_integer_dtype(data[field]), f"Field {field} should be integer type"
        
        # BOOL поля
        bool_fields = ["species_group_flag", "features_signal_peptide", "features_transmembrane"]
        for field in bool_fields:
            if field in data.columns:
                assert pd.api.types.is_bool_dtype(data[field]), f"Field {field} should be boolean type"

    def _validate_target_value_ranges(self, data: pd.DataFrame) -> None:
        """Валидация диапазонов значений в Target."""
        # Проверяем tax_id > 0
        if "tax_id" in data.columns:
            for value in data["tax_id"].dropna():
                assert value > 0, f"tax_id should be > 0, got {value}"
        
        # Проверяем sequence_length > 0
        if "sequence_length" in data.columns:
            for value in data["sequence_length"].dropna():
                assert value > 0, f"sequence_length should be > 0, got {value}"
        
        # Проверяем component_id > 0
        if "component_id" in data.columns:
            for value in data["component_id"].dropna():
                assert value > 0, f"component_id should be > 0, got {value}"
        
        # Проверяем HGNC ID паттерн
        if "hgnc_id" in data.columns:
            import re
            hgnc_pattern = re.compile(r'^HGNC:\d+$')
            for value in data["hgnc_id"].dropna():
                assert hgnc_pattern.match(str(value)), f"Invalid HGNC ID pattern: {value}"
        
        # Проверяем UniProt ID паттерн
        if "uniprot_id" in data.columns:
            import re
            uniprot_pattern = re.compile(r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$')
            for value in data["uniprot_id"].dropna():
                assert uniprot_pattern.match(str(value)), f"Invalid UniProt ID pattern: {value}"

    def test_target_pipeline_determinism(self, target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест детерминизма Target пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн дважды
        for run in [1, 2]:
            run_dir = temp_output_dir / f"run_{run}"
            run_dir.mkdir()
            
            cmd = [
                "bioactivity-data-acquisition", "get-target-data",
                "--config", "configs/config_target.yaml",
                "--input", str(input_file),
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
        run1_output = temp_output_dir / "run_1" / "targets.csv"
        run2_output = temp_output_dir / "run_2" / "targets.csv"
        
        if run1_output.exists() and run2_output.exists():
            df1 = pd.read_csv(run1_output)
            df2 = pd.read_csv(run2_output)
            
            # Проверяем что результаты идентичны
            pd.testing.assert_frame_equal(df1, df2, check_dtype=False)

    def test_target_pipeline_meta_files(self, target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест создания meta файлов Target пайплайна."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(5)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
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

    def test_target_pipeline_error_handling(self, temp_output_dir: Path) -> None:
        """Тест обработки ошибок Target пайплайна."""
        pytest.mark.integration
        
        # Создаем невалидный входной файл
        invalid_data = pd.DataFrame({
            "invalid_column": ["value1", "value2"]
        })
        
        input_file = temp_output_dir / "invalid_input.csv"
        invalid_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн с невалидными данными
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
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

    def test_target_pipeline_uniprot_integration(self, target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест интеграции с UniProt в Target пайплайне."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "3"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "targets.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Проверяем что есть данные из UniProt
                uniprot_fields = ["uniprot_id_primary", "recommendedName", "geneName", "sequence_length"]
                for field in uniprot_fields:
                    if field in output_data.columns:
                        non_null_count = output_data[field].notna().sum()
                        assert non_null_count > 0, f"UniProt field {field} should have some non-null values"
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")

    def test_target_pipeline_hgnc_integration(self, target_input_data: pd.DataFrame, temp_output_dir: Path) -> None:
        """Тест интеграции с HGNC в Target пайплайне."""
        pytest.mark.integration
        
        # Ограничиваем входные данные
        limited_data = target_input_data.head(3)
        
        # Создаем временный входной файл
        input_file = temp_output_dir / "test_target_input.csv"
        limited_data.to_csv(input_file, index=False)
        
        # Запускаем пайплайн
        cmd = [
            "bioactivity-data-acquisition", "get-target-data",
            "--config", "configs/config_target.yaml",
            "--input", str(input_file),
            "--output-dir", str(temp_output_dir),
            "--limit", "3"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
            
            # Проверяем выходной файл
            output_file = temp_output_dir / "targets.csv"
            if output_file.exists():
                output_data = pd.read_csv(output_file)
                
                # Проверяем что есть данные из HGNC
                hgnc_fields = ["hgnc_name", "hgnc_id"]
                for field in hgnc_fields:
                    if field in output_data.columns:
                        non_null_count = output_data[field].notna().sum()
                        assert non_null_count > 0, f"HGNC field {field} should have some non-null values"
                
        except subprocess.TimeoutExpired:
            pytest.fail("Pipeline timed out")
        except FileNotFoundError:
            pytest.skip("CLI not found")
