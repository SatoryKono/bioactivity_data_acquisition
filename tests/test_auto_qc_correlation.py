"""Тесты для проверки автоматического сохранения QC и корреляционных отчетов."""

import os
import sys
import tempfile
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import pytest

from library.etl.load import write_deterministic_csv
from library.config import PostprocessSettings, QCStepSettings, CorrelationSettings


class TestAutoQCCorrelation:
    """Тесты для проверки автоматического сохранения QC и корреляционных отчетов."""

    def test_auto_qc_correlation_generation(self):
        """Тест автоматической генерации QC и корреляционных отчетов."""
        # Создаем тестовые данные
        df = pd.DataFrame({
            'compound_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4'],
            'target': ['TARGET1', 'TARGET2', 'TARGET1', 'TARGET3'],
            'activity_value': [5.2, 7.1, 3.8, 9.4],
            'activity_type': ['IC50', 'EC50', 'IC50', 'Ki'],
            'reference': ['PMID123', 'PMID456', 'PMID789', 'PMID012']
        })
        
        # Создаем временный файл для данных
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            data_path = Path(tmp_file.name)
        
        try:
            # Записываем данные - это должно автоматически создать QC и корреляционные отчеты
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None
            )
            
            # Проверяем, что основной файл создался
            assert data_path.exists(), "Основной файл данных должен быть создан"
            
            # Проверяем, что создались QC отчеты
            data_dir = data_path.parent
            data_stem = data_path.stem
            
            # Базовые отчеты
            qc_path = data_dir / f"{data_stem}_quality_report.csv"
            corr_path = data_dir / f"{data_stem}_correlation_report.csv"
            
            assert qc_path.exists(), f"QC отчет должен быть создан: {qc_path}"
            assert corr_path.exists(), f"Корреляционный отчет должен быть создан: {corr_path}"
            
            # Расширенные отчеты
            enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
            assert enhanced_qc_path.exists(), f"Расширенный QC отчет должен быть создан: {enhanced_qc_path}"
            
            # Детальные отчеты QC
            detailed_qc_path = data_dir / f"{data_stem}_quality_report_detailed"
            assert detailed_qc_path.exists(), f"Директория детальных QC отчетов должна быть создана: {detailed_qc_path}"
            
            # Расширенные корреляционные отчеты
            enhanced_corr_path = data_dir / f"{data_stem}_correlation_report_enhanced"
            assert enhanced_corr_path.exists(), f"Директория расширенных корреляционных отчетов должна быть создана: {enhanced_corr_path}"
            
            # Детальные корреляционные отчеты
            detailed_corr_path = data_dir / f"{data_stem}_correlation_report_detailed"
            assert detailed_corr_path.exists(), f"Директория детальных корреляционных отчетов должна быть создана: {detailed_corr_path}"
            
            # Проверяем содержимое базового QC отчета
            qc_report = pd.read_csv(qc_path)
            assert len(qc_report) > 0, "QC отчет не должен быть пустым"
            assert 'metric' in qc_report.columns, "QC отчет должен содержать колонку 'metric'"
            
            # Проверяем содержимое базового корреляционного отчета
            corr_report = pd.read_csv(corr_path)
            assert len(corr_report) > 0, "Корреляционный отчет не должен быть пустым"
            
            # Проверяем содержимое расширенного QC отчета
            enhanced_qc_report = pd.read_csv(enhanced_qc_path)
            assert len(enhanced_qc_report) > 0, "Расширенный QC отчет не должен быть пустым"
            
            # Проверяем, что в детальной директории есть файлы
            qc_detail_files = list(detailed_qc_path.glob("*.csv"))
            assert len(qc_detail_files) > 0, "В детальной директории QC должны быть файлы"
            
            # Проверяем, что в расширенной корреляционной директории есть файлы
            corr_enhanced_files = list(enhanced_corr_path.glob("*.csv"))
            assert len(corr_enhanced_files) > 0, "В расширенной корреляционной директории должны быть файлы"
            
            # Проверяем, что в детальной корреляционной директории есть JSON файл
            corr_analysis_file = detailed_corr_path / "correlation_analysis.json"
            assert corr_analysis_file.exists(), "JSON файл анализа корреляций должен быть создан"
            
            print("Тест автоматической генерации QC и корреляционных отчетов прошел успешно!")
            
        finally:
            # Удаляем все созданные файлы
            for file_path in [
                data_path,
                qc_path,
                corr_path,
                enhanced_qc_path,
                detailed_qc_path,
                enhanced_corr_path,
                detailed_corr_path
            ]:
                if file_path.exists():
                    if file_path.is_file():
                        os.unlink(file_path)
                    elif file_path.is_dir():
                        import shutil
                        shutil.rmtree(file_path, ignore_errors=True)

    def test_auto_qc_correlation_with_empty_dataframe(self):
        """Тест обработки пустого DataFrame."""
        # Создаем пустой DataFrame
        df = pd.DataFrame()
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            data_path = Path(tmp_file.name)
        
        try:
            # Записываем пустые данные
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None
            )
            
            # Проверяем, что основной файл создался (может быть пустым)
            assert data_path.exists(), "Основной файл должен быть создан даже для пустых данных"
            
            # Проверяем, что QC и корреляционные отчеты НЕ создались для пустых данных
            data_dir = data_path.parent
            data_stem = data_path.stem
            
            qc_path = data_dir / f"{data_stem}_quality_report.csv"
            corr_path = data_dir / f"{data_stem}_correlation_report.csv"
            
            # Для пустых данных отчеты не должны создаваться
            assert not qc_path.exists(), "QC отчет не должен создаваться для пустых данных"
            assert not corr_path.exists(), "Корреляционный отчет не должен создаваться для пустых данных"
            
            print("Тест обработки пустого DataFrame прошел успешно!")
            
        finally:
            # Удаляем временный файл
            if data_path.exists():
                os.unlink(data_path)

    def test_auto_qc_correlation_file_naming(self):
        """Тест правильности именования файлов QC и корреляционных отчетов."""
        # Создаем тестовые данные
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        # Создаем временный файл с конкретным именем
        with tempfile.NamedTemporaryFile(mode='w', suffix='_test_data.csv', delete=False) as tmp_file:
            data_path = Path(tmp_file.name)
        
        try:
            # Записываем данные
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None
            )
            
            data_dir = data_path.parent
            data_stem = data_path.stem  # должно быть "_test_data"
            
            # Проверяем правильность именования файлов
            expected_qc_path = data_dir / f"{data_stem}_quality_report.csv"
            expected_corr_path = data_dir / f"{data_stem}_correlation_report.csv"
            expected_enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
            expected_detailed_qc_path = data_dir / f"{data_stem}_quality_report_detailed"
            expected_enhanced_corr_path = data_dir / f"{data_stem}_correlation_report_enhanced"
            expected_detailed_corr_path = data_dir / f"{data_stem}_correlation_report_detailed"
            
            assert expected_qc_path.exists(), f"QC отчет должен быть создан с правильным именем: {expected_qc_path}"
            assert expected_corr_path.exists(), f"Корреляционный отчет должен быть создан с правильным именем: {expected_corr_path}"
            assert expected_enhanced_qc_path.exists(), f"Расширенный QC отчет должен быть создан с правильным именем: {expected_enhanced_qc_path}"
            assert expected_detailed_qc_path.exists(), f"Директория детальных QC отчетов должна быть создана с правильным именем: {expected_detailed_qc_path}"
            assert expected_enhanced_corr_path.exists(), f"Директория расширенных корреляционных отчетов должна быть создана с правильным именем: {expected_enhanced_corr_path}"
            assert expected_detailed_corr_path.exists(), f"Директория детальных корреляционных отчетов должна быть создана с правильным именем: {expected_detailed_corr_path}"
            
            print("Тест правильности именования файлов прошел успешно!")
            
        finally:
            # Удаляем все созданные файлы
            for file_path in [
                data_path,
                expected_qc_path,
                expected_corr_path,
                expected_enhanced_qc_path,
                expected_detailed_qc_path,
                expected_enhanced_corr_path,
                expected_detailed_corr_path
            ]:
                if file_path.exists():
                    if file_path.is_file():
                        os.unlink(file_path)
                    elif file_path.is_dir():
                        import shutil
                        shutil.rmtree(file_path, ignore_errors=True)

    def test_auto_qc_correlation_content_validation(self):
        """Тест валидации содержимого созданных отчетов."""
        # Создаем тестовые данные с известными характеристиками
        df = pd.DataFrame({
            'numeric_col': [1, 2, 3, 4, 5],
            'string_col': ['a', 'b', 'c', 'd', 'e'],
            'mixed_col': [1, 'a', 2, 'b', 3],
            'null_col': [1, None, 3, None, 5]
        })
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            data_path = Path(tmp_file.name)
        
        # Определяем переменные для очистки заранее
        data_dir = data_path.parent
        data_stem = data_path.stem
        
        try:
            # Записываем данные
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None
            )
            
            # Проверяем содержимое базового QC отчета
            qc_path = data_dir / f"{data_stem}_quality_report.csv"
            qc_report = pd.read_csv(qc_path)
            
            # Должно быть 4 строки (по одной для каждой колонки) плюс summary
            assert len(qc_report) >= 4, f"QC отчет должен содержать минимум 4 строки, получено {len(qc_report)}"
            
            # Проверяем наличие ожидаемых колонок в QC отчете
            expected_qc_columns = ['metric', 'value', 'threshold', 'ratio', 'status']
            for col in expected_qc_columns:
                assert col in qc_report.columns, f"QC отчет должен содержать колонку '{col}'"
            
            # Проверяем содержимое расширенного QC отчета
            enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
            enhanced_qc_report = pd.read_csv(enhanced_qc_path)
            
            # Расширенный отчет должен содержать больше метрик
            assert len(enhanced_qc_report) >= 4, "Расширенный QC отчет должен содержать минимум 4 строки"
            
            # Проверяем наличие расширенных метрик
            enhanced_metrics = ['non_null', 'non_empty', 'empty_pct', 'unique_cnt', 'unique_pct_of_non_empty']
            for metric in enhanced_metrics:
                assert metric in enhanced_qc_report.columns, f"Расширенный QC отчет должен содержать метрику '{metric}'"
            
            print("Тест валидации содержимого отчетов прошел успешно!")
            
        finally:
            # Удаляем все созданные файлы
            if data_path.exists():
                os.unlink(data_path)
            
            # Удаляем созданные отчеты
            for suffix in ['_quality_report.csv', '_correlation_report.csv', '_quality_report_enhanced.csv']:
                report_path = data_dir / f"{data_stem}{suffix}"
                if report_path.exists():
                    os.unlink(report_path)
            
            for suffix in ['_quality_report_detailed', '_correlation_report_enhanced', '_correlation_report_detailed']:
                report_dir = data_dir / f"{data_stem}{suffix}"
                if report_dir.exists():
                    import shutil
                    shutil.rmtree(report_dir, ignore_errors=True)

    def test_auto_qc_correlation_disabled(self):
        """Тест отключения автоматической генерации QC и корреляционных отчетов."""
        # Создаем тестовые данные
        df = pd.DataFrame({
            'compound_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],
            'target': ['TARGET1', 'TARGET2', 'TARGET1'],
            'activity_value': [5.2, 7.1, 3.8],
            'activity_type': ['IC50', 'EC50', 'IC50'],
            'reference': ['PMID123', 'PMID456', 'PMID789']
        })
        
        # Создаем временный файл для данных
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            data_path = Path(tmp_file.name)
        
        # Определяем переменные для очистки заранее
        data_dir = data_path.parent
        data_stem = data_path.stem
        
        try:
            # Тест 1: Отключаем QC, но оставляем корреляцию включенной
            postprocess_qc_disabled = PostprocessSettings(
                qc=QCStepSettings(enabled=False),
                correlation=CorrelationSettings(enabled=True)
            )
            
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None,
                postprocess=postprocess_qc_disabled
            )
            
            # Проверяем, что основной файл создался
            assert data_path.exists(), "Основной файл данных должен быть создан"
            
            # QC отчеты НЕ должны создаться
            qc_path = data_dir / f"{data_stem}_quality_report.csv"
            enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
            detailed_qc_path = data_dir / f"{data_stem}_quality_report_detailed"
            
            assert not qc_path.exists(), "QC отчет НЕ должен создаваться когда qc.enabled=False"
            assert not enhanced_qc_path.exists(), "Расширенный QC отчет НЕ должен создаваться когда qc.enabled=False"
            assert not detailed_qc_path.exists(), "Детальные QC отчеты НЕ должны создаваться когда qc.enabled=False"
            
            # Корреляционные отчеты ДОЛЖНЫ создаться
            corr_path = data_dir / f"{data_stem}_correlation_report.csv"
            enhanced_corr_path = data_dir / f"{data_stem}_correlation_report_enhanced"
            detailed_corr_path = data_dir / f"{data_stem}_correlation_report_detailed"
            
            assert corr_path.exists(), "Корреляционный отчет ДОЛЖЕН создаваться когда correlation.enabled=True"
            assert enhanced_corr_path.exists(), "Расширенные корреляционные отчеты ДОЛЖНЫ создаваться когда correlation.enabled=True"
            assert detailed_corr_path.exists(), "Детальные корреляционные отчеты ДОЛЖНЫ создаваться когда correlation.enabled=True"
            
            # Удаляем созданные файлы для следующего теста
            for file_path in [corr_path, enhanced_corr_path, detailed_corr_path]:
                if file_path.exists():
                    if file_path.is_file():
                        os.unlink(file_path)
                    elif file_path.is_dir():
                        import shutil
                        shutil.rmtree(file_path, ignore_errors=True)
            
            # Тест 2: Отключаем корреляцию, но оставляем QC включенным
            postprocess_corr_disabled = PostprocessSettings(
                qc=QCStepSettings(enabled=True),
                correlation=CorrelationSettings(enabled=False)
            )
            
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None,
                postprocess=postprocess_corr_disabled
            )
            
            # QC отчеты ДОЛЖНЫ создаться
            assert qc_path.exists(), "QC отчет ДОЛЖЕН создаваться когда qc.enabled=True"
            assert enhanced_qc_path.exists(), "Расширенный QC отчет ДОЛЖЕН создаваться когда qc.enabled=True"
            assert detailed_qc_path.exists(), "Детальные QC отчеты ДОЛЖНЫ создаваться когда qc.enabled=True"
            
            # Корреляционные отчеты НЕ должны создаться
            assert not corr_path.exists(), "Корреляционный отчет НЕ должен создаваться когда correlation.enabled=False"
            assert not enhanced_corr_path.exists(), "Расширенные корреляционные отчеты НЕ должны создаваться когда correlation.enabled=False"
            assert not detailed_corr_path.exists(), "Детальные корреляционные отчеты НЕ должны создаваться когда correlation.enabled=False"
            
            # Удаляем созданные файлы для следующего теста
            for file_path in [qc_path, enhanced_qc_path, detailed_qc_path]:
                if file_path.exists():
                    if file_path.is_file():
                        os.unlink(file_path)
                    elif file_path.is_dir():
                        import shutil
                        shutil.rmtree(file_path, ignore_errors=True)
            
            # Тест 3: Отключаем и QC, и корреляцию
            postprocess_both_disabled = PostprocessSettings(
                qc=QCStepSettings(enabled=False),
                correlation=CorrelationSettings(enabled=False)
            )
            
            write_deterministic_csv(
                df,
                data_path,
                determinism=None,
                output=None,
                postprocess=postprocess_both_disabled
            )
            
            # Никакие отчеты НЕ должны создаться
            assert not qc_path.exists(), "QC отчет НЕ должен создаваться когда qc.enabled=False"
            assert not enhanced_qc_path.exists(), "Расширенный QC отчет НЕ должен создаваться когда qc.enabled=False"
            assert not detailed_qc_path.exists(), "Детальные QC отчеты НЕ должны создаваться когда qc.enabled=False"
            assert not corr_path.exists(), "Корреляционный отчет НЕ должен создаваться когда correlation.enabled=False"
            assert not enhanced_corr_path.exists(), "Расширенные корреляционные отчеты НЕ должны создаваться когда correlation.enabled=False"
            assert not detailed_corr_path.exists(), "Детальные корреляционные отчеты НЕ должны создаваться когда correlation.enabled=False"
            
            print("Тест отключения автоматической генерации QC и корреляционных отчетов прошел успешно!")
            
        finally:
            # Удаляем основной файл данных
            if data_path.exists():
                os.unlink(data_path)
            
            # Удаляем все возможные отчеты
            for suffix in ['_quality_report.csv', '_correlation_report.csv', '_quality_report_enhanced.csv']:
                report_path = data_dir / f"{data_stem}{suffix}"
                if report_path.exists():
                    os.unlink(report_path)
            
            for suffix in ['_quality_report_detailed', '_correlation_report_enhanced', '_correlation_report_detailed']:
                report_dir = data_dir / f"{data_stem}{suffix}"
                if report_dir.exists():
                    import shutil
                    shutil.rmtree(report_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__])
