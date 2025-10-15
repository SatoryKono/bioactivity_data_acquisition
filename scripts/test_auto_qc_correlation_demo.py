"""Демонстрационный скрипт для автоматического сохранения QC и корреляционных отчетов."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from library.etl.load import write_deterministic_csv


def main():
    """Демонстрация автоматического сохранения QC и корреляционных отчетов."""
    
    print("=== Демонстрация автоматического сохранения QC и корреляционных отчетов ===\n")
    
    # Создаем тестовые данные
    df = pd.DataFrame({
        'compound_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4', 'CHEMBL5'],
        'target': ['TARGET1', 'TARGET2', 'TARGET1', 'TARGET3', 'TARGET1'],
        'activity_value': [5.2, 7.1, 3.8, 9.4, 2.1],
        'activity_type': ['IC50', 'EC50', 'IC50', 'Ki', 'IC50'],
        'reference': ['PMID123', 'PMID456', 'PMID789', 'PMID012', 'PMID345'],
        'organism': ['Human', 'Mouse', 'Human', 'Rat', 'Human'],
        'assay_type': ['Binding', 'Functional', 'Binding', 'Binding', 'Functional'],
        'confidence_score': [8.5, 7.2, 9.1, 6.8, 8.9]
    })
    
    print("Исходные данные:")
    print(df)
    print(f"\nКоличество строк: {len(df)}")
    print(f"Количество столбцов: {len(df.columns)}")
    
    # Создаем выходной файл
    output_path = Path("data/output") / "demo_auto_qc_correlation.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nСохранение данных в: {output_path}")
    print("При сохранении автоматически будут созданы:")
    print("- Базовые QC и корреляционные отчеты")
    print("- Расширенные QC отчеты")
    print("- Расширенные корреляционные отчеты")
    print("- Детальные отчеты с инсайтами")
    
    # Записываем данные - это автоматически создаст все отчеты
    write_deterministic_csv(
        df,
        output_path,
        determinism=None,
        output=None
    )
    
    print("\n=== Анализ созданных файлов ===")
    
    # Проверяем созданные файлы
    data_dir = output_path.parent
    data_stem = output_path.stem
    
    created_files = []
    
    # Базовые отчеты
    qc_path = data_dir / f"{data_stem}_quality_report.csv"
    corr_path = data_dir / f"{data_stem}_correlation_report.csv"
    
    if qc_path.exists():
        created_files.append(str(qc_path))
        qc_report = pd.read_csv(qc_path)
        print(f"\nБазовый QC отчет: {qc_path}")
        print(f"  Строк: {len(qc_report)}")
        print(f"  Колонки: {list(qc_report.columns)}")
    
    if corr_path.exists():
        created_files.append(str(corr_path))
        corr_report = pd.read_csv(corr_path)
        print(f"\nБазовый корреляционный отчет: {corr_path}")
        print(f"  Строк: {len(corr_report)}")
        print(f"  Колонки: {list(corr_report.columns)}")
    
    # Расширенные QC отчеты
    enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
    detailed_qc_path = data_dir / f"{data_stem}_quality_report_detailed"
    
    if enhanced_qc_path.exists():
        created_files.append(str(enhanced_qc_path))
        enhanced_qc = pd.read_csv(enhanced_qc_path)
        print(f"\nРасширенный QC отчет: {enhanced_qc_path}")
        print(f"  Строк: {len(enhanced_qc)}")
        print(f"  Колонки: {list(enhanced_qc.columns)}")
    
    if detailed_qc_path.exists():
        qc_detail_files = list(detailed_qc_path.glob("*.csv"))
        created_files.extend([str(f) for f in qc_detail_files])
        print(f"\nДетальные QC отчеты: {detailed_qc_path}")
        print(f"  Количество файлов: {len(qc_detail_files)}")
        for file in qc_detail_files:
            print(f"    - {file.name}")
    
    # Расширенные корреляционные отчеты
    enhanced_corr_path = data_dir / f"{data_stem}_correlation_report_enhanced"
    detailed_corr_path = data_dir / f"{data_stem}_correlation_report_detailed"
    
    if enhanced_corr_path.exists():
        corr_enhanced_files = list(enhanced_corr_path.glob("*.csv"))
        created_files.extend([str(f) for f in corr_enhanced_files])
        print(f"\nРасширенные корреляционные отчеты: {enhanced_corr_path}")
        print(f"  Количество файлов: {len(corr_enhanced_files)}")
        for file in corr_enhanced_files:
            print(f"    - {file.name}")
    
    if detailed_corr_path.exists():
        corr_detail_files = list(detailed_corr_path.glob("*"))
        created_files.extend([str(f) for f in corr_detail_files])
        print(f"\nДетальные корреляционные отчеты: {detailed_corr_path}")
        print(f"  Количество файлов: {len(corr_detail_files)}")
        for file in corr_detail_files:
            print(f"    - {file.name}")
    
    print("\n=== Сводка созданных файлов ===")
    print(f"Всего создано файлов: {len(created_files)}")
    print("\nПолный список файлов:")
    for i, file_path in enumerate(created_files, 1):
        print(f"  {i}. {file_path}")
    
    print("\n=== Ключевые особенности автоматического сохранения ===")
    print("- QC и корреляционные отчеты создаются автоматически при каждом сохранении данных")
    print("- Генерируются как базовые, так и расширенные отчеты")
    print("- Имена файлов формируются на основе имени основного файла данных")
    print("- Поддерживаются все форматы вывода (CSV, Parquet)")
    print("- Отчеты содержат детальную информацию о качестве данных и корреляциях")
    print("- JSON файлы с анализом корреляций для сложных структур данных")
    print("- Автоматическое создание инсайтов и рекомендаций")
    
    print(f"\nОсновной файл данных: {output_path}")
    print("Демонстрация завершена успешно!")


if __name__ == "__main__":
    main()
