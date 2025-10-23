#!/usr/bin/env python3
"""
Валидация Pandera checks на фактических CSV данных.
Проверяет соответствие данных схемам и записывает failed checks.
"""

import json
import pandas as pd
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

# Добавляем src в путь для импорта библиотеки
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def validate_chembl_id_pattern(series: pd.Series) -> List[str]:
    """Валидация паттерна ChEMBL ID."""
    pattern = r'^CHEMBL\d+$'
    failed_values = series[~series.astype(str).str.match(pattern, na=False)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_doi_pattern(series: pd.Series) -> List[str]:
    """Валидация паттерна DOI."""
    pattern = r'^10\.\d+/[^\s]+$'
    failed_values = series[~series.astype(str).str.match(pattern, na=False)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_pmid_pattern(series: pd.Series) -> List[str]:
    """Валидация паттерна PMID."""
    pattern = r'^\d+$'
    failed_values = series[~series.astype(str).str.match(pattern, na=False)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_uniprot_pattern(series: pd.Series) -> List[str]:
    """Валидация паттерна UniProt ID."""
    pattern = r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$'
    failed_values = series[~series.astype(str).str.match(pattern, na=False)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_inchi_key_pattern(series: pd.Series) -> List[str]:
    """Валидация паттерна InChI Key."""
    pattern = r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$'
    failed_values = series[~series.astype(str).str.match(pattern, na=False)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_numeric_range(series: pd.Series, min_val: Optional[float] = None, 
                          max_val: Optional[float] = None) -> List[str]:
    """Валидация числового диапазона."""
    failed_values = []
    
    for val in series:
        if pd.isna(val):
            continue
        try:
            num_val = float(val)
            if min_val is not None and num_val < min_val:
                failed_values.append(str(val))
            elif max_val is not None and num_val > max_val:
                failed_values.append(str(val))
        except (ValueError, TypeError):
            failed_values.append(str(val))
    
    return failed_values

def validate_enum_values(series: pd.Series, allowed_values: List[str]) -> List[str]:
    """Валидация enum значений."""
    failed_values = series[~series.isin(allowed_values)].unique()
    return [str(v) for v in failed_values if pd.notna(v)]

def validate_entity_checks(entity: str, csv_path: Path, pandera_specs: Dict[str, Any]) -> pd.DataFrame:
    """Валидация checks для одной сущности."""
    print(f"Валидирую {entity}...")
    
    # Читаем CSV
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Ошибка чтения {csv_path}: {e}")
        return pd.DataFrame()
    
    validation_results = []
    
    # Проходим по всем схемам
    for schema_name, schema_info in pandera_specs.get('schemas', {}).items():
        fields = schema_info.get('fields', {})
        
        for field_name, field_info in fields.items():
            if field_name not in df.columns:
                continue
            
            column_series = df[field_name]
            checks = field_info.get('checks', [])
            
            for check in checks:
                check_result = {
                    'entity': entity,
                    'schema': schema_name,
                    'column': field_name,
                    'check_type': check,
                    'total_records': len(column_series),
                    'failed_count': 0,
                    'failed_examples': []
                }
                
                # Определяем тип валидации по check
                if 'str_matches' in check:
                    # Извлекаем паттерн из check
                    pattern_match = re.search(r'str_matches\(([^)]+)\)', check)
                    if pattern_match:
                        pattern = pattern_match.group(1).strip("'\"")
                        
                        # Определяем тип паттерна
                        if 'CHEMBL' in pattern:
                            failed_examples = validate_chembl_id_pattern(column_series)
                        elif r'10\.' in pattern and '/' in pattern:
                            failed_examples = validate_doi_pattern(column_series)
                        elif r'^\d+$' in pattern:
                            failed_examples = validate_pmid_pattern(column_series)
                        elif 'UniProt' in check or 'uniprot' in check.lower():
                            failed_examples = validate_uniprot_pattern(column_series)
                        elif 'InChIKey' in check or 'InChI' in check:
                            failed_examples = validate_inchi_key_pattern(column_series)
                        else:
                            # Общий regex паттерн
                            failed_values = column_series[~column_series.astype(str).str.match(pattern, na=False)].unique()
                            failed_examples = [str(v) for v in failed_values if pd.notna(v)]
                        
                        check_result['failed_count'] = len(failed_examples)
                        check_result['failed_examples'] = failed_examples[:10]  # Первые 10 примеров
                
                elif 'ge' in check or 'gte' in check:
                    # Больше или равно
                    min_val_match = re.search(r'ge\(([^)]+)\)', check)
                    if min_val_match:
                        min_val = float(min_val_match.group(1))
                        failed_examples = validate_numeric_range(column_series, min_val=min_val)
                        check_result['failed_count'] = len(failed_examples)
                        check_result['failed_examples'] = failed_examples[:10]
                
                elif 'le' in check or 'lte' in check:
                    # Меньше или равно
                    max_val_match = re.search(r'le\(([^)]+)\)', check)
                    if max_val_match:
                        max_val = float(max_val_match.group(1))
                        failed_examples = validate_numeric_range(column_series, max_val=max_val)
                        check_result['failed_count'] = len(failed_examples)
                        check_result['failed_examples'] = failed_examples[:10]
                
                elif 'in' in check:
                    # Enum значения
                    enum_match = re.search(r'in\(\[([^\]]+)\]\)', check)
                    if enum_match:
                        enum_str = enum_match.group(1)
                        allowed_values = [v.strip().strip("'\"") for v in enum_str.split(',')]
                        failed_examples = validate_enum_values(column_series, allowed_values)
                        check_result['failed_count'] = len(failed_examples)
                        check_result['failed_examples'] = failed_examples[:10]
                
                validation_results.append(check_result)
    
    return pd.DataFrame(validation_results)

def main():
    """Основная функция."""
    reports_dir = Path("metadata/reports")
    
    # Пути к данным
    output_dir = Path("data/output")
    csv_files = {
        "activity": output_dir / "activities" / "activities_20251023.csv",
        "assay": output_dir / "assays" / "assays_20251023.csv",
        "document": output_dir / "documents" / "documents_20251023.csv",
        "target": output_dir / "targets" / "targets_20251023.csv",
        "testitem": output_dir / "testitem" / "testitems_20251023.csv"
    }
    
    all_validation_results = []
    
    for entity, csv_path in csv_files.items():
        if not csv_path.exists():
            print(f"CSV файл не найден: {csv_path}")
            continue
        
        # Загружаем Pandera спецификации
        pandera_file = reports_dir / f"{entity}_pandera_specs.json"
        if not pandera_file.exists():
            print(f"Pandera спецификации не найдены: {pandera_file}")
            continue
        
        with open(pandera_file, 'r', encoding='utf-8') as f:
            pandera_specs = json.load(f)
        
        # Валидируем checks
        validation_df = validate_entity_checks(entity, csv_path, pandera_specs)
        
        if not validation_df.empty:
            # Сохраняем индивидуальный CSV
            output_file = reports_dir / f"{entity}_failed_checks.csv"
            validation_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Сохранено: {output_file}")
            
            all_validation_results.append(validation_df)
    
    # Объединяем все результаты
    if all_validation_results:
        combined_df = pd.concat(all_validation_results, ignore_index=True)
        
        # Сохраняем сводный CSV
        summary_file = reports_dir / "all_failed_checks.csv"
        combined_df.to_csv(summary_file, index=False, encoding='utf-8')
        print(f"Сводный файл сохранен: {summary_file}")
        
        # Создаем отчет
        create_validation_report(combined_df, reports_dir)

def create_validation_report(df: pd.DataFrame, reports_dir: Path):
    """Создать отчет по валидации."""
    
    # Группируем по сущностям
    entity_summary = df.groupby('entity').agg({
        'total_records': 'first',
        'failed_count': 'sum',
        'check_type': 'count'
    }).reset_index()
    entity_summary.columns = ['entity', 'total_records', 'total_failures', 'total_checks']
    
    # Создаем Markdown отчет
    report_lines = [
        "# Отчет по валидации Pandera checks",
        "",
        f"Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Сводка по сущностям",
        "",
        "| Entity | Total Records | Total Checks | Total Failures |",
        "|--------|---------------|--------------|----------------|"
    ]
    
    for _, row in entity_summary.iterrows():
        report_lines.append(f"| {row['entity']} | {row['total_records']} | {row['total_checks']} | {row['total_failures']} |")
    
    # Добавляем детали по каждой сущности
    for entity in df['entity'].unique():
        entity_df = df[df['entity'] == entity]
        failed_checks = entity_df[entity_df['failed_count'] > 0]
        
        if failed_checks.empty:
            continue
        
        report_lines.extend([
            "",
            f"## {entity.title()} - Failed Checks",
            ""
        ])
        
        for _, row in failed_checks.iterrows():
            report_lines.extend([
                f"### {row['column']} - {row['check_type']}",
                f"- **Failed Count**: {row['failed_count']}",
                f"- **Schema**: {row['schema']}",
                ""
            ])
            
            if row['failed_examples']:
                report_lines.append("**Examples of failed values:**")
                for example in row['failed_examples'][:5]:  # Первые 5 примеров
                    report_lines.append(f"- `{example}`")
                report_lines.append("")
    
    # Сохраняем отчет
    report_file = reports_dir / "validation_report.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет по валидации сохранен: {report_file}")

if __name__ == "__main__":
    main()
