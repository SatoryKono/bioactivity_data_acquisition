#!/usr/bin/env python3
"""
Анализ Pandera схем и их сопоставление с фактическими данными.
"""

import pandas as pd
import yaml
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import logging
import importlib.util
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PanderaSchemaAnalyzer:
    """Анализатор Pandera схем."""
    
    def __init__(self, output_dir: str = "data/output", schemas_dir: str = "src/library/schemas"):
        self.output_dir = Path(output_dir)
        self.schemas_dir = Path(schemas_dir)
        self.entities = ["activities", "assays", "documents", "targets", "testitem"]
        self.results = {}
        
    def load_schema_module(self, entity: str) -> Optional[Any]:
        """Загрузка модуля Pandera схемы."""
        schema_file = self.schemas_dir / f"{entity}_schema_normalized.py"
        if not schema_file.exists():
            logger.warning(f"Файл схемы {schema_file} не найден")
            return None
            
        try:
            spec = importlib.util.spec_from_file_location(f"{entity}_schema", schema_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            logger.error(f"Ошибка загрузки схемы {entity}: {e}")
            return None
    
    def analyze_schema_validation(self, entity: str, df: pd.DataFrame, schema_module: Any) -> Dict[str, Any]:
        """Анализ валидации схемы с фактическими данными."""
        analysis = {
            'schema_loaded': False,
            'validation_errors': [],
            'dtype_mismatches': [],
            'check_failures': [],
            'coerce_needed': []
        }
        
        try:
            # Получаем схему - правильные имена классов
            schema_class_names = {
                "activities": "ActivityNormalizedSchema",
                "assays": "AssayNormalizedSchema", 
                "documents": "DocumentNormalizedSchema",
                "targets": "TargetNormalizedSchema",
                "testitem": "TestitemNormalizedSchema"
            }
            
            class_name = schema_class_names.get(entity)
            if class_name and hasattr(schema_module, class_name):
                schema_class = getattr(schema_module, class_name)
                schema = schema_class.get_schema()
                analysis['schema_loaded'] = True
                
                # Попытка валидации
                try:
                    validated_df = schema.validate(df)
                    analysis['validation_passed'] = True
                    logger.info(f"✅ {entity}: Pandera валидация прошла успешно")
                except Exception as e:
                    analysis['validation_passed'] = False
                    analysis['validation_errors'].append(str(e))
                    logger.warning(f"⚠️ {entity}: Pandera валидация не прошла: {e}")
                    
                    # Анализ конкретных ошибок
                    if "dtype" in str(e).lower():
                        analysis['dtype_mismatches'].append("Обнаружены несоответствия типов данных")
                    if "check" in str(e).lower():
                        analysis['check_failures'].append("Провал проверок валидации")
                    if "coerce" in str(e).lower():
                        analysis['coerce_needed'].append("Требуется коэрсия типов")
                        
        except Exception as e:
            logger.error(f"Ошибка анализа схемы {entity}: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def analyze_column_types(self, entity: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ типов колонок в фактических данных."""
        analysis = {}
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null_count = df[col].count()
            null_count = len(df) - non_null_count
            
            # Анализ специальных типов
            if 'chembl_id' in col.lower():
                # Проверка формата ChEMBL ID
                try:
                    non_null_series = df[col].dropna()
                    if len(non_null_series) > 0 and non_null_series.dtype == 'object':
                        valid_format = non_null_series.str.match(r'^CHEMBL\d+$').sum()
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'valid_chembl_format_rate': valid_format / non_null_count if non_null_count > 0 else 0
                        }
                    else:
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'valid_chembl_format_rate': 0
                        }
                except Exception:
                    analysis[col] = {
                        'dtype': dtype,
                        'null_count': null_count,
                        'valid_chembl_format_rate': 0
                    }
            elif 'doi' in col.lower():
                # Проверка формата DOI
                try:
                    non_null_series = df[col].dropna()
                    if len(non_null_series) > 0 and non_null_series.dtype == 'object':
                        valid_format = non_null_series.str.match(r'^10\.\d+/.+$').sum()
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'valid_doi_format_rate': valid_format / non_null_count if non_null_count > 0 else 0
                        }
                    else:
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'valid_doi_format_rate': 0
                        }
                except Exception:
                    analysis[col] = {
                        'dtype': dtype,
                        'null_count': null_count,
                        'valid_doi_format_rate': 0
                    }
            elif 'pmid' in col.lower():
                # Проверка формата PMID
                try:
                    non_null_series = df[col].dropna()
                    if len(non_null_series) > 0 and non_null_series.dtype == 'object':
                        numeric_count = non_null_series.str.isdigit().sum()
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'numeric_format_rate': numeric_count / non_null_count if non_null_count > 0 else 0
                        }
                    else:
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'numeric_format_rate': 0
                        }
                except Exception:
                    analysis[col] = {
                        'dtype': dtype,
                        'null_count': null_count,
                        'numeric_format_rate': 0
                    }
            elif any(x in col.lower() for x in ['date', 'extracted_at', 'timestamp']):
                # Проверка формата дат
                try:
                    non_null_series = df[col].dropna()
                    if len(non_null_series) > 0 and non_null_series.dtype == 'object':
                        iso_format = non_null_series.str.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}').sum()
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'iso8601_format_rate': iso_format / non_null_count if non_null_count > 0 else 0
                        }
                    else:
                        analysis[col] = {
                            'dtype': dtype,
                            'null_count': null_count,
                            'iso8601_format_rate': 0
                        }
                except Exception:
                    analysis[col] = {
                        'dtype': dtype,
                        'null_count': null_count,
                        'iso8601_format_rate': 0
                    }
            else:
                analysis[col] = {
                    'dtype': dtype,
                    'null_count': null_count
                }
                
        return analysis
    
    def analyze_entity_schemas(self) -> Dict[str, Any]:
        """Анализ Pandera схем для всех сущностей."""
        logger.info("Начинаю анализ Pandera схем...")
        
        for entity in self.entities:
            logger.info(f"Анализирую схемы {entity}...")
            
            # Поиск актуального CSV файла
            entity_dir = self.output_dir / entity
            if not entity_dir.exists():
                logger.warning(f"Директория {entity_dir} не найдена")
                continue
                
            csv_files = list(entity_dir.glob("*.csv"))
            if not csv_files:
                logger.warning(f"CSV файлы не найдены в {entity_dir}")
                continue
                
            # Исключаем QC и summary файлы
            main_csv_files = [f for f in csv_files if not any(x in f.name.lower() for x in ['qc', 'summary', 'correlation', 'meta'])]
            if main_csv_files:
                latest_csv = max(main_csv_files, key=os.path.getmtime)
            else:
                latest_csv = max(csv_files, key=os.path.getmtime)
                
            try:
                # Читаем данные для анализа
                df = pd.read_csv(latest_csv, nrows=100)  # Ограничиваем для производительности
                
                # Загружаем схему
                schema_module = self.load_schema_module(entity)
                
                # Анализ валидации
                validation_analysis = self.analyze_schema_validation(entity, df, schema_module)
                
                # Анализ типов колонок
                column_analysis = self.analyze_column_types(entity, df)
                
                self.results[entity] = {
                    'csv_file': str(latest_csv),
                    'validation_analysis': validation_analysis,
                    'column_analysis': column_analysis,
                    'sample_size': len(df)
                }
                
                logger.info(f"✅ {entity}: анализ завершён")
                
            except Exception as e:
                logger.error(f"Ошибка при анализе {entity}: {e}")
                self.results[entity] = {'error': str(e)}
                
        return self.results
    
    def generate_schema_report(self) -> str:
        """Генерация отчёта по анализу схем."""
        report = []
        report.append("# Отчёт анализа Pandera схем")
        report.append("")
        report.append("## Executive Summary")
        report.append("")
        
        total_entities = len(self.entities)
        analyzed_entities = len([e for e in self.entities if e in self.results and 'error' not in self.results[e]])
        
        report.append(f"- **Entities проанализировано**: {analyzed_entities}/{total_entities}")
        report.append("")
        
        # Сводная таблица по схемам
        report.append("## Таблица анализа схем")
        report.append("")
        report.append("| Entity | Schema Loaded | Validation Passed | Dtype Issues | Check Issues | Coerce Needed |")
        report.append("|--------|---------------|-------------------|--------------|--------------|---------------|")
        
        for entity, result in self.results.items():
            if 'error' in result:
                report.append(f"| {entity} | ❌ | ❌ | ❌ | ❌ | ❌ |")
                continue
                
            validation = result['validation_analysis']
            schema_loaded = "✅" if validation['schema_loaded'] else "❌"
            validation_passed = "✅" if validation.get('validation_passed', False) else "❌"
            dtype_issues = "⚠️" if validation['dtype_mismatches'] else "✅"
            check_issues = "⚠️" if validation['check_failures'] else "✅"
            coerce_needed = "⚠️" if validation['coerce_needed'] else "✅"
            
            report.append(f"| {entity} | {schema_loaded} | {validation_passed} | {dtype_issues} | {check_issues} | {coerce_needed} |")
        
        report.append("")
        
        # Детальный анализ по сущностям
        report.append("## Детальный анализ по сущностям")
        report.append("")
        
        for entity, result in self.results.items():
            if 'error' in result:
                report.append(f"### {entity.title()}")
                report.append("")
                report.append(f"**Ошибка**: {result['error']}")
                report.append("")
                continue
                
            report.append(f"### {entity.title()}")
            report.append("")
            
            validation = result['validation_analysis']
            report.append(f"- **Схема загружена**: {'Да' if validation['schema_loaded'] else 'Нет'}")
            report.append(f"- **Валидация прошла**: {'Да' if validation.get('validation_passed', False) else 'Нет'}")
            report.append(f"- **Размер выборки**: {result['sample_size']} строк")
            
            if validation['validation_errors']:
                report.append(f"- **Ошибки валидации**: {len(validation['validation_errors'])}")
                for error in validation['validation_errors'][:3]:  # Показываем первые 3
                    report.append(f"  - {error[:100]}...")
                    
            if validation['dtype_mismatches']:
                report.append(f"- **Проблемы с типами**: {', '.join(validation['dtype_mismatches'])}")
                
            if validation['check_failures']:
                report.append(f"- **Провал проверок**: {', '.join(validation['check_failures'])}")
                
            if validation['coerce_needed']:
                report.append(f"- **Требуется коэрсия**: {', '.join(validation['coerce_needed'])}")
            
            # Анализ специальных форматов
            column_analysis = result['column_analysis']
            special_formats = []
            
            for col, analysis in column_analysis.items():
                if 'valid_chembl_format_rate' in analysis and analysis['valid_chembl_format_rate'] < 1.0:
                    special_formats.append(f"{col}: ChEMBL ID формат ({analysis['valid_chembl_format_rate']:.2%})")
                elif 'valid_doi_format_rate' in analysis and analysis['valid_doi_format_rate'] < 1.0:
                    special_formats.append(f"{col}: DOI формат ({analysis['valid_doi_format_rate']:.2%})")
                elif 'numeric_format_rate' in analysis and analysis['numeric_format_rate'] < 1.0:
                    special_formats.append(f"{col}: PMID формат ({analysis['numeric_format_rate']:.2%})")
                elif 'iso8601_format_rate' in analysis and analysis['iso8601_format_rate'] < 1.0:
                    special_formats.append(f"{col}: ISO 8601 формат ({analysis['iso8601_format_rate']:.2%})")
                    
            if special_formats:
                report.append("- **Проблемы с форматами**:")
                for fmt in special_formats[:5]:  # Показываем первые 5
                    report.append(f"  - {fmt}")
                    
            report.append("")
        
        return "\n".join(report)
    
    def run_analysis(self) -> str:
        """Запуск полного анализа схем."""
        logger.info("Запускаю анализ Pandera схем...")
        
        # Анализ схем
        self.analyze_entity_schemas()
        
        # Генерация отчёта
        report = self.generate_schema_report()
        
        # Сохранение отчёта
        report_path = Path("metadata/reports/pandera_schema_analysis.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
            
        logger.info(f"Отчёт сохранён: {report_path}")
        
        return report

def main():
    """Основная функция."""
    analyzer = PanderaSchemaAnalyzer()
    report = analyzer.run_analysis()
    # Вывод отчёта в файл для избежания проблем с кодировкой
    with open("pandera_schema_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    print("Отчёт анализа Pandera схем сохранён в pandera_schema_report.txt")

if __name__ == "__main__":
    main()
