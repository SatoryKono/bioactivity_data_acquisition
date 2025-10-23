"""
Pandera схемы для валидации данных activities с нормализацией.

Предоставляет схемы для входных, сырых и нормализованных данных activities
с атрибутами нормализации для каждой колонки.
"""

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema


def add_normalization_metadata(column: Column, functions: list[str]) -> Column:
    """Добавляет метаданные нормализации к колонке.
    
    Args:
        column: Колонка Pandera
        functions: Список функций нормализации
        
    Returns:
        Колонка с добавленными метаданными
    """
    metadata = column.metadata or {}
    metadata["normalization_functions"] = functions
    return Column(
        column.dtype,
        checks=column.checks,
        nullable=column.nullable,
        description=column.description,
        metadata=metadata
    )


class ActivityNormalizedSchema:
    """Схемы для нормализованных данных activities."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных activities."""
        return DataFrameSchema({
            # Основные поля
            "activity_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL activity ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID активности"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "assay_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL assay ID format")
                    ],
                    nullable=True,
                    description="ChEMBL ID assay"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "document_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format")
                    ],
                    nullable=True,
                    description="ChEMBL ID документа"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "target_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format")
                    ],
                    nullable=True,
                    description="ChEMBL ID target"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "molecule_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format")
                    ],
                    nullable=True,
                    description="ChEMBL ID молекулы"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            
            # Активность
            "activity_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип активности"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "activity_value": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Значение активности"),
                ["normalize_float", "normalize_activity_value"]
            ),
            "activity_unit": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Единица активности"),
                ["normalize_string_strip", "normalize_units"]
            ),
            "pchembl_value": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="pChEMBL значение"),
                ["normalize_float", "normalize_pchembl", "normalize_pchembl_range"]
            ),
            
            # Комментарии
            "data_validity_comment": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Комментарий валидности данных"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "activity_comment": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Комментарий активности"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # Интервальные данные
            "lower_bound": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Нижняя граница"),
                ["normalize_float", "normalize_activity_value"]
            ),
            "upper_bound": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Верхняя граница"),
                ["normalize_float", "normalize_activity_value"]
            ),
            "is_censored": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг цензурирования"),
                ["normalize_boolean"]
            ),
            
            # Опубликованные данные
            "published_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Опубликованный тип"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "published_relation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Опубликованное отношение"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "published_value": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Опубликованное значение"),
                ["normalize_float", "normalize_activity_value"]
            ),
            "published_units": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Опубликованные единицы"),
                ["normalize_string_strip", "normalize_units"]
            ),
            
            # Стандартизированные данные
            "standard_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартный тип"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "standard_relation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартное отношение"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "standard_value": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Стандартное значение"),
                ["normalize_float", "normalize_activity_value"]
            ),
            "standard_units": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартные единицы"),
                ["normalize_string_strip", "normalize_units"]
            ),
            "standard_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Стандартный флаг"),
                ["normalize_boolean"]
            ),
            
            # BAO данные
            "bao_endpoint": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO endpoint"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "bao_format": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO формат"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_bao_id"]
            ),
            "bao_label": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO метка"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            
            # Системные поля
            "index": add_normalization_metadata(
                Column(pa.Int64, nullable=False, description="Индекс записи"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "pipeline_version": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Версия пайплайна"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "source_system": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Исходная система"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "chembl_release": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Релиз ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "extracted_at": add_normalization_metadata(
                Column(
                    pa.DateTime,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Время извлечения данных"
                ),
                ["normalize_datetime_iso8601"]
            ),
            "hash_row": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш строки"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "hash_business_key": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш бизнес-ключа"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
        })
