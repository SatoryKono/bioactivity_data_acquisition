"""
Pandera схемы для валидации данных активностей.

Предоставляет схемы для входных, сырых и нормализованных данных активностей.
"""

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema


class ActivityInputSchema:
    """Схемы для входных данных активностей."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных активностей."""
        return DataFrameSchema({
            "activity_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL activity ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID активности"
            ),
            "assay_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL assay ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID ассая"
            ),
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            )
        })


class ActivityRawSchema:
    """Схемы для сырых данных активностей из API."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных активностей."""
        return DataFrameSchema({
            # Основные поля
            "activity_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL activity ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID активности"
            ),
            "assay_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL assay ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID ассая"
            ),
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            ),
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время получения данных"
            ),
            
            # Поля активности
            "activity_type": Column(pa.String, nullable=True, description="Тип активности"),
            "activity_value": Column(pa.Float, nullable=True, description="Значение активности"),
            "activity_unit": Column(pa.String, nullable=True, description="Единицы активности"),
            "data_validity_comment": Column(pa.String, nullable=True, description="Комментарий о валидности данных"),
            "activity_comment": Column(pa.String, nullable=True, description="Комментарий к активности"),
            "lower_bound": Column(pa.Float, nullable=True, description="Нижняя граница"),
            "upper_bound": Column(pa.Float, nullable=True, description="Верхняя граница"),
            "is_censored": Column(pa.Bool, nullable=True, description="Флаг цензурированных данных"),
            "published_type": Column(pa.String, nullable=True, description="Оригинальный тип опубликованной активности"),
            "published_relation": Column(pa.String, nullable=True, description="Отношение"),
            "published_value": Column(pa.Float, nullable=True, description="Оригинальное опубликованное значение"),
            "published_units": Column(pa.String, nullable=True, description="Оригинальные единицы"),
            "standard_type": Column(pa.String, nullable=True, description="Стандартизованный тип активности"),
            "standard_relation": Column(pa.String, nullable=True, description="Стандартизованное отношение"),
            "standard_value": Column(pa.Float, nullable=True, description="Стандартизованное значение"),
            "standard_units": Column(pa.String, nullable=True, description="Стандартизованные единицы"),
            "standard_flag": Column(pa.Bool, nullable=True, description="Флаг стандартизации"),
            "bao_endpoint": Column(pa.String, nullable=True, description="BAO endpoint классификация"),
            "bao_format": Column(pa.String, nullable=True, description="BAO format классификация"),
            "bao_label": Column(pa.String, nullable=True, description="BAO label классификация"),
        })


class ActivityNormalizedSchema:
    """Схемы для нормализованных данных активностей."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных активностей."""
        return DataFrameSchema({
            # Основные поля
            "activity_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL activity ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID активности"
            ),
            "assay_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL assay ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID ассая"
            ),
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            ),
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время получения данных"
            ),
            
            # Поля активности
            "activity_type": Column(
                pa.String,
                checks=[
                    Check.isin(["IC50", "EC50", "Ki", "Kd", "AC50"], error="Invalid activity type")
                ],
                nullable=True,
                description="Тип активности"
            ),
            "activity_value": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Activity value must be >= 0")
                ],
                nullable=True,
                description="Значение активности"
            ),
            "activity_unit": Column(
                pa.String,
                checks=[
                    Check.isin(["nM", "uM", "mM", "M", "%", "mg/ml", "ug/ml"], error="Invalid activity unit")
                ],
                nullable=True,
                description="Единицы активности"
            ),
            "pchembl_value": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(3.0, error="pChEMBL value must be >= 3.0"),
                    Check.less_than_or_equal_to(12.0, error="pChEMBL value must be <= 12.0")
                ],
                nullable=True,
                description="-log10(стандартизованное значение)"
            ),
            "data_validity_comment": Column(pa.String, nullable=True, description="Комментарий о валидности данных"),
            "activity_comment": Column(pa.String, nullable=True, description="Комментарий к активности"),
            "lower_bound": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Lower bound must be >= 0")
                ],
                nullable=True,
                description="Нижняя граница для цензурированных данных"
            ),
            "upper_bound": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Upper bound must be >= 0")
                ],
                nullable=True,
                description="Верхняя граница для цензурированных данных"
            ),
            "is_censored": Column(pa.Bool, nullable=True, description="Флаг цензурированных данных"),
            "published_type": Column(pa.String, nullable=True, description="Оригинальный тип опубликованной активности"),
            "published_relation": Column(
                pa.String,
                checks=[
                    Check.isin(["=", ">", "<", ">=", "<="], error="Invalid published relation")
                ],
                nullable=True,
                description="Отношение"
            ),
            "published_value": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Published value must be >= 0")
                ],
                nullable=True,
                description="Оригинальное опубликованное значение"
            ),
            "published_units": Column(pa.String, nullable=True, description="Оригинальные единицы"),
            "standard_type": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Стандартизованный тип активности"
            ),
            "standard_relation": Column(
                pa.String,
                checks=[
                    Check.isin(["=", ">", "<", ">=", "<="], error="Invalid standard relation")
                ],
                nullable=True,
                description="Стандартизованное отношение"
            ),
            "standard_value": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(1e-12, error="Standard value must be >= 1e-12"),
                    Check.less_than_or_equal_to(1e-3, error="Standard value must be <= 1e-3"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="Стандартизованное значение"
            ),
            "standard_units": Column(
                pa.String,
                checks=[
                    Check.isin(["nM", "uM", "mM", "M", "%"], error="Invalid standard units"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="Стандартизованные единицы"
            ),
            "standard_flag": Column(pa.Bool, nullable=True, description="Флаг стандартизации"),
            "bao_endpoint": Column(pa.String, nullable=True, description="BAO endpoint классификация"),
            "bao_format": Column(pa.String, nullable=True, description="BAO format классификация"),
            "bao_label": Column(pa.String, nullable=True, description="BAO label классификация"),
            
            # Системные поля
            "index": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Index must be >= 0"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="Порядковый номер записи"
            ),
            "pipeline_version": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Версия пайплайна"
            ),
            "source_system": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Система-источник"
            ),
            "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
            "extracted_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время извлечения данных"
            ),
            "hash_row": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Хеш строки SHA256"
            ),
            "hash_business_key": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Хеш бизнес-ключа SHA256"
            ),
            
            # Ошибки и статусы
            "extraction_errors": Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
            "validation_errors": Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
            "extraction_status": Column(
                pa.String,
                checks=[
                    Check.isin(["success", "partial", "failed"], error="Invalid extraction status")
                ],
                nullable=True,
                description="Статус извлечения"
            ),
        })


class ActivitySchemaValidator:
    """Валидатор схем активностей."""
    
    def __init__(self):
        self.input_schema = ActivityInputSchema.get_schema()
        self.raw_schema = ActivityRawSchema.get_schema()
        self.normalized_schema = ActivityNormalizedSchema.get_schema()
    
    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать входные данные активностей."""
        return self.input_schema.validate(df)
    
    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать сырые данные активностей."""
        return self.raw_schema.validate(df)
    
    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать нормализованные данные активностей."""
        return self.normalized_schema.validate(df)
    
    def get_schema_errors(self, df: pd.DataFrame, schema_type: str = "normalized") -> list:
        """Получить ошибки схемы."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return []
        except pa.errors.SchemaError as e:
            return [str(error) for error in e.failure_cases]
    
    def is_valid(self, df: pd.DataFrame, schema_type: str = "normalized") -> bool:
        """Проверить валидность данных."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return True
        except pa.errors.SchemaError:
            return False


# Глобальный экземпляр валидатора
activity_schema_validator = ActivitySchemaValidator()


def validate_activity_input(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать входные данные активностей."""
    return activity_schema_validator.validate_input(df)


def validate_activity_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать сырые данные активностей."""
    return activity_schema_validator.validate_raw(df)


def validate_activity_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать нормализованные данные активностей."""
    return activity_schema_validator.validate_normalized(df)


def get_activity_schema_errors(df: pd.DataFrame, schema_type: str = "normalized") -> list:
    """Получить ошибки схемы активностей."""
    return activity_schema_validator.get_schema_errors(df, schema_type)


def is_activity_valid(df: pd.DataFrame, schema_type: str = "normalized") -> bool:
    """Проверить валидность данных активностей."""
    return activity_schema_validator.is_valid(df, schema_type)
