"""
Pandera схемы для валидации данных теститемов.

Предоставляет схемы для входных, сырых и нормализованных данных теститемов.
"""

from typing import Optional
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check


class TestitemInputSchema:
    """Схемы для входных данных теститемов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных теститемов."""
        return DataFrameSchema({
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            )
        })


class TestitemRawSchema:
    """Схемы для сырых данных теститемов из API."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных теститемов."""
        return DataFrameSchema({
            # Основные поля ChEMBL
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "molregno": Column(pa.Int, nullable=True, description="Номер регистрации молекулы"),
            "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название молекулы"),
            "parent_chembl_id": Column(pa.String, nullable=True, description="ID родительской молекулы"),
            "max_phase": Column(pa.Float, nullable=True, description="Максимальная фаза разработки"),
            "therapeutic_flag": Column(pa.Bool, nullable=True, description="Флаг терапевтического применения"),
            "structure_type": Column(pa.String, nullable=True, description="Тип структуры"),
            "molecule_type": Column(pa.String, nullable=True, description="Тип молекулы"),
            "mw_freebase": Column(pa.Float, nullable=True, description="Молекулярная масса freebase"),
            "alogp": Column(pa.Float, nullable=True, description="ALogP значение"),
            "hba": Column(pa.Int, nullable=True, description="Количество акцепторов водорода"),
            "hbd": Column(pa.Int, nullable=True, description="Количество доноров водорода"),
            "psa": Column(pa.Float, nullable=True, description="Полярная площадь поверхности"),
            "rtb": Column(pa.Int, nullable=True, description="Количество вращающихся связей"),
            "ro3_pass": Column(pa.Bool, nullable=True, description="Проходит ли Rule of 3"),
            "num_ro5_violations": Column(pa.Int, nullable=True, description="Нарушений Rule of 5"),
            "qed_weighted": Column(pa.Float, nullable=True, description="Weighted QED значение"),
            "oral": Column(pa.Bool, nullable=True, description="Оральный путь введения"),
            "parenteral": Column(pa.Bool, nullable=True, description="Парентеральный путь введения"),
            "topical": Column(pa.Bool, nullable=True, description="Топический путь введения"),
            "withdrawn_flag": Column(pa.Bool, nullable=True, description="Отозванное лекарство"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время получения данных"
            ),
            
            # PubChem поля
            "pubchem_cid": Column(pa.Int, nullable=True, description="PubChem CID"),
            "pubchem_molecular_formula": Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
            "pubchem_molecular_weight": Column(pa.Float, nullable=True, description="Молекулярная масса PubChem"),
            "pubchem_canonical_smiles": Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
            "pubchem_inchi": Column(pa.String, nullable=True, description="InChI PubChem"),
            "pubchem_inchi_key": Column(pa.String, nullable=True, description="InChI Key PubChem"),
        })


class TestitemNormalizedSchema:
    """Схемы для нормализованных данных теститемов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных теститемов."""
        return DataFrameSchema({
            # Основные поля ChEMBL
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "molregno": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="Molregno must be > 0")
                ],
                nullable=True,
                description="Номер регистрации молекулы"
            ),
            "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название молекулы"),
            "parent_chembl_id": Column(pa.String, nullable=True, description="ID родительской молекулы"),
            "max_phase": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Max phase must be >= 0"),
                    Check.less_than_or_equal_to(4, error="Max phase must be <= 4")
                ],
                nullable=True,
                description="Максимальная фаза разработки"
            ),
            "therapeutic_flag": Column(pa.Bool, nullable=True, description="Флаг терапевтического применения"),
            "structure_type": Column(pa.String, nullable=True, description="Тип структуры"),
            "molecule_type": Column(pa.String, nullable=True, description="Тип молекулы"),
            "mw_freebase": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(50.0, error="Molecular weight must be >= 50.0"),
                    Check.less_than_or_equal_to(2000.0, error="Molecular weight must be <= 2000.0")
                ],
                nullable=True,
                description="Молекулярная масса freebase"
            ),
            "alogp": Column(pa.Float, nullable=True, description="ALogP значение"),
            "hba": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="HBA count must be >= 0")
                ],
                nullable=True,
                description="Количество акцепторов водорода"
            ),
            "hbd": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="HBD count must be >= 0")
                ],
                nullable=True,
                description="Количество доноров водорода"
            ),
            "psa": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="PSA must be >= 0")
                ],
                nullable=True,
                description="Полярная площадь поверхности"
            ),
            "rtb": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="RTB count must be >= 0")
                ],
                nullable=True,
                description="Количество вращающихся связей"
            ),
            "ro3_pass": Column(pa.Bool, nullable=True, description="Проходит ли Rule of 3"),
            "num_ro5_violations": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Ro5 violations must be >= 0"),
                    Check.less_than_or_equal_to(5, error="Ro5 violations must be <= 5")
                ],
                nullable=True,
                description="Нарушений Rule of 5"
            ),
            "qed_weighted": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(0, error="QED must be >= 0"),
                    Check.less_than_or_equal_to(1, error="QED must be <= 1")
                ],
                nullable=True,
                description="Weighted QED значение"
            ),
            "oral": Column(pa.Bool, nullable=True, description="Оральный путь введения"),
            "parenteral": Column(pa.Bool, nullable=True, description="Парентеральный путь введения"),
            "topical": Column(pa.Bool, nullable=True, description="Топический путь введения"),
            "withdrawn_flag": Column(pa.Bool, nullable=True, description="Отозванное лекарство"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время получения данных"
            ),
            
            # PubChem поля
            "pubchem_cid": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="PubChem CID must be > 0")
                ],
                nullable=True,
                description="PubChem CID"
            ),
            "pubchem_molecular_formula": Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
            "pubchem_molecular_weight": Column(
                pa.Float,
                checks=[
                    Check.greater_than_or_equal_to(50.0, error="PubChem molecular weight must be >= 50.0"),
                    Check.less_than_or_equal_to(2000.0, error="PubChem molecular weight must be <= 2000.0")
                ],
                nullable=True,
                description="Молекулярная масса PubChem"
            ),
            "pubchem_canonical_smiles": Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
            "pubchem_inchi": Column(pa.String, nullable=True, description="InChI PubChem"),
            "pubchem_inchi_key": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$', error="Invalid InChI Key format")
                ],
                nullable=True,
                description="InChI Key PubChem"
            ),
            
            # Стандартизированные поля
            "standardized_inchi": Column(pa.String, nullable=True, description="Стандартизированный InChI"),
            "standardized_inchi_key": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$', error="Invalid standardized InChI Key format")
                ],
                nullable=True,
                description="Стандартизированный InChI Key"
            ),
            "standardized_smiles": Column(pa.String, nullable=True, description="Стандартизированные SMILES"),
            
            # Системные поля
            "index": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Index must be >= 0"),
                    Check.not_null()
                ],
                nullable=False,
                description="Порядковый номер записи"
            ),
            "pipeline_version": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Версия пайплайна"
            ),
            "source_system": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Система-источник"
            ),
            "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
            "extracted_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время извлечения данных"
            ),
            "hash_row": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Хеш строки SHA256"
            ),
            "hash_business_key": Column(
                pa.String,
                checks=[Check.not_null()],
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


class TestitemSchemaValidator:
    """Валидатор схем теститемов."""
    
    def __init__(self):
        self.input_schema = TestitemInputSchema.get_schema()
        self.raw_schema = TestitemRawSchema.get_schema()
        self.normalized_schema = TestitemNormalizedSchema.get_schema()
    
    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать входные данные теститемов."""
        return self.input_schema.validate(df)
    
    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать сырые данные теститемов."""
        return self.raw_schema.validate(df)
    
    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать нормализованные данные теститемов."""
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
testitem_schema_validator = TestitemSchemaValidator()


def validate_testitem_input(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать входные данные теститемов."""
    return testitem_schema_validator.validate_input(df)


def validate_testitem_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать сырые данные теститемов."""
    return testitem_schema_validator.validate_raw(df)


def validate_testitem_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать нормализованные данные теститемов."""
    return testitem_schema_validator.validate_normalized(df)


def get_testitem_schema_errors(df: pd.DataFrame, schema_type: str = "normalized") -> list:
    """Получить ошибки схемы теститемов."""
    return testitem_schema_validator.get_schema_errors(df, schema_type)


def is_testitem_valid(df: pd.DataFrame, schema_type: str = "normalized") -> bool:
    """Проверить валидность данных теститемов."""
    return testitem_schema_validator.is_valid(df, schema_type)