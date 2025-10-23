"""
Pandera схемы для валидации данных теститемов.

Предоставляет схемы для входных, сырых и нормализованных данных теститемов.
"""

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema


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
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "nstereo": Column(pa.Int, nullable=True, description="Количество стереоизомеров"),
        }, strict=False)  # strict=False позволяет дополнительные колонки


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
                    Check(lambda x: x.notna())
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
                checks=[Check(lambda x: x.notna())],
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
            # Основные поля из входных данных
            "molecule_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID молекулы"
            ),
            "all_names": Column(pa.String, nullable=True, description="Все названия молекулы"),
            "canonical_smiles": Column(pa.String, nullable=True, description="Канонические SMILES"),
            "chirality": Column(pa.String, nullable=True, description="Хиральность"),
            "inchi_key_from_mol": Column(pa.String, nullable=True, description="InChI ключ из молекулы"),
            "molecule_type": Column(pa.String, nullable=True, description="Тип молекулы"),
            "mw_freebase": Column(pa.Float, nullable=True, description="Молекулярная масса freebase"),
            "nstereo": Column(pa.Int, nullable=True, description="Количество стереоизомеров"),
            "num_ro5_violations": Column(pa.Float, nullable=True, description="Нарушений Rule of 5"),
            "standard_inchi_key": Column(pa.String, nullable=True, description="Стандартный InChI ключ"),
            
            # Поля, добавленные при нормализации
            "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
            "pubchem_registry_id": Column(pa.String, nullable=True, description="PubChem Registry ID"),
            "direct_interaction": Column(pa.String, nullable=True, description="Прямое взаимодействие"),
            "molecular_mechanism": Column(pa.String, nullable=True, description="Молекулярный механизм"),
            "mechanism_of_action": Column(pa.String, nullable=True, description="Механизм действия"),
            "pubchem_rn": Column(pa.String, nullable=True, description="PubChem RN"),
            "parent_chembl_id": Column(pa.String, nullable=True, description="ID родительской молекулы"),
            "parent_molregno": Column(pa.String, nullable=True, description="Номер регистрации родительской молекулы"),
            "hash_business_key": Column(pa.String, nullable=True, description="Хеш бизнес-ключа SHA256"),
            "hash_row": Column(pa.String, nullable=True, description="Хеш строки SHA256"),
            
            # Распакованные поля из вложенных ChEMBL структур
            "atc_classifications": Column(pa.String, nullable=True, description="ATC классификации (JSON)"),
            "biotherapeutic": Column(pa.String, nullable=True, description="Биотерапевтическое соединение (JSON)"),
            "chemical_probe": Column(pa.Bool, nullable=True, description="Химический зонд"),
            "chirality_chembl": Column(pa.String, nullable=True, description="Хиральность из ChEMBL"),
            "cross_references": Column(pa.String, nullable=True, description="Перекрестные ссылки (JSON)"),
            "helm_notation": Column(pa.String, nullable=True, description="HELM нотация"),
            "molecule_hierarchy": Column(pa.String, nullable=True, description="Иерархия молекулы (JSON)"),
            "molecule_properties": Column(pa.String, nullable=True, description="Свойства молекулы (JSON)"),
            "molecule_structures": Column(pa.String, nullable=True, description="Структуры молекулы (JSON)"),
            "molecule_synonyms": Column(pa.String, nullable=True, description="Синонимы молекулы (JSON)"),
            "molecule_type_chembl": Column(pa.String, nullable=True, description="Тип молекулы из ChEMBL"),
            "orphan": Column(pa.Bool, nullable=True, description="Орфанное лекарство"),
            "standard_inchi": Column(pa.String, nullable=True, description="Стандартный InChI"),
            "veterinary": Column(pa.Bool, nullable=True, description="Ветеринарное лекарство"),
        }, strict=False)  # strict=False позволяет дополнительные колонки


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