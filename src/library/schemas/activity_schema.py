"""
Pandera схемы для валидации данных активностей.

Предоставляет схемы для входных, сырых и нормализованных данных активностей.
"""

# import pandas as pd  # Unused after removing legacy validators
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema


class ActivityInputSchema:
    """Схемы для входных данных активностей."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных активностей."""
        return DataFrameSchema(
            {
                "activity_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL activity ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID активности",
                ),
                "assay_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL assay ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID ассая",
                ),
                "document_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL document ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID документа",
                ),
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                ),
            }
        )


class ActivityRawSchema:
    """Схемы для сырых данных активностей из API."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных активностей."""
        return DataFrameSchema(
            {
                # Основные поля
                "activity_chembl_id": Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Activity ID (string)",
                ),
                "assay_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL assay ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID ассая",
                ),
                "document_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL document ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID документа",
                ),
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                ),
                # Поля активности (соответствуют реальным данным из ChEMBL API)
                "data_validity_comment": Column(pa.String, nullable=True, description="Комментарий о валидности данных"),
                "activity_comment": Column(pa.String, nullable=True, description="Комментарий к активности"),
                "published_type": Column(pa.String, nullable=True, description="Оригинальный тип опубликованной активности"),
                "published_relation": Column(pa.String, nullable=True, description="Отношение"),
                "published_value": Column(pa.String, nullable=True, description="Оригинальное опубликованное значение"),
                "published_units": Column(pa.String, nullable=True, description="Оригинальные единицы"),
                "standard_type": Column(pa.String, nullable=True, description="Стандартизованный тип активности"),
                "standard_relation": Column(pa.String, nullable=True, description="Стандартизованное отношение"),
                "standard_value": Column(pa.String, nullable=True, description="Стандартизованное значение"),
                "standard_units": Column(pa.String, nullable=True, description="Стандартизованные единицы"),
                "standard_flag": Column(pa.Int, nullable=True, description="Флаг стандартизации"),
                "pchembl_value": Column(pa.String, nullable=True, description="pChEMBL значение"),
                "bao_endpoint": Column(pa.String, nullable=True, description="BAO endpoint классификация"),
                "bao_format": Column(pa.String, nullable=True, description="BAO format классификация"),
                "bao_label": Column(pa.String, nullable=True, description="BAO label классификация"),
                "source_system": Column(pa.String, nullable=True, description="Система-источник"),
            }
        )


class ActivityNormalizedSchema:
    """Схемы для нормализованных данных активностей."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных активностей."""
        return DataFrameSchema(
            {
                # ACTIVITIES - основные поля
                "activity_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL activity ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID активности (PK)",
                ),
                "assay_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL assay ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID ассая",
                ),
                "document_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL document ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID документа",
                ),
                "record_id": Column(
                    pa.Int,
                    checks=[Check.greater_than_or_equal_to(1, error="Record ID must be >= 1"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="FK на COMPOUND_RECORDS",
                ),
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                ),
                # ACTIVITIES - данные активности
                "type": Column(pa.String, checks=[Check.str_length(max=250, error="Type must be <= 250 characters")], nullable=True, description="Исходный тип (Ki, IC50, %)"),
                "relation": Column(
                    pa.String,
                    checks=[Check.isin(["=", ">", ">=", "<", "<=", "~"], error="Invalid relation"), Check.str_length(max=50, error="Relation must be <= 50 characters")],
                    nullable=True,
                    description="Отношение",
                ),
                "value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Value must be a valid number or NaN")],
                    nullable=True,
                    description="Исходное значение",
                ),
                "units": Column(pa.String, checks=[Check.str_length(max=100, error="Units must be <= 100 characters")], nullable=True, description="Единицы (raw)"),
                "text_value": Column(
                    pa.String, checks=[Check.str_length(max=1000, error="Text value must be <= 1000 characters")], nullable=True, description="Текстовое значение"
                ),
                "upper_value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Upper value must be a valid number or NaN")],
                    nullable=True,
                    description="Верхняя граница диапазона",
                ),
                # ACTIVITIES - стандартизованные данные
                "standard_type": Column(
                    pa.String, checks=[Check.str_length(max=250, error="Standard type must be <= 250 characters")], nullable=True, description="Нормализованный тип"
                ),
                "standard_relation": Column(
                    pa.String, checks=[Check.isin(["=", ">", ">=", "<", "<=", "~"], error="Invalid standard relation")], nullable=True, description="Нормализованное отношение"
                ),
                "standard_value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Standard value must be a valid number or NaN")],
                    nullable=True,
                    description="Нормализованное значение",
                ),
                "standard_units": Column(
                    pa.String, checks=[Check.str_length(max=100, error="Standard units must be <= 100 characters")], nullable=True, description="Нормализованные ед."
                ),
                "standard_flag": Column(pa.Int, checks=[Check.isin([0, 1], error="Standard flag must be 0 or 1")], nullable=True, description="Признак стандартизации (1/0)"),
                "standard_text_value": Column(
                    pa.String, checks=[Check.str_length(max=1000, error="Standard text value must be <= 1000 characters")], nullable=True, description="Нормализованный текст"
                ),
                "standard_upper_value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Standard upper value must be a valid number or NaN")],
                    nullable=True,
                    description="Верхняя граница (std)",
                ),
                # ACTIVITIES - комментарии и метаданные
                "activity_comment": Column(
                    pa.String, checks=[Check.str_length(max=4000, error="Activity comment must be <= 4000 characters")], nullable=True, description="Комментарий активности"
                ),
                "data_validity_comment": Column(
                    pa.String, checks=[Check.str_length(max=1000, error="Data validity comment must be <= 1000 characters")], nullable=True, description="Комментарий валидности"
                ),
                "potential_duplicate": Column(pa.Int, checks=[Check.isin([0, 1], error="Potential duplicate must be 0 or 1")], nullable=True, description="Возможный дубликат"),
                "pchembl_value": Column(
                    pa.Float,
                    checks=[Check.greater_than_or_equal_to(0, error="pChEMBL value must be >= 0"), Check.less_than_or_equal_to(14, error="pChEMBL value must be <= 14")],
                    nullable=True,
                    description="pChEMBL (сравнение потентности)",
                ),
                # ACTIVITIES - онтологии
                "bao_endpoint": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^BAO_\d{7}$", error="Invalid BAO endpoint format"), Check.str_length(max=50, error="BAO endpoint must be <= 50 characters")],
                    nullable=True,
                    description="BAO endpoint ID",
                ),
                "uo_units": Column(pa.String, checks=[Check.str_matches(r"^UO_\d{7}$|^$", error="Invalid UO units format")], nullable=True, description="Unit Ontology"),
                "qudt_units": Column(pa.String, checks=[Check.str_length(max=500, error="QUDT units must be <= 500 characters")], nullable=True, description="QUDT URI"),
                "src_id": Column(pa.Int, checks=[Check.greater_than_or_equal_to(1, error="Source ID must be >= 1")], nullable=True, description="Источник"),
                "action_type": Column(
                    pa.String, checks=[Check.str_length(max=100, error="Action type must be <= 100 characters")], nullable=True, description="Тип действия лиганда"
                ),
                # ACTIVITY_PROPERTIES (развернутые)
                "activity_prop_type": Column(
                    pa.String, checks=[Check.str_length(max=250, error="Activity property type must be <= 250 characters")], nullable=True, description="Имя свойства/параметра"
                ),
                "activity_prop_relation": Column(
                    pa.String, checks=[Check.isin(["=", ">", ">=", "<", "<=", "~"], error="Invalid activity property relation")], nullable=True, description="Отношение"
                ),
                "activity_prop_value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Activity property value must be a valid number or NaN")],
                    nullable=True,
                    description="Значение",
                ),
                "activity_prop_units": Column(
                    pa.String, checks=[Check.str_length(max=100, error="Activity property units must be <= 100 characters")], nullable=True, description="Единицы"
                ),
                "activity_prop_text_value": Column(
                    pa.String, checks=[Check.str_length(max=2000, error="Activity property text value must be <= 2000 characters")], nullable=True, description="Текстовое значение"
                ),
                "activity_prop_standard_type": Column(
                    pa.String,
                    checks=[Check.str_length(max=250, error="Activity property standard type must be <= 250 characters")],
                    nullable=True,
                    description="Нормализованный тип",
                ),
                "activity_prop_standard_relation": Column(
                    pa.String,
                    checks=[Check.isin(["=", ">", ">=", "<", "<=", "~"], error="Invalid activity property standard relation")],
                    nullable=True,
                    description="Нормализованное отношение",
                ),
                "activity_prop_standard_value": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="Activity property standard value must be a valid number or NaN")],
                    nullable=True,
                    description="Нормализованное значение",
                ),
                "activity_prop_standard_units": Column(
                    pa.String,
                    checks=[Check.str_length(max=100, error="Activity property standard units must be <= 100 characters")],
                    nullable=True,
                    description="Нормализованные ед.",
                ),
                "activity_prop_standard_text_value": Column(
                    pa.String,
                    checks=[Check.str_length(max=2000, error="Activity property standard text value must be <= 2000 characters")],
                    nullable=True,
                    description="Нормализованный текст",
                ),
                "activity_prop_comments": Column(
                    pa.String,
                    checks=[Check.str_length(max=2000, error="Activity property comments must be <= 2000 characters")],
                    nullable=True,
                    description="Комментарий к свойству",
                ),
                "activity_prop_result_flag": Column(
                    pa.Int, checks=[Check.isin([0, 1], error="Activity property result flag must be 0 or 1")], nullable=True, description="1 если это результат"
                ),
                # LIGAND_EFF
                "bei": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="BEI must be a valid number or NaN")],
                    nullable=True,
                    description="Binding Efficiency Index",
                ),
                "sei": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="SEI must be a valid number or NaN")],
                    nullable=True,
                    description="Surface Efficiency Index",
                ),
                "le": Column(
                    pa.Float, checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="LE must be a valid number or NaN")], nullable=True, description="Ligand Efficiency"
                ),
                "lle": Column(
                    pa.Float,
                    checks=[Check(lambda x: x.notna() | (x.isna() & x.isna()), error="LLE must be a valid number or NaN")],
                    nullable=True,
                    description="Lipophilic Ligand Efficiency",
                ),
                # Системные поля
                "index": Column(
                    pa.Int,
                    checks=[Check.greater_than_or_equal_to(0, error="Index must be >= 0"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="Порядковый номер записи",
                ),
                "pipeline_version": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Версия пайплайна"),
                "source_system": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Система-источник"),
                "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
                "extracted_at": Column(pa.DateTime, checks=[Check(lambda x: x.notna())], nullable=False, description="Время извлечения данных"),
                "hash_row": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Хеш строки SHA256"),
                "hash_business_key": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Хеш бизнес-ключа SHA256"),
            }
        )


# DEPRECATED: Legacy validation functions removed.
# Use library.common.validation.validate_entity_data() instead.
#
# Example:
# from library.common.validation import validate_entity_data, ValidationStage
# result = validate_entity_data("activity", df, ValidationStage.NORMALIZED)
