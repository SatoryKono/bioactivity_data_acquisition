"""
Pandera схемы для валидации данных assays с нормализацией.

Предоставляет схемы для входных, сырых и нормализованных данных assays
с атрибутами нормализации для каждой колонки.
"""

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


class AssayNormalizedSchema:
    """Схемы для нормализованных данных assays."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных assays."""
        return DataFrameSchema({
            # Основные поля
            "assay_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL assay ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID assay"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "assay_type": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.isin(["B", "F", "A", "P", "T", "U"], error="Invalid assay type")
                    ],
                    nullable=True,
                    description="Тип assay"
                ),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "assay_category": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Категория assay"),
                ["normalize_string_strip", "normalize_string_upper"]
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
            "target_organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Организм target"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "target_tax_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Taxonomy ID target"),
                ["normalize_int", "normalize_int_positive"]
            ),
            
            # BAO поля
            "bao_format": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO формат"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_bao_id"]
            ),
            "bao_label": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO метка"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "bao_endpoint": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO endpoint"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "bao_assay_format": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO формат assay"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "bao_assay_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO тип assay"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "bao_assay_type_label": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO метка типа assay"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "bao_assay_type_uri": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO URI типа assay"),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "bao_assay_format_uri": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO URI формата assay"),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "bao_assay_format_label": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO метка формата assay"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "bao_endpoint_uri": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO URI endpoint"),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "bao_endpoint_label": add_normalization_metadata(
                Column(pa.String, nullable=True, description="BAO метка endpoint"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            
            # Variant поля
            "variant_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="ID варианта"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "is_variant": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг варианта"),
                ["normalize_boolean"]
            ),
            "variant_accession": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Акцессия варианта"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "variant_sequence_accession": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Акцессия последовательности варианта"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "variant_sequence_mutation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Мутация последовательности варианта"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "variant_mutations": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Мутации варианта"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "variant_sequence": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Последовательность варианта"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "variant_text": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Текст варианта"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "variant_sequence_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="ID последовательности варианта"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "variant_organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Организм варианта"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            
            # Target поля
            "target_uniprot_accession": add_normalization_metadata(
                Column(pa.String, nullable=True, description="UniProt акцессия target"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "target_isoform": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Изоформа target"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Assay поля
            "assay_organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Организм assay"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "assay_tax_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Taxonomy ID assay"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "assay_strain": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Штамм assay"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_tissue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ткань assay"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_cell_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип клетки assay"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_subcellular_fraction": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Субклеточная фракция assay"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "description": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Описание"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_parameters": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Параметры assay"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_parameters_json": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Параметры assay в JSON"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "assay_format": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Формат assay"),
                ["normalize_string_strip", "normalize_string_upper"]
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
