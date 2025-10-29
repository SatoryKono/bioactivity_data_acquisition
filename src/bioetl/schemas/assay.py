"""Assay schema for ChEMBL data according to IO_SCHEMAS_AND_DIAGRAMS.md."""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Int64, Series

from bioetl.schemas.base import BaseSchema


class AssaySchema(BaseSchema):
    """Schema for ChEMBL assay data.

    Primary Key: [assay_chembl_id, row_subtype, row_index]
    Supports exploded format with subtypes: assay, param, variant, class
    """

    # Primary Key fields
    assay_chembl_id: Series[str] = pa.Field(
        nullable=False,
        regex=r'^CHEMBL\d+$',
        description="ChEMBL assay identifier (PK)",
    )
    row_subtype: Series[str] = pa.Field(
        nullable=False,
        description="Тип развёрнутой строки (assay, param, variant, class)",
    )
    row_index: Series[Int64] = pa.Field(
        ge=0,
        nullable=False,
        description="Индекс для детерминизма",
    )

    # Основные поля
    assay_type: Series[str] = pa.Field(
        nullable=True,
        description="Тип ассая (B, F, A, P, T, U)",
    )
    assay_category: Series[str] = pa.Field(nullable=True, description="Категория ассая")
    assay_cell_type: Series[str] = pa.Field(nullable=True, description="Тип клеток для ассая")
    assay_classifications: Series[str] = pa.Field(nullable=True, description="Классификации ассая (JSON)")
    assay_group: Series[str] = pa.Field(nullable=True, description="Группа ассая")
    assay_organism: Series[str] = pa.Field(nullable=True, description="Организм для ассая")
    assay_parameters_json: Series[str] = pa.Field(nullable=True, description="Параметры ассая (JSON)")
    assay_strain: Series[str] = pa.Field(nullable=True, description="Штамм организма")
    assay_subcellular_fraction: Series[str] = pa.Field(nullable=True, description="Субклеточная фракция")
    assay_tax_id: Series[Int64] = pa.Field(
        ge=0,
        nullable=True,
        description="Таксономический ID организма",
    )
    assay_test_type: Series[str] = pa.Field(nullable=True, description="Тип теста ассая")
    assay_tissue: Series[str] = pa.Field(nullable=True, description="Ткань для ассая")
    assay_type_description: Series[str] = pa.Field(nullable=True, description="Описание типа ассая")

    # BAO fields
    bao_format: Series[str] = pa.Field(
        nullable=True,
        regex=r'^BAO_\d+$',
        description="BAO format классификация",
    )
    bao_label: Series[str] = pa.Field(nullable=True, description="BAO label классификация")
    bao_endpoint: Series[str] = pa.Field(
        nullable=True,
        regex=r'^BAO_\d{7}$',
        description="BAO endpoint",
    )

    # Связи
    cell_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL ID клетки")
    confidence_description: Series[str] = pa.Field(nullable=True, description="Описание уверенности")
    confidence_score: Series[Int64] = pa.Field(
        ge=0,
        le=9,
        nullable=True,
        description="Уровень уверенности (0-9)",
    )
    assay_description: Series[str] = pa.Field(nullable=True, description="Описание ассая")
    document_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="ChEMBL ID документа",
    )
    relationship_description: Series[str] = pa.Field(nullable=True, description="Описание связи")
    relationship_type: Series[str] = pa.Field(nullable=True, description="Тип связи с таргетом")
    src_assay_id: Series[str] = pa.Field(nullable=True, description="ID ассая в источнике")
    src_id: Series[Int64] = pa.Field(
        nullable=True,
        description="ID источника",
    )
    target_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="FK к target_dim",
    )
    tissue_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL ID ткани")
    variant_sequence_json: Series[str] = pa.Field(nullable=True, description="Последовательность варианта (JSON)")

    # Target enrichment (whitelist)
    pref_name: Series[str] = pa.Field(nullable=True, description="Whitelisted target preferred name")
    organism: Series[str] = pa.Field(nullable=True, description="Whitelisted organism name")
    target_type: Series[str] = pa.Field(nullable=True, description="Whitelisted target type")
    species_group_flag: Series[Int64] = pa.Field(
        nullable=True,
        ge=0,
        le=1,
        description="Whitelisted species group flag",
    )
    tax_id: Series[Int64] = pa.Field(
        nullable=True,
        ge=0,
        description="Whitelisted NCBI taxonomy ID",
    )
    component_count: Series[Int64] = pa.Field(
        nullable=True,
        ge=0,
        description="Whitelisted component count",
    )

    # ASSAY_PARAMETERS (развернутые из JSON)
    assay_param_type: Series[str] = pa.Field(nullable=True, description="Тип параметра ассея")
    assay_param_relation: Series[str] = pa.Field(
        nullable=True,
        description="Отношение параметра",
    )
    assay_param_value: Series[pd.Float64Dtype] = pa.Field(
        nullable=True,
        description="Значение параметра",
    )
    assay_param_units: Series[str] = pa.Field(nullable=True, description="Единицы параметра")
    assay_param_text_value: Series[str] = pa.Field(nullable=True, description="Текстовое значение параметра")
    assay_param_standard_type: Series[str] = pa.Field(nullable=True, description="Стандартизованный тип параметра")
    assay_param_standard_value: Series[pd.Float64Dtype] = pa.Field(
        nullable=True,
        description="Стандартизованное значение параметра",
    )
    assay_param_standard_units: Series[str] = pa.Field(nullable=True, description="Единицы стандартизованного параметра")

    # ASSAY_CLASS (из /assay_class endpoint)
    assay_class_id: Series[Int64] = pa.Field(
        nullable=True,
        description="Идентификатор класса ассея",
    )
    assay_class_bao_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^BAO_\d{7}$',
        description="BAO ID класса ассея",
    )
    assay_class_type: Series[str] = pa.Field(nullable=True, description="Тип класса ассея")
    assay_class_l1: Series[str] = pa.Field(nullable=True, description="Иерархия 1 класса ассея")
    assay_class_l2: Series[str] = pa.Field(nullable=True, description="Иерархия 2 класса ассея")
    assay_class_l3: Series[str] = pa.Field(nullable=True, description="Иерархия 3 класса ассея")
    assay_class_description: Series[str] = pa.Field(nullable=True, description="Описание класса ассея")

    # VARIANT_SEQUENCES (развернутые из JSON)
    variant_id: Series[Int64] = pa.Field(
        nullable=True,
        description="Идентификатор варианта",
    )
    variant_base_accession: Series[str] = pa.Field(
        nullable=True,
        description="UniProt акцессия базовой последовательности",
    )
    variant_mutation: Series[str] = pa.Field(nullable=True, description="Мутация варианта")
    variant_sequence: Series[str] = pa.Field(
        nullable=True,
        regex=r'^[A-Z\*]+$',
        description="Аминокислотная последовательность варианта",
    )
    variant_accession_reported: Series[str] = pa.Field(nullable=True, description="Сообщённая акцессия варианта")

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    # Column order: system/hash fields first (per BaseSchema), then business fields
    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "assay_chembl_id",
        "row_subtype",
        "row_index",
        "assay_type",
        "assay_category",
        "assay_cell_type",
        "assay_classifications",
        "assay_group",
        "assay_organism",
        "assay_parameters_json",
        "assay_strain",
        "assay_subcellular_fraction",
        "assay_tax_id",
        "assay_test_type",
        "assay_tissue",
        "assay_type_description",
        "bao_format",
        "bao_label",
        "bao_endpoint",
        "cell_chembl_id",
        "confidence_description",
        "confidence_score",
        "assay_description",
        "document_chembl_id",
        "relationship_description",
        "relationship_type",
        "src_assay_id",
        "src_id",
        "target_chembl_id",
        "tissue_chembl_id",
        "variant_sequence_json",
        "pref_name",
        "organism",
        "target_type",
        "species_group_flag",
        "tax_id",
        "component_count",
        "assay_param_type",
        "assay_param_relation",
        "assay_param_value",
        "assay_param_units",
        "assay_param_text_value",
        "assay_param_standard_type",
        "assay_param_standard_value",
        "assay_param_standard_units",
        "assay_class_id",
        "assay_class_bao_id",
        "assay_class_type",
        "assay_class_l1",
        "assay_class_l2",
        "assay_class_l3",
        "assay_class_description",
        "variant_id",
        "variant_base_accession",
        "variant_mutation",
        "variant_sequence",
        "variant_accession_reported",
    ]

    class Config:
        strict = True
        coerce = True
        ordered = True


class _AssayColumnOrderAccessor:
    """Descriptor returning the canonical column order for backwards compatibility."""

    def __get__(self, instance, owner) -> list[str]:
        return AssaySchema.get_column_order()


# Backwards compatibility shim: expose column_order on Config without
# registering it as a Pandera check during class creation.
AssaySchema.Config.column_order = _AssayColumnOrderAccessor()

