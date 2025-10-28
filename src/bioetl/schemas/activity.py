"""Pandera schemas for Activity data."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class ActivitySchema(BaseSchema):
    """Schema for ChEMBL Activity data according to IO_SCHEMAS_AND_DIAGRAMS.md."""

    # Primary Key
    activity_id: Series[int] = pa.Field(nullable=False, ge=1, description="Уникальный идентификатор активности")

    # Foreign Keys
    molecule_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="FK на молекулу",
    )
    assay_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="FK на ассай",
    )
    target_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="FK на таргет",
    )
    document_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="FK на документ",
    )

    # Published activity data (original from source)
    published_type: Series[str] = pa.Field(nullable=True, description="Оригинальный тип опубликованных данных")
    published_relation: Series[str] = pa.Field(nullable=True, description="Соотношение для published_value")
    published_value: Series[float] = pa.Field(nullable=True, ge=0, description="Оригинальное опубликованное значение")
    published_units: Series[str] = pa.Field(nullable=True, description="Единицы published_value")

    # Standardized activity data
    standard_type: Series[str] = pa.Field(nullable=True, description="Стандартизированный тип активности")
    standard_relation: Series[str] = pa.Field(nullable=True, description="Соотношение для standard_value (=, >, <, >=, <=)")
    standard_value: Series[float] = pa.Field(nullable=True, ge=0, description="Стандартизированное значение")
    standard_units: Series[str] = pa.Field(nullable=True, description="Единицы стандартизированного значения")
    standard_flag: Series[int] = pa.Field(nullable=True, description="Флаг стандартизации (0/1)")
    pchembl_value: Series[float] = pa.Field(nullable=True, ge=0, description="-log10 нормированное значение")

    # Boundaries and censorship
    lower_bound: Series[float] = pa.Field(nullable=True, description="Нижняя граница стандартизированного значения")
    upper_bound: Series[float] = pa.Field(nullable=True, description="Верхняя граница стандартизированного значения")
    is_censored: Series[bool] = pa.Field(nullable=True, description="Флаг цензурирования данных")

    # Comments
    activity_comment: Series[str] = pa.Field(nullable=True, description="Комментарий к активности")
    data_validity_comment: Series[str] = pa.Field(nullable=True, description="Комментарий о валидности данных")

    # BAO annotations
    bao_endpoint: Series[str] = pa.Field(nullable=True, description="BAO endpoint (Bioassay Ontology)")
    bao_format: Series[str] = pa.Field(nullable=True, description="BAO format (Bioassay Ontology)")
    bao_label: Series[str] = pa.Field(nullable=True, description="BAO label (Bioassay Ontology)")

    # Ontologies and metadata
    potential_duplicate: Series[int] = pa.Field(nullable=True, isin=[0, 1], description="Возможный дубликат активности")
    uo_units: Series[str] = pa.Field(nullable=True, regex=r'^UO_\d{7}$', description="Unit Ontology ID")
    qudt_units: Series[str] = pa.Field(nullable=True, description="QUDT URI для единиц измерения")
    src_id: Series[int] = pa.Field(nullable=True, description="ID источника данных")
    action_type: Series[str] = pa.Field(nullable=True, description="Тип действия лиганда")

    # Activity properties (JSON string)
    activity_properties_json: Series[str] = pa.Field(nullable=True, description="Свойства активности в формате JSON")

    # Ligand efficiency
    bei: Series[float] = pa.Field(nullable=True, description="Binding Efficiency Index")
    sei: Series[float] = pa.Field(nullable=True, description="Surface Efficiency Index")
    le: Series[float] = pa.Field(nullable=True, description="Ligand Efficiency")
    lle: Series[float] = pa.Field(nullable=True, description="Lipophilic Ligand Efficiency")

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    # Column order: business fields first, then system fields, then hash fields
    _column_order = [
        "activity_id",
        "molecule_chembl_id",
        "assay_chembl_id",
        "target_chembl_id",
        "document_chembl_id",
        "published_type",
        "published_relation",
        "published_value",
        "published_units",
        "standard_type",
        "standard_relation",
        "standard_value",
        "standard_units",
        "standard_flag",
        "lower_bound",
        "upper_bound",
        "is_censored",
        "pchembl_value",
        "activity_comment",
        "data_validity_comment",
        "bao_endpoint",
        "bao_format",
        "bao_label",
        "potential_duplicate",
        "uo_units",
        "qudt_units",
        "src_id",
        "action_type",
        "activity_properties_json",
        "bei",
        "sei",
        "le",
        "lle",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "hash_business_key",
        "hash_row",
        "index",
    ]

    class Config:
        strict = True
        coerce = True
        ordered = True

