"""Pandera schemas for Activity data."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema

RELATION_VALUES = ["=", ">", "<", ">=", "<="]
STANDARD_UNITS_ALLOWED = {
    "nM",
    "uM",
    "µM",
    "μM",
    "mM",
    "M",
    "pM",
    "fM",
    "mol/L",
    "mmol/L",
    "umol/L",
    "µmol/L",
    "nmol/L",
    "pmol/L",
    "mol%",
    "%",
    "mg/mL",
    "mg/ml",
    "ug/mL",
    "ug/ml",
    "µg/mL",
    "µg/ml",
    "μg/mL",
    "μg/ml",
    "ng/mL",
    "ng/ml",
    "pg/mL",
    "pg/ml",
    "mg/L",
    "ug/L",
    "ng/L",
    "pg/L",
    "g/L",
    "kg/L",
    "cells/mL",
    "cells/L",
    "mg/kg",
    "ug/kg",
    "ng/kg",
    "IU/mL",
    "IU/ml",
    "U/mL",
    "U/ml",
    "Molar",
    "mm",
    "cm",
    "nm",
    "µm",
    "um",
    "s",
    "sec",
    "min",
    "h",
    "hr",
    "day",
    "ratio",
}

COLUMN_ORDER = [
    "index",
    "hash_row",
    "hash_business_key",
    "pipeline_version",
    "source_system",
    "chembl_release",
    "extracted_at",
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
    "pchembl_value",
    "lower_bound",
    "upper_bound",
    "is_censored",
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
    "canonical_smiles",
    "target_organism",
    "target_tax_id",
    "activity_properties",
    "compound_key",
    "is_citation",
    "high_citation_rate",
    "exact_data_citation",
    "rounded_data_citation",
    "bei",
    "sei",
    "le",
    "lle",
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_code",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
]


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
    published_relation: Series[str] = pa.Field(
        nullable=True,
        isin=RELATION_VALUES,
        description="Соотношение для published_value",
    )
    published_value: Series[float] = pa.Field(nullable=True, ge=0, description="Оригинальное опубликованное значение")
    published_units: Series[str] = pa.Field(nullable=True, description="Единицы published_value")

    # Standardized activity data
    standard_type: Series[str] = pa.Field(nullable=True, description="Стандартизированный тип активности")
    standard_relation: Series[str] = pa.Field(
        nullable=True,
        isin=RELATION_VALUES,
        description="Соотношение для standard_value (=, >, <, >=, <=)",
    )
    standard_value: Series[float] = pa.Field(nullable=True, ge=0, description="Стандартизированное значение")
    standard_units: Series[str] = pa.Field(
        nullable=True,
        isin=STANDARD_UNITS_ALLOWED,
        description="Единицы стандартизированного значения",
    )
    standard_flag: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        coerce=False,
        description="Флаг стандартизации (0/1)",
    )
    pchembl_value: Series[float] = pa.Field(nullable=True, ge=0, description="-log10 нормированное значение")

    # Boundaries and censorship
    lower_bound: Series[float] = pa.Field(nullable=True, description="Нижняя граница стандартизированного значения")
    upper_bound: Series[float] = pa.Field(nullable=True, description="Верхняя граница стандартизированного значения")
    is_censored: Series[pd.BooleanDtype] = pa.Field(
        nullable=True,
        description="Флаг цензурирования данных",
    )

    # Comments
    activity_comment: Series[str] = pa.Field(nullable=True, description="Комментарий к активности")
    data_validity_comment: Series[str] = pa.Field(nullable=True, description="Комментарий о валидности данных")

    # BAO annotations
    bao_endpoint: Series[str] = pa.Field(nullable=True, description="BAO endpoint (Bioassay Ontology)")
    bao_format: Series[str] = pa.Field(nullable=True, description="BAO format (Bioassay Ontology)")
    bao_label: Series[str] = pa.Field(nullable=True, description="BAO label (Bioassay Ontology)")

    # Ontologies and metadata
    potential_duplicate: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        coerce=False,
        isin=[0, 1],
        description="Возможный дубликат активности",
    )
    uo_units: Series[str] = pa.Field(nullable=True, regex=r'^UO_\d{7}$', description="Unit Ontology ID")
    qudt_units: Series[str] = pa.Field(nullable=True, description="QUDT URI для единиц измерения")
    src_id: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        coerce=False,
        description="ID источника данных",
    )
    action_type: Series[str] = pa.Field(nullable=True, description="Тип действия лиганда")

    # Activity properties (JSON string)
    canonical_smiles: Series[str] = pa.Field(nullable=True, description="Канонический SMILES лиганда")
    target_organism: Series[str] = pa.Field(nullable=True, description="Организм таргета")
    target_tax_id: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        coerce=False,
        ge=1,
        description="NCBI Taxonomy ID таргета",
    )
    activity_properties: Series[str] = pa.Field(nullable=True, description="Свойства активности в каноническом виде")
    compound_key: Series[str] = pa.Field(nullable=True, description="Бизнес-ключ для связывания активности")
    is_citation: Series[bool] = pa.Field(nullable=True, description="Флаг наличия цитирования")
    high_citation_rate: Series[bool] = pa.Field(nullable=True, description="Высокая частота цитирований")
    exact_data_citation: Series[bool] = pa.Field(nullable=True, description="Флаг точного цитирования")
    rounded_data_citation: Series[bool] = pa.Field(nullable=True, description="Флаг округленного цитирования")

    # Ligand efficiency
    bei: Series[float] = pa.Field(nullable=True, description="Binding Efficiency Index")
    sei: Series[float] = pa.Field(nullable=True, description="Surface Efficiency Index")
    le: Series[float] = pa.Field(nullable=True, description="Ligand Efficiency")
    lle: Series[float] = pa.Field(nullable=True, description="Lipophilic Ligand Efficiency")

    # Fallback metadata
    fallback_reason: Series[str] = pa.Field(
        nullable=True,
        description="Reason why the fallback record was generated",
    )
    fallback_error_type: Series[str] = pa.Field(
        nullable=True,
        description="Exception class that triggered the fallback",
    )
    fallback_error_code: Series[str] = pa.Field(
        nullable=True,
        description="Normalized error code captured for the fallback",
    )
    fallback_error_message: Series[str] = pa.Field(
        nullable=True,
        description="Human readable error message captured for the fallback",
    )
    fallback_http_status: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="HTTP status associated with the fallback (if any)",
    )
    fallback_retry_after_sec: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Retry-After header (seconds) returned by the upstream API",
    )
    fallback_attempt: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Attempt number when the fallback was emitted",
    )
    fallback_timestamp: Series[str] = pa.Field(
        nullable=True,
        description="UTC timestamp when the fallback record was materialised",
    )

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    # Column order: system/hash fields first (per BaseSchema), then business fields
    _column_order = COLUMN_ORDER

    class Config(BaseSchema.Config):
        strict = True
        coerce = True
        ordered = False  # Column order enforced via pipeline determinism utilities


class _ActivityColumnOrderAccessor:
    """Descriptor exposing column order for backwards compatibility."""

    def __get__(self, instance, owner) -> list[str]:  # noqa: D401 - short accessor
        return ActivitySchema.get_column_order()


# Mirror behaviour from other schemas: surface column_order via Config without
# registering it as a Pandera check during model creation.
ActivitySchema.Config.ordered = False

