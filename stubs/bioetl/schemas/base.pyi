from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import DataFrameModel
from bioetl.pandera_typing import DataFrame as PanderaDataFrame, Series

FALLBACK_METADATA_COLUMN_ORDER: list[str]


class FallbackMetadataMixin:
    fallback_reason: Series[str]
    fallback_error_type: Series[str]
    fallback_error_code: Series[str]
    fallback_error_message: Series[str]
    fallback_http_status: Series[pd.Int64Dtype]
    fallback_retry_after_sec: Series[float]
    fallback_attempt: Series[pd.Int64Dtype]
    fallback_timestamp: Series[str]


class BaseSchema(DataFrameModel):
    index: Series[int]
    hash_row: Series[str]
    hash_business_key: Series[str]
    pipeline_version: Series[str]
    run_id: Series[str]
    source_system: Series[str]
    chembl_release: Series[str]
    extracted_at: Series[str]

    class Config:
        strict: bool
        coerce: bool
        ordered: bool

    @classmethod
    def validate(
        cls,
        check_obj: pd.DataFrame,
        head: int | None = ...,
        tail: int | None = ...,
        sample: int | None = ...,
        random_state: int | None = ...,
        lazy: bool = ...,
        inplace: bool = ...,
    ) -> PanderaDataFrame[BaseSchema]:
        ...

    @classmethod
    def get_column_order(cls) -> list[str]:
        ...


def expose_config_column_order(schema_cls: type[BaseSchema]) -> None:
    ...
