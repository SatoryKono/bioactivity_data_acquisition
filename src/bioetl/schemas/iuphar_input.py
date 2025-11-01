"""Pandera schema describing seed inputs for the IUPHAR pipeline."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series

__all__ = ["IupharInputSchema"]


class IupharInputSchema(pa.DataFrameModel):
    """Validate optional input files for ``gtp_iuphar`` executions."""

    targetId: Series[pd.Int64Dtype] = pa.Field(
        nullable=False,
        ge=0,
        description="Unique IUPHAR target identifier",
    )

    class Config:
        strict = True
        ordered = True
        coerce = True

