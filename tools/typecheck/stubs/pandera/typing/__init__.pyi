from __future__ import annotations

from typing import Generic, TypeVar
from pandas import DataFrame as PandasDataFrame, Series as PandasSeries

_T = TypeVar("_T")
_GenericDtype = TypeVar("_GenericDtype")

class SeriesBase:
    ...

class DataFrameBase:
    ...

class Series(SeriesBase, PandasSeries, Generic[_GenericDtype]):
    ...

class DataFrame(DataFrameBase, PandasDataFrame, Generic[_T]):
    ...

__all__ = ["Series", "DataFrame"]
