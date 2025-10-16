"""Transformation logic for the ETL pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from pandera.errors import SchemaErrors
from structlog.stdlib import BoundLogger

from library.schemas import NormalizedBioactivitySchema, RawBioactivitySchema

if TYPE_CHECKING:  # pragma: no cover - for type checking only
    from library.config import DeterminismSettings, TransformSettings


def _resolve_ascending(by: list[str], ascending: list[bool] | bool) -> list[bool] | bool:
    """Normalize the ascending parameter for ``DataFrame.sort_values``."""

    if isinstance(ascending, bool):
        return ascending
    if len(ascending) == len(by):
        return ascending
    if len(ascending) == 1:
        return ascending * len(by)
    raise ValueError("Ascending configuration must be a bool or match the sort keys length.")


def _convert_to_nanomolar(df: pd.DataFrame, unit_conversion: dict[str, float]) -> pd.Series:
    # Поддерживаем как старые, так и новые имена колонок из ChEMBL API
    units_col = "activity_units" if "activity_units" in df.columns else "standard_units"
    value_col = "activity_value" if "activity_value" in df.columns else "standard_value"
    
    factors = df[units_col].map(unit_conversion)
    if factors.isnull().any():
        unknown = sorted(df.loc[factors.isnull(), units_col].unique())
        raise ValueError(f"Unsupported activity units: {', '.join(unknown)}")
    return df[value_col].astype(float) * factors


def normalize_bioactivity_data(
    df: pd.DataFrame,
    *,
    transforms: TransformSettings | None = None,
    determinism: DeterminismSettings | None = None,
    logger: BoundLogger | None = None,
) -> pd.DataFrame:
    """Normalize raw bioactivity data to a consistent schema using configuration."""

    from library.config import DeterminismSettings as _DeterminismSettings
    from library.config import TransformSettings as _TransformSettings

    transforms = transforms or _TransformSettings()
    determinism = determinism or _DeterminismSettings()

    raw_schema = RawBioactivitySchema.to_schema()
    try:
        validated = raw_schema.validate(df, lazy=True)
    except SchemaErrors as exc:
        failure_cases = getattr(exc, "failure_cases", None)
        if failure_cases is not None and "column" in failure_cases:
            mask = failure_cases["column"] == "activity_units"
            if mask.any():
                invalid = sorted({str(value) for value in failure_cases.loc[mask, "failure_case"]})
                raise ValueError(f"Unsupported activity units: {', '.join(invalid)}") from exc
        raise
    if validated.empty:
        normalized_schema = NormalizedBioactivitySchema.to_schema()
        empty = normalized_schema.empty_dataframe()  # type: ignore[attr-defined]
        return normalized_schema.validate(empty, lazy=True)

    normalized = validated.copy()
    normalized["retrieved_at"] = pd.to_datetime(normalized["retrieved_at"], utc=True)
    
    # Маппим колонки из ChEMBL API в стандартный формат
    if "molecule_chembl_id" in normalized.columns:
        normalized = normalized.rename(columns={"molecule_chembl_id": "compound_id"})
    if "canonical_smiles" in normalized.columns:
        normalized = normalized.rename(columns={"canonical_smiles": "smiles"})
    if "target_pref_name" in normalized.columns:
        normalized = normalized.rename(columns={"target_pref_name": "target"})
    
    # Преобразуем значения активности
    normalized["activity_value"] = _convert_to_nanomolar(normalized, transforms.unit_conversion)
    normalized["activity_unit"] = "nM"
    
    # Удаляем старые колонки
    columns_to_drop = ["activity_units", "standard_units", "standard_value"]
    normalized = normalized.drop(columns=[col for col in columns_to_drop if col in normalized.columns], errors="ignore")

    # Сохраняем только колонки, указанные в column_order
    desired_order = [col for col in determinism.column_order if col in normalized.columns]
    # Исключаем лишние колонки - сохраняем только те, что указаны в конфигурации
    if desired_order:
        normalized = normalized[desired_order]

    sort_by = determinism.sort.by or desired_order
    sort_ascending = _resolve_ascending(sort_by, determinism.sort.ascending)
    normalized = normalized.sort_values(
        sort_by,
        ascending=sort_ascending,
        na_position=determinism.sort.na_position,
    ).reset_index(drop=True)

    result = NormalizedBioactivitySchema.validate(normalized, lazy=True)
    if logger is not None:
        logger.info("transform_complete", rows=len(result))
    return result


__all__ = ["normalize_bioactivity_data"]
