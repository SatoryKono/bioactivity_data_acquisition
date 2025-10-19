"""Activity ETL pipeline orchestration (по образцу documents)."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.activity.config import ActivityConfig
from library.config import APIClientConfig
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.etl.load import write_deterministic_csv
from library.etl.transform import normalize_bioactivity_data


class ActivityPipelineError(RuntimeError):
    """Base class for activity pipeline errors."""


class ActivityValidationError(ActivityPipelineError):
    """Raised when the input data does not meet schema expectations."""


class ActivityHTTPError(ActivityPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class ActivityQCError(ActivityPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class ActivityIOError(ActivityPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class ActivityETLResult:
    """Container for activity ETL artefacts."""

    activity: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


_REQUIRED_INPUT_COLUMNS = {"document_chembl_id", "assay_chembl_id", "target_chembl_id"}

logger = logging.getLogger(__name__)


def read_activity_input(path: Path) -> pd.DataFrame:
    """Load the input CSV containing identifiers for activity extraction."""

    try:
        frame = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise ActivityIOError(f"Input CSV not found: {path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise ActivityValidationError("Input CSV is empty") from exc
    except OSError as exc:  # pragma: no cover
        raise ActivityIOError(f"Failed to read input CSV: {exc}") from exc
    return frame


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalised = frame.copy()

    present = {column for column in normalised.columns}
    if not (present & _REQUIRED_INPUT_COLUMNS):
        raise ActivityValidationError("Input data must contain at least one of the following columns: " + ", ".join(sorted(_REQUIRED_INPUT_COLUMNS)))

    # Trim common identifiers if present
    for col in ["document_chembl_id", "assay_chembl_id", "target_chembl_id"]:
        if col in normalised.columns:
            normalised[col] = normalised[col].astype(str).str.strip()

    # Stable order for determinism in upstream enrichment
    sort_key = [c for c in ["assay_chembl_id", "target_chembl_id", "document_chembl_id"] if c in normalised.columns]
    if sort_key:
        normalised = normalised.sort_values(sort_key).reset_index(drop=True)

    return normalised


def _create_api_client(source: str, config: ActivityConfig) -> Any:
    """Create an API client for the specified source (chembl only for now)."""

    from library.clients.chembl import ChEMBLClient  # type: ignore
    from library.config import RateLimitSettings, RetrySettings

    source_config = config.sources.get(source)
    if not source_config:
        raise ActivityValidationError(f"Source '{source}' not found in configuration")

    timeout = source_config.http.timeout_sec or config.http.global_.timeout_sec

    # Merge headers: default + global + source-specific
    headers = {"User-Agent": "bioactivity-data-acquisition/0.1.0", "Accept": "application/json"}
    headers = {**headers, **config.http.global_.headers, **source_config.http.headers}

    # Use source-specific base_url or default ChEMBL
    base_url = source_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"

    retry_settings = RetrySettings(
        total=source_config.http.retries.get("total", config.http.global_.retries.total),
        backoff_multiplier=source_config.http.retries.get("backoff_multiplier", config.http.global_.retries.backoff_multiplier),
    )

    rate_limit = None
    if source_config.rate_limit:
        max_calls = source_config.rate_limit.get("max_calls")
        period = source_config.rate_limit.get("period")
        if max_calls is not None and period is not None:
            rate_limit = RateLimitSettings(max_calls=max_calls, period=period)

    api_config = APIClientConfig(
        name=source,
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
        endpoint=source_config.endpoint or "activity",
        params=source_config.params,
        pagination_param=source_config.pagination.page_param,
        page_size_param=source_config.pagination.size_param,
        page_size=source_config.pagination.size,
        max_pages=source_config.pagination.max_pages,
    )

    return ChEMBLClient(api_config, timeout=timeout)


def _calculate_business_key_hash(activity_id: Any) -> str:
    """Calculate hash for business key (activity_id)."""
    business_key = str(activity_id) if pd.notna(activity_id) else ""
    return hashlib.sha256(business_key.encode()).hexdigest()[:16]


def _calculate_row_hash(row_data: dict[str, Any]) -> str:
    """Calculate hash for entire row for deduplication."""
    # Создаем строку из всех значений, исключая хеши и технические поля
    filtered_data = {k: v for k, v in row_data.items() if not k.startswith("hash_") and k not in ["source_system", "chembl_release"]}
    row_string = json.dumps(filtered_data, sort_keys=True, default=str)
    return hashlib.sha256(row_string.encode()).hexdigest()[:16]


def _extract_activity_from_source(source: str, client: Any, frame: pd.DataFrame, config: ActivityConfig) -> pd.DataFrame:
    """Extract raw activity records from the configured API using identifiers in frame.

    Поддерживает 2 режима:
    - batch: запрос по списку activity_chembl_id из входного CSV
    - pagination (наследие): постраничная выборка без входных ID
    """

    from library.clients.bioactivity import BioactivityClient

    bio_client = BioactivityClient(client.config)
    source_config = config.sources.get(source)

    # Batch режим по ID
    if source_config and getattr(source_config, "batch", None) and source_config.batch.enabled:
        logger.info(f"Using batch mode for source '{source}'")

        id_field = source_config.batch.id_field
        if id_field not in frame.columns:
            raise ActivityValidationError(f"Batch mode enabled but '{id_field}' column not found in input data. Available columns: {list(frame.columns)}")

        activity_ids = frame[id_field].dropna().astype(str).unique().tolist()

        # Ограничения по числу батчей и общему лимиту
        if source_config.batch.max_batches:
            max_ids = source_config.batch.size * source_config.batch.max_batches
            activity_ids = activity_ids[:max_ids]

        if config.runtime.limit:
            activity_ids = activity_ids[: config.runtime.limit]

        logger.info(f"Fetching {len(activity_ids)} activities in batches of {source_config.batch.size}")

        if source_config.batch.parallel_workers and source_config.batch.parallel_workers > 1:
            records = bio_client.fetch_records_by_ids_parallel(
                identifiers=activity_ids,
                batch_size=source_config.batch.size,
                filter_param=source_config.batch.filter_param,
                max_workers=source_config.batch.parallel_workers,
            )
        else:
            records = bio_client.fetch_records_by_ids(
                identifiers=activity_ids,
                batch_size=source_config.batch.size,
                filter_param=source_config.batch.filter_param,
            )

    else:
        # Пагинация (legacy)
        # Применяем лимит из конфигурации к пагинации
        if config.runtime.limit and source_config and source_config.pagination:
            page_size = source_config.pagination.size
            max_pages_needed = (config.runtime.limit + page_size - 1) // page_size  # округляем вверх
            original_max_pages = source_config.pagination.max_pages
            source_config.pagination.max_pages = min(max_pages_needed, original_max_pages)

        records = bio_client.fetch_records()
    if not records:
        return pd.DataFrame(
            columns=[
                "source",
                "retrieved_at",
                "target_pref_name",
                "standard_value",
                "standard_units",
                "canonical_smiles",
            ]
        )

    df = pd.DataFrame(records)

    # Применяем лимит к результату
    if config.runtime.limit and len(df) > config.runtime.limit:
        df = df.head(config.runtime.limit)

    # Преобразуем retrieved_at в pd.Timestamp
    if "retrieved_at" in df.columns:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True)

    # Преобразуем числовые поля из строк в float
    numeric_fields = ["standard_value", "value", "standard_upper_value", "upper_value", "lower_value", "standard_lower_value", "pchembl_value"]
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce")

    return df


def run_activity_etl(config: ActivityConfig, frame: pd.DataFrame) -> ActivityETLResult:
    """Execute the activity ETL pipeline returning artefacts."""

    normalised_input = _normalise_columns(frame)

    enabled_sources = config.enabled_sources()
    if not enabled_sources:
        raise ActivityValidationError("No enabled sources configured")

    # For now single source (chembl)
    source = enabled_sources[0]

    # Получаем версию ChEMBL API
    client = _create_api_client(source, config)
    try:
        from library.assay.client import AssayClient

        # Используем AssayClient для получения статуса ChEMBL
        assay_client = AssayClient(client.config)
        status_info = assay_client.get_chembl_status()
        chembl_release = status_info.get("chembl_release", "unknown")
        if chembl_release is None:
            chembl_release = "unknown"
        logger.info(f"ChEMBL release: {chembl_release}")
    except Exception as e:
        logger.warning(f"Failed to get ChEMBL status: {e}")
        chembl_release = "unknown"

    logger.info(f"Extracting activity from {source}...")
    raw_df = _extract_activity_from_source(source, client, normalised_input, config)

    # Добавляем технические поля в сырые данные
    raw_df["source_system"] = "ChEMBL"
    raw_df["chembl_release"] = chembl_release

    # Добавляем хеши в сырые данные (для совместимости со схемой)
    raw_df["hash_business_key"] = raw_df["activity_id"].apply(_calculate_business_key_hash)
    raw_df["hash_row"] = raw_df.apply(lambda row: _calculate_row_hash(row.to_dict()), axis=1)

    # Debug: print raw data info
    logger.info(f"Raw data shape: {raw_df.shape}")
    logger.info(f"Raw data columns: {list(raw_df.columns)}")
    logger.info(f"Raw data dtypes: {raw_df.dtypes.to_dict()}")
    if not raw_df.empty:
        logger.info(f"First row sample: {raw_df.iloc[0].to_dict()}")

    # Normalize raw bioactivity records to consistent schema
    # Примечание: технические поля уже есть в raw_df, они будут скопированы в normalized_df
    normalized_df = normalize_bioactivity_data(
        raw_df,
        transforms=None,
        determinism=config.determinism,
        logger=logger,
    )

    # Пересчитываем хеш строки для нормализованных данных (так как данные изменились)
    normalized_df["hash_row"] = normalized_df.apply(lambda row: _calculate_row_hash(row.to_dict()), axis=1)

    # S07: Perform correlation analysis if enabled in config
    logger.info("S07: Performing correlation analysis...")
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None

    if hasattr(config, "postprocess") and hasattr(config.postprocess, "correlation") and config.postprocess.correlation.enabled and len(normalized_df) > 1:
        try:
            logger.info("Performing correlation analysis...")
            logger.info(f"Input data shape: {normalized_df.shape}")
            logger.info(f"Input columns: {list(normalized_df.columns)}")

            # Prepare data for correlation analysis
            analysis_df = prepare_data_for_correlation_analysis(normalized_df, data_type="activities", logger=logger)

            if len(analysis_df.columns) > 1:  # Need at least 2 numeric columns
                logger.info("Starting enhanced correlation analysis...")
                # Perform correlation analysis
                correlation_analysis = build_enhanced_correlation_analysis(analysis_df, logger)
                logger.info("Enhanced correlation analysis completed")

                logger.info("Building correlation reports...")
                correlation_reports = build_enhanced_correlation_reports(analysis_df, logger)
                logger.info(f"Generated {len(correlation_reports)} correlation reports")

                logger.info("Building correlation insights...")
                correlation_insights = build_correlation_insights(analysis_df, logger)
                logger.info(f"Correlation analysis completed. Found {len(correlation_insights)} insights.")
            else:
                logger.warning("Not enough numeric columns for correlation analysis")
                logger.warning(f"Available columns: {list(analysis_df.columns)}")

        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            import traceback

            logger.warning(f"Traceback: {traceback.format_exc()}")
            # Continue without correlation analysis

    # QC metrics
    qc_metrics = [
        {"metric": "row_count", "value": int(len(normalized_df))},
        {"metric": f"{source}_records", "value": int(len(raw_df))},
    ]
    qc = pd.DataFrame(qc_metrics)

    # Meta
    meta = {
        "pipeline": "activity",
        "pipeline_version": "1.0.0",
        "row_count": int(len(normalized_df)),
        "enabled_sources": enabled_sources,
        "chembl_release": chembl_release,
        "extraction_parameters": {
            "chembl_records": int(len(raw_df)),
            "correlation_analysis_enabled": config.postprocess.correlation.enabled,
            "correlation_insights_count": len(correlation_insights) if correlation_insights else 0,
        },
    }

    return ActivityETLResult(
        activity=normalized_df, qc=qc, meta=meta, correlation_analysis=correlation_analysis, correlation_reports=correlation_reports, correlation_insights=correlation_insights
    )


def _calculate_checksum(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def write_activity_outputs(result: ActivityETLResult, output_dir: Path, date_tag: str, config: ActivityConfig | None = None) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover
        raise ActivityIOError(f"Failed to create output directory: {exc}") from exc

    activity_path = output_dir / f"activity_{date_tag}.csv"
    qc_path = output_dir / f"activity_{date_tag}_qc.csv"
    meta_path = output_dir / f"activity_{date_tag}_meta.yaml"

    # Deterministic writes
    if config is not None:
        write_deterministic_csv(result.activity, activity_path, determinism=config.determinism, output=None)
        write_deterministic_csv(result.qc, qc_path, determinism=config.determinism, output=None)
    else:
        result.activity.to_csv(activity_path, index=False)
        result.qc.to_csv(qc_path, index=False)

    # Save metadata
    import yaml

    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

    # Save correlation reports if available
    outputs: dict[str, Path] = {"activity": activity_path, "qc": qc_path, "meta": meta_path}

    if result.correlation_reports:
        try:
            correlation_dir = output_dir / f"activity_correlation_report_{date_tag}"
            correlation_dir.mkdir(exist_ok=True)

            # Save each correlation report and expose as flat keys similar to documents
            for report_name, report_df in result.correlation_reports.items():
                if report_df is not None and not report_df.empty:
                    report_path = correlation_dir / f"{report_name}.csv"
                    report_df.to_csv(report_path, index=False)
                    outputs[f"correlation_{report_name}"] = report_path

            # Save insights as JSON if present
            if result.correlation_insights:
                import json

                insights_path = correlation_dir / "correlation_insights.json"
                with open(insights_path, "w", encoding="utf-8") as f:
                    json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                outputs["correlation_insights"] = insights_path

            logger.info(f"Correlation reports saved to: {correlation_dir}")

        except Exception as exc:
            logger.warning(f"Failed to save correlation reports: {exc}")

    # Add checksums
    result.meta["file_checksums"] = {
        "csv": _calculate_checksum(activity_path),
        "qc": _calculate_checksum(qc_path),
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

    return outputs


__all__ = [
    "ActivityETLResult",
    "ActivityHTTPError",
    "ActivityIOError",
    "ActivityPipelineError",
    "ActivityQCError",
    "ActivityValidationError",
    "read_activity_input",
    "run_activity_etl",
    "write_activity_outputs",
]
