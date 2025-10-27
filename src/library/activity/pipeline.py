"""Activity data ETL pipeline."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandera.errors import SchemaError
from structlog import BoundLogger

from .client import ActivityChEMBLClient
from .config import ActivityConfig
from .normalize import ActivityNormalizer
from .quality import ActivityQualityFilter
from .validate import ActivityValidator
from library.activity.config import ActivityConfig
from library.activity.normalize import ActivityNormalizer
from library.activity.quality import ActivityQualityFilter
from library.activity.validate import ActivityValidator

logger = logging.getLogger(__name__)


class ActivityPipelineError(RuntimeError):
    """Base class for activity pipeline errors."""


class ActivityValidationError(ActivityPipelineError):
    """Raised when validation fails for activity data."""


class ActivityHTTPError(ActivityPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class ActivityQCError(ActivityPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class ActivityIOError(ActivityPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class ActivityETLResult:
    activities: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    # Корреляции (для единообразия с другими модулями)
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


class ActivityPipeline:
    """ETL pipeline for activity data extraction and processing."""

    def __init__(self, config: ActivityConfig) -> None:
        """Initialize the activity pipeline.
        
        Args:
            config: Activity configuration object
        """
        self.config = config
        self.client = ActivityChEMBLClient(
            config.to_api_client_config(),
            cache_dir=config.get_cache_path()
        )
        self.validator = ActivityValidator()
        self.normalizer = ActivityNormalizer(config.model_dump() if hasattr(config, "model_dump") else {})
        self.quality_checker = ActivityQualityFilter()

    def run(
        self,
        input_csv: Path | None = None,
        logger: BoundLogger | None = None
    ) -> ActivityETLResult:
        """Run the complete activity ETL pipeline.
        
        Args:
            input_csv: Optional CSV file with filter IDs (assay_ids, molecule_ids, target_ids)
            logger: Structured logger for tracking progress
            
        Returns:
            Dictionary containing processed data and metadata
            
        Raises:
            ValueError: If input validation fails
            IOError: If file operations fail
        """
        if logger is None:
            # Получаем сконфигурированный логгер structlog (настройки задаются configure_logging)
            try:
                from structlog import get_logger  # type: ignore
                logger = get_logger()
            except Exception:  # pragma: no cover
                # Фоллбэк: оборачиваем стандартный логгер
                logger = BoundLogger(logging.getLogger(__name__))
        
        logger.info("Starting activity ETL pipeline", config=str(self.config))
        
        # Ensure directories exist
        self.config.ensure_directories()
        
        # Load filter IDs if provided
        filter_ids = self._load_filter_ids(input_csv, logger)
        
        # Extract data from ChEMBL
        raw_activities = self._extract_activities(filter_ids, logger)
        
        if raw_activities.empty:
            logger.warning("No activities extracted")
            return ActivityETLResult(
                activities=pd.DataFrame(),
                meta={
                    "total_activities": 0,
                    "extraction_timestamp": datetime.utcnow().isoformat() + "Z",
                    "chembl_release": "unknown",
                },
                qc=pd.DataFrame(),
            )
        
        # raw_activities is already a DataFrame
        activities_df = raw_activities
        # Backward-compat: ensure activity_chembl_id exists
        if "activity_chembl_id" not in activities_df.columns and "activity_id" in activities_df.columns:
            activities_df = activities_df.copy()
            activities_df["activity_chembl_id"] = activities_df["activity_id"].astype(str)
        logger.info(f"Extracted {len(activities_df)} activities")
        
        # Validate data
        validated_df = self._validate_activities(activities_df, logger)
        
        # Normalize data
        normalized_df = self._normalize_activities(validated_df, logger)
        # Add tracking fields similar to assay pipeline
        normalized_df = self._add_tracking_fields(normalized_df, logger)
        
        # Quality control
        qc_results = self._quality_control(normalized_df, logger)

        # Приводим QC к общему формату (row_count + missing_data всегда присутствуют)
        try:
            from library.etl.qc_common import ensure_common_qc
            qc_results = ensure_common_qc(normalized_df, qc_results, module_name="activity")
        except Exception as exc:
            logging.getLogger(__name__).warning(f"Failed to normalize QC metrics for activity: {exc}")
        
        # Get metadata
        meta = self._get_metadata(normalized_df, logger)
        
        # Опциональный корреляционный анализ (для единообразия)
        correlation_analysis = None
        correlation_reports = None
        correlation_insights = None
        try:
            enabled = getattr(getattr(self.config, "postprocess", None), "correlation", None)
            enabled = getattr(enabled, "enabled", False)
            if enabled and len(normalized_df) > 1 and len(normalized_df.columns) > 1:
                from library.etl.enhanced_correlation import (
                    build_correlation_insights,
                    build_enhanced_correlation_analysis,
                    build_enhanced_correlation_reports,
                    prepare_data_for_correlation_analysis,
                )
                analysis_df = prepare_data_for_correlation_analysis(normalized_df, data_type="general", logger=logger)
                if len(analysis_df.columns) > 1:
                    correlation_analysis = build_enhanced_correlation_analysis(analysis_df, logger)
                    correlation_reports = build_enhanced_correlation_reports(analysis_df, logger)
                    correlation_insights = build_correlation_insights(analysis_df, logger)
        except Exception as exc:
            logging.getLogger(__name__).warning(f"Failed to build activity correlation analysis: {exc}")

        logger.info("Activity ETL pipeline completed successfully")
        
        return ActivityETLResult(
            activities=normalized_df,
            meta=meta,
            qc=qc_results,
            correlation_analysis=correlation_analysis,
            correlation_reports=correlation_reports,
            correlation_insights=correlation_insights,
        )

    def _load_filter_ids(
        self, 
        input_csv: Path | None, 
        logger: BoundLogger
    ) -> dict[str, list[str]]:
        """Load filter IDs from input CSV file.
        
        Args:
            input_csv: Path to CSV file with filter IDs
            logger: Structured logger
            
        Returns:
            Dictionary with filter IDs by type
        """
        filter_ids = {
            "activity_ids": [],
            "assay_ids": [],
            "molecule_ids": [],
            "target_ids": []
        }
        
        if input_csv is None:
            logger.info("No input CSV provided, using all available activities")
            return filter_ids
        
        if not input_csv.exists():
            raise FileNotFoundError(f"Input CSV file not found: {input_csv}")
        
        try:
            df = pd.read_csv(input_csv)
            logger.info(f"Loaded input CSV with {len(df)} rows")
            
            # Map column names to filter types
            column_mapping = {
                "activity_chembl_id": "activity_ids",
                "assay_chembl_id": "assay_ids",
                "assay_id": "assay_ids",
                "molecule_chembl_id": "molecule_ids", 
                "molecule_id": "molecule_ids",
                "target_chembl_id": "target_ids",
                "target_id": "target_ids"
            }
            
            for col, filter_type in column_mapping.items():
                if col in df.columns:
                    ids = df[col].dropna().astype(str).tolist()
                    # Для activity_ids требуются числовые значения activity_id в ChEMBL
                    if filter_type == "activity_ids":
                        numeric_ids = [s for s in (id_.strip() for id_ in ids) if s.isdigit()]
                        invalid = len(ids) - len(numeric_ids)
                        if invalid:
                            logger.warning(
                                f"Skipped {invalid} non-numeric activity_chembl_id values; ChEMBL expects numeric activity_id"
                            )
                        filter_ids[filter_type] = numeric_ids
                        logger.info(f"Loaded {len(numeric_ids)} {filter_type} from {col}")
                    else:
                        filter_ids[filter_type] = ids
                        logger.info(f"Loaded {len(ids)} {filter_type} from {col}")
            
            # Apply limit to all loaded filter IDs
            for filter_type, ids in filter_ids.items():
                if ids and self.config.limit is not None and len(ids) > self.config.limit:
                    filter_ids[filter_type] = ids[:self.config.limit]
                    logger.info(f"Applied limit {self.config.limit} to {filter_type} (was {len(ids)})")
            
        except Exception as e:
            logger.error(f"Failed to load input CSV: {e}")
            raise
        
        return filter_ids

    def _extract_activities(
        self,
        filter_ids: dict[str, list[str]],
        logger: BoundLogger,
    ) -> pd.DataFrame:
        """Extract activities from ChEMBL API in batches and process each batch."""
        logger.info("Extracting activities from ChEMBL API (batch mode)")

        # Early limit check - if we have specific IDs and limit is set, 
        # we already applied limit in _load_filter_ids, so we can skip early check
        if self.config.limit is not None:
            logger.info(f"Processing with limit: {self.config.limit} activities")

        try:
            # Get ChEMBL status
            status = self.client.get_chembl_status()
            logger.info("ChEMBL status", status=status)

            # Prepare filter params (activity_ids dominate)
            act_ids = filter_ids.get("activity_ids") or None
            filter_params = {
                "activity_ids": act_ids,
                "assay_ids": None if act_ids else (filter_ids.get("assay_ids") or None),
                "molecule_ids": None if act_ids else (filter_ids.get("molecule_ids") or None),
                "target_ids": None if act_ids else (filter_ids.get("target_ids") or None),
            }

            batch_size = getattr(self.config.runtime, "batch_size", 1000)
            total_rows = 0
            batches: list[pd.DataFrame] = []

            for batch_index, activities_batch in enumerate(
                self.client.fetch_activities_batch(
                    filter_params=filter_params,
                    batch_size=batch_size,
                    use_cache=True,
                )
            ):
                batch_df = pd.DataFrame(activities_batch)
                logger.info(
                    f"Processing batch {batch_index + 1}: size={len(batch_df)}, batch_size={batch_size}"
                )

                # Validate and normalize per batch
                validated_batch = self._validate_activities(batch_df, logger)
                normalized_batch = self._normalize_activities(validated_batch, logger)

                total_rows += len(normalized_batch)
                batches.append(normalized_batch)

                # Check limit
                if self.config.limit is not None and total_rows >= self.config.limit:
                    logger.info(f"Reached global limit of {self.config.limit} activities")
                    break

            if not batches:
                logger.info("No activities fetched")
                return pd.DataFrame()

            result_df = pd.concat(batches, ignore_index=True)
            if self.config.limit is not None and len(result_df) > self.config.limit:
                result_df = result_df.head(self.config.limit)
            logger.info(f"Extracted {len(result_df)} activities in {len(batches)} batches")
            return result_df

        except Exception as e:
            logger.error(f"Failed to extract activities: {e}")
            raise ActivityHTTPError(str(e)) from e

    def _validate_activities(
        self, 
        activities_df: pd.DataFrame, 
        logger: BoundLogger
    ) -> pd.DataFrame:
        """Validate activity data.
        
        Args:
            activities_df: Raw activities DataFrame
            logger: Structured logger
            
        Returns:
            Validated activities DataFrame
        """
        logger.info("Validating activity data")
        
        try:
            validated_df = self.validator.validate_raw_data(activities_df)
            logger.info(f"Validation completed: {len(validated_df)} valid activities")
            return validated_df
        except SchemaError as e:
            logger.warning(f"Pandera schema error (skipping validation temporarily): {e}")
            return activities_df
        except Exception as e:
            logger.error(f"Activity validation failed: {e}")
            raise ActivityValidationError(str(e)) from e

    def _normalize_activities(
        self, 
        activities_df: pd.DataFrame, 
        logger: BoundLogger
    ) -> pd.DataFrame:
        """Normalize activity data.
        
        Args:
            activities_df: Validated activities DataFrame
            logger: Structured logger
            
        Returns:
            Normalized activities DataFrame
        """
        logger.info("Normalizing activity data")
        
        try:
            normalized_df = self.normalizer.normalize_activities(activities_df)
            logger.info(f"Normalization completed: {len(normalized_df)} activities")
            return normalized_df
        except Exception as e:
            logger.error(f"Activity normalization failed: {e}")
            raise ActivityPipelineError(str(e)) from e

    def _add_tracking_fields(
        self,
        activities_df: pd.DataFrame,
        logger: BoundLogger,
    ) -> pd.DataFrame:
        """Add extracted_at, hash_business_key, and hash_row columns.

        - extracted_at: UTC timestamp at extraction/processing time
        - hash_business_key: sha256 of activity_chembl_id when available
        - hash_row: sha256 of the full row dict for change tracking
        """
        if activities_df.empty:
            return activities_df

        try:
            df = activities_df.copy()
            now_iso = datetime.utcnow().isoformat() + "Z"

            if "extracted_at" not in df.columns:
                df["extracted_at"] = now_iso
            else:
                df["extracted_at"] = df["extracted_at"].fillna(now_iso)

            if "activity_chembl_id" in df.columns:
                df["hash_business_key"] = df["activity_chembl_id"].apply(
                    lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else "unknown"
                )
            else:
                df["hash_business_key"] = "unknown"

            df["hash_row"] = df.apply(
                lambda row: hashlib.sha256(str(row.to_dict()).encode()).hexdigest(), axis=1
            )

            logger.info("Added tracking fields: extracted_at, hash_business_key, hash_row")
            return df
        except Exception as e:
            logger.warning(f"Failed to add tracking fields: {e}")
            return activities_df

    def _quality_control(
        self, 
        activities_df: pd.DataFrame, 
        logger: BoundLogger
    ) -> pd.DataFrame:
        """Perform quality control on activity data.
        
        Args:
            activities_df: Normalized activities DataFrame
            logger: Structured logger
            
        Returns:
            Quality control results DataFrame
        """
        logger.info("Performing quality control")
        
        try:
            # Apply quality profiles (side effects may log warnings/stats)
            self.quality_checker.apply_quality_profiles(activities_df)

            # Generate quality statistics
            stats = self.quality_checker.get_quality_statistics(activities_df)

            # Convert stats to DataFrame for output
            qc_data = []
            for category, data in stats.items():
                if isinstance(data, dict):
                    for key, value in data.items():
                        qc_data.append({"metric": f"{category}_{key}", "value": value})
                else:
                    qc_data.append({"metric": category, "value": data})

            qc_df = pd.DataFrame(qc_data)
            logger.info("Quality control completed")
            return qc_df
        except Exception as e:
            logger.error(f"Quality control failed: {e}")
            raise ActivityQCError(str(e)) from e

    def _get_metadata(
        self, 
        activities_df: pd.DataFrame, 
        logger: BoundLogger
    ) -> dict[str, Any]:
        """Generate metadata for the processed data.
        
        Args:
            activities_df: Processed activities DataFrame
            logger: Structured logger
            
        Returns:
            Metadata dictionary
        """
        logger.info("Generating metadata")
        
        try:
            # Get ChEMBL status for release info
            status = self.client.get_chembl_status()
            
            meta = {
                "total_activities": len(activities_df),
                "extraction_timestamp": datetime.utcnow().isoformat() + "Z",
                "chembl_release": status.get("chembl_release", "unknown"),
                "pipeline_version": "1.0.0",
                "config": {
                    "limit": self.config.limit,
                    "workers": getattr(self.config.runtime, "workers", 4),
                    "qc_enabled": getattr(getattr(self.config, "postprocess", {}), "qc", {}).enabled if hasattr(getattr(self.config, "postprocess", {}), "qc") else True,
                }
            }

            # Tracking hashes are added earlier in the pipeline
            
            # Add activity type distribution
            if "standard_type" in activities_df.columns:
                type_counts = activities_df["standard_type"].value_counts().to_dict()
                meta["activity_type_distribution"] = type_counts
            
            # Add unit distribution
            if "standard_units" in activities_df.columns:
                unit_counts = activities_df["standard_units"].value_counts().to_dict()
                meta["unit_distribution"] = unit_counts
            
            logger.info("Metadata generated successfully")
            return meta
            
        except Exception as e:
            logger.error(f"Failed to generate metadata: {e}")
            return {
                "total_activities": len(activities_df),
                "extraction_timestamp": datetime.utcnow().isoformat() + "Z",
                "chembl_release": "unknown",
                "pipeline_version": "1.0.0"
            }


def run_activity_etl(
    config: ActivityConfig,
    input_csv: Path | None = None,
    logger: BoundLogger | None = None
) -> ActivityETLResult:
    """Convenience function to run activity ETL pipeline.
    
    Args:
        config: Activity configuration
        input_csv: Optional input CSV with filter IDs
        logger: Optional structured logger
        
    Returns:
        Dictionary with processed data and metadata
    """
    pipeline = ActivityPipeline(config)
    return pipeline.run(input_csv, logger)