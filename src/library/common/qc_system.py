"""Unified Quality Control (QC) system for the bioactivity data acquisition pipeline.

This module provides a comprehensive QC system that consolidates all quality
control functionality across different entity types and provides standardized
QC reporting, validation, and analysis capabilities.
"""

from __future__ import annotations

import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger

# from .exceptions import ValidationError

# Suppress numpy warnings for division by zero
warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in divide")

T = TypeVar("T")


class QCLevel(Enum):
    """QC analysis levels."""

    BASIC = "basic"  # Basic completeness metrics
    STANDARD = "standard"  # Standard QC with validation
    ENHANCED = "enhanced"  # Enhanced QC with detailed analysis
    COMPREHENSIVE = "comprehensive"  # Comprehensive QC with all metrics


class QCMetricType(Enum):
    """Types of QC metrics."""

    COMPLETENESS = "completeness"  # Data completeness metrics
    VALIDITY = "validity"  # Data validity metrics
    CONSISTENCY = "consistency"  # Data consistency metrics
    STATISTICAL = "statistical"  # Statistical metrics
    PATTERN = "pattern"  # Pattern-based metrics
    CUSTOM = "custom"  # Custom entity-specific metrics


@dataclass
class QCMetric:
    """QC metric definition."""

    name: str
    value: Any
    metric_type: QCMetricType
    description: str
    threshold: float | None = None
    status: str = "pass"  # "pass", "warning", "fail"
    severity: str = "medium"  # "low", "medium", "high", "critical"


@dataclass
class QCReport:
    """QC report container."""

    entity_type: str
    level: QCLevel
    metrics: list[QCMetric]
    summary: dict[str, Any]
    recommendations: list[str]
    overall_status: str = "pass"  # "pass", "warning", "fail"

    def to_dataframe(self) -> pd.DataFrame:
        """Convert QC report to DataFrame."""
        data = []
        for metric in self.metrics:
            data.append(
                {
                    "metric": metric.name,
                    "value": metric.value,
                    "type": metric.metric_type.value,
                    "description": metric.description,
                    "threshold": metric.threshold,
                    "status": metric.status,
                    "severity": metric.severity,
                }
            )

        return pd.DataFrame(data)

    def to_dict(self) -> dict[str, Any]:
        """Convert QC report to dictionary."""
        return {
            "entity_type": self.entity_type,
            "level": self.level.value,
            "overall_status": self.overall_status,
            "summary": self.summary,
            "recommendations": self.recommendations,
            "metrics": [
                {
                    "name": metric.name,
                    "value": metric.value,
                    "type": metric.metric_type.value,
                    "description": metric.description,
                    "threshold": metric.threshold,
                    "status": metric.status,
                    "severity": metric.severity,
                }
                for metric in self.metrics
            ],
        }


class QCAnalyzer(ABC):
    """Abstract base class for QC analyzers."""

    def __init__(self, entity_type: str, logger: BoundLogger | None = None):
        self.entity_type = entity_type
        self.logger = logger
        self._patterns = self._get_patterns()

    @abstractmethod
    def analyze(self, data: pd.DataFrame, level: QCLevel = QCLevel.STANDARD) -> QCReport:
        """Analyze data and generate QC report."""
        pass

    @abstractmethod
    def _get_key_columns(self) -> list[str]:
        """Get key columns for this entity type."""
        pass

    def _get_patterns(self) -> dict[str, re.Pattern]:
        """Get regex patterns for data validation."""
        return {
            "doi": re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE),
            "issn": re.compile(r"\b\d{4}-\d{3}[\dxX]\b"),
            "isbn": re.compile(r"\b(?:ISBN[- ]?(?:13|10)?:? )?(?:97[89][- ]?)?\d{1,5}[- ]?\d{1,7}[- ]?\d{1,6}[- ]?\d\b"),
            "url": re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE),
            "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
            "chembl_id": re.compile(r"^CHEMBL\d+$", re.IGNORECASE),
            "pmid": re.compile(r"^\d+$"),
            "smiles": re.compile(r"^[A-Za-z0-9@+\-\[\]()=#\\/]+$"),
        }

    def _analyze_completeness(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze data completeness."""
        metrics = []

        # Row count
        row_count = len(data)
        metrics.append(QCMetric(name="row_count", value=row_count, metric_type=QCMetricType.COMPLETENESS, description="Total number of records"))

        # Missing data analysis
        key_columns = self._get_key_columns()
        for column in key_columns:
            if column in data.columns:
                missing_count = int(data[column].isna().sum())
                missing_percentage = (missing_count / row_count * 100) if row_count > 0 else 0

                status = "pass"
                if missing_percentage > 10:
                    status = "warning"
                if missing_percentage > 50:
                    status = "fail"

                metrics.append(
                    QCMetric(
                        name=f"missing_{column}",
                        value=missing_count,
                        metric_type=QCMetricType.COMPLETENESS,
                        description=f"Missing values in {column}",
                        threshold=10.0,
                        status=status,
                    )
                )

        return metrics

    def _analyze_validity(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze data validity."""
        metrics = []

        # Pattern validation
        for pattern_name, pattern in self._patterns.items():
            if pattern_name in data.columns:
                valid_count = 0
                total_count = 0

                for value in data[pattern_name].dropna():
                    total_count += 1
                    if pattern.match(str(value)):
                        valid_count += 1

                if total_count > 0:
                    validity_percentage = valid_count / total_count * 100
                    status = "pass"
                    if validity_percentage < 90:
                        status = "warning"
                    if validity_percentage < 70:
                        status = "fail"

                    metrics.append(
                        QCMetric(
                            name=f"valid_{pattern_name}",
                            value=validity_percentage,
                            metric_type=QCMetricType.VALIDITY,
                            description=f"Valid {pattern_name} format percentage",
                            threshold=90.0,
                            status=status,
                        )
                    )

        return metrics

    def _analyze_consistency(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze data consistency."""
        metrics = []

        # Duplicate analysis
        key_columns = self._get_key_columns()
        for column in key_columns:
            if column in data.columns:
                duplicates = data[column].duplicated().sum()
                duplicate_percentage = (duplicates / len(data) * 100) if len(data) > 0 else 0

                status = "pass"
                if duplicate_percentage > 5:
                    status = "warning"
                if duplicate_percentage > 20:
                    status = "fail"

                metrics.append(
                    QCMetric(
                        name=f"duplicates_{column}",
                        value=duplicates,
                        metric_type=QCMetricType.CONSISTENCY,
                        description=f"Duplicate values in {column}",
                        threshold=5.0,
                        status=status,
                    )
                )

        return metrics

    def _analyze_statistics(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze statistical properties."""
        metrics = []

        # Numeric columns analysis
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        for column in numeric_columns:
            if data[column].notna().sum() > 0:
                # Outlier detection using IQR
                Q1 = data[column].quantile(0.25)
                Q3 = data[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                outliers = ((data[column] < lower_bound) | (data[column] > upper_bound)).sum()
                outlier_percentage = (outliers / len(data) * 100) if len(data) > 0 else 0

                status = "pass"
                if outlier_percentage > 10:
                    status = "warning"
                if outlier_percentage > 25:
                    status = "fail"

                metrics.append(
                    QCMetric(name=f"outliers_{column}", value=outliers, metric_type=QCMetricType.STATISTICAL, description=f"Outliers in {column}", threshold=10.0, status=status)
                )

        return metrics

    def _generate_recommendations(self, metrics: list[QCMetric]) -> list[str]:
        """Generate recommendations based on QC metrics."""
        recommendations = []

        failed_metrics = [m for m in metrics if m.status == "fail"]
        warning_metrics = [m for m in metrics if m.status == "warning"]

        if failed_metrics:
            recommendations.append(f"Critical issues found: {len(failed_metrics)} metrics failed validation")
            for metric in failed_metrics:
                recommendations.append(f"- {metric.description}: {metric.value} (threshold: {metric.threshold})")

        if warning_metrics:
            recommendations.append(f"Warning issues found: {len(warning_metrics)} metrics need attention")
            for metric in warning_metrics:
                recommendations.append(f"- {metric.description}: {metric.value} (threshold: {metric.threshold})")

        if not failed_metrics and not warning_metrics:
            recommendations.append("Data quality is good - no issues detected")

        return recommendations


class ActivityQCAnalyzer(QCAnalyzer):
    """QC analyzer for activity data."""

    def _get_key_columns(self) -> list[str]:
        return ["activity_chembl_id", "assay_chembl_id", "molecule_chembl_id", "target_chembl_id"]

    def analyze(self, data: pd.DataFrame, level: QCLevel = QCLevel.STANDARD) -> QCReport:
        """Analyze activity data quality."""
        if self.logger:
            self.logger.info(f"Starting QC analysis for {len(data)} activity records")

        metrics = []

        # Basic metrics
        metrics.extend(self._analyze_completeness(data))
        metrics.extend(self._analyze_validity(data))
        metrics.extend(self._analyze_consistency(data))

        if level in [QCLevel.ENHANCED, QCLevel.COMPREHENSIVE]:
            metrics.extend(self._analyze_statistics(data))
            metrics.extend(self._analyze_activity_specific(data))

        # Generate summary
        summary = self._generate_summary(data, metrics)

        # Generate recommendations
        recommendations = self._generate_recommendations(metrics)

        # Determine overall status
        overall_status = self._determine_overall_status(metrics)

        return QCReport(entity_type="activity", level=level, metrics=metrics, summary=summary, recommendations=recommendations, overall_status=overall_status)

    def _analyze_activity_specific(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze activity-specific metrics."""
        metrics = []

        # Activity value analysis
        if "activity_value" in data.columns:
            valid_activities = data["activity_value"].notna().sum()
            total_activities = len(data)
            activity_percentage = (valid_activities / total_activities * 100) if total_activities > 0 else 0

            status = "pass"
            if activity_percentage < 80:
                status = "warning"
            if activity_percentage < 50:
                status = "fail"

            metrics.append(
                QCMetric(
                    name="valid_activity_values",
                    value=activity_percentage,
                    metric_type=QCMetricType.CUSTOM,
                    description="Percentage of records with valid activity values",
                    threshold=80.0,
                    status=status,
                )
            )

        return metrics

    def _generate_summary(self, data: pd.DataFrame, metrics: list[QCMetric]) -> dict[str, Any]:
        """Generate summary statistics."""
        return {
            "total_records": len(data),
            "total_columns": len(data.columns),
            "metrics_analyzed": len(metrics),
            "failed_metrics": len([m for m in metrics if m.status == "fail"]),
            "warning_metrics": len([m for m in metrics if m.status == "warning"]),
            "passed_metrics": len([m for m in metrics if m.status == "pass"]),
        }

    def _determine_overall_status(self, metrics: list[QCMetric]) -> str:
        """Determine overall QC status."""
        if any(m.status == "fail" for m in metrics):
            return "fail"
        elif any(m.status == "warning" for m in metrics):
            return "warning"
        else:
            return "pass"


class DocumentQCAnalyzer(QCAnalyzer):
    """QC analyzer for document data."""

    def _get_key_columns(self) -> list[str]:
        return ["document_chembl_id", "doi", "title", "pmid"]

    def analyze(self, data: pd.DataFrame, level: QCLevel = QCLevel.STANDARD) -> QCReport:
        """Analyze document data quality."""
        if self.logger:
            self.logger.info(f"Starting QC analysis for {len(data)} document records")

        metrics = []

        # Basic metrics
        metrics.extend(self._analyze_completeness(data))
        metrics.extend(self._analyze_validity(data))
        metrics.extend(self._analyze_consistency(data))

        if level in [QCLevel.ENHANCED, QCLevel.COMPREHENSIVE]:
            metrics.extend(self._analyze_statistics(data))
            metrics.extend(self._analyze_document_specific(data))

        # Generate summary
        summary = self._generate_summary(data, metrics)

        # Generate recommendations
        recommendations = self._generate_recommendations(metrics)

        # Determine overall status
        overall_status = self._determine_overall_status(metrics)

        return QCReport(entity_type="document", level=level, metrics=metrics, summary=summary, recommendations=recommendations, overall_status=overall_status)

    def _analyze_document_specific(self, data: pd.DataFrame) -> list[QCMetric]:
        """Analyze document-specific metrics."""
        metrics = []

        # DOI coverage
        if "doi" in data.columns:
            doi_coverage = data["doi"].notna().sum()
            total_documents = len(data)
            doi_percentage = (doi_coverage / total_documents * 100) if total_documents > 0 else 0

            status = "pass"
            if doi_percentage < 70:
                status = "warning"
            if doi_percentage < 30:
                status = "fail"

            metrics.append(
                QCMetric(name="doi_coverage", value=doi_percentage, metric_type=QCMetricType.CUSTOM, description="Percentage of documents with DOI", threshold=70.0, status=status)
            )

        return metrics

    def _generate_summary(self, data: pd.DataFrame, metrics: list[QCMetric]) -> dict[str, Any]:
        """Generate summary statistics."""
        return {
            "total_records": len(data),
            "total_columns": len(data.columns),
            "metrics_analyzed": len(metrics),
            "failed_metrics": len([m for m in metrics if m.status == "fail"]),
            "warning_metrics": len([m for m in metrics if m.status == "warning"]),
            "passed_metrics": len([m for m in metrics if m.status == "pass"]),
        }

    def _determine_overall_status(self, metrics: list[QCMetric]) -> str:
        """Determine overall QC status."""
        if any(m.status == "fail" for m in metrics):
            return "fail"
        elif any(m.status == "warning" for m in metrics):
            return "warning"
        else:
            return "pass"


class QCSystem:
    """Unified QC system for all entity types."""

    def __init__(self, logger: BoundLogger | None = None):
        self.logger = logger
        self._analyzers = {
            "activity": ActivityQCAnalyzer("activity", logger),
            "document": DocumentQCAnalyzer("document", logger),
            # Add more analyzers as needed
        }

    def analyze(self, entity_type: str, data: pd.DataFrame, level: QCLevel = QCLevel.STANDARD) -> QCReport:
        """Analyze data quality for specific entity type."""
        if entity_type not in self._analyzers:
            raise ValueError(f"Unknown entity type: {entity_type}. Supported types: {list(self._analyzers.keys())}")

        analyzer = self._analyzers[entity_type]
        return analyzer.analyze(data, level)

    def get_supported_entities(self) -> list[str]:
        """Get list of supported entity types."""
        return list(self._analyzers.keys())

    def add_analyzer(self, entity_type: str, analyzer: QCAnalyzer) -> None:
        """Add custom analyzer for entity type."""
        self._analyzers[entity_type] = analyzer

    def generate_correlation_matrix(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate correlation matrix for numeric columns."""
        numeric = data.select_dtypes(include="number")
        if numeric.empty or len(numeric) < 2:
            return pd.DataFrame()

        try:
            corr_matrix = numeric.corr()
            # Replace NaN and inf with 0
            return corr_matrix.fillna(0.0).replace([np.inf, -np.inf], 0.0)
        except (ValueError, TypeError, MemoryError) as e:
            if self.logger:
                self.logger.warning(f"Failed to generate correlation matrix: {e}")
            return pd.DataFrame()

    def save_report(self, report: QCReport, path: str) -> None:
        """Save QC report to file."""
        import json
        from pathlib import Path

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        with open(path_obj, "w") as f:
            json.dump(report.to_dict(), f, indent=2, default=str)

        if self.logger:
            self.logger.info(f"QC report saved to {path_obj}")


# Global QC system instance
_qc_system = QCSystem()


def get_qc_system() -> QCSystem:
    """Get the global QC system instance."""
    return _qc_system


def analyze_quality(entity_type: str, data: pd.DataFrame, level: QCLevel = QCLevel.STANDARD) -> QCReport:
    """Analyze data quality for specific entity type."""
    return _qc_system.analyze(entity_type, data, level)


def generate_correlation_matrix(data: pd.DataFrame) -> pd.DataFrame:
    """Generate correlation matrix for numeric columns."""
    return _qc_system.generate_correlation_matrix(data)
