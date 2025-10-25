"""
Модуль для стандартизированных QC профилей и валидации качества данных.

Предоставляет унифицированные критерии качества и профили для всех ETL пайплайнов.
"""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field


class QualityRule(BaseModel):
    """Правило качества данных."""

    name: str = Field(..., description="Имя правила")
    description: str = Field(..., description="Описание правила")
    enabled: bool = Field(True, description="Включено ли правило")
    parameters: dict[str, Any] = Field(default_factory=dict, description="Параметры правила")


class QCThreshold(BaseModel):
    """Порог качества данных."""

    metric: str = Field(..., description="Имя метрики")
    threshold: float = Field(..., description="Пороговое значение (0.0-1.0)")
    fail_on_exceed: bool = Field(True, description="Завершать с ошибкой при превышении")


class QCProfile(BaseModel):
    """Профиль контроля качества."""

    name: str = Field(..., description="Имя профиля")
    description: str = Field(..., description="Описание профиля")
    fail_on_criteria: list[str] = Field(default_factory=list, description="Критерии для fail_on")
    thresholds: list[QCThreshold] = Field(default_factory=list, description="Пороги качества")
    rules: list[QualityRule] = Field(default_factory=list, description="Правила качества")


class QCValidator(ABC):
    """Базовый класс для валидации качества данных."""

    def __init__(self, profile: QCProfile):
        self.profile = profile

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Выполнить валидацию качества данных."""
        pass

    def check_thresholds(self, metrics: dict[str, float]) -> dict[str, bool]:
        """Проверить пороги качества."""
        results = {}

        for threshold in self.profile.thresholds:
            metric_value = metrics.get(threshold.metric, 0.0)
            passed = metric_value <= threshold.threshold
            results[threshold.metric] = passed

            if not passed and threshold.fail_on_exceed:
                raise ValueError(f"Порог качества превышен: {threshold.metric} = {metric_value:.3f} (порог: {threshold.threshold:.3f})")

        return results


class DocumentQCValidator(QCValidator):
    """Валидатор качества для документов."""

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Валидация качества документов."""
        metrics = {}

        # Базовые метрики
        total_rows = len(df)
        if total_rows == 0:
            return {"total_rows": 0, "quality_passed": False}

        # Проверка обязательных полей
        metrics["missing_document_chembl_id"] = df["document_chembl_id"].isna().sum() / total_rows
        metrics["missing_doi"] = df["doi"].isna().sum() / total_rows if "doi" in df.columns else 0.0
        metrics["missing_title"] = df["title"].isna().sum() / total_rows if "title" in df.columns else 0.0

        # Проверка дубликатов
        metrics["duplicate_primary_keys"] = df["document_chembl_id"].duplicated().sum() / total_rows

        # Проверка валидности DOI
        if "doi" in df.columns:
            valid_doi_pattern = r"^10\.\d+/[^\s]+$"
            metrics["invalid_doi"] = (~df["doi"].str.match(valid_doi_pattern, na=False)).sum() / total_rows

        # Проверка валидности PMID
        if "pmid" in df.columns:
            valid_pmid_pattern = r"^\d+$"
            metrics["invalid_pmid"] = (~df["pmid"].str.match(valid_pmid_pattern, na=False)).sum() / total_rows

        # Проверка длины заголовков
        if "title" in df.columns:
            metrics["short_titles"] = (df["title"].str.len() < 10).sum() / total_rows

        # Проверка порогов
        self.check_thresholds(metrics)

        return {"total_rows": total_rows, "metrics": metrics, "quality_passed": True}


class TargetQCValidator(QCValidator):
    """Валидатор качества для таргетов."""

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Валидация качества таргетов."""
        metrics = {}

        total_rows = len(df)
        if total_rows == 0:
            return {"total_rows": 0, "quality_passed": False}

        # Проверка обязательных полей
        metrics["missing_target_chembl_id"] = df["target_chembl_id"].isna().sum() / total_rows
        metrics["duplicate_primary_keys"] = df["target_chembl_id"].duplicated().sum() / total_rows

        # Проверка валидности ChEMBL ID
        valid_chembl_pattern = r"^CHEMBL\d+$"
        metrics["invalid_chembl_id"] = (~df["target_chembl_id"].str.match(valid_chembl_pattern, na=False)).sum() / total_rows

        # Проверка валидности UniProt ID
        if "uniprot_id_primary" in df.columns:
            valid_uniprot_pattern = r"^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$"
            metrics["invalid_uniprot_id"] = (~df["uniprot_id_primary"].str.match(valid_uniprot_pattern, na=False)).sum() / total_rows

        # Проверка валидности таксономических ID
        if "tax_id" in df.columns:
            metrics["invalid_taxonomy_id"] = (df["tax_id"] <= 0).sum() / total_rows

        # Проверка наличия данных из источников
        source_columns = [col for col in df.columns if any(source in col for source in ["chembl", "uniprot", "iuphar", "gtopdb"])]
        if source_columns:
            has_source_data = df[source_columns].notna().any(axis=1)
            metrics["no_source_data"] = (~has_source_data).sum() / total_rows

        # Проверка порогов
        self.check_thresholds(metrics)

        return {"total_rows": total_rows, "metrics": metrics, "quality_passed": True}


class AssayQCValidator(QCValidator):
    """Валидатор качества для ассаев."""

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Валидация качества ассаев."""
        metrics = {}

        total_rows = len(df)
        if total_rows == 0:
            return {"total_rows": 0, "quality_passed": False}

        # Проверка обязательных полей
        metrics["missing_assay_chembl_id"] = df["assay_chembl_id"].isna().sum() / total_rows
        metrics["duplicate_primary_keys"] = df["assay_chembl_id"].duplicated().sum() / total_rows

        # Проверка валидности ChEMBL ID
        valid_chembl_pattern = r"^CHEMBL\d+$"
        metrics["invalid_chembl_id"] = (~df["assay_chembl_id"].str.match(valid_chembl_pattern, na=False)).sum() / total_rows

        # Проверка валидности типа ассая
        if "assay_type" in df.columns:
            valid_types = ["B", "F", "A", "P", "T", "U"]
            metrics["invalid_assay_type"] = (~df["assay_type"].isin(valid_types)).sum() / total_rows

        # Проверка BAO формата
        if "bao_format" in df.columns:
            valid_bao_pattern = r"^BAO_\d+$"
            metrics["missing_bao_format"] = df["bao_format"].isna().sum() / total_rows
            metrics["invalid_bao_format"] = (~df["bao_format"].str.match(valid_bao_pattern, na=False)).sum() / total_rows

        # Проверка порогов
        self.check_thresholds(metrics)

        return {"total_rows": total_rows, "metrics": metrics, "quality_passed": True}


class ActivityQCValidator(QCValidator):
    """Валидатор качества для активностей."""

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Валидация качества активностей."""
        metrics = {}

        total_rows = len(df)
        if total_rows == 0:
            return {"total_rows": 0, "quality_passed": False}

        # Проверка обязательных полей
        metrics["missing_standard_value"] = df["standard_value"].isna().sum() / total_rows
        metrics["missing_standard_units"] = df["standard_units"].isna().sum() / total_rows

        # Проверка валидности значений
        if "standard_value" in df.columns:
            metrics["invalid_standard_value"] = ((df["standard_value"] < 1e-12) | (df["standard_value"] > 1e-3)).sum() / total_rows

        # Проверка валидности единиц
        if "standard_units" in df.columns:
            valid_units = ["nM", "uM", "mM", "M", "%"]
            metrics["invalid_standard_units"] = (~df["standard_units"].isin(valid_units)).sum() / total_rows

        # Проверка pChEMBL значений
        if "pchembl_value" in df.columns:
            metrics["missing_pchembl_value"] = df["pchembl_value"].isna().sum() / total_rows
            metrics["invalid_pchembl_value"] = ((df["pchembl_value"] < 3.0) | (df["pchembl_value"] > 12.0)).sum() / total_rows

        # Проверка порогов
        self.check_thresholds(metrics)

        return {"total_rows": total_rows, "metrics": metrics, "quality_passed": True}


class TestitemQCValidator(QCValidator):
    """Валидатор качества для теститемов."""

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """Валидация качества теститемов."""
        metrics = {}

        total_rows = len(df)
        if total_rows == 0:
            return {"total_rows": 0, "quality_passed": False}

        # Проверка обязательных полей
        metrics["missing_molecule_chembl_id"] = df["molecule_chembl_id"].isna().sum() / total_rows
        metrics["duplicate_primary_keys"] = df["molecule_chembl_id"].duplicated().sum() / total_rows

        # Проверка валидности ChEMBL ID
        valid_chembl_pattern = r"^CHEMBL\d+$"
        metrics["invalid_chembl_id"] = (~df["molecule_chembl_id"].str.match(valid_chembl_pattern, na=False)).sum() / total_rows

        # Проверка валидности PubChem CID
        if "pubchem_cid" in df.columns:
            valid_cid_pattern = r"^\d+$"
            metrics["invalid_pubchem_cid"] = (~df["pubchem_cid"].astype(str).str.match(valid_cid_pattern, na=False)).sum() / total_rows

        # Проверка валидности InChI Key
        if "inchi_key" in df.columns:
            valid_inchi_key_pattern = r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$"
            metrics["invalid_inchi_key"] = (~df["inchi_key"].str.match(valid_inchi_key_pattern, na=False)).sum() / total_rows

        # Проверка молекулярной массы
        if "molecular_weight" in df.columns:
            metrics["missing_molecular_weight"] = df["molecular_weight"].isna().sum() / total_rows
            metrics["invalid_molecular_weight"] = ((df["molecular_weight"] < 50.0) | (df["molecular_weight"] > 2000.0)).sum() / total_rows

        # Проверка порогов
        self.check_thresholds(metrics)

        return {"total_rows": total_rows, "metrics": metrics, "quality_passed": True}


# Стандартные QC профили
STRICT_PROFILE = QCProfile(
    name="strict",
    description="Строгий профиль качества - не допускает ошибок",
    fail_on_criteria=["missing_primary_key", "duplicate_primary_keys", "invalid_chembl_id"],
    thresholds=[
        QCThreshold(metric="missing_primary_key", threshold=0.0, fail_on_exceed=True),
        QCThreshold(metric="duplicate_primary_keys", threshold=0.0, fail_on_exceed=True),
        QCThreshold(metric="invalid_chembl_id", threshold=0.0, fail_on_exceed=True),
    ],
    rules=[QualityRule(name="valid_chembl_id", description="ChEMBL ID должен соответствовать паттерну ^CHEMBL\\d+$", enabled=True)],
)

MODERATE_PROFILE = QCProfile(
    name="moderate",
    description="Умеренный профиль качества - допускает до 10% ошибок",
    fail_on_criteria=["missing_primary_key", "duplicate_primary_keys"],
    thresholds=[
        QCThreshold(metric="missing_primary_key", threshold=0.0, fail_on_exceed=True),
        QCThreshold(metric="duplicate_primary_keys", threshold=0.0, fail_on_exceed=True),
        QCThreshold(metric="invalid_chembl_id", threshold=0.1, fail_on_exceed=True),
    ],
    rules=[QualityRule(name="valid_chembl_id", description="ChEMBL ID должен соответствовать паттерну ^CHEMBL\\d+$", enabled=True)],
)

PERMISSIVE_PROFILE = QCProfile(
    name="permissive",
    description="Разрешающий профиль качества - допускает до 20% ошибок",
    fail_on_criteria=["missing_primary_key"],
    thresholds=[
        QCThreshold(metric="missing_primary_key", threshold=0.0, fail_on_exceed=True),
        QCThreshold(metric="duplicate_primary_keys", threshold=0.1, fail_on_exceed=True),
        QCThreshold(metric="invalid_chembl_id", threshold=0.2, fail_on_exceed=True),
    ],
    rules=[QualityRule(name="valid_chembl_id", description="ChEMBL ID должен соответствовать паттерну ^CHEMBL\\d+$", enabled=True)],
)

# Специфичные профили для каждого типа сущности
DOCUMENT_PROFILES = {
    "strict": QCProfile(
        name="document_strict",
        description="Строгий профиль для документов",
        fail_on_criteria=["missing_doi", "missing_title"],
        thresholds=[
            QCThreshold(metric="missing_doi", threshold=0.1, fail_on_exceed=True),
            QCThreshold(metric="missing_title", threshold=0.05, fail_on_exceed=True),
        ],
    ),
    "moderate": QCProfile(
        name="document_moderate",
        description="Умеренный профиль для документов",
        fail_on_criteria=["missing_doi"],
        thresholds=[
            QCThreshold(metric="missing_doi", threshold=0.2, fail_on_exceed=True),
            QCThreshold(metric="missing_title", threshold=0.1, fail_on_exceed=True),
        ],
    ),
}

TARGET_PROFILES = {
    "strict": QCProfile(
        name="target_strict",
        description="Строгий профиль для таргетов",
        fail_on_criteria=["missing_target_chembl_id", "duplicate_primary_keys"],
        thresholds=[
            QCThreshold(metric="missing_target_chembl_id", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="duplicate_primary_keys", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="no_source_data", threshold=0.1, fail_on_exceed=True),
        ],
    )
}

ASSAY_PROFILES = {
    "strict": QCProfile(
        name="assay_strict",
        description="Строгий профиль для ассаев",
        fail_on_criteria=["missing_assay_chembl_id", "duplicate_primary_keys"],
        thresholds=[
            QCThreshold(metric="missing_assay_chembl_id", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="duplicate_primary_keys", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="missing_bao_format", threshold=0.1, fail_on_exceed=True),
        ],
    )
}

ACTIVITY_PROFILES = {
    "strict": QCProfile(
        name="activity_strict",
        description="Строгий профиль для активностей",
        fail_on_criteria=["missing_standard_value", "invalid_standard_units"],
        thresholds=[
            QCThreshold(metric="missing_standard_value", threshold=0.1, fail_on_exceed=True),
            QCThreshold(metric="invalid_standard_units", threshold=0.05, fail_on_exceed=True),
            QCThreshold(metric="missing_pchembl_value", threshold=0.2, fail_on_exceed=True),
        ],
    )
}

TESTITEM_PROFILES = {
    "strict": QCProfile(
        name="testitem_strict",
        description="Строгий профиль для теститемов",
        fail_on_criteria=["missing_molecule_chembl_id", "duplicate_primary_keys"],
        thresholds=[
            QCThreshold(metric="missing_molecule_chembl_id", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="duplicate_primary_keys", threshold=0.0, fail_on_exceed=True),
            QCThreshold(metric="missing_molecular_weight", threshold=0.1, fail_on_exceed=True),
        ],
    )
}

# Registry профилей
QC_PROFILES_REGISTRY = {
    "strict": STRICT_PROFILE,
    "moderate": MODERATE_PROFILE,
    "permissive": PERMISSIVE_PROFILE,
    "documents": DOCUMENT_PROFILES,
    "targets": TARGET_PROFILES,
    "assays": ASSAY_PROFILES,
    "activities": ACTIVITY_PROFILES,
    "testitems": TESTITEM_PROFILES,
}

# Registry валидаторов
QC_VALIDATORS_REGISTRY = {
    "documents": DocumentQCValidator,
    "targets": TargetQCValidator,
    "assays": AssayQCValidator,
    "activities": ActivityQCValidator,
    "testitems": TestitemQCValidator,
}


def get_qc_profile(entity_type: str, profile_name: str = "strict") -> QCProfile:
    """Получить QC профиль для типа сущности."""
    if entity_type in QC_PROFILES_REGISTRY:
        entity_profiles = QC_PROFILES_REGISTRY[entity_type]
        if isinstance(entity_profiles, dict) and profile_name in entity_profiles:
            return entity_profiles[profile_name]

    # Fallback на общие профили
    if profile_name in QC_PROFILES_REGISTRY:
        return QC_PROFILES_REGISTRY[profile_name]

    raise ValueError(f"Профиль качества не найден: {entity_type}.{profile_name}")


def get_qc_validator(entity_type: str, profile: QCProfile) -> QCValidator:
    """Получить QC валидатор для типа сущности."""
    if entity_type not in QC_VALIDATORS_REGISTRY:
        raise ValueError(f"Валидатор качества не найден для типа сущности: {entity_type}")

    validator_class = QC_VALIDATORS_REGISTRY[entity_type]
    return validator_class(profile)
