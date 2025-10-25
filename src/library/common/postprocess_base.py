"""
Базовый модуль для постобработки данных в ETL пайплайнах.

Предоставляет общие шаги постобработки и registry для динамической загрузки шагов.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, Field

from library.config import Config

# Use dict instead of Config to avoid circular imports
# Config can be imported later when needed
ConfigLike = dict[str, Any]


class PostprocessStep(Protocol):
    """Протокол для шагов постобработки."""

    def __call__(self, df: pd.DataFrame, config: ConfigLike, **kwargs) -> pd.DataFrame:
        """Выполнить шаг постобработки."""
        ...


class PostprocessStepConfig(BaseModel):
    """Конфигурация шага постобработки."""

    name: str = Field(..., description="Имя шага")
    enabled: bool = Field(True, description="Включен ли шаг")
    parameters: dict[str, Any] = Field(default_factory=dict, description="Параметры шага")
    priority: int = Field(0, description="Приоритет выполнения (меньше = раньше)")


class BasePostprocessor(ABC):
    """Базовый класс для постобработки данных."""

    def __init__(self, config: ConfigLike):
        self.config = config
        self.steps: list[PostprocessStepConfig] = []
        self._load_steps()

    def _load_steps(self) -> None:
        """Загрузить шаги из конфигурации."""
        if isinstance(self.config, dict):
            postprocess_config = self.config.get("postprocess", {})
            steps = postprocess_config.get("steps", [])
        else:
            postprocess_config = getattr(self.config, "postprocess", None)
            steps = getattr(postprocess_config, "steps", []) if postprocess_config else []
        
        for step_config in steps:
            if isinstance(step_config, dict):
                self.steps.append(PostprocessStepConfig(**step_config))
            else:
                self.steps.append(step_config)

        # Сортировка по приоритету
        self.steps.sort(key=lambda x: x.priority)

    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Выполнить все шаги постобработки."""
        result_df = df.copy()

        for step_config in self.steps:
            if not step_config.enabled:
                continue

            step_func = POSTPROCESS_STEPS_REGISTRY.get(step_config.name)
            if step_func is None:
                raise ValueError(f"Неизвестный шаг постобработки: {step_config.name}")

            try:
                result_df = step_func(result_df, self.config, **step_config.parameters, **kwargs)
            except Exception as e:
                raise RuntimeError(f"Ошибка в шаге '{step_config.name}': {e}") from e

        return result_df

    @abstractmethod
    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из нескольких источников."""
        pass

    @abstractmethod
    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты."""
        pass


class DocumentPostprocessor(BasePostprocessor):
    """Постпроцессор для документов."""

    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников документов."""
        # Приоритет источников: crossref > openalex > pubmed > semantic_scholar
        priority_sources = ["crossref", "openalex", "pubmed", "semantic_scholar"]

        for field in ["title", "abstract", "authors", "doi", "journal", "year", "volume", "issue"]:
            # Создать консолидированное поле
            consolidated_field = f"{field}_canonical"
            df[consolidated_field] = None

            for source in priority_sources:
                source_field = f"{source}_{field}"
                if source_field in df.columns:
                    # Заполнить только если консолидированное поле пустое
                    mask = df[consolidated_field].isna() & df[source_field].notna()
                    df.loc[mask, consolidated_field] = df.loc[mask, source_field]

        return df

    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты документов."""
        return df.drop_duplicates(subset=["document_chembl_id"], keep="first")

    def add_missing_document_fields(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Добавить недостающие поля документов."""
        if df.empty:
            return df

        # Добавляем недостающие поля документов
        missing_fields = {
            "chembl_error": None,
            "chembl_issn": None,
            "classification": None,
            "crossref_abstract": None,
            "crossref_issn": None,
            "crossref_journal": None,
            "crossref_pmid": None,
            "document_contains_external_links": False,
            "is_experimental_doc": False,
            "openalex_abstract": None,
            "openalex_authors": None,
            "openalex_crossref_doc_type": None,
            "openalex_doc_type": None,
            "openalex_first_page": None,
            "openalex_issn": None,
            "openalex_issue": None,
            "openalex_journal": None,
            "openalex_last_page": None,
            "openalex_pmid": None,
            "openalex_volume": None,
            "openalex_year": None,
            "openalex_doi": None,
            "pubmed_article_title": None,
            "pubmed_chemical_list": None,
            "pubmed_id": None,
            "pubmed_mesh_descriptors": None,
            "pubmed_mesh_qualifiers": None,
            "pubmed_doi": None,
            "pubmed_year_completed": None,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": None,
            "pubmed_month_revised": None,
            "pubmed_day_revised": None,
            "semantic_scholar_doc_type": None,
            "semantic_scholar_issn": None,
            "semantic_scholar_journal": None,
            "semantic_scholar_doi": None,
        }

        # Добавляем поля, если их нет
        for field, default_value in missing_fields.items():
            if field not in df.columns:
                df[field] = default_value

        return df


class TargetPostprocessor(BasePostprocessor):
    """Постпроцессор для таргетов."""

    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников таргетов."""
        # Приоритет источников: chembl > uniprot > iuphar > gtopdb
        # priority_sources = ["chembl", "uniprot", "iuphar", "gtopdb"]

        # Для таргетов основная логика уже в normalize.py
        return df

    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты таргетов."""
        return df.drop_duplicates(subset=["target_chembl_id"], keep="first")


class AssayPostprocessor(BasePostprocessor):
    """Постпроцессор для ассаев."""

    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников ассаев."""
        # Ассаи используют только ChEMBL
        return df

    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты ассаев."""
        return df.drop_duplicates(subset=["assay_chembl_id"], keep="first")

    def apply_bao_flags(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Применить BAO (BioAssay Ontology) флаги и классификации."""
        if df.empty:
            return df

        # Добавляем недостающие BAO поля
        bao_fields = {
            "bao_assay_format": None,
            "bao_assay_format_label": None,
            "bao_assay_format_uri": None,
            "bao_assay_type": None,
            "bao_assay_type_label": None,
            "bao_assay_type_uri": None,
            "bao_endpoint": None,
            "bao_endpoint_label": None,
            "bao_endpoint_uri": None,
            "is_variant": False,
            "target_isoform": None,
            "target_organism": None,
            "target_tax_id": None,
            "target_uniprot_accession": None,
            "variant_mutations": None,
            "variant_sequence": None,
            "chembl_release": None,
            "index": None,
            "pipeline_version": None,
        }

        # Добавляем поля, если их нет
        for field, default_value in bao_fields.items():
            if field not in df.columns:
                df[field] = default_value

        # Системные поля (index, pipeline_version, chembl_release) теперь добавляются
        # в нормализаторах через унифицированную утилиту add_system_metadata_fields()

        # Определяем is_variant на основе наличия variant полей
        if "is_variant" in df.columns:
            variant_indicators = ["variant_id", "variant_text", "variant_sequence_id"]
            # Проверяем, какие из variant полей есть в DataFrame
            existing_variant_indicators = [col for col in variant_indicators if col in df.columns]
            if existing_variant_indicators:
                df["is_variant"] = df[existing_variant_indicators].notna().any(axis=1)
            else:
                df["is_variant"] = False

        return df


class ActivityPostprocessor(BasePostprocessor):
    """Постпроцессор для активностей."""

    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников активностей."""
        # Активности используют только ChEMBL
        return df

    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты активностей."""
        return df.drop_duplicates(subset=["assay_chembl_id", "molecule_chembl_id", "standard_type"], keep="first")


class TestitemPostprocessor(BasePostprocessor):
    """Постпроцессор для теститемов."""

    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников теститемов."""
        # Приоритет источников: chembl > pubchem
        priority_sources = ["chembl", "pubchem"]

        for field in ["molecular_formula", "molecular_weight", "canonical_smiles", "inchi", "inchi_key"]:
            consolidated_field = f"standardized_{field}"
            df[consolidated_field] = None

            for source in priority_sources:
                source_field = f"{source}_{field}"
                if source_field in df.columns:
                    mask = df[consolidated_field].isna() & df[source_field].notna()
                    df.loc[mask, consolidated_field] = df.loc[mask, source_field]

        return df

    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты теститемов."""
        return df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

    def add_missing_testitem_fields(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Добавить недостающие поля теститемов."""
        if df.empty:
            return df

        # Добавляем недостающие поля теститемов согласно column_order из конфига
        missing_fields = {
            # Основные идентификаторы и метаданные
            "molecule_chembl_id": None,
            "pref_name": None,
            "pref_name_key": None,
            "parent_chembl_id": None,
            "parent_molregno": None,
            "max_phase": None,
            "therapeutic_flag": False,
            "dosed_ingredient": False,
            "first_approval": None,
            "structure_type": None,
            "molecule_type": None,
            # Физико-химические свойства ChEMBL
            "mw_freebase": None,
            "alogp": None,
            "hba": None,
            "hbd": None,
            "psa": None,
            "rtb": None,
            "ro3_pass": False,
            "num_ro5_violations": None,
            "acd_most_apka": None,
            "acd_most_bpka": None,
            "acd_logp": None,
            "acd_logd": None,
            "molecular_species": None,
            "full_mwt": None,
            "aromatic_rings": None,
            "heavy_atoms": None,
            "qed_weighted": None,
            "mw_monoisotopic": None,
            "full_molformula": None,
            "hba_lipinski": None,
            "hbd_lipinski": None,
            "num_lipinski_ro5_violations": None,
            # Пути введения и флаги
            "oral": False,
            "parenteral": False,
            "topical": False,
            "black_box_warning": False,
            "natural_product": False,
            "first_in_class": False,
            "chirality": None,
            "prodrug": False,
            "inorganic_flag": False,
            "polymer_flag": False,
            # Регистрация и отзыв
            "usan_year": None,
            "availability_type": None,
            "usan_stem": None,
            "usan_substem": None,
            "usan_stem_definition": None,
            "indication_class": None,
            "withdrawn_flag": False,
            "withdrawn_year": None,
            "withdrawn_country": None,
            "withdrawn_reason": None,
            # Механизм действия
            "mechanism_of_action": None,
            "direct_interaction": False,
            "molecular_mechanism": None,
            # Drug данные
            "drug_chembl_id": None,
            "drug_name": None,
            "drug_type": None,
            "drug_substance_flag": False,
            "drug_indication_flag": False,
            "drug_antibacterial_flag": False,
            "drug_antiviral_flag": False,
            "drug_antifungal_flag": False,
            "drug_antiparasitic_flag": False,
            "drug_antineoplastic_flag": False,
            "drug_immunosuppressant_flag": False,
            "drug_antiinflammatory_flag": False,
            # PubChem данные
            "pubchem_cid": None,
            "pubchem_molecular_formula": None,
            "pubchem_molecular_weight": None,
            "pubchem_canonical_smiles": None,
            "pubchem_isomeric_smiles": None,
            "pubchem_inchi": None,
            "pubchem_inchi_key": None,
            "pubchem_registry_id": None,
            "pubchem_rn": None,
            # Стандартизированные структуры
            "standardized_inchi": None,
            "standardized_inchi_key": None,
            "standardized_smiles": None,
            # Вложенные структуры ChEMBL (JSON/распакованные)
            "atc_classifications": None,
            "biotherapeutic": None,
            "chemical_probe": False,
            "cross_references": None,
            "helm_notation": None,
            "molecule_hierarchy": None,
            "molecule_properties": None,
            "molecule_structures": None,
            "molecule_synonyms": None,
            "orphan": False,
            "veterinary": False,
            "standard_inchi": None,
            "chirality_chembl": None,
            "molecule_type_chembl": None,
            # Входные данные из input файла
            "nstereo": None,
            "salt_chembl_id": None,
        }

        # Добавляем поля, если их нет
        for field, default_value in missing_fields.items():
            if field not in df.columns:
                df[field] = default_value

        return df


# Registry для шагов постобработки
POSTPROCESS_STEPS_REGISTRY: dict[str, PostprocessStep] = {}


def register_postprocess_step(name: str, step_func: PostprocessStep) -> None:
    """Зарегистрировать шаг постобработки."""
    POSTPROCESS_STEPS_REGISTRY[name] = step_func


def get_postprocess_step(name: str) -> PostprocessStep | None:
    """Получить шаг постобработки по имени."""
    return POSTPROCESS_STEPS_REGISTRY.get(name)


# Стандартные шаги постобработки
def merge_sources_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг объединения источников."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, "pipeline") and hasattr(config.pipeline, "entity_type"):
        entity_type = config.pipeline.entity_type
    else:
        # Попытаться определить по имени класса конфигурации
        class_name = config.__class__.__name__.lower()
        if "document" in class_name:
            entity_type = "documents"
        elif "target" in class_name:
            entity_type = "targets"
        elif "assay" in class_name:
            entity_type = "assays"
        elif "activity" in class_name:
            entity_type = "activities"
        elif "testitem" in class_name:
            entity_type = "testitems"
        else:
            raise ValueError(f"Не удалось определить тип сущности для конфигурации {config.__class__.__name__}")

    # Создать соответствующий постпроцессор
    if entity_type == "documents":
        processor = DocumentPostprocessor(config)
    elif entity_type == "targets":
        processor = TargetPostprocessor(config)
    elif entity_type == "assays":
        processor = AssayPostprocessor(config)
    elif entity_type == "activities":
        processor = ActivityPostprocessor(config)
    elif entity_type == "testitems":
        processor = TestitemPostprocessor(config)
    else:
        raise ValueError(f"Неподдерживаемый тип сущности: {entity_type}")

    return processor.merge_sources(df, **kwargs)


def deduplicate_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг дедупликации."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, "pipeline") and hasattr(config.pipeline, "entity_type"):
        entity_type = config.pipeline.entity_type
    else:
        class_name = config.__class__.__name__.lower()
        if "document" in class_name:
            entity_type = "documents"
        elif "target" in class_name:
            entity_type = "targets"
        elif "assay" in class_name:
            entity_type = "assays"
        elif "activity" in class_name:
            entity_type = "activities"
        elif "testitem" in class_name:
            entity_type = "testitems"
        else:
            raise ValueError(f"Не удалось определить тип сущности для конфигурации {config.__class__.__name__}")

    # Создать соответствующий постпроцессор
    if entity_type == "documents":
        processor = DocumentPostprocessor(config)
    elif entity_type == "targets":
        processor = TargetPostprocessor(config)
    elif entity_type == "assays":
        processor = AssayPostprocessor(config)
    elif entity_type == "activities":
        processor = ActivityPostprocessor(config)
    elif entity_type == "testitems":
        processor = TestitemPostprocessor(config)
    else:
        raise ValueError(f"Неподдерживаемый тип сущности: {entity_type}")

    return processor.deduplicate(df, **kwargs)


def apply_bao_flags_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг применения BAO флагов."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, "pipeline") and hasattr(config.pipeline, "entity_type"):
        entity_type = config.pipeline.entity_type
    else:
        class_name = config.__class__.__name__.lower()
        if "assay" in class_name:
            entity_type = "assays"
        else:
            raise ValueError(f"apply_bao_flags поддерживается только для assays, получен: {config.__class__.__name__}")

    # Создать соответствующий постпроцессор
    if entity_type == "assays":
        processor = AssayPostprocessor(config)
    else:
        raise ValueError(f"apply_bao_flags поддерживается только для assays, получен: {entity_type}")

    return processor.apply_bao_flags(df, **kwargs)


def add_missing_document_fields_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг добавления недостающих полей документов."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, "pipeline") and hasattr(config.pipeline, "entity_type"):
        entity_type = config.pipeline.entity_type
    else:
        class_name = config.__class__.__name__.lower()
        if "document" in class_name:
            entity_type = "documents"
        else:
            raise ValueError(f"add_missing_document_fields поддерживается только для documents, получен: {config.__class__.__name__}")

    # Создать соответствующий постпроцессор
    if entity_type == "documents":
        processor = DocumentPostprocessor(config)
    else:
        raise ValueError(f"add_missing_document_fields поддерживается только для documents, получен: {entity_type}")

    return processor.add_missing_document_fields(df, **kwargs)


def add_missing_testitem_fields_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг добавления недостающих полей теститемов."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, "pipeline") and hasattr(config.pipeline, "entity_type"):
        entity_type = config.pipeline.entity_type
    else:
        class_name = config.__class__.__name__.lower()
        if "testitem" in class_name:
            entity_type = "testitems"
        else:
            raise ValueError(f"add_missing_testitem_fields поддерживается только для testitems, получен: {config.__class__.__name__}")

    # Создать соответствующий постпроцессор
    if entity_type == "testitems":
        processor = TestitemPostprocessor(config)
    else:
        raise ValueError(f"add_missing_testitem_fields поддерживается только для testitems, получен: {entity_type}")

    return processor.add_missing_testitem_fields(df, **kwargs)


# Регистрация стандартных шагов
register_postprocess_step("merge_sources", merge_sources_step)  # type: ignore[arg-type]
register_postprocess_step("deduplicate", deduplicate_step)  # type: ignore[arg-type]
register_postprocess_step("apply_bao_flags", apply_bao_flags_step)  # type: ignore[arg-type]
register_postprocess_step("add_missing_document_fields", add_missing_document_fields_step)  # type: ignore[arg-type]
register_postprocess_step("add_missing_testitem_fields", add_missing_testitem_fields_step)  # type: ignore[arg-type]
