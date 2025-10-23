"""
Базовый модуль для постобработки данных в ETL пайплайнах.

Предоставляет общие шаги постобработки и registry для динамической загрузки шагов.
"""

from abc import ABC, abstractmethod
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, Field

from library.config import Config


class PostprocessStep(Protocol):
    """Протокол для шагов постобработки."""
    
    def __call__(self, df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
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
    
    def __init__(self, config: Config):
        self.config = config
        self.steps: list[PostprocessStepConfig] = []
        self._load_steps()
    
    def _load_steps(self) -> None:
        """Загрузить шаги из конфигурации."""
        if hasattr(self.config, 'postprocess') and hasattr(self.config.postprocess, 'steps'):
            for step_config in self.config.postprocess.steps:
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


class ActivityPostprocessor(BasePostprocessor):
    """Постпроцессор для активностей."""
    
    def merge_sources(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Объединить данные из источников активностей."""
        # Активности используют только ChEMBL
        return df
    
    def deduplicate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Удалить дубликаты активностей."""
        return df.drop_duplicates(
            subset=["assay_chembl_id", "molecule_chembl_id", "standard_type"], 
            keep="first"
        )


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
    if hasattr(config, 'pipeline') and hasattr(config.pipeline, 'entity_type'):
        entity_type = config.pipeline.entity_type
    else:
        # Попытаться определить по имени класса конфигурации
        class_name = config.__class__.__name__.lower()
        if 'document' in class_name:
            entity_type = 'documents'
        elif 'target' in class_name:
            entity_type = 'targets'
        elif 'assay' in class_name:
            entity_type = 'assays'
        elif 'activity' in class_name:
            entity_type = 'activities'
        elif 'testitem' in class_name:
            entity_type = 'testitems'
        else:
            raise ValueError(f"Не удалось определить тип сущности для конфигурации {config.__class__.__name__}")
    
    # Создать соответствующий постпроцессор
    if entity_type == 'documents':
        processor = DocumentPostprocessor(config)
    elif entity_type == 'targets':
        processor = TargetPostprocessor(config)
    elif entity_type == 'assays':
        processor = AssayPostprocessor(config)
    elif entity_type == 'activities':
        processor = ActivityPostprocessor(config)
    elif entity_type == 'testitems':
        processor = TestitemPostprocessor(config)
    else:
        raise ValueError(f"Неподдерживаемый тип сущности: {entity_type}")
    
    return processor.merge_sources(df, **kwargs)


def deduplicate_step(df: pd.DataFrame, config: Config, **kwargs) -> pd.DataFrame:
    """Стандартный шаг дедупликации."""
    # Определить тип постпроцессора по конфигурации
    if hasattr(config, 'pipeline') and hasattr(config.pipeline, 'entity_type'):
        entity_type = config.pipeline.entity_type
    else:
        class_name = config.__class__.__name__.lower()
        if 'document' in class_name:
            entity_type = 'documents'
        elif 'target' in class_name:
            entity_type = 'targets'
        elif 'assay' in class_name:
            entity_type = 'assays'
        elif 'activity' in class_name:
            entity_type = 'activities'
        elif 'testitem' in class_name:
            entity_type = 'testitems'
        else:
            raise ValueError(f"Не удалось определить тип сущности для конфигурации {config.__class__.__name__}")
    
    # Создать соответствующий постпроцессор
    if entity_type == 'documents':
        processor = DocumentPostprocessor(config)
    elif entity_type == 'targets':
        processor = TargetPostprocessor(config)
    elif entity_type == 'assays':
        processor = AssayPostprocessor(config)
    elif entity_type == 'activities':
        processor = ActivityPostprocessor(config)
    elif entity_type == 'testitems':
        processor = TestitemPostprocessor(config)
    else:
        raise ValueError(f"Неподдерживаемый тип сущности: {entity_type}")
    
    return processor.deduplicate(df, **kwargs)


# Регистрация стандартных шагов
register_postprocess_step("merge_sources", merge_sources_step)
register_postprocess_step("deduplicate", deduplicate_step)
