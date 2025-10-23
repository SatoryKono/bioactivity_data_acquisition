"""
Базовые Pandera схемы для унификации всех пайплайнов.
Содержит общие системные поля и конфигурацию.
"""

from __future__ import annotations

import importlib.util
import pandas as pd
from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class BaseNormalizedSchema(pa.DataFrameModel):
    """
    Базовая схема для нормализованных данных всех пайплайнов.
    
    Содержит общие системные поля, которые должны присутствовать
    во всех итоговых выходах пайплайнов.
    """
    
    # Общие системные поля для всех пайплайнов
    index: Series[int] = pa.Field(
        ge=0,
        nullable=False,
        description="Порядковый номер записи"
    )
    
    pipeline_version: Series[str] = pa.Field(
        nullable=False,
        description="Версия пайплайна"
    )
    
    source_system: Series[str] = pa.Field(
        nullable=False,
        description="Система-источник данных"
    )
    
    extracted_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Время извлечения данных в UTC"
    )
    
    hash_row: Series[str] = pa.Field(
        checks=[pa.Check.str_matches(r'^[a-f0-9]{64}$')],
        nullable=False,
        description="SHA256 хеш всей строки"
    )
    
    hash_business_key: Series[str] = pa.Field(
        checks=[pa.Check.str_matches(r'^[a-f0-9]{64}$')],
        nullable=False,
        description="SHA256 хеш бизнес-ключа"
    )
    
    class Config:
        strict = True
        coerce = True


class BaseInputSchema(pa.DataFrameModel):
    """
    Базовая схема для входных данных всех пайплайнов.
    
    Содержит общие поля, которые должны присутствовать
    во всех входных CSV файлах.
    """
    
    class Config:
        strict = False  # Разрешаем дополнительные колонки
        coerce = True


class BaseRawSchema(pa.DataFrameModel):
    """
    Базовая схема для сырых данных из API.
    
    Содержит общие поля для данных, полученных
    напрямую из внешних API.
    """
    
    # Общие поля для сырых данных
    source: Series[str] = pa.Field(
        nullable=False,
        description="Идентификатор источника данных"
    )
    
    retrieved_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Время получения данных из API"
    )
    
    class Config:
        strict = False  # Разрешаем дополнительные поля от разных API
        coerce = True


# Маппинг типов YAML ↔ Pandas ↔ Pandera
TYPE_MAPPING = {
    'STRING': {
        'pandas': 'object',
        'pandera': 'pa.String',
        'description': 'Строковый тип'
    },
    'INT': {
        'pandas': 'int64',
        'pandera': 'pa.Int',
        'description': 'Целочисленный тип'
    },
    'DECIMAL': {
        'pandas': 'float64',
        'pandera': 'pa.Float',
        'description': 'Десятичный тип'
    },
    'FLOAT': {
        'pandas': 'float64',
        'pandera': 'pa.Float',
        'description': 'Число с плавающей точкой'
    },
    'BOOL': {
        'pandas': 'bool',
        'pandera': 'pa.Bool',
        'description': 'Булевый тип'
    },
    'TIMESTAMP': {
        'pandas': 'datetime64[ns]',
        'pandera': 'pd.Timestamp',
        'description': 'Временная метка'
    },
    'DATE': {
        'pandas': 'datetime64[ns]',
        'pandera': 'pd.Timestamp',
        'description': 'Дата'
    },
    'TEXT': {
        'pandas': 'object',
        'pandera': 'pa.String',
        'description': 'Текстовый тип'
    }
}

# Стандартные паттерны валидации
VALIDATION_PATTERNS = {
    'chembl_id': {
        'pattern': r'^CHEMBL\d+$',
        'description': 'ChEMBL идентификатор'
    },
    'doi': {
        'pattern': r'^10\.\d+/[^\s]+$',
        'description': 'DOI идентификатор'
    },
    'pmid': {
        'pattern': r'^\d+$',
        'description': 'PubMed идентификатор'
    },
    'uniprot_id': {
        'pattern': r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$',
        'description': 'UniProt идентификатор'
    },
    'inchi_key': {
        'pattern': r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$',
        'description': 'InChI Key'
    },
    'inchi': {
        'pattern': r'^InChI=1S?/[^\s]+$',
        'description': 'InChI строка'
    },
    'iso8601_date': {
        'pattern': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$',
        'description': 'ISO 8601 дата'
    },
    'sha256_hash': {
        'pattern': r'^[a-f0-9]{64}$',
        'description': 'SHA256 хеш'
    }
}

# Стандартные диапазоны значений
VALUE_RANGES = {
    'molecular_weight': {
        'min': 50.0,
        'max': 2000.0,
        'description': 'Молекулярная масса в Da'
    },
    'pchembl_value': {
        'min': 3.0,
        'max': 12.0,
        'description': 'pChEMBL значение'
    },
    'standard_value': {
        'min': 1e-12,
        'max': 1e-3,
        'description': 'Стандартное значение активности'
    },
    'year': {
        'min': 1900,
        'max': 2030,
        'description': 'Год публикации'
    }
}
