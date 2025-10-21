# API Reference

Автогенерируемая документация API по модулям src/library/.

## Пайплайны

### Documents

::: library.documents.pipeline

### Targets

::: library.target.pipeline

### Assays

::: library.assay.pipeline

### Activities

::: library.activity.pipeline

### Testitems

::: library.testitem.pipeline

## Клиенты

### ChEMBL

::: library.clients.chembl

### Crossref

::: library.clients.crossref

### OpenAlex

::: library.clients.openalex

### PubMed

::: library.clients.pubmed

### Semantic Scholar

::: library.clients.semantic_scholar

### PubChem

::: library.clients.pubchem

## Схемы валидации

### Документы

::: library.schemas.document_input_schema

::: library.schemas.document_output_schema

### Активности

::: library.schemas.input_schema

::: library.schemas.output_schema

### Targets

::: library.schemas.target_schema

### Assays

::: library.schemas.assay_schema

### Testitems

::: library.schemas.testitem_schema

## ETL утилиты

### Extract

::: library.etl.extract

### Transform

::: library.etl.transform

### Load

::: library.etl.load

### QC

::: library.etl.qc

::: library.etl.enhanced_qc

## Конфигурация

::: library.config

## CLI

::: library.cli

## Утилиты

### Логирование

::: library.logging_setup

### Телеметрия

::: library.telemetry

## Примеры использования

### Базовое использование

```python
from library.cli import app
from library.config import Config
from library.documents.pipeline import DocumentsPipeline

# Загрузка конфигурации
config = Config.from_file("configs/config_documents_full.yaml")

# Создание пайплайна
pipeline = DocumentsPipeline(config)

# Запуск пайплайна
result = pipeline.run()
```

### Работа с клиентами

```python
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient

# Создание клиентов
chembl_client = ChEMBLClient()
crossref_client = CrossrefClient()

# Извлечение данных
chembl_data = chembl_client.get_documents(limit=100)
crossref_data = crossref_client.search_works(query="chembl")
```

### Валидация данных

```python
from library.schemas.document_input_schema import DocumentInputSchema
from library.validation.invariants import validate_document_data_quality

# Валидация схемы
validated_df = DocumentInputSchema.validate(df)

# Проверка инвариантов
quality_results = validate_document_data_quality(validated_df)
```

### Конфигурация

```python
from library.config import Config

# Загрузка конфигурации
config = Config.from_file("configs/config.yaml")

# Доступ к настройкам
timeout = config.http.timeout_sec
sources = config.sources
```

## Типы данных

### Основные типы

```python
from typing import Dict, List, Optional, Union
from datetime import datetime
import pandas as pd

# Основные типы данных
DocumentData = pd.DataFrame
TargetData = pd.DataFrame
AssayData = pd.DataFrame
ActivityData = pd.DataFrame
TestitemData = pd.DataFrame

# Конфигурационные типы
ConfigDict = Dict[str, Any]
SourceConfig = Dict[str, Any]
HttpConfig = Dict[str, Any]
```

### Перечисления

```python
from enum import Enum

class PipelineStatus(Enum):
    """Статус пайплайна."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class DataSource(Enum):
    """Источник данных."""
    CHEMBL = "chembl"
    CROSSREF = "crossref"
    OPENALEX = "openalex"
    PUBMED = "pubmed"
    SEMANTIC_SCHOLAR = "semantic_scholar"
    PUBCHEM = "pubchem"
    UNIPROT = "uniprot"
    IUPHAR = "iuphar"

class ValidationLevel(Enum):
    """Уровень валидации."""
    STRICT = "strict"
    MODERATE = "moderate"
    LENIENT = "lenient"
```

## Исключения

### Основные исключения

```python
class BioactivityDataError(Exception):
    """Базовое исключение для ошибок биоактивностных данных."""
    pass

class ValidationError(BioactivityDataError):
    """Ошибка валидации данных."""
    pass

class ConfigurationError(BioactivityDataError):
    """Ошибка конфигурации."""
    pass

class APIClientError(BioactivityDataError):
    """Ошибка API клиента."""
    pass

class PipelineError(BioactivityDataError):
    """Ошибка пайплайна."""
    pass
```

### Обработка исключений

```python
from library.exceptions import ValidationError, ConfigurationError, APIClientError

try:
    # Выполнение операции
    result = pipeline.run()
except ValidationError as e:
    logger.error(f"Ошибка валидации: {e}")
    # Обработка ошибки валидации
except ConfigurationError as e:
    logger.error(f"Ошибка конфигурации: {e}")
    # Обработка ошибки конфигурации
except APIClientError as e:
    logger.error(f"Ошибка API клиента: {e}")
    # Обработка ошибки API
except Exception as e:
    logger.error(f"Неожиданная ошибка: {e}")
    # Обработка неожиданной ошибки
```

## Константы

### API константы

```python
# ChEMBL API
CHEMBL_BASE_URL = "https://www.ebi.ac.uk/chembl/api/data"
CHEMBL_TIMEOUT = 60.0
CHEMBL_MAX_RETRIES = 5

# Crossref API
CROSSREF_BASE_URL = "https://api.crossref.org/works"
CROSSREF_TIMEOUT = 30.0
CROSSREF_MAX_RETRIES = 3

# OpenAlex API
OPENALEX_BASE_URL = "https://api.openalex.org/works"
OPENALEX_TIMEOUT = 30.0
OPENALEX_MAX_RETRIES = 3

# PubMed API
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PUBMED_TIMEOUT = 60.0
PUBMED_MAX_RETRIES = 10

# Semantic Scholar API
SEMANTIC_SCHOLAR_BASE_URL = "https://api.semanticscholar.org/graph/v1/paper"
SEMANTIC_SCHOLAR_TIMEOUT = 60.0
SEMANTIC_SCHOLAR_MAX_RETRIES = 15

# PubChem API
PUBCHEM_BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
PUBCHEM_TIMEOUT = 45.0
PUBCHEM_MAX_RETRIES = 8

# UniProt API
UNIPROT_BASE_URL = "https://rest.uniprot.org"
UNIPROT_TIMEOUT = 45.0
UNIPROT_MAX_RETRIES = 5
```

### Константы валидации

```python
# Пороговые значения качества
MIN_FILL_RATE = 0.8
MAX_DUPLICATE_RATE = 0.01
MIN_RECORDS_COUNT = 100

# Форматы данных
CHEMBL_ID_PATTERN = r"^CHEMBL\d+$"
DOI_PATTERN = r"^10\.\d+/[^\s]+$"
UNIPROT_ID_PATTERN = r"^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$"

# Единицы измерения
VALID_UNITS = ["nM", "uM", "mM", "M", "ng/ml", "ug/ml", "mg/ml", "g/ml"]

# Типы активностей
VALID_ACTIVITY_TYPES = ["IC50", "EC50", "Ki", "Kd", "AC50", "Potency"]

# Типы ассев
VALID_ASSAY_TYPES = ["B", "F", "P", "U"]
```

## Вспомогательные функции

### Функции работы с данными

```python
def normalize_chembl_id(chembl_id: str) -> str:
    """Нормализация ChEMBL ID."""
    if not chembl_id:
        return ""
    return chembl_id.upper().strip()

def validate_doi(doi: str) -> bool:
    """Валидация DOI."""
    import re
    return bool(re.match(DOI_PATTERN, doi))

def calculate_fill_rate(df: pd.DataFrame) -> float:
    """Расчёт fill rate для DataFrame."""
    return df.notna().mean().mean()

def detect_duplicates(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Обнаружение дубликатов по указанным колонкам."""
    return df[df.duplicated(subset=columns, keep=False)]
```

### Функции работы с файлами

```python
def ensure_directory(path: str) -> None:
    """Создание директории если не существует."""
    Path(path).mkdir(parents=True, exist_ok=True)

def get_timestamp() -> str:
    """Получение временной метки в формате YYYYMMDD."""
    return datetime.now().strftime("%Y%m%d")

def calculate_file_hash(file_path: str) -> str:
    """Расчёт SHA256 хеша файла."""
    import hashlib
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()
```

### Функции логирования

```python
def setup_logging(level: str = "INFO") -> None:
    """Настройка логирования."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/app.log')
        ]
    )

def log_validation_results(results: Dict[str, Any]) -> None:
    """Логирование результатов валидации."""
    logger = logging.getLogger(__name__)
    for key, value in results.items():
        logger.info(f"{key}: {value}")
```

## Расширение API

### Создание нового клиента

```python
from library.clients.base import BaseApiClient

class NewApiClient(BaseApiClient):
    """Новый API клиент."""
    
    def __init__(self, base_url: str, timeout: float = 30.0):
        super().__init__(base_url, timeout)
    
    def get_data(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Получение данных из API."""
        return self._make_request(endpoint, params)
```

### Создание новой схемы валидации

```python
import pandera as pa
from pandera import Field, Check

class NewDataSchema(pa.DataFrameModel):
    """Схема для валидации новых данных."""
    
    id: pa.typing.Series[str] = Field(description="Идентификатор")
    name: pa.typing.Series[str] = Field(description="Название")
    value: pa.typing.Series[float] = Field(
        description="Значение",
        checks=[Check.greater_than(0)]
    )
    
    class Config:
        coerce = True
        strict = True
```

### Создание нового пайплайна

```python
from library.pipelines.base import BasePipeline

class NewPipeline(BasePipeline):
    """Новый пайплайн."""
    
    def __init__(self, config: Config):
        super().__init__(config)
    
    def extract(self) -> pd.DataFrame:
        """Извлечение данных."""
        # Реализация извлечения
        pass
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформация данных."""
        # Реализация трансформации
        pass
    
    def load(self, df: pd.DataFrame) -> None:
        """Загрузка данных."""
        # Реализация загрузки
        pass
```

## Производительность

### Оптимизация

```python
# Использование батчевой обработки
def process_in_batches(df: pd.DataFrame, batch_size: int = 1000) -> pd.DataFrame:
    """Обработка данных батчами."""
    results = []
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        processed_batch = process_batch(batch)
        results.append(processed_batch)
    return pd.concat(results, ignore_index=True)

# Использование кэширования
from functools import lru_cache

@lru_cache(maxsize=128)
def cached_api_call(endpoint: str, params: str) -> Dict[str, Any]:
    """Кэшированный API вызов."""
    return api_client.get(endpoint, params)
```

### Мониторинг

```python
import time
from contextlib import contextmanager

@contextmanager
def measure_time(operation_name: str):
    """Измерение времени выполнения операции."""
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"{operation_name} выполнена за {duration:.2f} секунд")

# Использование
with measure_time("Извлечение данных"):
    data = extract_data()
```

## Безопасность

### Валидация входных данных

```python
def validate_input_data(data: Any) -> bool:
    """Валидация входных данных."""
    if not isinstance(data, (str, int, float, list, dict)):
        return False
    
    # Дополнительные проверки безопасности
    if isinstance(data, str):
        # Проверка на SQL инъекции
        if any(keyword in data.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
            return False
    
    return True
```

### Обработка секретов

```python
import os
from typing import Optional

def get_secret(secret_name: str) -> Optional[str]:
    """Безопасное получение секрета."""
    return os.getenv(secret_name)

def validate_api_key(api_key: str) -> bool:
    """Валидация API ключа."""
    if not api_key:
        return False
    
    # Проверка формата API ключа
    if len(api_key) < 10:
        return False
    
    return True
```
