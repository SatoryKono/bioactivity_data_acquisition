# Схема конфигурации

## Обзор

Данный документ описывает детальную схему конфигурации проекта Bioactivity Data Acquisition, включая все параметры Pydantic моделей и их назначение.

## Основные модели конфигурации

### Config

**Расположение**: `src/library/config.py`

**Назначение**: Главная модель конфигурации пайплайна

**Основные секции**:
- `sources`: Настройки источников данных
- `transforms`: Параметры трансформации
- `io`: Настройки ввода/вывода
- `determinism`: Параметры детерминированности
- `validation`: Настройки валидации

**Пример конфигурации**:
```yaml
version: "1.0.0"
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 10
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 45
    retries: 3
    rate_limit: 5

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 100

io:
  input:
    data_path: "data/input/"
  output:
    data_path: "data/output/"
    csv_format:
      column_order: ["assay_id", "molecule_id", "activity_value"]

determinism:
  enable_deterministic_sorting: true
  sort_keys: ["assay_id", "molecule_id", "activity_id"]
  generate_checksums: true

validation:
  enable_pandera_validation: true
  strict_mode: false
```

### APIClientConfig

**Назначение**: Конфигурация для API клиентов

**Параметры**:
- `base_url`: Базовый URL API
- `timeout`: Таймаут запросов в секундах
- `retries`: Количество повторных попыток
- `rate_limit`: Лимит запросов в секунду
- `api_key`: API ключ (опционально)

**Пример**:
```yaml
api_clients:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 10
    api_key: null
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 45
    retries: 3
    rate_limit: 5
    api_key: null
```

### HTTPSettings

**Назначение**: Настройки HTTP соединений

**Параметры**:
- `timeout`: Таймаут соединения
- `retries`: Количество повторных попыток
- `backoff_factor`: Коэффициент экспоненциального backoff
- `max_retries`: Максимальное количество попыток

**Пример**:
```yaml
http_settings:
  timeout: 60
  retries: 5
  backoff_factor: 2.0
  max_retries: 10
```

### RetrySettings

**Назначение**: Настройки повторных попыток

**Параметры**:
- `max_attempts`: Максимальное количество попыток
- `base_delay`: Базовая задержка в секундах
- `max_delay`: Максимальная задержка в секундах
- `exponential_base`: База для экспоненциального backoff

**Пример**:
```yaml
retry_settings:
  max_attempts: 5
  base_delay: 1.0
  max_delay: 60.0
  exponential_base: 2.0
```

### RateLimitSettings

**Назначение**: Настройки ограничения скорости запросов

**Параметры**:
- `requests_per_second`: Запросов в секунду
- `burst_size`: Размер пакета запросов
- `window_size`: Размер окна в секундах

**Пример**:
```yaml
rate_limit_settings:
  requests_per_second: 10
  burst_size: 50
  window_size: 60
```

## Специализированные конфигурации

### DocumentConfig

**Расположение**: `src/library/documents/config.py`

**Назначение**: Конфигурация для пайплайна документов

**Основные секции**:
- `sources`: Настройки источников данных
- `input`: Параметры входных данных
- `output`: Параметры выходных данных
- `http`: Настройки HTTP клиентов

**Пример**:
```yaml
sources:
  pubmed:
    enabled: true
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    timeout: 30
    retries: 3
    rate_limit: 3
  
  crossref:
    enabled: true
    base_url: "https://api.crossref.org/"
    timeout: 30
    retries: 3
    rate_limit: 50
  
  openalex:
    enabled: true
    base_url: "https://api.openalex.org/"
    timeout: 30
    retries: 3
    rate_limit: 10
  
  semantic_scholar:
    enabled: true
    base_url: "https://api.semanticscholar.org/"
    timeout: 30
    retries: 3
    rate_limit: 100

input:
  data_path: "data/input/documents.csv"
  encoding: "utf-8"
  delimiter: ","

output:
  data_path: "data/output/documents_enriched.csv"
  qc_report_path: "data/output/documents_qc_report.json"
  meta_path: "data/output/meta.yaml"
```

### IOSettings

**Назначение**: Настройки ввода/вывода данных

**Параметры**:
- `input`: Настройки входных данных
- `output`: Настройки выходных данных
- `csv_format`: Формат CSV файлов
- `parquet_format`: Формат Parquet файлов

**Пример**:
```yaml
io_settings:
  input:
    data_path: "data/input/"
    encoding: "utf-8"
    delimiter: ","
  
  output:
    data_path: "data/output/"
    qc_report_path: "data/output/qc_report.json"
    correlation_path: "data/output/correlation_matrix.json"
    meta_path: "data/output/meta.yaml"
  
  csv_format:
    column_order: ["assay_id", "molecule_id", "activity_value"]
    delimiter: ","
    encoding: "utf-8"
    index: false
  
  parquet_format:
    compression: "snappy"
    engine: "pyarrow"
```

### TransformSettings

**Назначение**: Настройки трансформации данных

**Параметры**:
- `enable_normalization`: Включить нормализацию
- `enable_enrichment`: Включить обогащение
- `batch_size`: Размер батча для обработки
- `parallel_workers`: Количество параллельных процессов

**Пример**:
```yaml
transform_settings:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 100
  parallel_workers: 4
```

### ValidationSettings

**Назначение**: Настройки валидации данных

**Параметры**:
- `enable_pandera_validation`: Включить валидацию Pandera
- `strict_mode`: Строгий режим валидации
- `lazy_validation`: Ленивая валидация
- `error_threshold`: Порог ошибок

**Пример**:
```yaml
validation_settings:
  enable_pandera_validation: true
  strict_mode: false
  lazy_validation: true
  error_threshold: 0.1
```

## Валидация конфигурации

### Pydantic валидация

**Автоматическая валидация**:
```python
from library.config import Config

# Загрузка и валидация конфигурации
try:
    config = Config.from_yaml("configs/config.yaml")
    print("Конфигурация валидна")
except ValidationError as e:
    print(f"Ошибка валидации: {e}")
```

**Проверка обязательных полей**:
```python
def validate_required_fields(config: Config) -> bool:
    """Проверяет наличие обязательных полей."""
    
    required_sections = ["sources", "io", "transforms"]
    
    for section in required_sections:
        if not hasattr(config, section):
            raise ValueError(f"Отсутствует секция: {section}")
    
    return True
```

### Проверка значений

**Валидация URL**:
```python
def validate_url(url: str) -> bool:
    """Проверяет корректность URL."""
    
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False
```

**Валидация путей**:
```python
def validate_path(path: str) -> bool:
    """Проверяет корректность пути к файлу."""
    
    try:
        Path(path).resolve()
        return True
    except Exception:
        return False
```

## Загрузка конфигурации

### Из YAML файла

```python
from library.config import Config

# Загрузка из файла
config = Config.from_yaml("configs/config.yaml")

# Загрузка с валидацией
config = Config.from_yaml("configs/config.yaml", validate=True)
```

### Из переменных окружения

```python
import os
from library.config import Config

# Установка переменных окружения
os.environ["CHEMBL_API_KEY"] = "your_api_key"
os.environ["PUBMED_API_KEY"] = "your_pubmed_key"

# Загрузка с переменными окружения
config = Config.from_env(prefix="BIOACTIVITY_")
```

### Из словаря

```python
from library.config import Config

# Конфигурация в виде словаря
config_dict = {
    "version": "1.0.0",
    "sources": {
        "chembl": {
            "base_url": "https://www.ebi.ac.uk/chembl/api/data",
            "timeout": 60
        }
    }
}

# Создание конфигурации
config = Config(**config_dict)
```

## Переменные окружения

### Префиксы

**Основной префикс**: `BIOACTIVITY_`

**Примеры переменных**:
```bash
# Основные настройки
export BIOACTIVITY_VERSION="1.0.0"
export BIOACTIVITY_LOG_LEVEL="INFO"

# API ключи
export BIOACTIVITY_CHEMBL_API_KEY="your_chembl_key"
export BIOACTIVITY_PUBMED_API_KEY="your_pubmed_key"
export BIOACTIVITY_CROSSREF_API_KEY="your_crossref_key"

# Пути к данным
export BIOACTIVITY_INPUT_PATH="data/input/"
export BIOACTIVITY_OUTPUT_PATH="data/output/"

# Настройки HTTP
export BIOACTIVITY_HTTP_TIMEOUT="60"
export BIOACTIVITY_HTTP_RETRIES="5"
export BIOACTIVITY_RATE_LIMIT="10"
```

### Приоритет настроек

1. **Переменные окружения** (высший приоритет)
2. **YAML файл конфигурации**
3. **Значения по умолчанию** (низший приоритет)

## Примеры конфигураций

### Минимальная конфигурация

```yaml
version: "1.0.0"
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60

io:
  input:
    data_path: "data/input/"
  output:
    data_path: "data/output/"
```

### Полная конфигурация

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 10
    api_key: null
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 45
    retries: 3
    rate_limit: 5
    api_key: null

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 100
  parallel_workers: 4

io:
  input:
    data_path: "data/input/"
    encoding: "utf-8"
    delimiter: ","
  
  output:
    data_path: "data/output/"
    qc_report_path: "data/output/qc_report.json"
    correlation_path: "data/output/correlation_matrix.json"
    meta_path: "data/output/meta.yaml"
  
  csv_format:
    column_order: ["assay_id", "molecule_id", "activity_value"]
    delimiter: ","
    encoding: "utf-8"
    index: false
  
  parquet_format:
    compression: "snappy"
    engine: "pyarrow"

determinism:
  enable_deterministic_sorting: true
  sort_keys: ["assay_id", "molecule_id", "activity_id"]
  generate_checksums: true
  include_meta_yaml: true

validation:
  enable_pandera_validation: true
  strict_mode: false
  lazy_validation: true
  error_threshold: 0.1

http:
  timeout: 60
  retries: 5
  backoff_factor: 2.0
  max_retries: 10

rate_limit:
  requests_per_second: 10
  burst_size: 50
  window_size: 60
```

### Конфигурация для документов

```yaml
version: "1.0.0"

sources:
  pubmed:
    enabled: true
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    timeout: 30
    retries: 3
    rate_limit: 3
    api_key: null
  
  crossref:
    enabled: true
    base_url: "https://api.crossref.org/"
    timeout: 30
    retries: 3
    rate_limit: 50
    api_key: null
  
  openalex:
    enabled: true
    base_url: "https://api.openalex.org/"
    timeout: 30
    retries: 3
    rate_limit: 10
    api_key: null
  
  semantic_scholar:
    enabled: true
    base_url: "https://api.semanticscholar.org/"
    timeout: 30
    retries: 3
    rate_limit: 100
    api_key: null

input:
  data_path: "data/input/documents.csv"
  encoding: "utf-8"
  delimiter: ","

output:
  data_path: "data/output/documents_enriched.csv"
  qc_report_path: "data/output/documents_qc_report.json"
  meta_path: "data/output/meta.yaml"

http:
  timeout: 30
  retries: 3
  backoff_factor: 2.0
  max_retries: 10

rate_limit:
  requests_per_second: 10
  burst_size: 50
  window_size: 60
```

## Обработка ошибок конфигурации

### Типы ошибок

**ValidationError**: Ошибки валидации Pydantic
**FileNotFoundError**: Файл конфигурации не найден
**YAMLError**: Ошибка парсинга YAML
**ValueError**: Некорректные значения параметров

### Обработка ошибок

```python
from library.config import Config
from pydantic import ValidationError
import yaml

def load_config_safely(config_path: str) -> Config:
    """Безопасная загрузка конфигурации."""
    
    try:
        config = Config.from_yaml(config_path)
        return config
    except FileNotFoundError:
        print(f"Файл конфигурации не найден: {config_path}")
        return Config()  # Конфигурация по умолчанию
    except yaml.YAMLError as e:
        print(f"Ошибка парсинга YAML: {e}")
        raise
    except ValidationError as e:
        print(f"Ошибка валидации конфигурации: {e}")
        raise
```

## Мониторинг конфигурации

### Логирование изменений

```python
import logging

logger = logging.getLogger(__name__)

def log_config_changes(old_config: Config, new_config: Config):
    """Логирует изменения в конфигурации."""
    
    changes = []
    
    # Сравнение основных параметров
    if old_config.version != new_config.version:
        changes.append(f"version: {old_config.version} -> {new_config.version}")
    
    if old_config.sources != new_config.sources:
        changes.append("sources: изменены настройки источников")
    
    if changes:
        logger.info(f"Изменения в конфигурации: {', '.join(changes)}")
```

### Валидация во время выполнения

```python
def validate_config_at_runtime(config: Config) -> bool:
    """Валидирует конфигурацию во время выполнения."""
    
    # Проверка доступности источников
    for source_name, source_config in config.sources.items():
        if not validate_url(source_config.base_url):
            logger.error(f"Некорректный URL для источника {source_name}")
            return False
    
    # Проверка путей к файлам
    if not validate_path(config.io.input.data_path):
        logger.error("Некорректный путь к входным данным")
        return False
    
    return True
```
