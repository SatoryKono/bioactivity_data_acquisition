# Примеры конфигураций

## Обзор

Данный документ содержит практические примеры конфигураций для различных сценариев использования проекта Bioactivity Data Acquisition.

## Базовые конфигурации

### Минимальная конфигурация

**Файл**: `configs/config_minimal.yaml`

**Назначение**: Базовая конфигурация для быстрого старта

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

transforms:
  enable_normalization: true
  batch_size: 100
```

### Стандартная конфигурация

**Файл**: `configs/config.yaml`

**Назначение**: Рекомендуемая конфигурация для большинства случаев

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

## Конфигурации по источникам данных

### Только ChEMBL

**Файл**: `configs/config_chembl_only.yaml`

**Назначение**: Конфигурация для работы только с ChEMBL API

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 10
    api_key: null

transforms:
  enable_normalization: true
  enable_enrichment: false
  batch_size: 100

io:
  input:
    data_path: "data/input/"
  output:
    data_path: "data/output/"

http:
  timeout: 60
  retries: 5

rate_limit:
  requests_per_second: 10
```

### ChEMBL + PubChem

**Файл**: `configs/config_chembl_pubchem.yaml`

**Назначение**: Конфигурация для обогащения данных ChEMBL через PubChem

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
    qc_report_path: "data/output/qc_report.json"

http:
  timeout: 60
  retries: 5

rate_limit:
  requests_per_second: 8  # Суммарный лимит для двух источников
```

## Конфигурации для документов

### Базовый пайплайн документов

**Файл**: `configs/config_documents.yaml`

**Назначение**: Конфигурация для обогащения документов

```yaml
version: "1.0.0"

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

http:
  timeout: 30
  retries: 3

rate_limit:
  requests_per_second: 10
```

### Только PubMed

**Файл**: `configs/config_documents_pubmed.yaml`

**Назначение**: Конфигурация для работы только с PubMed

```yaml
version: "1.0.0"

sources:
  pubmed:
    enabled: true
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    timeout: 30
    retries: 3
    rate_limit: 3
  
  crossref:
    enabled: false
  
  openalex:
    enabled: false
  
  semantic_scholar:
    enabled: false

input:
  data_path: "data/input/documents.csv"

output:
  data_path: "data/output/documents_pubmed.csv"
  qc_report_path: "data/output/documents_pubmed_qc.json"

http:
  timeout: 30
  retries: 3

rate_limit:
  requests_per_second: 3
```

## Конфигурации для разных сред

### Разработка

**Файл**: `configs/config_development.yaml`

**Назначение**: Конфигурация для разработки и тестирования

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 30
    retries: 2
    rate_limit: 5
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 30
    retries: 2
    rate_limit: 3

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 50  # Меньший размер батча для быстрого тестирования
  parallel_workers: 2

io:
  input:
    data_path: "data/input/"
  output:
    data_path: "data/output/"
    qc_report_path: "data/output/qc_report.json"

validation:
  enable_pandera_validation: true
  strict_mode: false
  lazy_validation: true
  error_threshold: 0.2  # Более мягкий порог ошибок

http:
  timeout: 30
  retries: 2

rate_limit:
  requests_per_second: 5
```

### Продакшен

**Файл**: `configs/config_production.yaml`

**Назначение**: Конфигурация для продакшен среды

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 120
    retries: 10
    rate_limit: 20
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 90
    retries: 5
    rate_limit: 10

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 500  # Больший размер батча для эффективности
  parallel_workers: 8

io:
  input:
    data_path: "/data/input/"
  output:
    data_path: "/data/output/"
    qc_report_path: "/data/output/qc_report.json"
    correlation_path: "/data/output/correlation_matrix.json"
    meta_path: "/data/output/meta.yaml"

validation:
  enable_pandera_validation: true
  strict_mode: true  # Строгий режим для продакшена
  lazy_validation: false
  error_threshold: 0.01  # Строгий порог ошибок

determinism:
  enable_deterministic_sorting: true
  sort_keys: ["assay_id", "molecule_id", "activity_id"]
  generate_checksums: true
  include_meta_yaml: true

http:
  timeout: 120
  retries: 10
  backoff_factor: 2.0
  max_retries: 20

rate_limit:
  requests_per_second: 20
  burst_size: 100
  window_size: 60
```

### Тестирование

**Файл**: `configs/config_test.yaml`

**Назначение**: Конфигурация для автоматических тестов

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 10
    retries: 1
    rate_limit: 1
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 10
    retries: 1
    rate_limit: 1

transforms:
  enable_normalization: true
  enable_enrichment: false
  batch_size: 10
  parallel_workers: 1

io:
  input:
    data_path: "tests/data/input/"
  output:
    data_path: "tests/data/output/"
    qc_report_path: "tests/data/output/qc_report.json"

validation:
  enable_pandera_validation: true
  strict_mode: true
  lazy_validation: false
  error_threshold: 0.0  # Нулевой порог ошибок для тестов

http:
  timeout: 10
  retries: 1

rate_limit:
  requests_per_second: 1
```

## Специализированные конфигурации

### Высокопроизводительная обработка

**Файл**: `configs/config_high_performance.yaml`

**Назначение**: Конфигурация для обработки больших объемов данных

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 180
    retries: 15
    rate_limit: 50
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 120
    retries: 10
    rate_limit: 20

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 1000
  parallel_workers: 16

io:
  input:
    data_path: "/data/input/"
  output:
    data_path: "/data/output/"
    qc_report_path: "/data/output/qc_report.json"
    correlation_path: "/data/output/correlation_matrix.json"
    meta_path: "/data/output/meta.yaml"
  
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
  error_threshold: 0.05

http:
  timeout: 180
  retries: 15
  backoff_factor: 1.5
  max_retries: 30

rate_limit:
  requests_per_second: 50
  burst_size: 200
  window_size: 60
```

### Конфигурация с API ключами

**Файл**: `configs/config_with_api_keys.yaml`

**Назначение**: Конфигурация с использованием API ключей для увеличения лимитов

```yaml
version: "1.0.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 100  # Увеличенный лимит с API ключом
    api_key: "${CHEMBL_API_KEY}"
  
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout: 45
    retries: 3
    rate_limit: 20
    api_key: "${PUBCHEM_API_KEY}"

transforms:
  enable_normalization: true
  enable_enrichment: true
  batch_size: 200
  parallel_workers: 6

io:
  input:
    data_path: "data/input/"
  output:
    data_path: "data/output/"
    qc_report_path: "data/output/qc_report.json"

http:
  timeout: 60
  retries: 5

rate_limit:
  requests_per_second: 50
```

## Переменные окружения

### Настройка через переменные окружения

**Файл**: `.env`

```bash
# Основные настройки
BIOACTIVITY_VERSION=1.0.0
BIOACTIVITY_LOG_LEVEL=INFO

# API ключи
CHEMBL_API_KEY=your_chembl_api_key
PUBCHEM_API_KEY=your_pubchem_api_key
PUBMED_API_KEY=your_pubmed_api_key
CROSSREF_API_KEY=your_crossref_api_key

# Пути к данным
BIOACTIVITY_INPUT_PATH=/data/input/
BIOACTIVITY_OUTPUT_PATH=/data/output/

# Настройки HTTP
BIOACTIVITY_HTTP_TIMEOUT=60
BIOACTIVITY_HTTP_RETRIES=5
BIOACTIVITY_RATE_LIMIT=20

# Настройки трансформации
BIOACTIVITY_BATCH_SIZE=200
BIOACTIVITY_PARALLEL_WORKERS=6
```

### Загрузка с переменными окружения

```python
import os
from library.config import Config

# Установка переменных окружения
os.environ["CHEMBL_API_KEY"] = "your_api_key"
os.environ["BIOACTIVITY_BATCH_SIZE"] = "200"

# Загрузка конфигурации
config = Config.from_env(prefix="BIOACTIVITY_")
```

## Docker конфигурации

### Docker Compose

**Файл**: `docker-compose.yml`

```yaml
version: '3.8'

services:
  bioactivity-pipeline:
    build: .
    environment:
      - CHEMBL_API_KEY=${CHEMBL_API_KEY}
      - PUBCHEM_API_KEY=${PUBCHEM_API_KEY}
      - BIOACTIVITY_INPUT_PATH=/data/input/
      - BIOACTIVITY_OUTPUT_PATH=/data/output/
    volumes:
      - ./data:/data
      - ./configs:/app/configs
    command: bioactivity-data-acquisition pipeline --config configs/config.yaml
```

### Dockerfile

**Файл**: `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV BIOACTIVITY_INPUT_PATH=/data/input/
ENV BIOACTIVITY_OUTPUT_PATH=/data/output/

CMD ["bioactivity-data-acquisition", "pipeline", "--config", "configs/config.yaml"]
```

## Примеры использования

### Загрузка конфигурации

```python
from library.config import Config

# Загрузка стандартной конфигурации
config = Config.from_yaml("configs/config.yaml")

# Загрузка конфигурации для документов
doc_config = DocumentConfig.from_yaml("configs/config_documents.yaml")

# Загрузка с переменными окружения
config = Config.from_env(prefix="BIOACTIVITY_")
```

### Переопределение параметров

```python
from library.config import Config

# Загрузка базовой конфигурации
config = Config.from_yaml("configs/config.yaml")

# Переопределение параметров
config.transforms.batch_size = 500
config.sources.chembl.rate_limit = 20

# Сохранение измененной конфигурации
config.to_yaml("configs/config_custom.yaml")
```

### Валидация конфигурации

```python
from library.config import Config
from pydantic import ValidationError

try:
    config = Config.from_yaml("configs/config.yaml")
    print("Конфигурация валидна")
except ValidationError as e:
    print(f"Ошибка валидации: {e}")
```

## Лучшие практики

### Организация конфигураций

1. **Разделение по средам**: отдельные файлы для dev, test, prod
2. **Именование файлов**: понятные имена с указанием назначения
3. **Документирование**: комментарии в YAML файлах
4. **Версионирование**: отслеживание изменений в конфигурациях

### Безопасность

1. **API ключи**: использование переменных окружения
2. **Секреты**: не коммитить ключи в репозиторий
3. **Права доступа**: ограничение доступа к конфигурационным файлам
4. **Валидация**: проверка конфигурации перед использованием

### Производительность

1. **Размер батча**: оптимизация для объема данных
2. **Параллелизм**: настройка количества процессов
3. **Rate limiting**: соблюдение лимитов API
4. **Таймауты**: адекватные значения для сетевых запросов
