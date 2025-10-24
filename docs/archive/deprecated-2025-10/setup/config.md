# Конфигурация

Полная документация структуры конфигурации, переменных окружения и политик детерминизма.

## Обзор configs/

Структура конфигурационных файлов:

```
configs/
├── config.yaml                    # Основной конфиг (базовый)
├── config.example.yaml            # Пример конфигурации
├── config_documents_full.yaml     # Полная конфигурация для документов
├── config_target_full.yaml        # Полная конфигурация для мишеней
├── config_assay_full.yaml         # Полная конфигурация для ассев
├── config_activity_full.yaml      # Полная конфигурация для активностей
├── config_testitem_full.yaml      # Полная конфигурация для молекул
├── config_test.yaml               # Тестовая конфигурация
├── logging.yaml                   # Конфигурация логирования
├── schema.json                    # JSON Schema для валидации
└── dictionary/                    # Словари для IUPHAR
    └── _target/
        ├── iuphar_targets.csv
        └── iuphar_families.csv
```

## Основные конфигурационные файлы

### config.yaml

Базовый конфигурационный файл с минимальными настройками:

```yaml
http:
  global:
    timeout_sec: 30.0
    retries:
      total: 5
      backoff_multiplier: 2.0
    headers:
      User-Agent: bioactivity-data-acquisition/0.1.0

sources:
  chembl:
    name: chembl
    enabled: true
    endpoint: document
    # ... остальные настройки

io:
  input:
    documents_csv: data/input/documents.csv
  output:
    data_path: data/output/full/documents.csv
    format: csv

runtime:
  workers: 4
  limit: null
  dry_run: false

validation:
  strict: true
  qc:
    max_missing_fraction: 0.05
    max_duplicate_fraction: 0.01

determinism:
  sort:
    by: [document_chembl_id, title]
    ascending: [true, true]
    na_position: last
  column_order:
    - document_chembl_id
    - title
    - doi
    # ... фиксированный порядок колонок
```

### config_*_full.yaml

Полные конфигурации для каждого пайплайна с детальными настройками всех источников данных.

## Секреты и переменные окружения

### .env файл

Создайте `.env` файл в корне проекта на основе `.env.example`:

```bash
# API ключи (опционально)
CHEMBL_API_TOKEN=your_chembl_token_here
PUBMED_API_KEY=your_pubmed_key_here
SEMANTIC_SCHOLAR_API_KEY=your_semantic_scholar_key_here

# Конфигурация окружения
ENV=development
LOG_LEVEL=INFO

# Пути (при необходимости переопределить)
# DATA_DIR=data
# CACHE_DIR=.cache
```

### Поддерживаемые переменные

| Переменная | Описание | Обязательная | По умолчанию |
|------------|----------|--------------|--------------|
| `CHEMBL_API_TOKEN` | API токен для ChEMBL | Нет | - |
| `PUBMED_API_KEY` | API ключ для PubMed | Нет | - |
| `SEMANTIC_SCHOLAR_API_KEY` | API ключ для Semantic Scholar | Нет | - |
| `ENV` | Окружение (development/production) | Нет | development |
| `LOG_LEVEL` | Уровень логирования | Нет | INFO |
| `DATA_DIR` | Директория для данных | Нет | data |
| `CACHE_DIR` | Директория для кэша | Нет | .cache |

### Настройка API ключей

Используйте встроенную утилиту для настройки:

```bash
# Windows
make setup-api-keys

# Linux/Mac
python scripts/setup_api_keys.py
```

## Политики детерминизма

### Секция determinism

```yaml
determinism:
  sort:
    by: [document_chembl_id, title]
    ascending: [true, true]
    na_position: last
  column_order:
    - document_chembl_id
    - title
    - doi
    - journal
    # ... фиксированный порядок колонок
```

### Гарантии воспроизводимости

1. **Стабильная сортировка**: Фиксированные ключи и порядок
2. **Порядок колонок**: Явно заданный `column_order`
3. **Форматирование**: Консистентное форматирование чисел и дат
4. **Хеширование**: SHA256 для проверки целостности

### Настройки форматирования

```yaml
io:
  output:
    csv:
      encoding: utf-8
      float_format: "%.3f"        # Фиксированный формат float
      date_format: "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601 в UTC
```

## Версионирование

### pipeline_version

Каждый конфигурационный файл содержит версию пайплайна:

```yaml
pipeline:
  version: 1.1.0
  allow_parent_missing: false
  enable_pubchem: true
```

### Привязка к релизу ChEMBL

```yaml
sources:
  chembl:
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      # Автоматически фиксируется релиз ChEMBL при запуске
```

### Правила изменения схем

- **MAJOR** (1.0.0 → 2.0.0): Несовместимые изменения схемы данных
- **MINOR** (1.0.0 → 1.1.0): Добавление новых полей, обратно совместимо
- **PATCH** (1.0.0 → 1.0.1): Исправления ошибок, полная совместимость

## HTTP настройки

### Глобальные настройки

```yaml
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 10
      backoff_multiplier: 3.0
    rate_limit:
      max_calls: 3
      period: 15.0
    headers:
      Accept: application/json
      User-Agent: bioactivity-data-acquisition/0.1.0
```

### Настройки по источникам

```yaml
sources:
  chembl:
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      timeout_sec: 60.0
      headers:
        Authorization: "Bearer {chembl_api_token}"
      health_endpoint: "status"
  
  pubmed:
    rate_limit:
      max_calls: 2
      period: 1.0
    http:
      base_url: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/
      timeout_sec: 60.0
      headers:
        api_key: "{PUBMED_API_KEY}"
```

## Валидация конфигурации

### Pydantic модели

Конфигурация валидируется через Pydantic модели:

```python
from library.config import Config

# Загрузка и валидация
config = Config.from_file("configs/config.yaml")
```

### JSON Schema

Дополнительная валидация через `configs/schema.json`:

```bash
# Валидация конфигурации
python -c "from library.config import validate_config; validate_config('configs/config.yaml')"
```

## Логирование

### Конфигурация logging.yaml

```yaml
version: 1
disable_existing_loggers: false

formatters:
  standard:
    format: '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
  json:
    format: '%(asctime)s %(levelname)s %(name)s %(message)s'

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: standard
    stream: ext://sys.stdout
  
  file:
    class: logging.handlers.RotatingFileHandler
    level: DEBUG
    formatter: json
    filename: logs/app.log
    maxBytes: 10485760  # 10MB
    backupCount: 10

loggers:
  library:
    level: DEBUG
    handlers: [console, file]
    propagate: false

root:
  level: INFO
  handlers: [console]
```

## Runtime настройки

### Основные параметры

```yaml
runtime:
  workers: 4                    # Количество воркеров
  limit: null                   # Ограничение записей (null = без ограничений)
  dry_run: false               # Тестовый режим без записи
  date_tag: null               # Автоматически генерируется если null
  dev_mode: false              # Режим разработки
  allow_incomplete_sources: false  # Разрешить неполные источники
```

### Настройки кэша

```yaml
runtime:
  cache_dir: .cache/chembl
  pubchem_cache_dir: .cache/pubchem
  cache_ttl: 86400  # 24 часа
```

## Валидация данных

### QC настройки

```yaml
validation:
  strict: true
  qc:
    max_missing_fraction: 0.05    # Максимум 5% пропусков
    max_duplicate_fraction: 0.01  # Максимум 1% дубликатов
    min_records: 100              # Минимум записей для обработки
```

### Pandera схемы

```yaml
validation:
  schemas:
    input: library.schemas.document_input_schema
    output: library.schemas.document_output_schema
```

## Postprocess настройки

### QC отчёты

```yaml
postprocess:
  qc:
    enabled: true
    detailed: true
    include_correlations: true
```

### Корреляционный анализ

```yaml
postprocess:
  correlation:
    enabled: true
    method: pearson
    threshold: 0.7
```

### Нормализация журналов

```yaml
postprocess:
  journal_normalization:
    enabled: true
    columns: ["journal", "pubmed_journal", "chembl_journal", "crossref_journal"]
```

## Примеры конфигураций

### Минимальная конфигурация

```yaml
sources:
  chembl:
    enabled: true
    endpoint: document

io:
  input:
    documents_csv: data/input/documents.csv
  output:
    data_path: data/output/documents.csv

runtime:
  workers: 1
  limit: 100
```

### Продакшен конфигурация

```yaml
http:
  global:
    timeout_sec: 120.0
    retries:
      total: 15
      backoff_multiplier: 5.0

sources:
  chembl:
    enabled: true
    pagination:
      size: 500
      max_pages: 100

runtime:
  workers: 8
  limit: null
  dry_run: false

validation:
  strict: true
  qc:
    max_missing_fraction: 0.02
    max_duplicate_fraction: 0.005
```

## Troubleshooting

### Частые проблемы

1. **Невалидный YAML**
   ```bash
   # Проверка синтаксиса
   python -c "import yaml; yaml.safe_load(open('configs/config.yaml'))"
   ```

2. **Отсутствующие обязательные поля**
   ```bash
   # Валидация через Pydantic
   python -c "from library.config import Config; Config.from_file('configs/config.yaml')"
   ```

3. **Проблемы с переменными окружения**
   ```bash
   # Проверка переменных
   python -c "import os; print(os.environ.get('CHEMBL_API_TOKEN', 'NOT_SET'))"
   ```

### Отладка конфигурации

```bash
# Подробный вывод конфигурации
python -c "from library.config import Config; import json; print(json.dumps(Config.from_file('configs/config.yaml').dict(), indent=2))"
```
