# API Reference

Автогенерированная документация по всем модулям и функциям библиотеки Bioactivity Data Acquisition.

## Модули

### [Конфигурация](config.md)

Управление конфигурацией ETL-пайплайна.

### [HTTP клиенты](clients.md)

Клиенты для работы с внешними API источниками.

### [ETL пайплайн](etl.md)

Основные компоненты ETL-процесса.

### [Схемы данных](schemas.md)

Pandera схемы для валидации данных.

## Как использовать API

### Импорт модулей

```python
from library.config import Config
from library.etl.run import run_pipeline
from library.clients.chembl import ChEMBLClient
```

### Основной пайплайн

```python
# Загрузка конфигурации
config = Config.from_yaml("configs/config.yaml")

# Запуск пайплайна
from library.logging_setup import get_logger
logger = get_logger("pipeline")
output_path = run_pipeline(config, logger)
```

### Работа с клиентами

```python
# Создание клиента
client = ChEMBLClient(config.clients[0])

# Получение данных
data = client.fetch_data()
```

## Соглашения

### Типы данных
- Все функции используют типизацию Python
- Pandera схемы для валидации DataFrame
- Pydantic модели для конфигурации

### Обработка ошибок
- `ApiClientError` для ошибок API
- `ValidationError` для ошибок валидации
- `RateLimitError` для превышения лимитов

### Логирование
- Структурированное логирование через `structlog`
- Контекстные метки для отслеживания
- Различные уровни детализации

---

## Дополнения (CLI/Конфигурация/ETL/Документы)

### CLI


- `library.cli:app` — Typer-приложение (команды: `pipeline`, `get-document-data`, `version`)
- `library.cli:get_document_data` — обогащение документов
- `library.cli:pipeline` — основной ETL пайплайн

### Конфигурация


- `library.config.Config.load(path, overrides=None, env_prefix="BIOACTIVITY__")`
- Модели: `HTTPSettings`, `SourceSettings`, `IOSettings`, `OutputSettings`, `ValidationSettings`, `DeterminismSettings`, `PostprocessSettings`

### ETL


- `library.etl.run:run_pipeline(config, logger)` — e2e пайплайн
- `library.etl.load:write_deterministic_csv(df, destination, ...)`
- `library.etl.load:write_qc_artifacts(df, qc_path, corr_path, ...)`

### Документы


- `library.documents.pipeline:run_document_etl(config, frame)` — ETL для документов
- `library.documents.pipeline:write_document_outputs(result, output_dir, date_tag, config=None)`