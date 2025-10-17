# API Reference

Автогенерированная документация по всем модулям и функциям библиотеки Bioactivity Data Acquisition.

## Модули

### [Конфигурация](config.md)
Управление конфигурацией ETL-пайплайна.

::: library.config

### [HTTP клиенты](clients.md)
Клиенты для работы с внешними API источниками.

::: library.clients

### [ETL пайплайн](etl.md)
Основные компоненты ETL-процесса.

::: library.etl

### [Схемы данных](schemas.md)
Pandera схемы для валидации данных.

::: library.schemas

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
