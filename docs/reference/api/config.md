# Конфигурация

Модуль для управления конфигурацией ETL-пайплайна.

## Основные классы

::: library.config.Config

## Утилиты

::: library.config.ensure_output_directories_exist

## Примеры использования

### Загрузка конфигурации

```python
from library.config import Config

# Из YAML файла
config = Config.from_yaml("configs/config.yaml")

# С переопределениями
config = Config.from_yaml(
    "configs/config.yaml",
    overrides={"runtime.workers": 8}
)
```

### Создание директорий

```python
from library.config import ensure_output_directories_exist

# Создание выходных директорий
ensure_output_directories_exist(config)
```

### Валидация секретов

```python
# Проверка наличия API ключей
config.validate_secrets()
```
