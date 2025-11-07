# 02. Переменные окружения и pydantic-settings

> **Статус**: реализовано. Поддерживается `pydantic-settings>=2.0`.

## 1. Принцип работы

1. YAML профили и конфигмердж формируют базовую карту.
2. CLI `--set key=value` накладывает точечные переопределения.
3. Переменные окружения с префиксами `BIOETL__` или `BIOACTIVITY__` загружаются через pydantic-settings и имеют высший приоритет.

Обработка окружения выполняется в `src/bioetl/config/loader.py` с помощью внутренней модели `_PipelineEnvSettings`, где `env_nested_delimiter="__"`. Ключи автоматически приводятся к нижнему регистру.

## 2. Формат переменных

```text
BIOETL__SECTION__SUBSECTION__KEY=value
```

- Префикс (`BIOETL__`, `BIOACTIVITY__`) обязательный.
- Двойное подчеркивание отделяет уровень вложенности.
- Значения приводятся через `yaml.safe_load`, поэтому можно передавать числа, булевы и списки.

### Примеры

```bash
export BIOETL__PIPELINE__NAME=activity_override
export BIOETL__IO__OUTPUT__PARTITION_BY='["year"]'
export BIOACTIVITY__RUNTIME__PARALLELISM=8
export BIOETL__DETERMINISM__HASHING__ALGORITHM=blake2b
```

## 3. Типичные кейсы

- Быстрое изменение путей: `BIOETL__IO__OUTPUT__PATH=/tmp/out.parquet`.
- Переопределение таймаутов без правки YAML: `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`.
- Инжекция секретов и токенов: `BIOETL__SOURCES__CHEMBL__PARAMETERS__api_key=...` (значение не попадает в репозиторий).

## 4. Ограничения

- Пустой сегмент (`BIOETL__`) игнорируется.
- Неизвестные ключи запрещены за счёт `extra="forbid"` в Pydantic-моделях.
- Значения, не распознаваемые `yaml.safe_load`, остаются строкой.

## 5. Рекомендации

1. Используйте `.env` (поддерживается `pydantic-settings`) только локально, добавляя файл в `.gitignore`.
2. Для продакшна храните секреты в менеджере секретов и прокидывайте через переменные окружения.
3. Проверьте настройки через `bioetl config inspect` (см. CLI документацию), чтобы убедиться, что значения применились.

Эти рекомендации помогают сохранить детерминизм и повторяемость окружения.
