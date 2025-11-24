---
trigger: model_decision
description: USE WHEN fixing test failures; enforce determinism, schema validation, logging, and architectural boundaries
---

# Fix Test Errors

> Scope:
> - USE WHEN fixing test failures; enforce determinism, schema validation, logging, and architectural boundaries
> - Use when editing files matching: `tests/**/*.py`, fixing pytest errors

## Контекст
Ты - инженер-помощник проекта BioETL. Твоя задача - исправить ошибки в тестах, соблюдая строгие инварианты фреймворка: детерминизм, валидацию Pandera, структурированное логирование и трёхуровневую архитектуру.

## Ключевые инварианты (обязательно соблюдать)

### 1. Детерминизм I/O
- Все DataFrame должны иметь стабильную сортировку (`sort_values()` с фиксированным порядком колонок)
- Временные метки только в UTC ISO format
- Атомарная запись файлов через `write_dataset_atomic()`
- Используй `ensure_hash_columns()` для консистентности хешей

### 2. Валидация схем Pandera
- Все данные должны проходить через `schema.validate(df)` перед записью
- Соблюдай порядок колонок из `COLUMN_ORDER` констант
- Используй [get_schema()](cci:1://file:///e:/github/bioactivity_data_acquisition1/src/bioetl/schemas/__init__.py:744:0-756:21) для получения схем реестра
- Обрабатывай `SchemaValidationError` с информативными сообщениями

### 3. Структурированное логирование
- Только через `UnifiedLogger`, никаких `print()`
- Используй `LogEvents` для событий
- Контекст через `bind_pipeline_context()` и `pipeline_stage()`
- Обязательные поля: `pipeline_code`, `run_id`, `stage`

### 4. Архитектурные границы
- Orchestration layer (`bioetl.pipelines`) → Domain layer (`bioetl.schemas`) → Infrastructure layer (`bioetl.clients`)
- Тесты не должны нарушать направление зависимостей
- Mock'и только для infrastructure layer, domain logic тестируется детерминированно

## Паттерны тестирования

### Unit-тесты (простой путь)
```python
# Используй упрощённый конструктор для быстрого тестирования
pipeline = ChemblActivityPipeline(source=test_data)
# Не требует полной конфигурации
```

### Интеграционные тесты (полный путь)
```python
# Полная конфигурация для end-to-end тестов
config = load_config("configs/pipelines/activity/test.yaml")
pipeline = ChemblActivityPipeline(config, run_id="test-123", source=test_data)
```

### Mock'ирование HTTP-клиентов
```python
from unittest.mock import Mock
mock_client = Mock(spec=ChemblClient)
mock_client.get.return_value = Mock(json=lambda: mock_response)
```

## Частые проблемы и решения

1. **Ошибки импорта из-за lazy loading**
```python
# Неправильно: from bioetl.clients import ChemblClient
# Правильно: импортировать через __getattr__ или напрямую
from bioetl.clients.client_chembl import ChemblClient
```

2. **Несоответствие схем**
```python
# Всегда нормализуй тестовые данные
df = pd.DataFrame(test_data)
df = ensure_columns(df, required_columns=schema.columns.keys())
df = schema.validate(df)
```

3. **Проблемы с временными зонами**
```python
from datetime import datetime, timezone
now = datetime.now(timezone.utc)  # Только UTC!
```

4. **Batch extraction ошибки**
```python
# Для тестов batched extraction используй моки
stats = BatchExtractionStats(
    total_requested=len(ids),
    total_extracted=len(results),
    api_calls=api_call_count,
    duration_ms=duration_ms
)
```

## Структура исправления

1. **Анализ ошибки**: Определи тип (импорт, схема, HTTP, архитектура)
2. **Корневая причина**: Найди нарушение инварианта
3. **Минимальное исправление**: Исправь только необходимое
4. **Регрессионный тест**: Добавь тест для предотвращения повтора
5. **Валидация**: Запусти pytest tests/ - все должны проходить

## Пример исправления

**Было (ошибка схемы):**
```python
def test_activity_transform():
    df = pd.DataFrame({"activity_id": [1], "invalid_col": ["x"]})
    result = pipeline.transform(df)
```

**Стало (с валидацией):**
```python
def test_activity_transform():
    df = pd.DataFrame({
        "activity_id": ["CHEMBL1"],
        "standard_type": ["IC50"],
        "standard_relation": ["="],
        "standard_value": [100.0]
    })
    df = ensure_columns(df, ACTIVITY_COLUMN_ORDER)
    result = pipeline.transform(df)
    assert result.shape[0] == 1
```

## Команды для валидации

```bash
# Запуск конкретных тестов
pytest tests/bioetl/pipelines/chembl/test_activity.py -v

# С покрытием
pytest tests/ --cov=src/bioetl --cov-report=html

# Детерминизм проверка
python -m bioetl.tools.determinism_check
```

## Документация

- docs/pipelines/chembl/00-architecture.md - архитектура пайплайнов
- docs/styleguide/05-testing-standards.md - стандарты тестирования
- docs/styleguide/04-deterministic-io.md - детерминизм

При исправлении всегда спрашивай: "Сохраняет ли это изменение детерминизм, валидацию схемы и архитектурные границы?"
