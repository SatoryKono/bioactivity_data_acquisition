# Отчет о нарушениях архитектурных правил

**Дата**: 2025-01-29  
**Ветка**: test_refactoring_32  
**Аннотация**: Выявлены нарушения архитектурных правил проекта с привязкой к конкретным файлам и строкам. Каждое нарушение снабжено оценкой последствий, планом исправления и требуемыми тестами.

## Сводная таблица нарушений

| rule | file:line | snippet | impact | fix | required_tests | severity |
|------|-----------|---------|--------|-----|----------------|----------|
| Сетевые I/O вне client/ | src/bioetl/utils/chembl.py:114 | `response = requests.get(full_url, timeout=30)` | Нарушает централизацию HTTP-клиентов, нет rate limiting/retry/circuit breaker | Использовать `UnifiedAPIClient.request_json()` или создать обертку | unit тест с моком `UnifiedAPIClient` | High |
| Импорт requests вне core/api_client | src/bioetl/sources/chembl/document/client/document_client.py:9 | `import requests` | Используется только для `requests.exceptions.ReadTimeout` в except | Заменить на `from requests.exceptions import ReadTimeout` или использовать `UnifiedAPIClient` исключения | unit тест обработки timeout | Low |
| Сетевые I/O вне client/ | src/bioetl/utils/chembl.py:110-117 | `_request_status()` использует прямой `requests.get()` | Нет централизованного управления таймаутами, retry, кэшированием | Переписать на `UnifiedAPIClient` | интеграционный тест fetch_chembl_release | High |

## Детализация нарушений

### 1. Сетевые I/O вне client/

**Правило**: Все сетевые вызовы должны выполняться только через `core/api_client.py` или модули в `clients/`.

**Нарушение 1**: `src/bioetl/utils/chembl.py:114`

```python
def _request_status(base_url: str) -> Mapping[str, Any]:
    full_url = urljoin(base_url.rstrip("/") + "/", "status.json")
    response = requests.get(full_url, timeout=30)  # Прямой вызов вне client/
    response.raise_for_status()
    payload: Mapping[str, Any] = response.json()
    return payload
```

**Последствия**:
- Нет rate limiting
- Нет retry с backoff
- Нет circuit breaker
- Нет централизованного кэширования
- Нет единого логирования HTTP-запросов

**Исправление**:
1. Изменить сигнатуру `fetch_chembl_release()` для обязательного использования `UnifiedAPIClient`
2. Переписать `_request_status()` на `api_client.request_json("/status.json")`
3. Удалить fallback для строки base_url или создать временный клиент

**Требуемые тесты**:
- `tests/unit/utils/test_chembl.py`: мок `UnifiedAPIClient`, проверка вызова `request_json()`
- `tests/integration/utils/test_chembl.py`: интеграционный тест с реальным клиентом

**Severity**: High

**Нарушение 2**: `src/bioetl/sources/chembl/document/client/document_client.py:9`

```python
import requests  # Импорт для использования requests.exceptions.ReadTimeout
```

Используется только в строке 130:
```python
except requests.exceptions.ReadTimeout:
```

**Последствия**: Минимальное - только импорт неиспользуемого модуля

**Исправление**:
```python
from requests.exceptions import ReadTimeout
# ...
except ReadTimeout:
```

**Требуемые тесты**: Текущие тесты должны покрывать, проверить coverage

**Severity**: Low

### 2. Диск I/O вне output/ или без атомарности

**Правило**: Все записи файлов должны использовать атомарную запись через `_atomic_write()` или `UnifiedOutputWriter`.

**Проверка**: 
- `core/output_writer.py:438,449` - `to_csv()` и `to_parquet()` вызываются внутри `AtomicWriter.write()`, который должен использоваться через `_atomic_write()` - **КОРРЕКТНО**
- Все вызовы `AtomicWriter.write()` проверяются через контекст `_atomic_write()` - **КОРРЕКТНО**

**Вывод**: Нарушений не обнаружено. Все записи используют атомарную запись.

### 3. Импорт-циклы

**Правило**: Запрещены циклические зависимости между модулями.

**Метод проверки**: Ручной анализ импортов ключевых модулей.

**Выявленные проблемы**:
- Требуется полный анализ графа зависимостей (не завершен из-за таймаутов инструментов)
- Рекомендуется использовать `pydeps` или аналоги для автоматического построения графа

**Статус**: Требует дополнительного анализа

### 4. Бизнес-логика в CLI

**Правило**: CLI команды должны быть только обертками над pipeline, без бизнес-логики.

**Проверка**: `src/bioetl/cli/commands/*.py`

**Вывод**: Все CLI команды корректны - только фабрики `build_command_config()` без бизнес-логики. **Нарушений не обнаружено**.

### 5. Несогласованность схем

**Правило**: Одинаковые сущности должны иметь согласованные определения в схемах.

**Требует**: Сравнение `schemas/activity.py` vs `schemas/chembl/activity.py` и других пар.

**Статус**: Требует дополнительного анализа (таймауты при чтении файлов)

### 6. Нестабильные сортировки

**Правило**: Все сортировки выводов должны быть детерминированными с фиксированным ключом.

**Проверка**: Найдено 14 использований `sort_values()`.

**Выявленные проблемы**:

1. `src/bioetl/sources/iuphar/pipeline.py:247`
   ```python
   best_classifications.sort_values(...)
   ```
   **Статус**: Требует проверки наличия фиксированного ключа

2. `src/bioetl/sources/chembl/target/merge/gold.py:53,58,71,74`
   - Все используют `kind="stable"` - **КОРРЕКТНО**
   
3. `src/bioetl/pipelines/base.py:1043`
   - Используется `sort_by` из конфига - **КОРРЕКТНО**, но нужно проверить наличие дефолта

4. `src/bioetl/core/output_writer.py:1283`
   ```python
   tidy_df.sort_values(["feature_x", "feature_y"])
   ```
   **Статус**: Фиксированный ключ - **КОРРЕКТНО**

5. `src/bioetl/sources/chembl/document/request/external.py:488`
   ```python
   df = df.sort_values(by="source")
   ```
   **Статус**: Требует проверки дополнительных ключей для детерминизма

**Вывод**: Большинство сортировок корректны, но некоторые требуют верификации полноты ключей.

**Severity**: Medium (требует ручной проверки)

### 7. Разнобой логгера

**Правило**: Все логирование через `UnifiedLogger.get()`, запрещен `logging.getLogger()` и `print()`.

**Проверка**:
- `src/bioetl/core/logger.py:436` - `logging.getLogger()` используется внутри `UnifiedLogger` для инициализации root logger - **КОРРЕКТНО**
- `print()` не найдено в `src/bioetl/` - **КОРРЕКТНО**

**Вывод**: Нарушений не обнаружено.

## Статистика

- **Всего нарушений**: 3
- **Blocking**: 0
- **High**: 2 (сетевые I/O вне client/)
- **Medium**: 1 (сортировки требуют проверки)
- **Low**: 1 (избыточный импорт requests)

## Приоритеты исправления

1. **Приоритет 1 (High)**: Устранение прямых `requests.get()` в `utils/chembl.py`
2. **Приоритет 2 (Medium)**: Верификация всех `sort_values()` на полноту ключей для детерминизма
3. **Приоритет 3 (Low)**: Замена избыточного импорта `requests` на точечный

## Рекомендации

1. Добавить в CI проверку на прямые вызовы `requests.get/post/put/delete` вне `core/api_client.py`
2. Добавить в CI проверку на использование `print()` вместо логгера
3. Провести полный анализ графа зависимостей с помощью автоматических инструментов
4. Создать golden-тесты для проверки детерминизма сортировок

