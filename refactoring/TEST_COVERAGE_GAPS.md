# Пробелы в покрытии тестами

**Дата**: 2025-01-29
**Ветка**: test_refactoring_32
**Аннотация**: Выявлены области, где требуются дополнительные тесты для обеспечения качества и предотвращения регрессий при рефакторинге.

## Golden-тесты

### Где нужны golden-тесты

1. **Критичные pipeline outputs** (для проверки детерминизма):
   - `activity.csv` - [ref: repo:src/bioetl/pipelines/chembl_activity.py@test_refactoring_32]
   - `assay.csv` - [ref: repo:src/bioetl/pipelines/chembl_assay.py@test_refactoring_32]
   - `target.csv` - [ref: repo:src/bioetl/pipelines/target_gold.py@test_refactoring_32]
   - `testitem.csv` - [ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32]
   - `document.csv` - [ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32]

   **Требования**:
   - Бит-в-бит идентичность outputs при одинаковых входных данных
   - Проверка стабильности сортировки и хешей
   - Проверка после каждого рефакторинга

2. **Схемы данных** (для проверки контрактов):
   - Все Pandera схемы должны иметь golden-тесты на валидные и невалидные данные
   - Файлы: `src/bioetl/schemas/*.py`

3. **Метаданные outputs** (`meta.yaml`):
   - Проверка корректности генерации метаданных
   - Файлы: `src/bioetl/core/output_writer.py` - класс `OutputMetadata`

### Рекомендации

- Создать `tests/golden/pipelines/` с эталонными outputs
- Использовать `pytest-golden` или аналоги
- Проверять хеши (BLAKE2) для быстрой верификации детерминизма

## Property-based тесты

### Где нужны property-based тесты

1. **Нормализация данных**:
   - `src/bioetl/normalizers/*.py` - проверить индемпотентность, детерминизм
   - Пример: `normalize_doi()` должна быть индемпотентной: `normalize(normalize(x)) == normalize(x)`

2. **Сортировка и дедупликация**:
   - Проверить, что сортировка детерминирована для всех входных данных
   - Проверить, что дедупликация корректна (нет false positives/negatives)

3. **Хеширование**:
   - `src/bioetl/core/hashing.py` - проверить коллизии, детерминизм
   - Проверить `generate_hash_row()` и `generate_hash_business_key()`

4. **Парсинг JSON**:
   - Парсеры API responses должны быть устойчивы к вариациям формата

### Рекомендации

- Использовать `hypothesis` для генерации тестовых данных
- Примеры свойств:
  - Индемпотентность нормализаторов
  - Детерминизм сортировок
  - Отсутствие коллизий в хешах (для бизнес-ключей)

## Контрактные тесты

### Где нужны контрактные тесты

1. **HTTP-клиенты**:
   - `src/bioetl/core/api_client.py` - UnifiedAPIClient
   - Проверить соответствие контракту: rate limiting, retry, circuit breaker
   - Проверить обработку всех типов ошибок (429, 5xx, timeout)

2. **Pipeline контракты**:
   - Все pipeline должны следовать контракту: `extract → normalize → validate → write`
   - Проверить, что каждый pipeline реализует все обязательные методы
   - Файлы: `src/bioetl/pipelines/*.py`

3. **Adapter контракты**:
   - Все адаптеры должны соответствовать `Adapter` protocol
   - Файлы: `src/bioetl/adapters/*.py`

4. **Schema контракты**:
   - Все схемы должны иметь обязательные поля и валидацию
   - Проверить совместимость версий схем

### Рекомендации

- Использовать `pytest-contract` или аналоги
- Документировать контракты в docstrings или Protocol classes
- Проверять нарушение контрактов в CI

## Интеграционные тесты

### Где нужны интеграционные тесты

1. **fetch_chembl_release** (после батча 1):
   - [ref: repo:src/bioetl/utils/chembl.py@test_refactoring_32]
   - Тест с реальным `UnifiedAPIClient` и моком API
   - Проверить все пути выполнения (строка vs клиент)

2. **CLI команды** (после батча 2):
   - `tests/e2e/test_cli_crossref.py`
   - `tests/e2e/test_cli_openalex.py`
   - `tests/e2e/test_cli_pubmed.py`
   - `tests/e2e/test_cli_semantic_scholar.py`
   - Проверить все параметры командной строки

3. **External source pipelines**:
   - Полный E2E тест для каждого external source
   - Проверить работу с реальными данными (или моками API)

### Рекомендации

- Использовать `pytest-httpserver` для моков API
- Изолировать тесты от внешних API (без сети)
- Использовать fixtures для подготовки данных

## Unit тесты

### Где нужны дополнительные unit тесты

1. **utils/chembl.py** (после батча 1):
   - Тест `_request_status()` с моком `UnifiedAPIClient`
   - Тест `fetch_chembl_release()` с обоими типами входов
   - Тест обработки ошибок

2. **CLI commands** (после батча 2):
   - Тест `build_external_source_command_config()` для каждого source
   - Тест регистрации команд
   - Тест валидации параметров

3. **Сортировки** (после батча 4):
   - Тесты на детерминизм каждого `sort_values()`
   - Проверить полноту ключей для уникальности

4. **Импорт-циклы** (батч 5):
   - Статический анализ импортов
   - Тесты на отсутствие циклов

## Рекомендации по приоритетам

1. **Приоритет 1 (High)**: Golden-тесты для критичных pipeline outputs
2. **Приоритет 2 (Medium)**: Интеграционные тесты для fetch_chembl_release и CLI команд
3. **Приоритет 3 (Medium)**: Property-based тесты для нормализаторов и хеширования
4. **Приоритет 4 (Low)**: Контрактные тесты для всех Protocol/интерфейсов
5. **Приоритет 5 (Low)**: Unit тесты для утилит и helper функций

## Инструменты

- **Golden-тесты**: `pytest-golden` или кастомные fixtures
- **Property-based**: `hypothesis` (уже в dev dependencies)
- **Контрактные**: `pytest-contract` или custom проверки через Protocol
- **Интеграционные**: `pytest-httpserver` (уже в dev dependencies)
- **Статический анализ**: `pydeps`, `import-linter`, `mypy`

## Метрики покрытия

**Текущие требования** (из `pyproject.toml`):
- `--cov-fail-under=85`

**Рекомендации для критичных модулей**:
- `core/api_client.py`: ≥90%
- `core/output_writer.py`: ≥90%
- `utils/chembl.py`: ≥90% (после рефакторинга)
- `cli/commands/*.py`: ≥85%
