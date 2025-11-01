# Промпт: Устранить противоречия в структуре тестов (P0-3) — Вариант А

## Контекст задачи

**Проблема:** Документы требуют `tests/sources/<source>/` для всех источников, но фактическая структура:

- `tests/sources/` существует для адаптеров (crossref, pubmed, openalex, semantic_scholar, document, iuphar, uniprot, chembl, pubchem)
- `tests/integration/pipelines/` для E2E тестов пайплайнов
- `tests/unit/` для unit-тестов компонентов

**Статус:** ❌ **НЕ ИСПРАВЛЕНО** (P0-3)

**Ссылки:**

- `refactoring/AUDIT_PLAN_REPORT_2025.md` (строка 115): "Противоречие в структуре тестов"
- `refactoring/MODULE_RULES.md` (строка 39): Требует `tests/sources/<source>/` с `test_client.py`, `test_parser.py`, `test_normalizer.py`, `test_schema.py`, `test_pipeline_e2e.py`
- `refactoring/PIPELINES.md` (строка 191): Ссылается на `tests/sources/<source>/`
- `refactoring/REFACTOR_PLAN.md` (строка 714): Ссылается на `tests/sources/crossref/*`

## Текущая ситуация

### Существующая структура `tests/sources/`

**Источники с полным набором тестов:**

- `tests/sources/crossref/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py, test_pagination.py, test_merge.py
- `tests/sources/pubmed/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py, test_pagination.py, test_merge.py
- `tests/sources/openalex/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py, test_pagination.py, test_merge.py
- `tests/sources/semantic_scholar/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py
- `tests/sources/document/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py
- `tests/sources/iuphar/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py
- `tests/sources/uniprot/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py, conftest.py
- `tests/sources/chembl/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py, test_pipeline_e2e.py
- `tests/sources/pubchem/` — test_client.py, test_parser.py, test_normalizer.py, test_schema.py (⚠️ отсутствует test_pipeline_e2e.py)

**Источники, требующие проверки:**

- Проверить наличие всех 5 обязательных тестов для каждого источника
- Проверить покрытие тестами всех модулей из `src/bioetl/sources/<source>/`

### Другие структуры тестов

**`tests/integration/pipelines/`:**

- `test_activity_pipeline.py` — E2E тесты Activity пайплайна
- `test_bit_identical_output.py` — проверка бит-идентичности
- `test_enrichment_stages.py` — тесты обогащения
- `test_extended_mode_outputs.py` — тесты расширенного режима
- `test_document_pipeline_enrichment.py` — E2E тесты Document пайплайна
- `test_document_pipeline_merge_policy.py` — тесты политики слияния

**`tests/unit/`:**

- Unit-тесты для компонентов core/
- Unit-тесты для отдельных модулей

## Вариант А: Привести структуру в соответствие с MODULE_RULES.md (РЕКОМЕНДУЕТСЯ)

**Приоритет:** Высокий (соответствует MODULE_RULES.md)

**Преимущества:**

- Полностью соответствует `MODULE_RULES.md` (правильная организация)
- Унифицирует структуру тестов для всех источников
- Упрощает навигацию и поддержку тестов
- Следует принципу "одно место для одного типа тестов"

**Недостатки:**

- Требует реорганизации существующих тестов
- Требует проверки всех источников на полноту тестов
- Требует обновления документации

## Процесс реорганизации

### Этап 1: Инвентаризация текущих тестов (1-2 часа)

**Задачи:**

1. Проверить наличие всех обязательных тестов для каждого источника:
   - `test_client.py` — тесты клиента API
   - `test_parser.py` — тесты парсера
   - `test_normalizer.py` — тесты нормализатора
   - `test_schema.py` — тесты Pandera схем
   - `test_pipeline_e2e.py` — E2E тесты пайплайна
2. Проверить опциональные тесты (если применимо):
   - `test_pagination.py` — тесты пагинации (crossref, pubmed, openalex)
   - `test_merge.py` — тесты политики слияния (crossref, pubmed, openalex, document)
   - `test_request.py` — тесты построителя запросов (если есть)
3. Составить список источников с недостающими тестами

**Ожидаемый результат:**

- Таблица соответствия: источник → наличие тестов → статус
- Список недостающих тестов для каждого источника

### Этап 2: Создание недостающих тестов (4-8 часов)

**Задачи:**

1. Для источников без `test_pipeline_e2e.py`:
   - Создать E2E тесты пайплайна
   - Перенести соответствующие тесты из `tests/integration/pipelines/`, если применимо
   - Убедиться, что тесты покрывают все этапы пайплайна (extract, transform, validate, export)
2. Для источников с неполным набором тестов:
   - Создать недостающие тесты по образцу существующих
   - Использовать фикстуры из `tests/sources/_mixins.py` и `tests/fixtures/`
3. Для источников с дополнительными модулями:
   - Создать тесты для опциональных модулей (pagination, merge, request), если они используются

**Ожидаемый результат:**

- Все источники имеют полный набор обязательных тестов
- Все тесты следуют единому стилю и структуре

### Этап 3: Реорганизация тестов из tests/integration/pipelines/ (2-4 часа)

**Задачи:**

1. Анализ тестов в `tests/integration/pipelines/`:
   - Определить, какие тесты относятся к конкретным источникам
   - Определить, какие тесты являются общими (core компоненты, интеграция нескольких источников)
2. Перенос тестов источников:
   - Тесты, специфичные для источника → `tests/sources/<source>/test_pipeline_e2e.py`
   - Обновить импорты и пути к фикстурам
3. Сохранение общих тестов:
   - Тесты core компонентов → оставить в `tests/integration/` или перенести в `tests/unit/core/`
   - Тесты интеграции нескольких источников → оставить в `tests/integration/pipelines/`
   - Тесты бит-идентичности → оставить в `tests/integration/pipelines/golden/` или создать `tests/golden/`

**Ожидаемый результат:**

- Тесты источников перенесены в `tests/sources/<source>/`
- Общие тесты остаются в `tests/integration/` или `tests/unit/`
- Структура тестов соответствует `MODULE_RULES.md`

### Этап 4: Обновление документации (1-2 часа)

**Задачи:**

1. Обновить `refactoring/MODULE_RULES.md`:
   - Уточнить структуру тестов: `tests/sources/<source>/` для тестов источников
   - Указать, что `tests/integration/pipelines/` — для общих E2E тестов
   - Указать, что `tests/unit/` — для unit-тестов компонентов
2. Обновить `refactoring/PIPELINES.md`:
   - Синхронизировать описание структуры тестов
   - Указать примеры использования тестов
3. Обновить `refactoring/REFACTOR_PLAN.md`:
   - Обновить ссылки на структуру тестов
   - Указать статус реорганизации тестов (✅ завершено)

**Ожидаемый результат:**

- Документация синхронизирована с фактической структурой тестов
- Все ссылки на тесты актуальны

### Этап 5: Валидация и проверка (1-2 часа)

**Задачи:**

1. Запустить все тесты:

   ```bash
   pytest tests/sources/ -v
   pytest tests/integration/ -v
   pytest tests/unit/ -v
   ```

2. Проверить покрытие тестами:

   ```bash
   pytest --cov=src/bioetl/sources --cov-report=term-missing tests/sources/
   ```

3. Проверить соответствие структуры `MODULE_RULES.md`:
   - Все источники имеют `tests/sources/<source>/`
   - Все источники имеют обязательные тесты
   - Нет дублирования тестов между `tests/sources/` и `tests/integration/`

**Ожидаемый результат:**

- Все тесты проходят
- Покрытие тестами соответствует требованиям
- Структура тестов соответствует `MODULE_RULES.md`

## Критерии приемки

### Функциональные требования

- [ ] Все источники из `src/bioetl/sources/` имеют структуру `tests/sources/<source>/`
- [ ] Все источники имеют обязательные тесты:
  - [ ] `test_client.py`
  - [ ] `test_parser.py`
  - [ ] `test_normalizer.py`
  - [ ] `test_schema.py`
  - [ ] `test_pipeline_e2e.py`
- [ ] Все источники с опциональными модулями имеют соответствующие тесты:
  - [ ] `test_pagination.py` (для источников с пагинацией)
  - [ ] `test_merge.py` (для источников с политикой слияния)
  - [ ] `test_request.py` (для источников с построителем запросов)
- [ ] Тесты из `tests/integration/pipelines/` реорганизованы:
  - [ ] Тесты источников перенесены в `tests/sources/<source>/`
  - [ ] Общие тесты остаются в `tests/integration/`
- [ ] Все тесты проходят успешно
- [ ] Покрытие тестами соответствует требованиям проекта

### Требования к документации

- [ ] `refactoring/MODULE_RULES.md` обновлен с описанием структуры тестов
- [ ] `refactoring/PIPELINES.md` синхронизирован с фактической структурой
- [ ] `refactoring/REFACTOR_PLAN.md` обновлен со статусом реорганизации
- [ ] `refactoring/AUDIT_PLAN_REPORT_2025.md` обновлен со статусом P0-3 (✅ исправлено)

### Требования к качеству

- [ ] Все тесты следуют единому стилю и структуре
- [ ] Все тесты используют фикстуры из `tests/sources/_mixins.py` и `tests/fixtures/`
- [ ] Нет дублирования тестов между `tests/sources/` и `tests/integration/`
- [ ] Структура тестов соответствует `MODULE_RULES.md`

## Структура тестов после реорганизации

```
tests/
├── sources/                    # Тесты источников (MUST для всех источников)
│   ├── __init__.py
│   ├── _mixins.py             # Общие миксины для тестов
│   ├── test_base_adapter.py   # Тесты базового адаптера
│   ├── chembl/
│   │   ├── __init__.py
│   │   ├── test_client.py
│   │   ├── test_parser.py
│   │   ├── test_normalizer.py
│   │   ├── test_schema.py
│   │   └── test_pipeline_e2e.py
│   ├── crossref/
│   │   ├── __init__.py
│   │   ├── test_client.py
│   │   ├── test_parser.py
│   │   ├── test_normalizer.py
│   │   ├── test_schema.py
│   │   ├── test_pipeline_e2e.py
│   │   ├── test_pagination.py    # Опционально
│   │   └── test_merge.py         # Опционально
│   ├── pubmed/
│   │   └── ...                   # Аналогично crossref
│   ├── openalex/
│   │   └── ...                   # Аналогично crossref
│   ├── semantic_scholar/
│   │   └── ...                   # Базовый набор тестов
│   ├── document/
│   │   └── ...                   # Базовый набор + test_merge.py
│   ├── iuphar/
│   │   └── ...                   # Базовый набор тестов
│   ├── uniprot/
│   │   ├── conftest.py          # Опционально
│   │   └── ...                   # Базовый набор тестов
│   └── pubchem/
│       └── ...                   # Базовый набор тестов
├── integration/                 # Интеграционные тесты
│   ├── pipelines/              # E2E тесты пайплайнов (общие)
│   │   ├── golden/              # Golden-тесты
│   │   │   └── activity_column_order.json
│   │   ├── test_bit_identical_output.py
│   │   ├── test_enrichment_stages.py
│   │   └── test_extended_mode_outputs.py
│   └── qc/
│       └── test_unified_qc.py
└── unit/                        # Unit-тесты компонентов
    ├── core/                    # Тесты core компонентов
    ├── schemas/                 # Тесты схем
    └── ...                      # Другие unit-тесты
```

## Примеры миграции тестов

### Пример 1: Перенос E2E тестов Document пайплайна

**До:**

- `tests/integration/test_document_pipeline_enrichment.py` — тесты обогащения Document пайплайна
- `tests/integration/test_document_pipeline_merge_policy.py` — тесты политики слияния

**После:**

- `tests/sources/document/test_pipeline_e2e.py` — включает тесты обогащения и политики слияния
- Или разделить на:
  - `tests/sources/document/test_pipeline_e2e.py` — базовые E2E тесты
  - `tests/sources/document/test_merge.py` — тесты политики слияния (если еще нет)

### Пример 2: Добавление test_pipeline_e2e.py для PubChem

**Задача:**
Создать `tests/sources/pubchem/test_pipeline_e2e.py` по образцу существующих тестов.

**Структура теста:**

```python
import pytest
from bioetl.sources.pubchem.pipeline import PubChemPipeline

def test_pipeline_e2e_extract(mock_pubchem_api):
    """Test pipeline extract stage."""
    pipeline = PubChemPipeline(config=mock_config)
    result = pipeline.extract(smiles="CCO")
    assert result is not None
    assert len(result) > 0

def test_pipeline_e2e_transform(mock_pubchem_data):
    """Test pipeline transform stage."""
    pipeline = PubChemPipeline(config=mock_config)
    transformed = pipeline.transform(mock_pubchem_data)
    assert transformed is not None

def test_pipeline_e2e_validate(mock_pubchem_data):
    """Test pipeline validate stage."""
    pipeline = PubChemPipeline(config=mock_config)
    pipeline.validate(mock_pubchem_data)

def test_pipeline_e2e_export(mock_pubchem_data, tmp_path):
    """Test pipeline export stage."""
    pipeline = PubChemPipeline(config=mock_config)
    output_path = tmp_path / "pubchem_output.csv"
    pipeline.export(mock_pubchem_data, output_path)
    assert output_path.exists()
```

### Пример 3: Реорганизация общих тестов

**Задача:**
Определить, какие тесты из `tests/integration/pipelines/` являются общими и должны остаться там.

**Тесты, которые остаются в `tests/integration/pipelines/`:**

- `test_bit_identical_output.py` — проверка бит-идентичности (общая для всех пайплайнов)
- `test_enrichment_stages.py` — тесты этапов обогащения (общие для нескольких пайплайнов)
- `test_extended_mode_outputs.py` — тесты расширенного режима (общие для всех пайплайнов)

**Тесты, которые переносятся в `tests/sources/<source>/`:**

- `test_document_pipeline_enrichment.py` → `tests/sources/document/test_pipeline_e2e.py`
- `test_document_pipeline_merge_policy.py` → `tests/sources/document/test_merge.py` (или включить в test_pipeline_e2e.py)

## Риски и митигации

### Риск 1: Потеря тестов при реорганизации

**Митигация:**

- Составить полный список тестов перед реорганизацией
- Запускать тесты после каждого этапа миграции
- Использовать git для отслеживания изменений

### Риск 2: Конфликты импортов

**Митигация:**

- Обновлять импорты постепенно
- Использовать относительные импорты где возможно
- Проверять импорты после каждого изменения

### Риск 3: Дублирование тестов

**Митигация:**

- Четко разделить тесты источников и общие тесты
- Удалять тесты из старых мест после переноса
- Проверять отсутствие дублирования перед завершением

## Связь с другими задачами

### Связанные задачи

- **P0-1 (Дублирование ChEMBL)**: После решения дублирования ChEMBL пайплайнов, тесты также будут реорганизованы
- **P0-2 (Артефакт инвентаризации)**: Артефакт должен включать информацию о тестах для каждого источника
- **P1-2 (Property-based тесты)**: После создания структуры тестов, можно добавить property-based тесты в соответствующие модули

### Зависимости

- Требуется актуальная структура `src/bioetl/sources/` (все источники должны быть в правильной структуре)
- Требуется актуальная документация в `refactoring/MODULE_RULES.md`

## Определение готовности (Definition of Done)

### Критерии готовности

- [ ] Все источники имеют структуру `tests/sources/<source>/` с обязательными тестами
- [ ] Все тесты проходят успешно (`pytest tests/sources/ -v`)
- [ ] Покрытие тестами соответствует требованиям проекта
- [ ] Документация обновлена и синхронизирована
- [ ] Структура тестов соответствует `MODULE_RULES.md`
- [ ] Проблема P0-3 помечена как исправленная в `AUDIT_PLAN_REPORT_2025.md`

### Чек-лист перед завершением

- [ ] Инвентаризация тестов завершена
- [ ] Недостающие тесты созданы
- [ ] Тесты из `tests/integration/pipelines/` реорганизованы
- [ ] Документация обновлена
- [ ] Все тесты проходят
- [ ] Покрытие тестами проверено
- [ ] Структура тестов соответствует `MODULE_RULES.md`
- [ ] Статус проблемы P0-3 обновлен в аудите

---

**Примечание:** Этот промпт описывает вариант А — приведение структуры тестов в полное соответствие с `MODULE_RULES.md`. Если выбран вариант Б (уточнение документации), см. соответствующую задачу.
