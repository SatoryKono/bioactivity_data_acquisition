# План миграции PubChem к структуре sources/<source>/

> **Примечание:** Структура `src/bioetl/sources/` — правильная организация для внешних источников данных. Внешние источники (crossref, pubmed, openalex, semantic_scholar, iuphar, uniprot) уже следуют модульной схеме (client/, request/, parser/, normalizer/, output/, pipeline.py; дополнительно schema/, merge/, pagination/ при необходимости). Для ChEMBL исторические файлы в `src/bioetl/pipelines/*.py` выступают совместимыми реэкспортами (например, `src/bioetl/pipelines/activity.py` перенаправляет на `src/bioetl/sources/chembl/activity/pipeline.py`, `src/bioetl/pipelines/document.py` — на `src/bioetl/sources/chembl/document/pipeline.py`). Целевая структура закреплена в `src/bioetl/sources/chembl/<entity>/pipeline.py`.

**Дата:** 2025-01-29
**Цель:** Миграция всех компонентов PubChem из `pipelines/pubchem.py` и `adapters/pubchem.py` в модульную структуру `sources/pubchem/` согласно MODULE_RULES.md, без поддержки обратной совместимости.

## Текущее состояние

### Монолитные компоненты
- `src/bioetl/pipelines/pubchem.py` (360 строк) — пайплайн с логикой extract/transform/validate
- `src/bioetl/adapters/pubchem.py` (377 строк) — адаптер с client/parser/normalizer логикой
- `src/bioetl/sources/pubchem/pipeline.py` — только прокси-файл для совместимости

### Существующие модули
- `src/bioetl/schemas/pubchem.py` — Pandera-схема (сохранить)
- `tests/sources/pubchem/` — частичные тесты (расширить)

## Целевая структура

```
src/bioetl/sources/pubchem/
├── __init__.py
├── pipeline.py                    # Основной пайплайн (координация)
├── client/
│   ├── __init__.py
│   └── pubchem_client.py          # HTTP-клиент и CID resolution
├── request/
│   ├── __init__.py
│   └── builder.py                 # Построение URL и параметров
├── parser/
│   ├── __init__.py
│   └── pubchem_parser.py          # Парсинг JSON ответов API
├── normalizer/
│   ├── __init__.py
│   └── pubchem_normalizer.py      # Нормализация к UnifiedSchema
├── schema/
│   ├── __init__.py
│   └── pubchem_schema.py          # Pandera-схема (переместить из schemas/)
└── output/
    ├── __init__.py
    └── pubchem_output.py          # Детерминированная запись (опционально)
```

## Этапы миграции

### Этап 1: Анализ и декомпозиция

#### 1.1 Инвентаризация функций

**Из `adapters/pubchem.py`:**
- `_resolve_cid_by_inchikey()` → `client/pubchem_client.py::resolve_cid()`
- `_resolve_cids_batch()` → `client/pubchem_client.py::resolve_cids_batch()`
- `_fetch_properties_batch()` → `client/pubchem_client.py::fetch_properties_batch()`
- `_rate_limit()` → удалить (использовать UnifiedAPIClient rate limiting)
- Парсинг JSON ответов → `parser/pubchem_parser.py`
- `normalize_record()` → `normalizer/pubchem_normalizer.py`
- `enrich_with_pubchem()` → `normalizer/pubchem_normalizer.py::enrich()`

**Из `pipelines/pubchem.py`:**
- `extract()` → `pipeline.py::extract()` (упростить)
- `transform()` → `pipeline.py::transform()` (декомпозировать)
- `validate()` → `pipeline.py::validate()` (использовать UnifiedOutputWriter)
- `_create_pubchem_adapter()` → `client/pubchem_client.py::from_config()`
- `_normalise_pubchem_types()` → `normalizer/pubchem_normalizer.py`
- `_ensure_pubchem_columns()` → `normalizer/pubchem_normalizer.py`
- `_resolve_lookup_source()` → `pipeline.py` (helper)

#### 1.2 Матрица зависимостей

| Компонент | Зависит от | Импорты |
|-----------|------------|---------|
| `client/` | `core/api_client`, `core/logger` | UnifiedAPIClient, UnifiedLogger |
| `request/` | `client/` (опционально) | — |
| `parser/` | `core/` | — (чистые функции) |
| `normalizer/` | `parser/`, `schema/` | PubChemParser, PubChemSchema |
| `schema/` | `core/schema_registry` | BaseSchema |
| `pipeline.py` | все модули | Все |

### Этап 2: Создание модульной структуры

#### 2.1 Client модуль (`client/pubchem_client.py`)

**Ответственность:**
- HTTP-запросы к PubChem PUG-REST API
- CID resolution через InChIKey lookup
- Batch fetching properties
- Интеграция с UnifiedAPIClient

**Методы:**
```python
class PubChemClient:
    def __init__(self, api_client: UnifiedAPIClient, batch_size: int = 100)
    
    def resolve_cid(self, inchikey: str) -> int | None
    def resolve_cids_batch(self, inchikeys: list[str]) -> dict[str, int | None]
    def fetch_properties_batch(self, cids: list[int]) -> list[dict[str, Any]]
    def enrich_batch(self, inchikeys: list[str]) -> list[dict[str, Any]]
    
    @classmethod
    def from_config(cls, config: PipelineConfig) -> PubChemClient | None
```

**Миграция:**
- Извлечь `_resolve_cid_by_inchikey()`, `_resolve_cids_batch()`, `_fetch_properties_batch()` из адаптера
- Заменить `_rate_limit()` на UnifiedAPIClient rate limiting
- Использовать `UnifiedAPIClient.request_json()` для запросов
- Интегрировать с `APIClientFactory.from_pipeline_config()`

#### 2.2 Request модуль (`request/builder.py`)

**Ответственность:**
- Построение URL для PubChem endpoints
- Формирование параметров запросов

**Методы:**
```python
class PubChemRequestBuilder:
    @staticmethod
    def build_cid_lookup_url(inchikey: str) -> str
    @staticmethod
    def build_properties_url(cids: list[int], properties: list[str]) -> str
    @staticmethod
    def get_default_properties() -> list[str]
```

**Миграция:**
- Извлечь URL-строительство из `client/` методов

#### 2.3 Parser модуль (`parser/pubchem_parser.py`)

**Ответственность:**
- Парсинг JSON ответов PubChem API
- Извлечение структур данных (CID lists, PropertyTable)
- Чистые функции без IO

**Методы:**
```python
class PubChemParser:
    @staticmethod
    def parse_cid_response(response: dict[str, Any]) -> int | None
    @staticmethod
    def parse_properties_response(response: dict[str, Any]) -> list[dict[str, Any]]
    @staticmethod
    def extract_cids_from_identifier_list(data: dict[str, Any]) -> list[int]
    @staticmethod
    def extract_properties_from_table(data: dict[str, Any]) -> list[dict[str, Any]]
```

**Миграция:**
- Извлечь логику парсинга из `_resolve_cid_by_inchikey()` и `_fetch_properties_batch()`
- Убрать сетевые вызовы, оставить только трансформации

#### 2.4 Normalizer модуль (`normalizer/pubchem_normalizer.py`)

**Ответственность:**
- Нормализация PubChem записей к UnifiedSchema
- Преобразование типов данных
- Обеспечение полноты колонок
- Обогащение DataFrame

**Методы:**
```python
class PubChemNormalizer:
    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]
    def normalize_types(self, df: pd.DataFrame) -> pd.DataFrame
    def ensure_columns(self, df: pd.DataFrame) -> pd.DataFrame
    def enrich_dataframe(
        self,
        df: pd.DataFrame,
        inchi_key_col: str = "standard_inchi_key",
        client: PubChemClient
    ) -> pd.DataFrame
```

**Миграция:**
- Извлечь `normalize_record()` из адаптера
- Извлечь `_normalise_pubchem_types()`, `_ensure_pubchem_columns()` из пайплайна
- Извлечь `enrich_with_pubchem()` логику из адаптера
- Использовать UnifiedSchema нормализаторы где возможно

#### 2.5 Schema модуль (`schema/pubchem_schema.py`)

**Ответственность:**
- Pandera-схема для PubChem данных
- Валидация выходных данных

**Миграция:**
- Переместить `src/bioetl/schemas/pubchem.py` → `sources/pubchem/schema/pubchem_schema.py`
- Обновить импорты в `__init__.py`
- Сохранить `_column_order` и все поля

#### 2.6 Pipeline модуль (`pipeline.py`)

**Ответственность:**
- Координация extract/transform/validate
- Интеграция всех модулей
- Использование UnifiedOutputWriter

**Структура:**
```python
class PubChemPipeline(PipelineBase):
    def __init__(self, config: PipelineConfig, run_id: str)
    def extract(self, input_file: Path | None = None) -> pd.DataFrame
    def transform(self, df: pd.DataFrame) -> pd.DataFrame
    def validate(self, df: pd.DataFrame) -> pd.DataFrame
    def close_resources(self) -> None
```

**Миграция:**
- Упростить `extract()` — только чтение входной таблицы
- Декомпозировать `transform()`:
  - Создать PubChemClient через `PubChemClient.from_config()`
  - Использовать PubChemNormalizer для обогащения
  - Применить UnifiedOutputWriter для финализации
- Упростить `validate()` — использовать UnifiedOutputWriter валидацию
- Удалить `_create_pubchem_adapter()` — заменить на `PubChemClient.from_config()`
- Удалить helper методы — переместить в нормализатор

### Этап 3: Обновление тестов

#### 3.1 Структура тестов

```
tests/sources/pubchem/
├── __init__.py
├── test_client.py                 # Расширить существующие тесты
├── test_parser.py                 # Новый модуль
├── test_normalizer.py             # Расширить существующие тесты
├── test_schema.py                 # Новый модуль (из schemas/)
└── test_pipeline_e2e.py           # Новый модуль
```

#### 3.2 Тестовые случаи

**test_client.py:**
- `test_resolve_cid()` — единичный InChIKey lookup
- `test_resolve_cids_batch()` — батч lookup
- `test_fetch_properties_batch()` — батч properties
- `test_rate_limiting()` — проверка UnifiedAPIClient integration
- `test_from_config()` — создание из конфига

**test_parser.py:**
- `test_parse_cid_response()` — парсинг IdentifierList
- `test_parse_properties_response()` — парсинг PropertyTable
- `test_extract_cids_empty()` — edge cases
- `test_extract_properties_empty()` — edge cases

**test_normalizer.py:**
- `test_normalize_record()` — единичная запись
- `test_normalize_types()` — типы DataFrame
- `test_ensure_columns()` — полнота колонок
- `test_enrich_dataframe()` — обогащение DataFrame
- `test_enrich_empty()` — edge cases

**test_schema.py:**
- Переместить из `tests/schemas/test_pubchem.py`
- Валидация всех полей
- Проверка `_column_order`

**test_pipeline_e2e.py:**
- Полный цикл extract → transform → validate
- Проверка интеграции всех модулей
- Golden тесты для детерминизма

### Этап 4: Обновление зависимостей

#### 4.1 Удаление монолитных импортов

**Файлы для обновления:**
- `src/bioetl/pipelines/__init__.py` — удалить PubChemPipeline
- `src/bioetl/adapters/__init__.py` — удалить PubChemAdapter
- `src/scripts/run_*.py` — обновить импорты на `sources/pubchem/pipeline`
- `tests/unit/test_pubchem_pipeline.py` — обновить импорты
- `tests/unit/test_pipelines.py` — удалить PubChem тесты

#### 4.2 Обновление конфигурации

**Конфиг:**
- `src/bioetl/configs/pipelines/pubchem.yaml` — проверить совместимость
- Использовать `APIClientFactory` для создания клиентов

#### 4.3 Обновление документации

**Файлы:**
- `docs/requirements/sources/pubchem/README.md` — создать/обновить
- Описать структуру модулей
- Примеры использования
- Контракты API

### Этап 5: Удаление старого кода

#### 5.1 Удаление файлов

- `src/bioetl/pipelines/pubchem.py` — удалить после миграции
- `src/bioetl/adapters/pubchem.py` — удалить после миграции
- `src/bioetl/sources/pubchem/pipeline.py` (старый прокси) — заменить новым

#### 5.2 Обновление экспортов

- `src/bioetl/sources/pubchem/__init__.py` — экспортировать PubChemPipeline
- Удалить из `pipelines/__init__.py` и `adapters/__init__.py`

## Детали реализации

### Client: CID Resolution Strategy

**Текущая логика:**
- InChIKey → `/compound/inchikey/{inchikey}/cids/JSON`
- Response: `{"IdentifierList": {"CID": [1234567]}}`

**Новая реализация:**
```python
class PubChemClient:
    def resolve_cid(self, inchikey: str) -> int | None:
        url = PubChemRequestBuilder.build_cid_lookup_url(inchikey)
        response = self.api_client.request_json(url)
        return PubChemParser.parse_cid_response(response)
```

### Parser: JSON Response Parsing

**Текущая логика:**
- Встроена в client методы
- Прямое извлечение из response dict

**Новая реализация:**
```python
class PubChemParser:
    @staticmethod
    def parse_cid_response(response: dict[str, Any]) -> int | None:
        if "IdentifierList" not in response:
            return None
        identifier_list = response["IdentifierList"]
        if "CID" not in identifier_list:
            return None
        cids = identifier_list["CID"]
        return int(cids[0]) if cids else None
```

### Normalizer: Record Normalization

**Текущая логика:**
- `normalize_record()` в адаптере
- Прямое преобразование полей

**Новая реализация:**
```python
class PubChemNormalizer:
    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = dict.fromkeys(self._PUBCHEM_COLUMNS)
        # Использовать UnifiedSchema нормализаторы
        cid = record.get("CID")
        if cid is not None:
            normalized["pubchem_cid"] = NORMALIZER_ID.normalize_int(cid)
        # ... остальные поля
        return normalized
```

### Pipeline: Integration

**Текущая логика:**
- Монолитный `transform()` с встроенным адаптером

**Новая реализация:**
```python
class PubChemPipeline(PipelineBase):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Создать клиент
        client = PubChemClient.from_config(self.config)
        if client is None:
            return self._handle_disabled_enrichment(df)
        
        # Обогащение через нормализатор
        normalizer = PubChemNormalizer()
        enriched = normalizer.enrich_dataframe(
            df,
            inchi_key_col="standard_inchi_key",
            client=client
        )
        
        # Нормализация типов
        enriched = normalizer.normalize_types(enriched)
        enriched = normalizer.ensure_columns(enriched)
        
        # Финализация через UnifiedOutputWriter
        return finalize_output_dataset(
            enriched,
            business_key="molecule_chembl_id",
            schema=PubChemSchema,
            ...
        )
```

## Критерии приемки

1. ✅ Все модули созданы согласно MODULE_RULES.md
2. ✅ Матрица зависимостей соблюдена
3. ✅ Тесты покрывают все модули
4. ✅ E2E тесты проходят
5. ✅ Старый код удален
6. ✅ Импорты обновлены
7. ✅ Конфигурация совместима
8. ✅ Документация обновлена
9. ✅ CI/CD проходит (ruff, black, mypy, pytest)
10. ✅ Детерминизм сохранен (golden тесты)

## Риски и митигация

### Риск 1: Нарушение детерминизма
- **Митигация:** Golden тесты до/после миграции

### Риск 2: Регрессия функциональности
- **Митигация:** E2E тесты с реальными данными

### Риск 3: Производительность
- **Митигация:** Benchmark тесты для критичных путей

### Риск 4: Зависимости между модулями
- **Митигация:** Строгая проверка матрицы импортов

## Порядок выполнения

1. **Подготовка** (1 день)
   - Создать структуру директорий
   - Написать заглушки модулей

2. **Миграция Client** (1 день)
   - Извлечь и рефакторить client логику
   - Тесты

3. **Миграция Parser** (0.5 дня)
   - Извлечь парсинг
   - Тесты

4. **Миграция Normalizer** (1 день)
   - Извлечь нормализацию
   - Интеграция с UnifiedSchema
   - Тесты

5. **Миграция Schema** (0.5 дня)
   - Переместить схему
   - Обновить импорты

6. **Миграция Pipeline** (1 день)
   - Рефакторинг pipeline.py
   - Интеграция модулей
   - E2E тесты

7. **Очистка** (1 день)
   - Удаление старого кода
   - Обновление импортов
   - Обновление документации

**Итого:** ~6 дней

## Следующие шаги

1. Создать ветку `migrate/pubchem-modular`
2. Начать с Этапа 1 (анализ)
3. Поэтапная реализация с проверками на каждом этапе
4. Code review перед merge
5. Обновление CHANGELOG.md
