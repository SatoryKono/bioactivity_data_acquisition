# План миграции PubChem к структуре sources/<source>/

**Дата обновления:** 2025-03-??
**Цель:** Зафиксировать факт завершённой миграции компонентов PubChem из `pipelines/pubchem.py` и `adapters/pubchem.py` в модульную структуру `sources/pubchem/` согласно MODULE_RULES.md, а также определить оставшиеся пост-миграционные задачи (QC, валидация, smoke-тесты).

## Текущее состояние

### Модульный пайплайн (факт)
- `src/bioetl/sources/pubchem/pipeline.py` наследуется от `PipelineBase` и инкапсулирует стадии extract/transform/load.
- `src/bioetl/sources/pubchem/client/pubchem_client.py` реализует HTTP-доступ через `UnifiedAPIClient`.
- `src/bioetl/sources/pubchem/parser/pubchem_parser.py` и `normalizer/pubchem_normalizer.py` отвечают за разбор и нормализацию данных.
- `src/bioetl/sources/pubchem/output/pubchem_output.py` обеспечивает детерминированную запись и отчёты QC.
- `src/bioetl/sources/pubchem/schema/pubchem_schema.py` содержит Pandera-схемы, используемые валидацией.
- Тестовый каркас перенесён в `tests/sources/pubchem/` и использует новую модульную архитектуру.

### Наследие (исторически)
- `src/bioetl/pipelines/pubchem.py` и `src/bioetl/adapters/pubchem.py` удалены из актуального дерева.
- Документация и вспомогательные скрипты всё ещё ссылаются на монолит и нуждаются в корректировке.

## Целевая структура (достигнута)

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
│   └── pubchem_schema.py          # Pandera-схема (перенесено из schemas/)
└── output/
    ├── __init__.py
    └── pubchem_output.py          # Детерминированная запись и QC
```

## Этапы миграции (архив)

> Секция сохранена как исторический план миграции 2025-01. Все перечисленные задачи выполнены; используйте как справку при анализе решенных решений.

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

## Компоненты модульного пайплайна

#### Client (`client/pubchem_client.py`)

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

**Статус:**
- Реализован, использует `UnifiedAPIClient` и backoff-ретраи.
- Поддерживает построение батчей CID/свойств и отдаёт JSON-ответы парсеру.

**Пост-миграционные действия:**
- Добавить smoke-тесты `PubChemPipeline` с моками клиента, чтобы зафиксировать основные HTTP последовательности.
- Задокументировать лимиты PubChem API (строка/мин) в `docs/requirements/sources/pubchem/README.md`.

#### Request (`request/builder.py`)

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

**Статус:**
- Вынесен в отдельный модуль; методы используются клиентом и тестируются юнит-тестами.

**Пост-миграционные действия:**
- Дополнить покрытие негативными кейсами (невалидные идентификаторы) в `tests/sources/pubchem/test_client.py`.

#### Parser (`parser/pubchem_parser.py`)

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

**Статус:**
- Парсинг вынесен в чистые функции; покрыт тестами `tests/sources/pubchem/test_parser.py`.

**Пост-миграционные действия:**
- Добавить проверку на пустые `PropertyTable` с логированием предупреждения в pipeline (см. QC задачи).

#### Normalizer (`normalizer/pubchem_normalizer.py`)

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

**Статус:**
- Нормализация и детерминизм перенесены; класс предоставляет `ensure_columns()` и `normalize_types()`.

**Пост-миграционные действия:**
- Расширить QC-показатели (расхождение типов, доля null-значений) и выгружать их через `pubchem_output`.
- Проверить Pandera-валидацию enrich-результатов на наборах >10k записей (нагрузочное тестирование).

#### Schema (`schema/pubchem_schema.py`)

**Ответственность:**
- Pandera-схема для PubChem данных
- Валидация выходных данных

**Статус:**
- Схема перенесена, зарегистрирована в `schema_registry` и используется pipeline.

**Пост-миграционные действия:**
- Уточнить ограничения (nullable/unique) в соответствии с фактическими данными 2025 года.
- Добавить экспорт схемы в QC-отчёт (версия, дата обновления).

#### Pipeline (`pipeline.py`)

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

**Статус:**
- Реализован на базе `PipelineBase`; интегрирует QC-метрики и детерминированную выгрузку.

**Пост-миграционные действия:**
- Настроить smoke-тест `tests/sources/pubchem/test_pipeline_smoke.py` с использованием фикстуры моков HTTP.
- Расширить отчёт QC метриками покрытия InChIKey и процента успешного обогащения (порог из конфигурации).

## Тестирование и контроль качества

### Текущее покрытие
- `tests/sources/pubchem/test_client.py` — мокирует HTTP-вызовы и проверяет CID/property батчи.
- `tests/sources/pubchem/test_parser.py` — покрывает позитивные и негативные сценарии парсинга JSON.
- `tests/sources/pubchem/test_normalizer.py` — проверяет enrich, нормализацию типов и полноту колонок.
- `tests/sources/pubchem/test_schema.py` — гарантирует соответствие Pandera-схемы и нормализатора.

### План расширения (Q2 2025)
1. Добавить smoke-тест `tests/sources/pubchem/test_pipeline_smoke.py` с моками `PubChemClient` и фиксацией QC-метрик.
2. Подготовить e2e-тест с небольшим фиктивным входным CSV (10–20 молекул) и golden-выходом для проверки детерминизма.
3. Вынести генерацию QC-отчёта в `src/bioetl/sources/pubchem/output/pubchem_output.py` и покрыть его unit-тестом.
4. Настроить property-based тесты для `PubChemNormalizer.enrich_dataframe()` (hypothesis) с рандомизированными наборами InChIKey.

## Пост-миграционные задачи (backlog)

| Статус | Задача | Детали |
|--------|--------|--------|
| ⏳ | QC отчёт | Расширить `pubchem_output` метриками покрытия InChIKey, уровня обогащения и версией схемы. |
| ⏳ | Smoke-тесты | Создать `test_pipeline_smoke.py` + зафиксировать последовательность логов/метрик. |
| ⏳ | Валидация на объёмах | Прогнать pipeline на 100k записей, оценить производительность и ошибки Pandera. |
| ⏳ | Документация | Обновить `docs/requirements/sources/pubchem/README.md` с описанием модульной архитектуры и лимитов API. |
| ⏳ | Конфигурация | Синхронизировать `configs/pipelines/pubchem.yaml` с новыми порогами QC (`pubchem.min_*`). |

## Критерии приемки пост-миграции

1. ✅ Модульный пайплайн зарегистрирован в `schema_registry` и используется pipeline.
2. ✅ Наследные файлы `pipelines/pubchem.py` и `adapters/pubchem.py` удалены.
3. ⏳ Smoke-тест и golden-тест фиксируют детерминизм и критичные метрики.
4. ⏳ QC-отчёт содержит coverage/enrichment и версию схемы.
5. ⏳ Документация и конфигурация синхронизированы с новой архитектурой.
6. ✅ CI (ruff, mypy, pytest) проходит на модульной реализации.

## Риски и митигация

- **Недостаточный QC (⏳):** пока не все метрики выводятся. *Митигация:* добавить контроль порогов в smoke-тест и отчёт.
- **Регрессии на больших объёмах (⏳):** нет нагрузочных проверок. *Митигация:* выполнить прогон на 100k записей и задокументировать результаты в `reports/pubchem_load_test.md`.
- **Документационные расхождения (⏳):** часть руководств ссылается на монолит. *Митигация:* обновить `docs/requirements/sources/pubchem/README.md` и ссылаться на модульный пайплайн.

## Рекомендованный порядок работ

1. **QC и отчёты** — расширить `pubchem_output` + обновить конфигурацию порогов.
2. **Smoke / e2e тесты** — добавить тесты и golden-наборы; интегрировать в CI.
3. **Документация** — обновить требования и developer guide.
4. **Нагрузочное тестирование** — прогон на 100k записей, собрать отчёт.

## Следующие шаги

1. Создать задачу "PubChem QC & Smoke Tests" (интегрирует пункты backlog).
2. Подготовить фиктивный входной CSV и golden-выход для smoke/e2e тестов.
3. Обновить документацию и конфиги после внедрения тестов и расширенного QC.

