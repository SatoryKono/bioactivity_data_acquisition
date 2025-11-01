# Публичное API BioETL

Этот файл фиксирует поддерживаемые точки интеграции и канонические пути
импортов после миграции структуры проекта. Все модули располагаются внутри
пакета `bioetl`; упоминания корневого пакета верхнего уровня и устаревшего
пространства `scripts` больше не используются.

## Точки входа CLI

### Основной CLI
- `bioetl.cli.main:app` - основное Typer-приложение (`python -m bioetl.cli.main`)
  - Команда `list` - список доступных пайплайнов
  - Команды пайплайнов регистрируются автоматически из реестра

### Скрипты запуска (в scripts/)
Тонкие обертки над `bioetl.cli.main`, сохраняют обратную совместимость:
- `scripts/run_activity.py` - запуск activity пайплайна
- `scripts/run_assay.py` - запуск assay пайплайна
- `scripts/run_document.py` - запуск document пайплайна
- `scripts/run_target.py` - запуск target пайплайна
- `scripts/run_testitem.py` - запуск testitem пайплайна
- `scripts/run_chembl_activity.py` - запуск ChEMBL activity пайплайна
- `scripts/run_chembl_assay.py` - запуск ChEMBL assay пайплайна
- `scripts/run_chembl_document.py` - запуск ChEMBL document пайплайна
- `scripts/run_chembl_target.py` - запуск ChEMBL target пайплайна
- `scripts/run_chembl_testitem.py` - запуск ChEMBL testitem пайплайна
- `scripts/run_crossref.py` - запуск crossref пайплайна
- `scripts/run_openalex.py` - запуск openalex пайплайна
- `scripts/run_pubmed.py` - запуск pubmed пайплайна
- `scripts/run_semantic_scholar.py` - запуск semantic_scholar пайплайна
- `scripts/run_pubchem.py` - запуск pubchem пайплайна
- `scripts/run_uniprot.py` - запуск uniprot пайплайна
- `scripts/run_gtp_iuphar.py` - запуск iuphar пайплайна

### CLI утилиты
- `bioetl.cli.app`
  - `create_pipeline_app`: сборка Typer-приложения для одного пайплайна
  - `register_pipeline_command`: регистрация отдельных команд в общем CLI
  - `PIPELINE_COMMAND_REGISTRY` и `PIPELINE_REGISTRY`: отображения имён CLI в
    конфигурации и классы пайплайнов соответственно
- `bioetl.cli.command`
  - `PipelineCommandConfig`, `create_pipeline_command`: сборка исполняемых
    функций CLI из конфигураций
- `bioetl.cli.registry`
  - Реестр CLI команд и их конфигураций

## Конфигурация

### Основной API (после миграции)
- `bioetl.config.loader`
  - `load_config`, `parse_cli_overrides`: загрузка и применение CLI override
  - `deep_merge`: глубокое объединение словарей
- `bioetl.config.models`
  - Pydantic-модели конфигураций пайплайнов, включая `PipelineConfig` и
    `DeterminismConfig`
- `bioetl.config.paths`
  - `get_config_path`, `get_configs_root`, `resolve_config_path`: утилиты путей
- `bioetl.config`
  - Реэкспорт: `PipelineConfig`, `load_config`, `parse_cli_overrides`

### Устаревшие импорты (deprecated, удаление через 2 MINOR)
- `bioetl.configs` → использовать `bioetl.config` вместо
- `bioetl.configs.models` → использовать `bioetl.config.models` вместо

## Пайплайны и оркестрация

- `bioetl.pipelines.registry`
  - `PIPELINE_REGISTRY`, `get_pipeline`, `iter_pipelines`: канонический реестр
    пайплайнов
- `bioetl.pipelines.base`
  - `PipelineBase`: базовый класс для всех пайплайнов
- `bioetl.pipelines.*`
  - Конкретные реализации (`chembl_activity`, `chembl_assay`, `chembl_target`,
    `document`, `testitem`), наследующие `PipelineBase`
- `bioetl.pipelines.external_source`
  - `ExternalSourcePipeline`: базовый класс для внешних источников
- `bioetl.pipelines.chembl.*`
  - ChEMBL-специфичные пайплайны (`chembl_activity`, `chembl_assay`,
    `chembl_document`, `chembl_target`, `chembl_testitem`)

## Core компоненты

### Логирование
- `bioetl.core.logger`
  - `UnifiedLogger`: структурированное логирование через structlog

### HTTP клиент
- `bioetl.core.api_client`
  - `UnifiedAPIClient`, `APIConfig`: унифицированный HTTP клиент с backoff и retry
  - `CircuitBreakerOpenError`: ошибка circuit breaker

### Output writer
- `bioetl.core.output_writer`
  - `UnifiedOutputWriter`: унифицированный writer с атомарной записью
  - `OutputArtifacts`, `OutputMetadata`: метаданные артефактов
- `bioetl.io.atomic_write`
  - `atomic_write`: атомарная запись файлов через `os.replace`

### Реестр схем
- `bioetl.core.unified_schema`
  - `SchemaRegistry`, `SchemaRegistration`: реестр Pandera схем
  - `get_schema`, `get_schema_metadata`, `register_schema`: функции работы со схемами
  - `get_registry`: получение реестра схем

### Устаревшие импорты (deprecated, удаление через 2 MINOR)
- `bioetl.schemas.registry` → использовать `bioetl.core.unified_schema` вместо

## Схемы валидации

- `bioetl.schemas.*`
  - Pandera-схемы входных и выходных данных:
    - `bioetl.schemas.activity`: `ActivitySchema`
    - `bioetl.schemas.assay`: `AssaySchema`
    - `bioetl.schemas.document`: `DocumentSchema`
    - `bioetl.schemas.target`: `TargetSchema`
    - `bioetl.schemas.testitem`: `TestItemSchema`
    - `bioetl.schemas.uniprot`: `UniProtSchema`
    - `bioetl.schemas.input_schemas`: входные схемы для пайплайнов
- `bioetl.schemas.base`
  - `BaseSchema`: базовый класс схем с поддержкой `column_order`
- `bioetl.schemas.chembl.*`
  - ChEMBL-специфичные схемы

## Клиенты и адаптеры источников данных

### HTTP клиенты
- `bioetl.clients.*`
  - `bioetl.clients.chembl_activity`: ChEMBL Activity API клиент
  - `bioetl.clients.chembl_assay`: ChEMBL Assay API клиент

### Адаптеры
- `bioetl.adapters.base`
  - `ExternalAdapter`, `AdapterConfig`, `AdapterFetchError`: базовый класс адаптера
- `bioetl.adapters.*`
  - `bioetl.adapters.crossref`: CrossRef адаптер
  - `bioetl.adapters.openalex`: OpenAlex адаптер
  - `bioetl.adapters.pubmed`: PubMed адаптер
  - `bioetl.adapters.semantic_scholar`: Semantic Scholar адаптер
  - `bioetl.adapters.chembl_activity`: ChEMBL Activity адаптер (после миграции)
  - `bioetl.adapters.chembl_assay`: ChEMBL Assay адаптер (после миграции)

### Устаревшие импорты (deprecated, удаление через 2 MINOR)
- `bioetl.transform.adapters.*` → использовать `bioetl.adapters.*` вместо

### Источники данных
- `bioetl.sources.*`
  - HTTP-клиенты с поддержкой backoff и пайплайны для ChEMBL, PubChem, UniProt,
    IUPHAR и других сервисов

## Утилиты

- `bioetl.utils.dataframe`
  - Утилиты для нормализации DataFrame и генерации детерминированных хешей
- `bioetl.utils.output`
  - Запись CSV-артефактов и отчётов качества
- `bioetl.utils.io`
  - `load_input_frame`, `resolve_input_path`: утилиты чтения входных данных
  - После миграции: реэкспорт из `bioetl.io.readers`
- `bioetl.utils.validation`
  - `summarize_schema_errors`: агрегация ошибок валидации схем
- `bioetl.utils.qc`
  - Утилиты для quality control отчётов
- `bioetl.utils.chembl`
  - ChEMBL-специфичные утилиты

## Нормализаторы

- `bioetl.normalizers.registry`
  - Реестр нормализаторов
- `bioetl.normalizers.*`
  - Специализированные нормализаторы:
    - `bioetl.normalizers.bibliography`: библиографические нормализаторы
    - `bioetl.normalizers.chemistry`: химические нормализаторы
    - `bioetl.normalizers.date`: нормализация дат
    - `bioetl.normalizers.identifier`: нормализация идентификаторов
    - `bioetl.normalizers.numeric`: нормализация чисел
    - `bioetl.normalizers.string`: нормализация строк

## Hashing и детерминизм

- `bioetl.core.hashing`
  - `generate_hash_business_key`: генерация BLAKE2 хеша бизнес-ключа

## Materialization

- `bioetl.core.materialization`
  - `MaterializationManager`: менеджер материализации данных

## Таблица совместимости импортов

| Старый импорт | Новый импорт | Статус |
|---------------|--------------|--------|
| `bioetl.configs` | `bioetl.config` | Deprecated (удаление через 2 MINOR) |
| `bioetl.configs.models` | `bioetl.config.models` | Deprecated (удаление через 2 MINOR) |
| `bioetl.schemas.registry` | `bioetl.core.unified_schema` | Deprecated (удаление через 2 MINOR) |
| `bioetl.transform.adapters.*` | `bioetl.adapters.*` | Deprecated (удаление через 2 MINOR) |
| `bioetl.utils.io.*` | `bioetl.io.readers.*` | Deprecated (удаление через 2 MINOR) |
| `bioetl.core.output_writer::_atomic_write` | `bioetl.io.atomic_write` | Internal (не публичный API) |

## Утилиты для тестов

- `tests.conftest`: общие фикстуры pytest
- `tests.fixtures`: тестовые данные и фабрики

## Принципы использования

1. **Все импорты из `bioetl`**: Используйте только импорты вида `from bioetl.X import Y`
2. **Избегайте прямых импортов из корня**: Не используйте `import scripts` или
   обращение к корневым пакетам без префикса `bioetl.`
3. **Следуйте матрице зависимостей**: Соблюдайте правила зависимостей между слоями
   (см. `LAYER_MATRIX.md`)
4. **Используйте реэкспорты**: Приоритет отдавайте импортам из публичных `__init__.py`
5. **Обновляйте deprecated импорты**: При обнаружении deprecated импортов
   обновляйте код на новые пути
