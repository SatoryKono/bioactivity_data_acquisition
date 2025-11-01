# Целевая файловая структура BioETL

## Общие принципы

- **src-layout**: Пакет `bioetl` в `src/bioetl/` для предотвращения фантомных импортов
- **Зеркалирование тестов**: Структура `tests/unit/` повторяет `src/bioetl/`
- **Централизация схем**: Единый реестр схем в `core/unified_schema.py`
- **Атомарная запись**: Модуль `io/atomic_write.py` с использованием `os.replace`
- **MkDocs структура**: Конфигурация в `docs/mkdocs.yml`, навигация по разделам

## Целевое дерево каталогов

```
/src/bioetl/
  __init__.py              # Версия пакета
  py.typed                  # Маркер типизированного пакета
  
  core/                     # Базовый слой: протоколы, ошибки, логирование, хеширование
    __init__.py             # Реэкспорты: UnifiedLogger, UnifiedAPIClient, UnifiedOutputWriter
    api_client.py           # UnifiedAPIClient с backoff и retry
    client_factory.py       # Фабрика клиентов по источникам
    deprecation.py          # Утилиты для депрекаций
    fallback_manager.py     # Менеджер fallback значений
    hashing.py              # Генерация хешей (BLAKE2)
    logger.py               # UnifiedLogger (structlog)
    materialization.py      # MaterializationManager
    output_writer.py        # UnifiedOutputWriter (использует io/atomic_write)
    pagination/
      __init__.py
      strategy.py           # Стратегии пагинации
    unified_schema.py       # Единый реестр Pandera-схем (объединяет schemas/registry.py)
    chembl/
      __init__.py
      client.py             # ChEMBL клиент контекст
      output.py             # ChEMBL-специфичные выходы
  
  clients/                  # HTTP/SDK-клиенты внешних источников
    __init__.py
    chembl_activity.py     # ChEMBL Activity API клиент
    chembl_assay.py        # ChEMBL Assay API клиент
    # Будущие клиенты: pubchem, uniprot, iuphar
  
  adapters/                 # Адаптеры источников, маппинг сырья → внутренние модели
    __init__.py             # Реэкспорты: ExternalAdapter, AdapterConfig
    base.py                 # Базовый класс адаптера и конфигурация
    _normalizer_helpers.py  # Вспомогательные функции нормализации
    crossref.py             # CrossRef адаптер
    openalex.py             # OpenAlex адаптер
    pubmed.py               # PubMed адаптер
    semantic_scholar.py     # Semantic Scholar адаптер
    # ChEMBL адаптеры консолидируются из transform/adapters/
  
  pipelines/                # Пайплайны по источникам (по одному публичному на источник)
    __init__.py             # Реэкспорты публичных пайплайнов
    base.py                 # PipelineBase - базовый класс
    registry.py             # Реестр пайплайнов
    activity.py             # Activity пайплайн (legacy)
    assay.py                # Assay пайплайн (legacy)
    document.py             # Document пайплайн
    document_enrichment.py  # Document enrichment логика
    external_source.py      # ExternalSourcePipeline базовый класс
    target.py               # Target пайплайн (legacy)
    target_gold.py          # Target gold пайплайн
    testitem.py             # TestItem пайплайн (legacy)
    chembl/
      __init__.py
      chembl_activity.py    # ChEMBL Activity пайплайн
      chembl_assay.py       # ChEMBL Assay пайплайн
      chembl_document.py    # ChEMBL Document пайплайн
      chembl_target.py      # ChEMBL Target пайплайн
      chembl_testitem.py    # ChEMBL TestItem пайплайн
    # Legacy файлы: chembl_activity.py, chembl_assay.py - остаются как реэкспорты
  
  io/                       # Атомарная запись/чтение, форматы
    __init__.py
    atomic_write.py         # Атомарная запись через os.replace
    readers.py              # Детерминированное чтение CSV/Parquet (вынести из utils/io.py)
  
  schemas/                  # Pandera-схемы и централизованный реестр
    __init__.py             # Реэкспорты основных схем
    base.py                 # BaseSchema с column_order
    registry.py             # Реэкспорт из core/unified_schema (deprecated)
    activity.py             # ActivitySchema
    assay.py                # AssaySchema
    document.py             # DocumentSchema
    target.py               # TargetSchema
    testitem.py             # TestItemSchema
    uniprot.py              # UniProtSchema
    input_schemas.py        # Входные схемы для пайплайнов
    iuphar_input.py         # IUPHAR входная схема
    chembl/
      __init__.py
      activity.py           # ChEMBL Activity схема
      assay.py              # ChEMBL Assay схема
      document.py           # ChEMBL Document схема
      target.py             # ChEMBL Target схема
      testitem.py            # ChEMBL TestItem схема
  
  utils/                    # Утилиты без побочных эффектов
    __init__.py
    chembl.py               # ChEMBL-специфичные утилиты
    column_constants.py     # Константы колонок
    config.py               # Утилиты конфигурации (legacy)
    dataframe.py            # DataFrame утилиты (сортировка, хеширование)
    dtypes.py               # Преобразование типов
    fallback.py             # Fallback утилиты
    io.py                   # IO утилиты (move to io/readers.py)
    json.py                 # JSON канонизация
    output.py               # Output утилиты
    qc.py                   # Quality control утилиты
    validation.py           # Валидация утилиты
  
  cli/                      # Entrypoints и команды
    __init__.py             # Lazy import для избежания циклических зависимостей
    app.py                  # CLI утилиты и регистрация команд
    command.py              # PipelineCommandConfig и создание команд
    main.py                 # Typer app (точка входа)
    registry.py             # Реестр CLI команд
    limits.py               # Лимиты для CLI
    commands/
      __init__.py
      chembl_activity.py    # CLI команда для activity
      chembl_assay.py       # CLI команда для assay
      chembl_document.py    # CLI команда для document
      chembl_target.py      # CLI команда для target
      chembl_testitem.py    # CLI команда для testitem
      crossref.py           # CLI команда для crossref
      openalex.py           # CLI команда для openalex
      pubmed.py             # CLI команда для pubmed
      semantic_scholar.py   # CLI команда для semantic_scholar
      pubchem_molecule.py   # CLI команда для pubchem
      uniprot_protein.py    # CLI команда для uniprot
      gtp_iuphar.py         # CLI команда для iuphar
      iuphar_target.py      # Alias для gtp_iuphar
  
  config/                   # Конфигурации (объединены config/ + configs/)
    __init__.py             # Реэкспорты: PipelineConfig, load_config
    loader.py               # Загрузчик YAML конфигураций
    models.py               # Pydantic модели конфигураций
    paths.py                # Утилиты путей конфигураций
  
  normalizers/              # Нормализаторы данных (оставить как есть)
    __init__.py
    base.py                 # Базовый нормализатор
    bibliography.py         # Библиографические нормализаторы
    chemistry.py            # Химические нормализаторы
    constants.py            # Константы нормализации
    date.py                 # Нормализация дат
    helpers.py              # Вспомогательные функции
    identifier.py           # Нормализация идентификаторов
    numeric.py              # Нормализация чисел
    registry.py             # Реестр нормализаторов
    string.py               # Нормализация строк
  
  sources/                   # Источники данных (детальная структура per-source)
    __init__.py
    common/                 # Общие компоненты источников
      __init__.py
      request.py            # Базовые request builders
    chembl/                 # ChEMBL источник (детальная структура)
      # activity/, assay/, document/, target/, testitem/
    crossref/               # CrossRef источник
    openalex/               # OpenAlex источник
    pubmed/                 # PubMed источник
    semantic_scholar/        # Semantic Scholar источник
    pubchem/                # PubChem источник
    uniprot/                # UniProt источник
    iuphar/                 # IUPHAR источник
    document/               # Document общие компоненты
  
  inventory/                # Инвентаризация пайплайнов
    __init__.py
    collector.py            # Сборщик метаданных
    config.py               # Конфигурация инвентаризации
    models.py               # Модели инвентаризации
  
  transform/                # Transform компоненты (deprecated, migrate to adapters/)
    # Оставить временно для обратной совместимости, затем удалить
    adapters/               # Переместить в adapters/
  
  pandera_pandas.py         # Pandera pandas интеграция
  pandera_typing.py         # Pandera typing интеграция

/configs/                   # Конфигурационные файлы (YAML)
  profiles/                 # Профили окружений
    dev.yaml                # Development профиль
    test.yaml               # Test профиль
    prod.yaml               # Production профиль
    document_test.yaml      # Document test профиль
  sources/                  # Настройки конкретных источников (из src/bioetl/configs/includes/)
    chembl_source.yaml
    crossref_source.yaml
    openalex_source.yaml
    pubmed_source.yaml
    semantic_scholar_source.yaml
    pubchem_source.yaml
    uniprot_source.yaml
    uniprot_idmapping_source.yaml
    uniprot_orthologs_source.yaml
    iuphar_source.yaml
  pipelines/                # Опции запуска пайплайнов
    activity.yaml
    assay.yaml
    target.yaml
    testitem.yaml
    document.yaml
    crossref.yaml
    openalex.yaml
    pubmed.yaml
    semantic_scholar.yaml
    pubchem.yaml
    uniprot.yaml
    iuphar.yaml
    chembl/                 # ChEMBL-специфичные конфиги
      activity.yaml
      assay.yaml
      document.yaml
      target.yaml
      testitem.yaml
  base.yaml                 # Базовый конфиг (переместить из src/bioetl/configs/)
  determinism.yaml          # Детерминизм конфиг (в includes/)
  cache.yaml                # Cache конфиг (в includes/)
  fallback_columns.yaml     # Fallback колонки (в includes/)
  inventory.yaml            # Инвентаризация конфиг

/tests/
  __init__.py
  conftest.py               # Общие фикстуры pytest
  
  unit/                     # Юнит-тесты (зеркалирует src/bioetl/)
    __init__.py
    core/                   # Тесты core/
    clients/                # Тесты clients/
    adapters/               # Тесты adapters/
    pipelines/              # Тесты pipelines/
    io/                     # Тесты io/
    schemas/                # Тесты schemas/
    utils/                  # Тесты utils/
    cli/                    # Тесты cli/
    config/                 # Тесты config/
    normalizers/            # Тесты normalizers/
    sources/                # Тесты sources/ (детальная структура)
    document/               # Тесты document компонентов
    pagination/             # Тесты pagination
    validation/             # Тесты validation
    test_*.py               # Тесты без четкой категории (мигрировать)
  
  integration/              # Интеграционные тесты
    __init__.py
    pipelines/              # Интеграционные тесты пайплайнов
    core/                   # Интеграционные тесты core
    qc/                     # Quality control тесты
  
  e2e/                      # End-to-end тесты
    __init__.py
    helpers.py              # Вспомогательные функции
    test_cli_commands.py    # Тесты CLI команд
    test_cli_golden.py      # Golden тесты CLI
    test_unified_schema_golden.py  # Golden тесты схем
  
  fixtures/                 # Тестовые фикстуры
    # Данные для тестов
  
  data/                     # Тестовые данные
    golden/                 # Golden файлы
      unified_schema_metadata.yaml
    integration/            # Интеграционные тестовые данные
      pipelines/
        *.json

/docs/
  mkdocs.yml                # Конфигурация MkDocs (переместить из tools/)
  INDEX.md                  # Главная страница документации
  architecture/             # Архитектурная документация
    *.md
  pipelines/                # Документация пайплайнов
    *.md
    sources/
      *.md
  cli/                      # CLI документация
    CLI.md
    prompts/
      *.md
  configs/                  # Документация конфигураций
    CONFIGS.md
  qc/                       # Quality control документация
    QA_QC.md
  *.md                      # Другие документы верхнего уровня

/scripts/                   # Вспомогательные скрипты (тонкие обертки над bioetl.cli)
  __init__.py
  run_*.py                  # Скрипты запуска пайплайнов (из tools/ и src/scripts/)
  validate_columns.py       # Скрипт валидации колонок
  generate_pipeline_metrics.py  # Генерация метрик пайплайнов
  run_inventory.py          # Инвентаризация пайплайнов
  run_fix_markdown.py       # Исправление markdown
  local/                    # Локальные скрипты
    README.md
    run_tests.sh
    run_tests.ps1

/tools/                     # Инвентаризация, линтеры дерева, QA инструменты
  qa/                       # QA инструменты
    analyze_codebase.py
    check_dependency_matrix.py
    check_module_names.py
    check_required_docs.py
  scripts/                  # Вспомогательные скрипты
    fix_all_md.py
    fix_markdown_errors.py
    test_*.py
  typecheck/                # Type stubs для внешних библиотек
    stubs/
  Makefile                  # Makefile для инструментов
  pyrightconfig.json        # Pyright конфигурация
  PROJECT_RULES.md          # Правила проекта
  USER_RULES.md             # Правила пользователя
  ROOT_FILES_GUIDE.md       # Руководство по файлам корня
  CHANGELOG.md              # Changelog
  DEPRECATIONS.md            # Deprecations (переместить в docs/)
  PUBLIC_API.md             # Public API (переместить в docs/)
  TARGET_TREE.md            # Этот файл (переместить в docs/)
  requirements.txt          # Requirements для инструментов

/artifacts/                  # Артефакты сборки (переместить из tools/artifacts/)
  baselines/                 # Baseline файлы

/logs/                      # Логи (переместить из data/logs/)
  .gitkeep

/data/                      # Данные проекта
  cache/                    # Кэш (в .gitignore)
  input/                    # Входные данные
  output/                   # Выходные данные (в .gitignore)

## Семантика слоев

### core/
Базовый слой, не зависит от других слоев `bioetl`. Содержит:
- Логирование (UnifiedLogger)
- HTTP клиент (UnifiedAPIClient)
- Output writer (UnifiedOutputWriter)
- Реестр схем (SchemaRegistry)
- Хеширование
- Materialization

### clients/
HTTP/SDK клиенты для внешних источников. Зависят только от `core`.

### adapters/
Адаптеры для преобразования данных из внешних источников во внутренние модели.
Зависят от: `normalizers`, `schemas`, `core`.

### pipelines/
Пайплайны обработки данных. Зависят от: `sources`, `schemas`, `config`, `core`.

### sources/
Детальные реализации источников данных с клиентами, парсерами, нормализаторами.
Зависят от: `clients`, `adapters`, `normalizers`, `schemas`, `config`, `core`.

### schemas/
Pandera схемы данных. Зависят от: `core.unified_schema` (только для регистрации).

### utils/
Утилиты без побочных эффектов. Зависят от: `core`, стандартная библиотека.

### io/
Атомарная запись/чтение файлов. Зависят от: `core`, стандартная библиотека.

### config/
Система конфигурации. Зависят от: `core.logger`.

### normalizers/
Нормализаторы данных. Зависят от: `core`, стандартная библиотека.

### cli/
CLI интерфейс. Зависят от: `pipelines`, `config`, `core`.

## Критерии допустимых зависимостей

- **Разрешенные**: Зависимости "сверху вниз" (cli → pipelines → sources → adapters → clients → core)
- **Запрещенные**: 
  - Обратные зависимости (core → cli)
  - Циклические зависимости между слоями одного уровня
  - Импорты из `src/scripts/` в `bioetl/`

## Исключения и переходный период

- `transform/adapters/` остается временно для обратной совместимости, затем удаляется
- `configs/` остается как реэкспорт `config/` с deprecation warning
- `schemas/registry.py` остается как реэкспорт `core/unified_schema` с deprecation warning
