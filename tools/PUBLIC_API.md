# Публичное API BioETL

Этот файл фиксирует поддерживаемые точки интеграции и канонические пути
импортов. Все модули располагаются внутри пакета `bioetl`; упоминания корневого
пакета верхнего уровня и устаревшего пространства `scripts` больше не
используются.

## CLI и оркестрация

- `bioetl.cli.app`
  - `create_pipeline_app`: сборка Typer-приложения для одного пайплайна.
  - `register_pipeline_command`: регистрация отдельных команд в общем CLI.
  - `PIPELINE_COMMAND_REGISTRY` и `PIPELINE_REGISTRY`: отображения имён CLI в
    конфигурации и классы пайплайнов соответственно.
- `bioetl.cli.main`
  - `app`: основное Typer-приложение (`python -m bioetl.cli.main`).
- `bioetl.cli.command`
  - `PipelineCommandConfig`, `create_pipeline_command`: сборка исполняемых
    функций CLI из конфигураций.

## Конфигурация

- `bioetl.config.loader`
  - `load_config`, `parse_cli_overrides`: загрузка и применение CLI override.
- `bioetl.config.models`
  - Pydantic-модели конфигураций пайплайнов, включая `PipelineConfig` и
    `DeterminismConfig`.

## Пайплайны и утилиты

- `bioetl.pipelines.registry`
  - `PIPELINE_REGISTRY`, `get_pipeline`, `iter_pipelines`: канонический реестр
    пайплайнов.
- `bioetl.pipelines.*`
  - Конкретные реализации (`chembl_activity`, `chembl_assay`, `chembl_target`,
    и т.д.), наследующие `PipelineBase`.
- `bioetl.utils.dataframe`
  - Утилиты для нормализации DataFrame и генерации детерминированных хешей.
- `bioetl.utils.output`
  - Запись CSV-артефактов и отчётов качества.

## Схемы валидации

- `bioetl.schemas.*`
  - Pandera-схемы входных и выходных данных (например, `bioetl.schemas.activity`
    и `bioetl.schemas.document`).

## Клиенты источников данных

- `bioetl.clients.*` и `bioetl.sources.*`
  - HTTP-клиенты с поддержкой backoff и пайплайны для ChEMBL, PubChem, UniProt,
    IUPHAR и других сервисов.

## Утилиты для тестов

- `tests.factories` и специализированные фикстуры pytest.

> **Важно:** все новые интеграции должны импортировать код только из пространства
> имён `bioetl`. Импорты вида `import scripts` или обращение к корневым пакетам
> без префикса `bioetl.` считаются устаревшими.
