# BioETL Scaffold

Базовый скелет репозитория BioETL, отражающий трехслойную архитектуру:

- **Orchestration** (`src/bioetl/pipelines`, `src/bioetl/cli`): абстракции пайплайнов, Typer-CLI и фабрики стадий.
- **Domain** (`src/bioetl/domain`, `src/bioetl/schemas`): чистые преобразования и Pandera-схемы без прямого I/O.
- **Infrastructure** (`src/bioetl/clients`, `src/bioetl/core`, `src/bioetl/storage`): клиенты Unified API, HTTP-адаптеры, writer'ы и кэш.

## Быстрый старт

```bash
python -m bioetl.cli.cli_app list
```

Команда выводит зарегистрированные пайплайны (по умолчанию реестр пуст). Для
добавления новых команд объявляйте фабрики в `bioetl.cli.cli_registry` и
используйте `create_pipeline_command` для генерации CLI-команд.

## Разработка

- Публичные типы хранятся в `bioetl.core.pipeline.types`.
- Базовый протокол пайплайна описан в `bioetl.pipelines.base`, шаблонная
  реализация с run() — в `bioetl.pipelines.unified_base`.
- Логирование — через `bioetl.core.logging.UnifiedLogger` (JSON-лог в stdout).
- Загрузчик конфигурации задокументирован в `bioetl.config.loader` и может быть
  расширен под YAML/TOML.

Структура подготовлена для дальнейшей интеграции Pandera, backoff/requests и
конкретных ETL-пайплайнов под ChEMBL.
