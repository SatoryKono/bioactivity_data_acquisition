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
- Базовый протокол пайплайна описан в `bioetl.core.pipeline.unified`, там же 
  находится базовая реализация `UnifiedPipelineBase`.
- Логирование — через `bioetl.core.logging.UnifiedLogger` (JSON-лог в stdout).
- Загрузчик конфигурации задокументирован в `bioetl.config.loader` и может быть
  расширен под YAML/TOML.

Структура подготовлена для дальнейшей интеграции Pandera, backoff/requests и
конкретных ETL-пайплайнов под ChEMBL.

## Клиентский слой и совместимые алиасы

- Реестр клиентских фабрик расположен в `bioetl.clients.base`: функции
  `register_factory`/`get_factory` управляют общим `FACTORIES`, а
  `register_domain_factories` удобно прокидывает алиасы `chembl` и `enricher`.
- В `bioetl.clients` доступен готовый `default_chembl_factory` и вспомогательный
  `make_chembl_client`, позволяющие быстро собрать адаптеры ChEMBL с нужной
  пагинацией и транспортом.
- Старые алиасы HTTP/пагинации сохраняются для обратной совместимости
  (`ApiTransportProtocol`, `PaginationStrategy` и т.п.) и при обращении
  проксируются в `bioetl.core.http` с `DeprecationWarning`.

Минимальный пример инициализации фабрики ChEMBL в приложении:

```python
from bioetl.clients import (
    default_chembl_factory,
    get_factory,
    register_domain_factories,
)

register_domain_factories(chembl_factory=default_chembl_factory())
target_client = get_factory("chembl").create("target")
with target_client as client:
    # дальнейшая логика обхода/выгрузки
    ...
```

CLI также умеет использовать зарегистрированные пайплайны/клиенты:

```bash
python -m bioetl.cli.cli_app run-chembl-all --config configs/example.yaml --dry-run
```

## Конфигурация HTTP-клиентов через YAML

- Технические настройки клиентов (endpoint, id_field, фильтры, дефолтная
  пагинация) хранятся в отдельных файлах `configs/<source>.yaml`. Часть имени
  клиента (`<source>.<resource>`) используется для поиска нужного блока
  настроек.
- Загрузка реализована через `bioetl.config.load_client_config`, поэтому
  конструкторы клиентов принимают только `name` и `transport` и поднимают
  конфигурацию автоматически.

Пример содержимого `configs/pubmed.yaml`:

```yaml
pagination:
  page_size: 50
resources:
  article:
    endpoint: "/article"
    id_field: "pmid"
    filter_mapping:
      status: "term"
```

При отсутствии ресурса или файла конфигурации конструктор клиента завершится
ошибкой с явным сообщением, что упрощает отладку неверных имён.
