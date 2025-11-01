# CLI справочник {#cli}

## Точка входа {#cli-entry}

- Основная команда: `python -m bioetl.cli.main`.
- При установке пакета доступен alias `bioetl` через `pyproject.toml`.
- Реестр команд формируется из

  [`PIPELINE_COMMAND_REGISTRY`][ref: repo:src/scripts/__init__.py@test_refactoring_32]
  и регистрируется в [`cli/main.py`][ref: repo:src/bioetl/cli/main.py@test_refactoring_32].

## Доступные команды {#cli-commands}

| Команда | Описание | Основные параметры | Конфиг по умолчанию |
| --- | --- | --- | --- |
| `list` | Перечислить доступные пайплайны | — | — |
| `activity` | Запуск ChEMBL Activity | `--config`, `--input-file`, `--output-dir`, `--dry-run` | `src/bioetl/configs/pipelines/activity.yaml` |
| `assay` | Запуск ChEMBL Assay | Те же | `src/bioetl/configs/pipelines/assay.yaml` |
| `target` | Таргеты с UniProt/IUPHAR | `--mode`, `--set`, `--extended` | `src/bioetl/configs/pipelines/target.yaml` |
| `document` | Документы с внешним энрихментом | `--mode`, `--set`, `--dry-run` | `src/bioetl/configs/pipelines/document.yaml` |
| `testitem` | Молекулы + PubChem | `--dry-run`, `--sample` | `src/bioetl/configs/pipelines/testitem.yaml` |
| `pubchem` | Standalone PubChem | `--input-file` InChIKey CSV | `src/bioetl/configs/pipelines/pubchem.yaml` |
| `gtp_iuphar` | Guide to Pharmacology | `--set sources.iuphar.api_key=...` | `src/bioetl/configs/pipelines/iuphar.yaml` |
| `uniprot` | UniProt выгрузка | `--sample`, `--extended` | `src/bioetl/configs/pipelines/uniprot.yaml` |

[ref: repo:src/scripts/__init__.py@test_refactoring_32]

## Глобальные флаги {#cli-flags}

| Флаг | Описание | Инварианты |
| --- | --- | --- |
| `--config PATH` | Явный путь к YAML конфигурации | MUST существовать до запуска |
| `--input-file/-i PATH` | Входной CSV/Parquet (если применяется) | По умолчанию `data/input/<pipeline>.csv` |
| `--output-dir/-o PATH` | Каталог результата | Создаётся автоматически при необходимости |
| `--mode NAME` | Режим выполнения (зависит от пайплайна) | Должен входить в `mode_choices` конфигурации |
| `--dry-run` | Запуск без записи файла | Завершает стадии до `export()` |
| `--extended/--no-extended` | Управление расширенными артефактами | По умолчанию из конфигурации |
| `--sample N` | Ограничение количества записей | Применяется в `read_input_table` |
| `--limit N` | Легаси-алиас для `--sample` | Выдаёт предупреждение в лог |
| `--fail-on-schema-drift/--allow-schema-drift` | Контроль реакции на schema drift | По умолчанию `True` |
| `--validate-columns/--no-validate-columns` | Проверка столбцов | По умолчанию `True` |
| `--verbose/-v` | Подробное логирование | Включает режим development логгера |
| `--golden PATH` | Golden-датасет для сравнения | Используется в smoke-проверках |
| `--set key=value` | Точечные overrides | Поддерживает вложенные ключи, массивы через `[,]` |

Флаги и опции описаны в фабрике команд
[`create_pipeline_command`][ref: repo:src/bioetl/cli/command.py@test_refactoring_32].
Переопределение значений возможно через `--set key=value`, который разбирается
функцией `parse_cli_overrides`.[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]

## Режимы и специальные опции {#cli-modes}

- `document --mode chembl` отключает внешние адаптеры; `--mode all` включает их.
- `target --mode smoke` активирует облегчённый профиль с ограничениями на объём.[ref: repo:src/scripts/__init__.py@test_refactoring_32]
- Адаптеры могут принимать дополнительные параметры через `--set sources.<name>.<key>=value`.

## Примеры {#cli-examples}

```bash

# Production run activity с профилем prod

python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input-file data/input/activity.csv \
  --output-dir data/output/activity

# Dry-run для документов с переопределением лимита PubMed

python -m bioetl.cli.main document \
  --config src/bioetl/configs/pipelines/document.yaml \
  --input-file data/input/document.csv \
  --output-dir data/output/documents \
  --mode all \
  --set sources.pubmed.batch_size=100 \
  --dry-run

# Полный запуск таргетов с расширенными артефактами

python -m bioetl.cli.main target \
  --extended \
  --set sources.iuphar.api_key=${IUPHAR_API_KEY}
```

## Обработка ошибок {#cli-error-handling}

- Отсутствие файла конфигурации или входного CSV приводит к `FileNotFoundError` до

  стадии `extract()`.

- Pandera-ошибки уровня `error` завершают процесс с кодом `1`; предупреждения

  остаются в QC.

- Неизвестная команда вызывает подсказку Typer с доступными опциями.

## Инварианты CLI {#cli-invariants}

- Команда `list` MUST инициализировать логгер в режиме production, как реализовано в

  [`cli/main.py`][ref: repo:src/bioetl/cli/main.py@test_refactoring_32].

- Неверное имя пайплайна MUST вызывать `KeyError` через

  [`get_pipeline_command_config`][ref: repo:src/scripts/__init__.py@test_refactoring_32].

- CLI SHOULD уважать `PIPELINE_COMMAND_REGISTRY.default_config` даже при отсутствии файла —

  `read_input_table()` создаёт пустой DataFrame с ожидаемыми колонками.

## Интеграция с CI {#cli-ci-integration}

Команда `python -m tools.qa.check_required_docs` проверяет наличие ключевых
документов перед запуском пайплайнов в CI. Рекомендуется включать `bioetl ... --dry-run`
в pre-commit для smoke-проверок конфигураций.
