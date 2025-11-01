# CLI справочник {#cli}

## Точка входа {#cli-entry}

- Основная команда: `python -m bioetl.cli.main`.
- При установке пакета доступен alias `bioetl` через [`pyproject.toml`][ref: repo:pyproject.toml@test_refactoring_32].
- Реестр команд формируется через [`build_registry()`][ref: repo:src/bioetl/cli/registry.py@test_refactoring_32] и регистрируется в [`cli/main.py`][ref: repo:src/bioetl/cli/main.py@test_refactoring_32].

## Доступные команды {#cli-commands}

| Команда | Описание | Основные параметры | Конфиг по умолчанию |
| --- | --- | --- | --- |
| `list` | Перечислить доступные пайплайны | — | — |
| `activity` | Запуск ChEMBL Activity | `--config`, `--input-file`, `--output-dir`, `--dry-run` | [`pipelines/chembl/activity.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@test_refactoring_32] |
| `assay` | Запуск ChEMBL Assay | `--config`, `--input-file`, `--output-dir`, `--dry-run` | [`pipelines/chembl/assay.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/assay.yaml@test_refactoring_32] |
| `target` | Таргеты ChEMBL + обогащение | `--mode`, `--set`, `--extended` | [`pipelines/chembl/target.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@test_refactoring_32] |
| `document` | Документы ChEMBL + внешние адаптеры | `--mode`, `--set`, `--dry-run` | [`pipelines/chembl/document.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@test_refactoring_32] |
| `testitem` | Молекулы ChEMBL + PubChem | `--dry-run`, `--sample` | [`pipelines/chembl/testitem.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@test_refactoring_32] |
| `pubchem` | Standalone PubChem обогащение | `--input-file` InChIKey CSV | [`pipelines/pubchem.yaml`][ref: repo:src/bioetl/configs/pipelines/pubchem.yaml@test_refactoring_32] |
| `gtp_iuphar` | Guide to Pharmacology | `--set sources.iuphar.api_key=...` | [`pipelines/iuphar.yaml`][ref: repo:src/bioetl/configs/pipelines/iuphar.yaml@test_refactoring_32] |
| `uniprot` | UniProt выгрузка | `--sample`, `--extended` | [`pipelines/uniprot.yaml`][ref: repo:src/bioetl/configs/pipelines/uniprot.yaml@test_refactoring_32] |
| `openalex` | OpenAlex standalone | `--mode`, `--set` | [`pipelines/openalex.yaml`][ref: repo:src/bioetl/configs/pipelines/openalex.yaml@test_refactoring_32] |
| `crossref` | Crossref standalone | `--set sources.crossref.mailto=...` | [`pipelines/crossref.yaml`][ref: repo:src/bioetl/configs/pipelines/crossref.yaml@test_refactoring_32] |
| `pubmed` | PubMed standalone | `--set sources.pubmed.api_key=...` | [`pipelines/pubmed.yaml`][ref: repo:src/bioetl/configs/pipelines/pubmed.yaml@test_refactoring_32] |
| `semantic_scholar` | Semantic Scholar standalone | `--set sources.semantic_scholar.api_key=...` | [`pipelines/semantic_scholar.yaml`][ref: repo:src/bioetl/configs/pipelines/semantic_scholar.yaml@test_refactoring_32] |

Все команды регистрируются через [`build_registry()`][ref: repo:src/bioetl/cli/registry.py@test_refactoring_32]. Легаси-алиасы (`chembl_activity`, `chembl_assay` и т.д.) поддерживаются для обратной совместимости.

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

### ChEMBL пайплайны

```bash
# Production run activity
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/chembl/activity.yaml \
  --input-file data/input/activity.csv \
  --output-dir data/output/activity

# Assay с dry-run
python -m bioetl.cli.main assay \
  --config src/bioetl/configs/pipelines/chembl/assay.yaml \
  --input-file data/input/assay.csv \
  --output-dir data/output/assay \
  --dry-run

# Target с обогащением и расширенными артефактами
python -m bioetl.cli.main target \
  --extended \
  --set sources.iuphar.api_key=${IUPHAR_API_KEY}

# Document с внешними адаптерами
python -m bioetl.cli.main document \
  --config src/bioetl/configs/pipelines/chembl/document.yaml \
  --input-file data/input/document.csv \
  --output-dir data/output/documents \
  --mode all \
  --set sources.pubmed.batch_size=100 \
  --dry-run

# TestItem с ограничением выборки
python -m bioetl.cli.main testitem \
  --sample 1000 \
  --dry-run
```

### Внешние источники

```bash
# PubChem обогащение
python -m bioetl.cli.main pubchem \
  --input-file data/input/pubchem_lookup.csv \
  --output-dir data/output/pubchem

# UniProt выгрузка
python -m bioetl.cli.main uniprot \
  --sample 500 \
  --extended

# Guide to Pharmacology IUPHAR
python -m bioetl.cli.main gtp_iuphar \
  --set sources.iuphar.api_key=${IUPHAR_API_KEY}

# Crossref standalone
python -m bioetl.cli.main crossref \
  --set sources.crossref.mailto=${CROSSREF_MAILTO}
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

- Неверное имя пайплайна MUST вызывать `KeyError` через [`get_command_config()`][ref: repo:src/bioetl/cli/registry.py@test_refactoring_32] с поддержкой легаси-алиасов.

- CLI SHOULD уважать `PIPELINE_COMMAND_REGISTRY.default_config` даже при отсутствии файла —

  `read_input_table()` создаёт пустой DataFrame с ожидаемыми колонками.

## Интеграция с CI {#cli-ci-integration}

Команда `python -m tools.qa.check_required_docs` проверяет наличие ключевых
документов перед запуском пайплайнов в CI. Рекомендуется включать `bioetl ... --dry-run`
в pre-commit для smoke-проверок конфигураций.
