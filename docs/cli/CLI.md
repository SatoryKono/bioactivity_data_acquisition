# CLI справочник {#cli}

## Точка входа {#cli-entry}

- Основная команда: `python -m bioetl.cli.main`.
- При установке пакета доступен alias `bioetl` через `pyproject.toml`.
- Реестр команд формируется из
  [`PIPELINE_COMMAND_REGISTRY`][ref: repo:src/scripts/__init__.py@test_refactoring_32]
  и регистрируется в [`cli/main.py`][ref: repo:src/bioetl/cli/main.py@test_refactoring_32].

## Доступные команды {#cli-commands}

| Команда | Описание | Конфиг по умолчанию | Вход | Выход |
| --- | --- | --- | --- | --- |
| `activity` | ChEMBL Activity | `pipelines/activity.yaml` | `data/input/activity.csv` | `data/output/activity` |
| `assay` | ChEMBL Assay | `pipelines/assay.yaml` | `data/input/assay.csv` | `data/output/assay` |
| `target` | ChEMBL Target + enrichment | `pipelines/target.yaml` | `data/input/target.csv` | `data/output/target` |
| `document` | ChEMBL Document + внешние источники | `pipelines/document.yaml` | `data/input/document.csv` | `data/output/documents` |
| `testitem` | ChEMBL molecules + PubChem | `pipelines/testitem.yaml` | `data/input/testitem.csv` | `data/output/testitems` |
| `pubchem` | Standalone PubChem | `pipelines/pubchem.yaml` | `data/input/pubchem_lookup.csv` | `data/output/pubchem` |
| `uniprot` | Standalone UniProt | `pipelines/uniprot.yaml` | `data/input/uniprot.csv` | `data/output/uniprot` |
| `gtp_iuphar` | Guide to Pharmacology | `pipelines/iuphar.yaml` | `data/input/iuphar_targets.csv` | `data/output/iuphar` |
| `list` | Вывести доступные команды | — | — | stdout |

## Общие флаги {#cli-flags}

| Флаг | Значение | Применимость |
| --- | --- | --- |
| `--config PATH` | Переопределить путь к YAML-конфигу. | Все pipeline-команды |
| `--input PATH` | CSV с идентификаторами/входными данными. | Все pipeline-команды |
| `--output-dir PATH` | Каталог материализации. | Все pipeline-команды |
| `--mode NAME` | Режим пайплайна (`DOCUMENT_PIPELINE_MODES`, `target` modes). | Команды с `mode_choices` |
| `--dry-run / --no-dry-run` | Запуск без записи на диск. | Поддержка зависит от пайплайна |
| `--extended` | Включить расширенный вывод (target/document). | Соответствующие пайплайны |
| `--verbose` | Повысить уровень логирования (`UnifiedLogger`). | Все |

Флаги и опции описаны в фабрике команд
[`create_pipeline_command`][ref: repo:src/bioetl/cli/command.py@test_refactoring_32].

## Примеры {#cli-examples}

```bash
# Production run activity c профилем prod
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input data/input/activity.csv \
  --output-dir data/output/activity

# Документный пайплайн только с внешними энричерами
python -m bioetl.cli.main document \
  --mode enrichment-only \
  --config src/bioetl/configs/pipelines/document.yaml \
  --input data/input/document.csv \
  --output-dir data/output/documents
```

## Инварианты CLI {#cli-invariants}
- Команда `list` MUST инициализировать логгер в режиме production, как реализовано в
  [`cli/main.py`][ref: repo:src/bioetl/cli/main.py@test_refactoring_32].
- Неверное имя пайплайна MUST вызывать `KeyError` через
  [`get_pipeline_command_config`][ref: repo:src/scripts/__init__.py@test_refactoring_32].
- CLI SHOULD уважать `PIPELINE_COMMAND_REGISTRY.default_config` даже при отсутствии файла —
  `read_input_table()` создаёт пустой DataFrame с ожидаемыми колонками.
