# CLI спецификация

## обзор

Консольный интерфейс строится на Typer-приложении [ref: repo:src/bioetl/cli/main.py@test_refactoring_32]. Команды регистрируются автоматически из реестра `PIPELINE_COMMAND_REGISTRY` ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).

## базовые-команды

```bash
# список доступных пайплайнов и описаний
python -m bioetl.cli.main list

# запуск конкретного пайплайна
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input data/input/activity.csv \
  --output data/output/activity \
  --dry-run
```

### общие-флаги

| Флаг | Значение | Поведение |
| --- | --- | --- |
| `--config PATH` | YAML конфиг | **MUST** ссылаться на валидный `PipelineConfig`. |
| `--input PATH` | CSV/Parquet вход | При отсутствии использует значение из реестра. |
| `--output PATH` | Каталог вывода | Создаётся автоматически; запись атомарна. |
| `--mode NAME` | Альтернативный режим | Доступен, если `mode_choices` указаны в конфиге (например, `target` поддерживает `smoke`). |
| `--sample N` | Ограничение количества строк | Применяется на этапе `extract`. |
| `--dry-run` | Без записи артефактов | Выполняет `extract/transform/validate`, минуя `export`. |
| `--extended` | Расширенные QC | Добавляет доп. отчёты (`correlation`, `summary_statistics`). |
| `--validate-columns/--no-validate-columns` | Контроль схемы | По умолчанию включён, использует `schema_registry`. |

## зарегистрированные-команды

| Команда | Пайплайн | Конфиг по умолчанию | Описание |
| --- | --- | --- | --- |
| `activity` | `ActivityPipeline` | `src/bioetl/configs/pipelines/activity.yaml` | Выгрузка активностей ChEMBL. |
| `assay` | `AssayPipeline` | `src/bioetl/configs/pipelines/assay.yaml` | Ассайные данные ChEMBL. |
| `target` | `TargetPipeline` | `src/bioetl/configs/pipelines/target.yaml` | Мишени с обогащением UniProt/IUPHAR. |
| `testitem` | `TestItemPipeline` | `src/bioetl/configs/pipelines/testitem.yaml` | Молекулы + PubChem. |
| `document` | `DocumentPipeline` | `src/bioetl/configs/pipelines/document.yaml` | Документы ChEMBL + внешние источники. |
| `pubchem` | `PubChemPipeline` | `src/bioetl/configs/pipelines/pubchem.yaml` | Стандалон-энрихмент PubChem. |
| `gtp_iuphar` | `GtpIupharPipeline` | `src/bioetl/configs/pipelines/iuphar.yaml` | Классификация GtoP. |
| `uniprot` | `UniProtPipeline` | `src/bioetl/configs/pipelines/uniprot.yaml` | Обогащение по UniProt API. |

## best-practices

- Перед запуском **MUST** быть выполнена инициализация логгера: `UnifiedLogger.setup` выполняется автоматически в `PipelineBase.run()`.
- Контактные данные для внешних API **MUST** задаваться переменными окружения (см. `docs/configs/CONFIGS.md`).
- Для smoke-тестов используйте `--sample` и режимы `smoke`, чтобы ограничить нагрузку.
- Команда `python src/scripts/run_inventory.py --config configs/inventory.yaml` **SHOULD** использоваться перед релизом для проверки покрытий.

## интеграция-с-CI

- GitHub Actions вызывает те же команды через точку входа `bioetl` (`pyproject.toml [project.scripts]`).
- Для override параметров в CI используйте переменные `BIOETL__SECTION__KEY` ([ref: repo:pyproject.toml@test_refactoring_32]).


