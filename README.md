# BioETL

## Что это и зачем {#what-this-and-why}

BioETL объединяет ETL-пайплайны для загрузки данных ChEMBL и внешних энрихеров
с детерминированной загрузкой в CSV/Parquet. Каркас построен на `PipelineBase`
с унифицированными стадиями `extract → transform → validate → write`,
оркестрируемыми методом `run()`, типобезопасных конфигурациях и стандартизованном логировании. Каждая реализация
наследует базовую оркестрацию
[`PipelineBase`][ref: repo:src/bioetl/pipelines/base.py@refactoring_001]
и использует строгие схемы Pandera, HTTP-клиенты с backoff и единый логгер.
Бизнес-логика живёт в модулях `src/bioetl/pipelines/` и `src/bioetl/sources/`,
где каждый источник реализует клиентов, пагинацию, парсеры и нормализаторы с
Pandera-схемами. Публичное API описано в документации, на которую ссылается этот
README.

## Быстрый старт {#quick-start}

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
python -m bioetl.cli.main list
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --input-file data/input/activity.csv \
  --output-dir data/output/activity \
  --dry-run
```

Команда `list` показывает все зарегистрированные пайплайны Typer, а запуск в
режиме `--dry-run` выполняет стадии до записи файла и валидирует конфигурацию
без побочных эффектов.[ref: repo:src/bioetl/cli/app.py@refactoring_001]
Команда автоматически применяет структуру конфигурации
[`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@refactoring_001]
и включённые значения из `base.yaml` и `determinism.yaml`.

## Где искать документацию {#where-to-find-docs}

- **Navigation**: [`docs/INDEX.md`][ref: repo:docs/INDEX.md@refactoring_001] — The main entry point and map of all documentation sections.

- **ETL Contract**: [`docs/etl_contract/00-etl-overview.md`][ref: repo:docs/etl_contract/00-etl-overview.md@refactoring_001] — Core principles and the `PipelineBase` architecture.

- **Pipeline Catalog**: [`docs/pipelines/10-chembl-pipelines-catalog.md`][ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001] — A detailed catalog of all ChEMBL pipelines.

- **Configurations**: [`docs/configs/00-typed-configs-and-profiles.md`][ref: repo:docs/configs/00-typed-configs-and-profiles.md@refactoring_001] — The Pydantic-based configuration system.

- **CLI Reference**: [`docs/cli/00-cli-overview.md`][ref: repo:docs/cli/00-cli-overview.md@refactoring_001] — The guide to the Command-Line Interface.

> **Конвенция именования файлов:** Файлы документации пайплайнов именуются по правилу `<NN>-<entity>-<source>-<topic>.md` (например, `09-document-chembl-extraction.md`). См. [Naming Conventions](docs/styleguide/00-naming-conventions.md#11-pipeline-documentation-file-naming).

## Поддерживаемые источники данных и их статус {#supported-sources}

### Пайплайны ChEMBL

| Пайплайн | Основной источник | Целевые сущности | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- | --- |
| Activity | ChEMBL `/activity.json` | Активности | `bioetl.cli.main activity_chembl` | [`pipelines/chembl/activity.yaml`][ref: repo:configs/pipelines/chembl/activity.yaml@refactoring_001] | Production |
| Assay | ChEMBL `/assay.json` | Ассайы | `bioetl.cli.main assay_chembl` | [`pipelines/chembl/assay.yaml`][ref: repo:configs/pipelines/chembl/assay.yaml@refactoring_001] | Production |
| Target | ChEMBL `/target.json` + UniProt/IUPHAR обогащения | Таргеты + обогащение | `bioetl.cli.main target` | [`pipelines/chembl/target.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@refactoring_001] | Production |
| Document | ChEMBL документы + PubMed/Crossref/OpenAlex/Semantic Scholar | Документы | `bioetl.cli.main document` | [`pipelines/chembl/document.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@refactoring_001] | Production |
| TestItem | ChEMBL молекулы (обогащение PubChem вынесено в отдельный пайплайн) | Тест-айтемы | `bioetl.cli.main testitem` | [`pipelines/chembl/testitem.yaml`][ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@refactoring_001] | Production |

### Внешние энричеры и standalone источники

Обогащение молекул данными PubChem выполняет отдельный пайплайн `PubChem`; `testitem` покрывает только данные из ChEMBL.

| Пайплайн | Источник | Целевые сущности | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- | --- |
| PubChem | PubChem PUG REST | Свойства молекул | `bioetl.cli.main pubchem` | [`pipelines/pubchem.yaml`][ref: repo:src/bioetl/configs/pipelines/pubchem.yaml@refactoring_001] | Production |
| UniProt | UniProt REST API | Белковые записи | `bioetl.cli.main uniprot` | [`pipelines/uniprot.yaml`][ref: repo:src/bioetl/configs/pipelines/uniprot.yaml@refactoring_001] | Production |
| GtP IUPHAR | Guide to Pharmacology API | Таргеты и классификации | `bioetl.cli.main gtp_iuphar` | [`pipelines/iuphar.yaml`][ref: repo:src/bioetl/configs/pipelines/iuphar.yaml@refactoring_001] | Production |

### Внешние адаптеры

| Адаптер | API | Использование | Лимиты/аутентификация | Статус |
| --- | --- | --- | --- | --- |
| PubMed | E-utilities (`efetch`, `esearch`) | Документ-пайплайн | `tool`, `email`, `api_key`; 3 req/sec без ключа.[ref: repo:src/bioetl/sources/pubmed/request/builder.py@refactoring_001] | Production |
| Crossref | REST `/works` | Документ-пайплайн | `mailto` в User-Agent, 2 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@refactoring_001] | Production |
| OpenAlex | REST `/works` | Документ-пайплайн | `mailto`, 10 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@refactoring_001] | Production |
| Semantic Scholar | Graph API `/paper/batch` | Документ-пайплайн | API key опционален, 1 req/1.25s без ключа.[ref: repo:src/bioetl/configs/pipelines/document.yaml@refactoring_001] | Production |
| UniProt ID Mapping | REST `/idmapping` | Таргет-пайплайн | Квота 2 req/sec, кэширование включено.[ref: repo:src/bioetl/configs/pipelines/target.yaml@refactoring_001] | Production |
| IUPHAR | `/targets`, `/targets/families` | Таргет и GTP-IUPHAR пайплайны | `x-api-key`, 6 req/sec.[ref: repo:src/bioetl/configs/pipelines/target.yaml@refactoring_001] | Production |

## Лицензия и обратная связь {#license-and-feedback}

Проект распространяется по лицензии MIT (см. [`pyproject.toml`][ref: repo:pyproject.toml@refactoring_001], секция `project.license`). Ошибки и предложения отправляйте через задачи в репозитории или по контактам команды BioETL. Вопросы и предложения по качеству данных и пайплайнам направляйте через issues или PR, соблюдая правила из [`tools/PROJECT_RULES.md`][ref: repo:tools/PROJECT_RULES.md@refactoring_001] и [`tools/USER_RULES.md`][ref: repo:tools/USER_RULES.md@refactoring_001]. Для срочных вопросов используйте e-mail, указанный в `CROSSREF_MAILTO`/`OPENALEX_MAILTO` в локальной конфигурации.
