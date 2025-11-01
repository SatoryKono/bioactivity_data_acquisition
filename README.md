# BioETL

## Что это и зачем {#what-this-and-why}

BioETL объединяет ETL-пайплайны для загрузки данных ChEMBL и внешних энрихеров
с детерминированной загрузкой в CSV/Parquet. Каркас построен на `PipelineBase`
с унифицированными стадиями `extract → transform → validate → write → run`,
типобезопасных конфигурациях и стандартизованном логировании. Каждая реализация
наследует базовую оркестрацию
[`PipelineBase`][ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]
и использует строгие схемы Pandera, HTTP-клиенты с backoff и единый логгер.
Бизнес-логика живёт в модулях `src/bioetl/pipelines/` и `src/bioetl/sources/`,
где каждый источник реализует клиентов, пагинацию, парсеры и нормализаторы с
Pandera-схемами. Публичное API описано в документации, на которую ссылается этот
README.

## Быстрый старт {#quick-start}

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -m bioetl.cli.main list
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input-file data/input/activity.csv \
  --output-dir data/output/activity \
  --dry-run
```
Команда `list` показывает все зарегистрированные пайплайны Typer, а запуск в
режиме `--dry-run` выполняет стадии до записи файла и валидирует конфигурацию
без побочных эффектов.[ref: repo:src/scripts/__init__.py@test_refactoring_32]
Команда автоматически применяет структуру конфигурации
[`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32]
и включённые значения из `base.yaml` и `determinism.yaml`.

## Где искать документацию {#where-to-find-docs}

- Навигация: [`docs/INDEX.md`][ref: repo:docs/INDEX.md@test_refactoring_32] —

  единая точка входа и карта разделов.

- Архитектура уровней и глоссарий:

  [`docs/architecture/00-architecture-overview.md`][ref: repo:docs/architecture/00-architecture-overview.md@test_refactoring_32]

- Источники данных и схемы:

  [`docs/architecture/03-data-sources-and-spec.md`][ref: repo:docs/architecture/03-data-sources-and-spec.md@test_refactoring_32]

- Контракты пайплайнов:

  [`docs/pipelines/PIPELINES.md`][ref: repo:docs/pipelines/PIPELINES.md@test_refactoring_32]

- Конфигурации и профили:

  [`docs/configs/CONFIGS.md`][ref: repo:docs/configs/CONFIGS.md@test_refactoring_32]

- CLI и команды:

  [`docs/cli/CLI.md`][ref: repo:docs/cli/CLI.md@test_refactoring_32]

- Контроль качества и тесты:

  [`docs/qc/QA_QC.md`][ref: repo:docs/qc/QA_QC.md@test_refactoring_32]

## Поддерживаемые источники данных и их статус {#supported-sources}

### Пайплайны ChEMBL

| Пайплайн | Основной источник | Целевые сущности | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- | --- |
| Activity | ChEMBL `/activity.json` | Активности | `bioetl.cli.main activity` | [`pipelines/activity.yaml`][ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32] | Production |
| Assay | ChEMBL `/assay.json` | Ассайы | `bioetl.cli.main assay` | [`pipelines/assay.yaml`][ref: repo:src/bioetl/configs/pipelines/assay.yaml@test_refactoring_32] | Production |
| Target | ChEMBL `/target.json` + UniProt/IUPHAR обогащения | Таргеты + обогащение | `bioetl.cli.main target` | [`pipelines/target.yaml`][ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32] | Production |
| Document | ChEMBL документы + PubMed/Crossref/OpenAlex/Semantic Scholar | Документы | `bioetl.cli.main document` | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] | Production |
| TestItem | ChEMBL молекулы с обогащением PubChem | Тест-айтемы | `bioetl.cli.main testitem` | [`pipelines/testitem.yaml`][ref: repo:src/bioetl/configs/pipelines/testitem.yaml@test_refactoring_32] | Production |

### Внешние энричеры и standalone источники

| Пайплайн | Источник | Целевые сущности | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- | --- |
| PubChem | PubChem PUG REST | Свойства молекул | `bioetl.cli.main pubchem` | [`pipelines/pubchem.yaml`][ref: repo:src/bioetl/configs/pipelines/pubchem.yaml@test_refactoring_32] | Production |
| UniProt | UniProt REST API | Белковые записи | `bioetl.cli.main uniprot` | [`pipelines/uniprot.yaml`][ref: repo:src/bioetl/configs/pipelines/uniprot.yaml@test_refactoring_32] | Beta |
| GtP IUPHAR | Guide to Pharmacology API | Таргеты и классификации | `bioetl.cli.main gtp_iuphar` | [`pipelines/iuphar.yaml`][ref: repo:src/bioetl/configs/pipelines/iuphar.yaml@test_refactoring_32] | Beta |

### Внешние адаптеры

| Адаптер | API | Использование | Лимиты/аутентификация | Статус |
| --- | --- | --- | --- | --- |
| PubMed | E-utilities (`efetch`, `esearch`) | Документ-пайплайн | `tool`, `email`, `api_key`; 3 req/sec без ключа.[ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32] | Production |
| Crossref | REST `/works` | Документ-пайплайн | `mailto` в User-Agent, 2 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] | Production |
| OpenAlex | REST `/works` | Документ-пайплайн | `mailto`, 10 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] | Production |
| Semantic Scholar | Graph API `/paper/batch` | Документ-пайплайн | API key опционален, 1 req/1.25s без ключа.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] | Production |
| UniProt ID Mapping | REST `/idmapping` | Таргет-пайплайн | Квота 2 req/sec, кэширование включено.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32] | Production |
| IUPHAR | `/targets`, `/targets/families` | Таргет и GTP-IUPHAR пайплайны | `x-api-key`, 6 req/sec.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32] | Production |

## Лицензия и обратная связь {#license-and-feedback}

Проект распространяется по лицензии MIT, указанной в
[`pyproject.toml`][ref: repo:pyproject.toml@test_refactoring_32]. Ошибки и
предложения отправляйте через задачи в репозитории или по контактам команды
BioETL. Вопросы и предложения по качеству данных и пайплайнам направляйте через
issues или PR, соблюдая правила из PROJECT_RULES.md и USER_RULES.md. Для срочных
вопросов используйте e-mail, указанный в `CROSSREF_MAILTO`/`OPENALEX_MAILTO` в
локальной конфигурации.
