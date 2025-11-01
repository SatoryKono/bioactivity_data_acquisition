# BioETL

## Что это и зачем {#what-this-and-why}

BioETL объединяет пайплайны извлечения, нормализации и детерминированной
материализации биоактивных данных из ChEMBL и сопряжённых источников. Каждая
реализация наследует базовую оркестрацию
[`PipelineBase`][ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]
и использует строгие схемы Pandera, HTTP-клиенты с backoff и единый логгер.
Публичное API описано в документации, на которую ссылается этот README.

## Быстрый старт {#quick-start}

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# выполнение производственного пайплайна activity с профилем dev
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input data/input/activity.csv \
  --output-dir data/output/activity

```

Команда автоматически применяет структуру конфигурации
[`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32]
и включённые значения из `base.yaml` и `determinism.yaml`.

## Где искать документацию {#where-to-find-docs}

- Навигация: [`docs/INDEX.md`][ref: repo:docs/INDEX.md@test_refactoring_32]
- Архитектура уровней и глоссарий:
  [`docs/requirements/00-architecture-overview.md`][ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32]
- Источники данных и схемы:
  [`docs/requirements/03-data-sources-and-spec.md`][ref: repo:docs/requirements/03-data-sources-and-spec.md@test_refactoring_32]
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

| Пайплайн | Основной источник | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- |
| Activity | ChEMBL `/activity.json` | `bioetl.cli.main activity` | [`pipelines/activity.yaml`][ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32] | Production (unit + integration тесты) |
| Assay | ChEMBL `/assay.json` | `bioetl.cli.main assay` | [`pipelines/assay.yaml`][ref: repo:src/bioetl/configs/pipelines/assay.yaml@test_refactoring_32] | Production |
| Target | ChEMBL `/target.json` + UniProt/IUPHAR обогащения | `bioetl.cli.main target` | [`pipelines/target.yaml`][ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32] | Production |
| Document | ChEMBL документы + PubMed/Crossref/OpenAlex/Semantic Scholar | `bioetl.cli.main document` | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] | Production |
| TestItem | ChEMBL молекулы с обогащением PubChem | `bioetl.cli.main testitem` | [`pipelines/testitem.yaml`][ref: repo:src/bioetl/configs/pipelines/testitem.yaml@test_refactoring_32] | Production |

### Внешние энричеры и standalone источники

| Пайплайн | Источник | CLI команда | Конфигурация | Статус |
| --- | --- | --- | --- | --- |
| PubChem | PubChem PUG REST | `bioetl.cli.main pubchem` | [`pipelines/pubchem.yaml`][ref: repo:src/bioetl/configs/pipelines/pubchem.yaml@test_refactoring_32] | Production |
| UniProt | UniProt REST API | `bioetl.cli.main uniprot` | [`pipelines/uniprot.yaml`][ref: repo:src/bioetl/configs/pipelines/uniprot.yaml@test_refactoring_32] | Production |
| GtP IUPHAR | Guide to Pharmacology API | `bioetl.cli.main gtp_iuphar` | [`pipelines/iuphar.yaml`][ref: repo:src/bioetl/configs/pipelines/iuphar.yaml@test_refactoring_32] | Production |

## Лицензия и обратная связь {#license-and-feedback}

Проект распространяется по лицензии MIT, указанной в
[`pyproject.toml`][ref: repo:pyproject.toml@test_refactoring_32]. Ошибки и
предложения отправляйте через задачи в репозитории или по контактам команды
BioETL.
