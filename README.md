# BioETL

## что-это-и-зачем
BioETL объединяет ETL-пайплайны для загрузки данных ChEMBL и внешних энрихеров
с детерминированной загрузкой в CSV/Parquet. Каркас построен на `PipelineBase`
с унифицированными стадиями `extract → transform → validate → write → run`,
типобезопасных конфигурациях и стандартизованном логировании. Бизнес-логика
живёт в модулях `src/bioetl/pipelines/` и `src/bioetl/sources/`, где каждый
источник реализует клиентов, пагинацию, парсеры и нормализаторы с Pandera-
схемами.[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

## быстрый-старт
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

## где-искать-документацию
- [docs/INDEX.md](docs/INDEX.md) — единая точка входа и карта разделов.
- [docs/requirements/00-architecture-overview.md](docs/requirements/00-architecture-overview.md) — архитектура, глоссарий и диаграммы.
- [docs/requirements/03-data-sources-and-spec.md](docs/requirements/03-data-sources-and-spec.md) — источники, схемы и инварианты.
- [docs/pipelines/PIPELINES.md](docs/pipelines/PIPELINES.md) — публичные контракты пайплайнов и примеры конфигов.
- [docs/cli/CLI.md](docs/cli/CLI.md) и [docs/qc/QA_QC.md](docs/qc/QA_QC.md) — CLI и гарантии качества.

## поддерживаемые-источники-данных-и-статус
### первичные-пайплайны
| Пайплайн | Основной источник | Целевые сущности | Статус | Примечания |
| --- | --- | --- | --- | --- |
| `activity` | ChEMBL Activity API | Активности | Stable | Бэтч-запросы по `activity_id` с fallback-записями.[ref: repo:src/bioetl/sources/chembl/activity/pipeline.py@test_refactoring_32]
| `assay` | ChEMBL Assay API | Ассайы | Stable | Гармонизация BAO и Pandera-проверки схем.[ref: repo:src/bioetl/sources/chembl/assay/pipeline.py@test_refactoring_32]
| `target` | ChEMBL Target API | Таргеты + обогащение UniProt/IUPHAR | Stable | Многоступенчатый энрихмент, материализация gold-слоя.[ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32]
| `document` | ChEMBL Document API | Документы | Stable | Режимы `chembl` и `all` с внешними адаптерами.[ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32]
| `testitem` | ChEMBL Molecule API | Тест-айтемы | Stable | Обогащение PubChem по CID и синонимам.[ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32]
| `pubchem` | PubChem PUG-REST | Свойства молекул | Stable | Standalone dataset для энрихмента молекул.[ref: repo:src/bioetl/sources/pubchem/pipeline.py@test_refactoring_32]
| `gtp_iuphar` | Guide to Pharmacology | Таргеты и классификации | Beta | Пагинация по страницам и дополнительные таблицы классификаций.[ref: repo:src/bioetl/sources/iuphar/pipeline.py@test_refactoring_32]
| `uniprot` | UniProt REST | Белковые записи | Beta | Используется как самостоятельный энрихер и в таргет-пайплайне.[ref: repo:src/bioetl/sources/uniprot/pipeline.py@test_refactoring_32]

### внешние-адаптеры
| Адаптер | API | Использование | Лимиты/аутентификация | Статус |
| --- | --- | --- | --- | --- |
| PubMed | E-utilities (`efetch`, `esearch`) | Документ-пайплайн | `tool`, `email`, `api_key`; 3 req/sec без ключа.[ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32]
| Crossref | REST `/works` | Документ-пайплайн | `mailto` в User-Agent, 2 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
| OpenAlex | REST `/works` | Документ-пайплайн | `mailto`, 10 req/sec конфигом.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
| Semantic Scholar | Graph API `/paper/batch` | Документ-пайплайн | API key опционален, 1 req/1.25s без ключа.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
| UniProt ID Mapping | REST `/idmapping` | Таргет-пайплайн | Квота 2 req/sec, кэширование включено.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]
| IUPHAR | `/targets`, `/targets/families` | Таргет и GTP-IUPHAR пайплайны | `x-api-key`, 6 req/sec.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]

## лицензия-и-обратная-связь
Проект распространяется по лицензии MIT. Вопросы и предложения по качеству
данных и пайплайнам направляйте через issues или PR, соблюдая правила из
PROJECT_RULES.md и USER_RULES.md. Для срочных вопросов используйте e-mail,
указанный в `CROSSREF_MAILTO`/`OPENALEX_MAILTO` в локальной конфигурации.
