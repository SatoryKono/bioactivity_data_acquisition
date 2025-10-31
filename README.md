# BioETL

## что-это-и-зачем

BioETL — каркас для детерминированного извлечения биоактивностных данных из ChEMBL и внешних источников. Архитектура строится на композиции `PipelineBase`, унифицированных HTTP-клиентов, Pandera-схем и атомарной системы записи ([ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32], [ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32]).

## быстрый-старт

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# заполните переменные окружения
cp .env.example .env
# отредактируйте .env и загрузите удобным для вас способом (source/. env, setx и т.д.)

# прогон ограниченного пайплайна
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --input data/input/activity.csv \
  --output data/output/activity --sample 10 --dry-run
```

Минимальные переменные окружения описаны в `.env.example` (PubMed, Crossref, OpenAlex, Semantic Scholar, IUPHAR). Отсутствие обязательного ключа приводит к ошибке загрузки конфига.

## где-искать-документацию

- `docs/INDEX.md` — оглавление и глоссарий.
- `docs/requirements/00-architecture-overview.md` — уровни системы и потоки данных.
- `docs/requirements/03-data-sources-and-spec.md` — требования к источникам, бизнес-ключи и дедуп.
- `docs/pipelines/PIPELINES.md` — публичные контракты шагов `extract/normalize/validate/write/run`.
- `docs/configs/CONFIGS.md` — профили, наследование, инварианты конфигураций.
- `docs/cli/CLI.md` — команды Typer, общие флаги и примеры запуска.
- `docs/qc/QA.md` — тестовые контуры, golden-наборы и проверки документации.

## поддерживаемые-источники

| Команда | Сущность | Основные источники | Статус | Примечания |
| --- | --- | --- | --- | --- |
| `activity` | Активности | ChEMBL | production | Fallback-записи и QC по количеству/доле замен. |
| `assay` | Ассайы | ChEMBL + BAO | production | Нормализация категорий BAO. |
| `target` | Мишени | ChEMBL + UniProt + IUPHAR | production | Профиль `--mode smoke` для быстрой проверки. |
| `testitem` | Молекулы | ChEMBL + PubChem | production | Слияние солей/parent, QC на дубли. |
| `document` | Документы | ChEMBL + PubMed + Crossref + OpenAlex + Semantic Scholar | production | Merge-приоритеты и источники фиксируются в колонках `*_source`. |
| `pubchem` | Энрихмент PubChem | PubChem PUG REST | beta | Используется для вспомогательных таблиц. |
| `gtp_iuphar` | Классификации | Guide to Pharmacology | beta | Требует `IUPHAR_API_KEY`. |
| `uniprot` | Белковые аннотации | UniProt REST | beta | Применяется как обогащение для `target`. |

## лицензия-и-обратная-связь

- Лицензия: MIT (см. [ref: repo:pyproject.toml@test_refactoring_32]).
- Обратная связь и вопросы: создавайте issue в репозитории или используйте почту, указанную в `.env.example` для контактных лиц.
- Изменения публичных контрактов **MUST** сопровождаться записью в `CHANGELOG.md`.

