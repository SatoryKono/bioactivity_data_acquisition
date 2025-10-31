# 1. Область действия и принципы

Объект: унифицированные пайплайны `src/bioetl/pipelines/*.py`, Typer-CLI (`src/bioetl/cli/main.py`) и вспомогательные команды в `src/scripts/`. Пайплайны строятся на общих слоях `core/`, `normalizers/`, `schemas/` и используют конфигурации `src/bioetl/configs/pipelines/*.yaml`. [ref: repo:src/bioetl/pipelines/base.py@HEAD] [ref: repo:src/bioetl/cli/main.py@HEAD] [ref: repo:src/bioetl/configs/pipelines/activity.yaml@HEAD]

Документ опирается на нормативы из `refactoring/MODULE_RULES.md` и детализирует шаги миграции к целевой архитектуре.

**Принцип единообразия**: каждый пайплайн предоставляет один публичный класс (экспорт через `__all__`) и регистрируется в Typer-реестре. Структура ввода/вывода, набор QC-метрик и материализация едины (см. §4–§6). [ref: repo:src/scripts/__init__.py@HEAD]

**Детерминизм вывода**: сортировка и порядок колонок фиксированы, запись артефактов атомарна (`temp` → `os.replace`). [ref: repo:src/bioetl/core/output_writer.py@HEAD]

**Строгая валидация**: Pandera-схемы используются на входе и выходе, конфигурации валидируются Pydantic-моделями. [ref: repo:src/bioetl/schemas/__init__.py@HEAD] [ref: repo:src/bioetl/config/models.py@HEAD]

**Backoff/Retry**: сетевые вызовы проходят через `UnifiedAPIClient` с учётом Retry-After и экспоненциального бэкоффа. [ref: repo:src/bioetl/core/api_client.py@HEAD]

# 2. Цели унификации (проверяемые требования)

- **MUST**: все пайплайны используют `PipelineBase` и общий `UnifiedOutputWriter`.
- **MUST**: CLI-реестр (`scripts.PIPELINE_COMMAND_REGISTRY`) содержит единственный источник правды для команд, тесты проверяют его целостность.
- **MUST**: конфигурации лежат в `src/bioetl/configs/pipelines/` и валидируются при запуске.
- **SHOULD**: каждый пайплайн имеет golden-тесты и покрытие основными QC-проверками.
- **MUST NOT**: прямые зависимости `core/` или `normalizers/` от `pipelines/`.

# 3. Каталог пайплайнов и сервисных модулей

| Модуль | Назначение | Тестовое покрытие | Конфиг/артефакты |
|--------|------------|-------------------|------------------|
| `pipelines/activity.py` | Экстракция и запись активности ChEMBL. [ref: repo:src/bioetl/pipelines/activity.py@HEAD] | `tests/golden/test_cli_golden.py::test_activity` | `src/bioetl/configs/pipelines/activity.yaml` |
| `pipelines/assay.py` | Управление материализацией ассайев ChEMBL. [ref: repo:src/bioetl/pipelines/assay.py@HEAD] | `tests/golden/test_cli_golden.py::test_assay` | `src/bioetl/configs/pipelines/assay.yaml` |
| `pipelines/target.py` | Синхронизация таргетов (ChEMBL, UniProt, IUPHAR). [ref: repo:src/bioetl/pipelines/target.py@HEAD] | `tests/golden/test_cli_golden.py::test_target` | `src/bioetl/configs/pipelines/target.yaml` |
| `pipelines/document.py` | Публикации и внешнее обогащение. [ref: repo:src/bioetl/pipelines/document.py@HEAD] | `tests/golden/test_cli_golden.py::test_document` | `src/bioetl/configs/pipelines/document.yaml` |
| `pipelines/testitem.py` | Тест-айтемы и идентификаторы PubChem. [ref: repo:src/bioetl/pipelines/testitem.py@HEAD] | `tests/golden/test_cli_golden.py::test_testitem` | `src/bioetl/configs/pipelines/testitem.yaml` |
| `pipelines/document_enrichment.py` | Стадии enrichment для документов. [ref: repo:src/bioetl/pipelines/document_enrichment.py@HEAD] | `tests/pipelines/test_materialization_manager.py::test_document_enrichment` | Использует `document.yaml` |
| `pipelines/target_gold.py` | Golden-наборы таргетов и материализация. [ref: repo:src/bioetl/pipelines/target_gold.py@HEAD] | `tests/pipelines/test_materialization_manager.py::test_target_gold` | Делегирует `target.yaml` |

Карта CLI ↔ пайплайн отражена в `scripts/__init__.py` и поддерживается golden-тестами. [ref: repo:src/scripts/__init__.py@HEAD] [ref: repo:tests/unit/test_cli_contract.py@HEAD]

# 4. Единый публичный API

```python
# [ref: repo:src/bioetl/pipelines/base.py@HEAD]
class PipelineBase(Protocol):
    def extract(self) -> Iterable[dict]: ...
    def normalize(self, rows: Iterable[dict]) -> Iterable[dict]: ...
    def validate(self, rows: Iterable[dict]) -> Iterable[dict]: ...
    def write(self, rows: Iterable[dict]) -> "WriteResult": ...
    def run(self) -> "RunResult": ...
```

Обязательные компоненты:

- `UnifiedOutputWriter` с атомарной записью и учётом QC. [ref: repo:src/bioetl/core/output_writer.py@HEAD]
- `MaterializationManager` для golden-наборов. [ref: repo:src/bioetl/core/materialization.py@HEAD]
- `UnifiedLogger` и `update_summary_metrics` для структурированных отчётов. [ref: repo:src/bioetl/core/logger.py@HEAD] [ref: repo:src/bioetl/utils/qc.py@HEAD]

# 5. Поддерживающие слои

- **Core**: клиенты, writer, материализация, фабрики (`src/bioetl/core/*`). [ref: repo:src/bioetl/core/__init__.py@HEAD]
- **Normalizers**: чистые преобразования без IO (`src/bioetl/normalizers/*`). [ref: repo:src/bioetl/normalizers/__init__.py@HEAD]
- **Schemas**: Pandera-схемы и реестр (`src/bioetl/schemas/*`). [ref: repo:src/bioetl/schemas/__init__.py@HEAD]
- **Config**: модели и доступ к путям (`src/bioetl/config/models.py`, `src/bioetl/config/paths.py`). [ref: repo:src/bioetl/config/paths.py@HEAD]
- **CLI**: Typer-приложение (`src/bioetl/cli/main.py`) и вспомогательные скрипты (`src/scripts/run_*.py`). [ref: repo:src/scripts/run_activity.py@HEAD]
- **Тесты**: `tests/unit`, `tests/pipelines`, `tests/golden` с общей фикстурой CLI. [ref: repo:tests/golden/test_cli_golden.py@HEAD]

# 6. Backlog проверяемых действий

| Приоритет | Область | Действия | Ответственный артефакт |
|-----------|---------|----------|------------------------|
| P0 | CLI & Registry | Синхронизировать `PIPELINE_COMMAND_REGISTRY` с фактическими пайплайнами, проверить help/описания. | `src/scripts/__init__.py` |
| P0 | Конфиги | Провести аудит плейсхолдеров секретов (`${VAR}`) и задокументировать обязательные переменные. | `src/bioetl/configs/pipelines/*.yaml` |
| P1 | Schemas | Сверить Pandera-схемы с фактическим выводом golden-файлов, добавить недостающие проверки типов. | `src/bioetl/schemas/*` |
| P1 | QC | Расширить отчёты `update_summary_metrics` новыми метриками покрытия источников. | `src/bioetl/utils/qc.py` |
| P2 | Docs | Обновить `docs/requirements/PIPELINES.inventory.csv` ссылками на новые тесты/конфиги. | `docs/requirements/PIPELINES.inventory.csv` |
| P2 | Tooling | Подготовить шаблон добавления пайплайна (cookiecutter/документация). | `refactoring/templates/pipeline.md` (создать) |

# 7. Этапы внедрения

1. **Stabilize**: зафиксировать текущую раскладку модулей и Typer-реестр (P0 задачи). Проверить CI (`tests/golden/test_cli_golden.py`).
2. **Harmonize**: выровнять схемы и QC, внедрить обязательные плейсхолдеры секретов (P1 задачи).
3. **Expand**: добавить шаблон создания пайплайна, обновить документацию и инвентарь (P2 задачи). Закрепить регламент линк-чека.

Каждый этап завершается обзором ссылок (`python scripts/link_check.py`) и smoke-запуском ключевых CLI-команд.

# 8. Тестовая стратегия и контроль качества

- Юнит-тесты: `pytest tests/unit` (CLI, API клиенты, нормализаторы).
- Интеграционные/golden: `pytest tests/golden` — проверка CLI и артефактов.
- Пайплайновые проверки: `pytest tests/pipelines` — материализация, QC, линейдж.
- Статический анализ: `ruff`, `mypy` (обязательные в pre-commit).
- Link-check: запускать после обновления документации (см. §9).

# 9. Документация и контроль ссылок

- Обновлять `refactoring/MODULE_RULES.md` и `docs/requirements/PIPELINES.inventory.csv` при добавлении пайплайна.
- После правок документации выполнять линк-чек (`python -m scripts.link_check` или аналогичный скрипт, см. commit history) для проверки путей.
- Поддерживать README/CLI help в актуальном состоянии, синхронизируя описания с `PIPELINE_COMMAND_REGISTRY`.

# 10. Риски и смягчающие меры

| Риск | Последствие | Митигирующие действия |
|------|-------------|------------------------|
| Расхождение Typer-реестра и CLI | Команды недоступны или с неверным help | Автотест `tests/unit/test_cli_contract.py`, ручной аудит перед релизом |
| Несогласованность схем Pandera | Ошибки валидации на продакшене | Golden-тесты + сравнение схем с актуальными CSV |
| Утечка секретов в конфиге/логах | Компрометация API ключей | Использование `${VAR}` плейсхолдеров, фильтры логгера (`core/logger.py`) |
| Недетерминированный вывод | Сложность регрессионного анализа | Детерминизм writer + сортировка колонок в конфиге |

