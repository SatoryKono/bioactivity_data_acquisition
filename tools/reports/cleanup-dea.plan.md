<!-- c7755aec-6a28-4bf6-94fc-e1d4c8104e4e 955fbf9e-b199-40d8-9de2-f4422e88f29c -->
# План очистки репозитория test_refactoring_32

## Источники истины

- Ветка: `test_refactoring_32` репозитория `SatoryKono/bioactivity_data_acquisition`
- Все ссылки: `[ref: repo:<path>@test_refactoring_32]`

## Обнаруженные проблемы

### 1. Временные файлы в корне (подлежат удалению)

- `md_scan*.txt` (7 файлов) — результаты markdown-линтера
- `coverage.xml` — артефакт pytest-cov (должен быть в .gitignore)
- `md_report.txt` — временный отчёт

### 2. Устаревшие отчёты в docs/ (архивировать)

**Файлы для переноса в `docs/reports/archived/2025-11-01/`:**

- `SCHEMA_SYNC_PROGRESS.md` — пустой файл
- `SCHEMA_SYNC_*.md` (7 файлов) — промежуточные отчёты миграции схем
- `FINAL_*.md` (4 файла) — отчёты о завершении работ
- `PROGRESS_SUMMARY.md` — промежуточная сводка
- `RUN_RESULTS_SUMMARY.md` — результаты прогонов
- `DOCUMENT_PIPELINE_VERIFICATION.md` — верификация пайплайна
- `COMPLETED_IMPLEMENTATION.md` — отчёт о реализации
- `implementation-status.md`, `implementation-examples.md` — промежуточная документация
- `acceptance-criteria-document.md`, `acceptance-criteria.md` — критерии приёмки
- `assessment.md`, `gaps.md`, `pr-plan.md`, `test-plan.md` — планирование
- `REQUIREMENTS_AUDIT.md`, `REQUIREMENTS_UPDATED.md` — аудиты требований
- `RISK_REGISTER.md` — реестр рисков
- `SCHEMA_COMPLIANCE_REPORT.md`, `SCHEMA_GAP_ANALYSIS.md` — анализ схем

**Оставить в docs/ (актуальная документация из INDEX.md):**

- `INDEX.md`, `CONFIGURATION_GUIDE.md`, `ADAPTERS_USAGE.md`, `COLUMN_SOURCE_MAPPING.md`, `COMMANDS.md`, `TESTING.md`
- Подкаталоги: `architecture/`, `cli/`, `configs/`, `pipelines/`, `qc/`

### 3. Неиспользуемые импорты (автофикс)

**Найдено ruff (F401/F841):**

- `src/bioetl/cli/limits.py:8` — `pandas` не используется
- `src/bioetl/pipelines/chembl_activity.py:9` — `typing.cast` не используется
- `src/bioetl/sources/chembl/document/merge/enrichment.py:5` — `typing.Any` не используется
- `src/bioetl/sources/chembl/testitem/normalizer/dataframe.py:6` — `typing.Any` не используется
- `src/scripts/run_fix_markdown.py:56,174` — неиспользуемые переменные
- Тесты: 3 неиспользуемых импорта

**Найдено vulture:**

- `src/bioetl/config/loader.py:12` — `YamlNode` не используется
- `src/bioetl/pandera_pandas.py:36-41` — неиспользуемые переменные экспорта (regex, ge, gt, lt, coerce)
- `src/bioetl/schemas/activity.py:261`, `src/bioetl/schemas/base.py:127` — неиспользуемые переменные `instance`
- Тесты: `frozen_time`, `exc_type`, `traceback` — неиспользуемые переменные в фикстурах

### 4. Скрипт run_fix_markdown.py

Статус: содержит неиспользуемые переменные; проверить, используется ли в workflow или Makefile.

### 5. Патчи в docs/patches/

5 файлов *.patch — исторические патчи, уже применённые. Переместить в `docs/reports/archived/patches/`.

## Пошаговый план

### Этап 1: Базовая проверка (readonly)

1. Запустить тесты: `pytest tests/ -v`
2. Запустить линтер: `ruff check src/ scripts/ tests/`
3. Зафиксировать базовую метрику

### Этап 2: Удаление временных файлов

1. Удалить md_scan*.txt (7 файлов)
2. Удалить md_report.txt
3. Добавить coverage.xml в .gitignore (уже есть через htmlcov/, проверить)
4. Удалить coverage.xml

### Этап 3: Архивация устаревших отчётов

1. Создать `docs/reports/archived/2025-11-01/`
2. Переместить 30+ файлов отчётов из docs/ в архив (git mv)
3. Создать `docs/reports/archived/patches/`
4. Переместить 5 файлов *.patch из docs/patches/ в архив

### Этап 4: Автофикс неиспользуемых импортов

1. Запустить `ruff check --fix --select F401,F841 src/ scripts/ tests/`
2. Ручная проверка изменений в `src/bioetl/pandera_pandas.py` (могут быть реэкспорты)
3. Проверить `src/scripts/run_fix_markdown.py` на использование в CI/Makefile

- Если используется — исправить переменные
- Если нет — переместить в архив или удалить

### Этап 5: Проверка после изменений

1. Запустить тесты: `pytest tests/ -v`
2. Запустить линтер: `ruff check src/ scripts/ tests/`
3. Проверить CLI-команды из [ref: repo:docs/cli/CLI.md@test_refactoring_32]
4. Убедиться, что нет регрессий

### Этап 6: Обновление DEPRECATIONS.md

1. Добавить секцию "Удалено [2025-11-01]"
2. Перечислить удалённые временные файлы и архивированные отчёты
3. Указать commit hash после применения изменений

### Этап 7: Создание отчёта CLEANUP_REPORT.md

1. Создать `docs/reports/CLEANUP_REPORT.md`
2. Структура: Сводка / Методика / Перечень удалений / Безопасность
3. Указать количество удалённых/архивированных файлов
4. Перечислить все изменения с обоснованием

### Этап 8: Улучшение pre-commit хуков (опционально)

1. Проверить, есть ли в `.pre-commit-config.yaml` защита от пустых файлов
2. Добавить хук для блокировки md_scan*.txt и подобных временных файлов (если нужно)

## Риски и митигация

**Риск 1:** Файлы в `src/bioetl/pandera_pandas.py` могут быть реэкспортами для публичного API

- **Митигация:** Проверить импорты этого модуля в кодовой базе перед удалением

**Риск 2:** `run_fix_markdown.py` может использоваться в CI/скриптах

- **Митигация:** Grep по `.github/workflows/`, `Makefile`, `scripts/`

**Риск 3:** Архивированные отчёты могут ссылаться друг на друга

- **Митигация:** Проверить внутренние ссылки в архиве, обновить при необходимости

**Риск 4:** Удаление coverage.xml может сломать CI-артефакты

- **Митигация:** Проверить .github/workflows/ на использование coverage.xml

## Критерии приёмки

✅ Все тесты зелёные
✅ ruff без ошибок F401/F841
✅ CLI-команды работают
✅ Нет временных файлов в корне
✅ Устаревшие отчёты в архиве
✅ DEPRECATIONS.md обновлён
✅ CLEANUP_REPORT.md создан
✅ Ссылки в docs/INDEX.md валидны

### To-dos

- [x] Запустить тесты и линтер для фиксации базовой метрики
- [x] Проверить использование run_fix_markdown.py в CI/Makefile
- [x] Проверить реэкспорты в src/bioetl/pandera_pandas.py
- [x] Удалить временные файлы md_scan*.txt, md_report.txt, coverage.xml
- [x] Архивировать устаревшие отчёты в docs/reports/archived/
- [x] Архивировать патчи из docs/patches/
- [x] Автофикс неиспользуемых импортов через ruff --fix
- [x] Запустить тесты, линтер, проверить CLI после изменений
- [x] Обновить DEPRECATIONS.md с перечнем удалений
- [ ] Создать docs/reports/CLEANUP_REPORT.md
