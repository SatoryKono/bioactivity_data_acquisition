# Отчёт по очистке репозитория

## Обзор

- Цель: убрать кэши/артефакты из Git, настроить LFS/pre-commit/CI и унифицировать конфиги
- История Git не переписывалась
- Исключения: директория `data/` не изменялась

## Изменения

- .gitignore: добавлены OS/IDE, test outputs, site/, временные, dist/
- Git LFS: включены паттерны `*.parquet`, `*.pkl`, `*.xlsm`, `*.png`, `*.jpg`
- pre-commit: check-added-large-files (500 КБ), блокировка артефактов `logs/` и `reports/`
- CI: загрузка artifacts — test outputs, coverage, bandit отчёт
- Конфиги: `config/postprocess_documents.example.yaml` → `configs/`
- Документация: обновлён `docs/how-to/contribute.md`, `docs/changelog.md`, чек-лист `docs/CHECKLIST_postprocess_documents.md`
- Архивные доки: перенесены в ветку `archive/internal-reports` и удалены из ветки чистки

## Удалено из Git (оставлено локально)

- `src/__pycache__/`, `tests/__pycache__/`
- `temp_added_files.txt`, `temp_files.txt`
- `CLI_INTEGRATION_SNIPPET.py`
- `logs/app.log`
- `tests/test_outputs/*.csv`, `tests/test_outputs/*.json`

## Добавлено

- `.gitattributes`, `.pre-commit-config.yaml`
- `.github/workflows/ci.yaml`, `.github/workflows/docs.yml`
- `tests/test_outputs/.gitkeep`
- `scripts/generate_cleanup_manifest.py`, `CLEANUP_MANIFEST.json`

## Манифест

Сгенерирован `CLEANUP_MANIFEST.json` с перечнем крупных файлов (>500 КБ), логов, временных, test outputs и pycache.

## Последующие шаги

- Обновить README разделом о CI artifacts при необходимости
- Запустить локально: `pre-commit run --all-files`, `pytest -v`, `mkdocs build --strict`
- Создать PR из ветки `cleanup/repository-cleanup`
