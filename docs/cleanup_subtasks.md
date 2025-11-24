# Подзадачи по чистке кода и ссылки на запуск

Ниже — разбивка работ по удалению мёртвого/устаревшего кода и консолидации дубликатов. Каждая подзадача включает рекомендуемые команды для проверки.

## Этап 1. Безопасные удаления
- Выполнено: удалены учебные и неиспользуемые модули (`src/bioetl/utils/dead_code_example.py`) и локальные декораторы (`deprecated` в `src/bioetl/utils/deprecation.py`).
- Рекомендуемые проверки после правок:
  - `make lint`
  - `make typecheck`
  - `pytest tests/utils tests/core`

## Этап 2. Обновление внутренних импортов и депрекейт-шимов
- Выполнено: добавлены `DeprecationWarning` в `bioetl.pipelines.errors` и `bioetl.pipelines.common`, тесты переведены на прямой импорт `bioetl.utils.ensure_directory`.
- Следующий шаг: заменить внутренние импорты на `bioetl.core.pipeline.errors` там, где ещё остались ссылки на shim (если появятся в будущих правках).
- Проверки:
  - `pytest tests/pipelines`
  - `python -m bioetl.cli.cli_app list`

## Этап 3. Консолидация клиентского слоя
- Сделано: пайплайны ChEMBL переводятся на фабрику `bioetl.infrastructure.clients.default_chembl_factory`, которая проксирует HTTP-клиент и сущностные адаптеры через единый протокол.
- Следующий шаг: обновить оставшиеся вызовы, которые напрямую создают `ChemblEntityClientFactory`, и при необходимости добавить предупреждения в старые импорт-пути `bioetl.clients`.
- Проверки:
  - `pytest tests/clients tests/pipelines`
  - Smoke-запуск типового пайплайна, например: `python -m bioetl.cli.cli_app run chembl --dry-run`

## Этап 4. Завершение депрекейтов
- Выполнено: удалены `_legacy_init.py`, `bioetl.pipelines.errors` и `bioetl.pipelines.common` после периода поддержки.
- Проверки:
  - `make qa`
  - Smoke CLI: `bioetl --help`

## Общие рекомендации по откату
- Каждый этап оформлять отдельным коммитом/PR; при проблемах выполнить `git revert <commit>` соответствующего этапа.
- Перед удалением публичных путей фиксировать `DeprecationWarning` и упоминание в CHANGELOG.
