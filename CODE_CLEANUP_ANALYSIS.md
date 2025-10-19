# CODE CLEANUP ANALYSIS

Дата: 2025-10-19

## Обзор
Консервативный аудит кодовой базы, направленный на поиск и устранение явно мёртвого кода: неиспользуемые импорты, закомментированные блоки, ссылки на удалённые модули. Обратная совместимость сохранена.

## Итоги статического анализа (ruff F401)
Выявлены и устранены неиспользуемые импорты:

- `src/library/__init__.py`: удалён импорт `ExitCode`
- `src/library/assay/pipeline.py`: удалён импорт `numpy as np`
- `src/library/cli/__init__.py`: удалены неиспользуемые импорты из `library.testitem.config` (`ALLOWED_SOURCES`→`TESTITEM_ALLOWED_SOURCES`, `DEFAULT_ENV_PREFIX`→`TESTITEM_DEFAULT_ENV_PREFIX`, `ConfigLoadError`→`TestitemConfigLoadError`, `TestitemConfig`), а также `TestitemPipelineError`, `read_testitem_input` из `library.testitem.pipeline`
- `src/library/clients/circuit_breaker.py`: удалён неиспользуемый `ABC`
- `src/library/clients/health.py`: удалены `Literal`, `typer` (не использовались)
- `src/library/utils/logging.py`: удалены `logging`, `cast`, `structlog` (тип `BoundLogger` сохранён)

Повторная проверка `ruff check --select F401` — нарушений нет.

## Закомментированный код
- `src/library/testitem/pipeline.py`: удалена закомментированная строка валидации Pandera (оставлен рабочий код без комментария).

## Подозрения на устаревшие модули (без удаления)
- `src/library/etl/extract.py` и `src/library/etl/run.py` используются в публичном интерфейсе (`library/__init__.py`) и CLI, поэтому удаление не выполнялось.
- `src/library/io_/normalize.py` активно используется (`etl/load.py`, а также в собственных parse- и normalize-хелперах). Исключение из coverage указывает на вспомогательный характер, но модуль актуален.
- `src/library/utils/logging.py` и `src/library/logger.py` помечены как DEPRECATED, однако оставлены для обратной совместимости; при этом их функции проксируют на `library.logging_setup`.

## Дублирование (выявлено, но без рефакторинга в рамках консервативного цикла)
- Создание клиентов: `_create_api_client` (activity/assay), `_create_chembl_client`/`_create_pubchem_client` (testitem).
- Расчёт хешей/чек-сумм: присутствуют аналоги в `activity/pipeline.py`, `assay/pipeline.py`, `testitem/pipeline.py`.
- Нормализация полей в `testitem/pipeline.py` локально дублирует идеи из общих нормализаторов; перенос в общий модуль отложен.

## Рекомендации (следующие итерации, без breaking changes)
- Выделить общий конструктор клиентских конфигураций (timeout/headers/retries/rate_limit) в утилиту.
- Консолидировать расчёты хешей в утилитарный модуль с единым контрактом.
- Постепенно переводить легаси-логирование на `logging_setup`, сохраняя шимирующие функции для обратной совместимости.
- При необходимости расширить покрытие `io_/normalize.py` и вернуть в coverage.

## Проверки
- Линтинг: `ruff F401` — пройдено.
- Билд/тесты: рекомендуется локально запустить `pytest` с исключением `integration/network/slow` при необходимости.


