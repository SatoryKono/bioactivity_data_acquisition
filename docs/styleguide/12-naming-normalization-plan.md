# Naming Normalization Plan

## Scope And Inputs

- Основания: `00-naming-conventions.md`, `11-naming-policy.md`, `docs/styleguide/VIOLATIONS_TABLE.md`.
- Цель: привести имена модулей, классов, функций и констант к обязательным паттернам, сохранив детерминизм, схемы Pandera и публичные контракты.
- Охват: весь пакет `bioetl`, включая CLI, клиенты, пайплайны, схемы, утилиты, инструменты QC и документацию.

## Обязательные Требования

- Модули: `snake_case`, без CamelCase/пробелов; допускается только `__init__.py`.
- Классы и протоколы: `PascalCase`, суффиксы `Client`, `Service`, `Validator`, `Protocol` и т.д.
- Функции/методы: `snake_case`; приватные допускают префикс `_`; dunder-методы считаются допустимыми.
- Константы: `UPPER_SNAKE_CASE`; приватные константы — `_UPPER_SNAKE_CASE`.
- Документы и артефакты: двухзначный префикс `NN-topic-name.md`.
- Любые допущенные исключения должны быть зафиксированы в линтере и документации.

## Категории Нарушений

- **Модули** — ложные срабатывания на `__init__.py`; подтвердить корректность инструмента и исключить файлы из отчёта.
- **Функции** — основная масса `_name` и dunder; требуется whitelisting. Фактические нарушения: приватные константы, методы без dunder.
- **Классы/протоколы** — привести `_TargetEntityConfigDict`, `_RetryState`, `_QueryProtocol` и подобные к `PascalCase`, при необходимости сделать их защищёнными через `_` + Pascal.
- **Константы** — поправить `msg`, `actor`, `pages`, `params`, `success_count`, `chunk_size` и т.д. на верхний snake; проверить side-effects на логгеры, метрики и golden-файлы.

## Чек-листы По Директориям

- **`src/bioetl/cli`**
  - Константы `msg`, структурные счётчики → `MSG`, `SUCCESS_COUNT`.
  - Согласовать Typer-команды и help-тексты после переименований.
  - Убедиться, что реестр команд (`cli/registry.py`) обновлён.
- **`src/bioetl/clients`**
  - Привести счётчики/поля (`records_fetched`, `chunk_size`, `msg`) к верхнему snake.
  - Классы `_TargetEntityConfigDict` и аналогичные → `TargetEntityConfigDict`.
  - Обновить фабрику `core/client_factory.py` и потребителей.
- **`src/bioetl/config`**
  - Константы `msg`/`actor` и производные → верхний snake.
  - Функции `_normalize_*` оставить, задокументировав допустимость приватных хелперов.
  - Проверить влияние на загрузчик конфигов и unit-тесты.
- **`src/bioetl/core`**
  - `_RetryState` → `RetryState`, другие структуры привести к Pascal.
  - Константы таймеров/счётчиков (`waited`, `attempt`) → верхний snake.
  - Обновить unit-тесты API-клиента, ensure deterministic logging.
- **`src/bioetl/pipelines`**
  - Массово обновить константы статуса: `actor`, `pages`, `params`, `success_count`, `fallback_count`, `error_count`, `cache_hits`, `api_calls`, `batch_size` и т.д.
  - Синхронизировать Pandera-схемы, golden-файлы и QC-отчёты с новыми именами.
  - Проверить Typer-команды `/run_*` и meta.yaml генерацию.
- **`src/bioetl/schemas`**
  - Константы и проверки (`msg`, `valid`, `_default_vocab_path`) → верхний snake.
  - Задокументировать приватные функции `_is_valid_*` как допустимые.
- **`src/bioetl/tools`**
  - Константы `pattern`, `page_count`, `total_removed` → верхний snake.
  - Протоколы `_QueryProtocol`, `_ResourceProtocol` → `QueryProtocol`, `ResourceProtocol`.
  - Провести регенерацию отчётов, если имена участвуют в выходных данных.

## Инфраструктура Контроля

- Обновить конфиг `ruff`/`flake8-naming` для разрешения:
  - dunder-методов (`__init__`, `__iter__`, `__call__`).
  - приватных хелперов `_snake_case`.
- Добавить проверку отчёта `docs/styleguide/VIOLATIONS_TABLE.md` в CI:
  - скрипт сравнивает фактические нарушения с ожидаемым пустым списком.
- Расширить pre-commit: запуск пользовательской проверки, обеспечивающей отсутствие новых нарушений.
- Документация:
  - Обновить `00-naming-conventions.md` (раздел исключений) и ссылку на данный план.
  - Добавить ссылку в onboarding/README для новых контрибьюторов.

## Контроль Качества И Детерминизм

- После переименований выполнить `ruff`, `black`, `isort`, `mypy --strict`, `pytest --no-network`, `pandera` проверки.
- Перегенерировать golden-файлы и таблицы только при детерминизме; зафиксировать хеши `blake2` в meta.yaml.
- Прогнать `schema_guard`, `run_test_report`, `determinism_check`.
- Вручную подтвердить стабильность ключей бизнес-логики (ChEMBL ID, DOI, и т.д.).

## Последовательность Выполнения

1. Обновить линтеры и документацию по исключениям.
2. Выполнить переименования по директориям (CLI → clients → config → core → pipelines → schemas/tools).
3. Синхронизировать тесты, golden-артефакты, meta.yaml/QC.
4. Запустить полный набор проверок и зафиксировать результаты.

## Трекинг И Отчётность

- Вести чек-лист прогресса по директориям в issue/PR.
- Приложить diff `docs/styleguide/VIOLATIONS_TABLE.md`, демонстрирующий отсутствие нарушений.
- Документировать отклонения или утверждённые исключения непосредственно в этом плане и `CHANGELOG.md`.
