# Changelog

## [Unreleased]

### Breaking

- CLI и внешние интеграции должны импортировать обёртки из `bioetl.cli.app`; все
  ссылки на модуль `scripts` и корневой пакет считаются устаревшими и больше не
  поддерживаются.

### Added

- Создана структура [`docs/requirements/`][ref: repo:docs/requirements/@test_refactoring_32] для референтной документации:
  - [`00-architecture-overview.md`][ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32] — архитектура уровней, поток данных, компоненты, глоссарий
  - [`03-data-sources-and-spec.md`][ref: repo:docs/requirements/03-data-sources-and-spec.md@test_refactoring_32] — референтный документ по источникам, сущностям, бизнес-ключам

- Полное обновление документации: навигация, архитектура и спецификации данных в [`docs/INDEX.md`][ref: repo:docs/INDEX.md@test_refactoring_32],
  [`docs/pipelines/PIPELINES.md`][ref: repo:docs/pipelines/PIPELINES.md@test_refactoring_32],
  [`docs/configs/CONFIGS.md`][ref: repo:docs/configs/CONFIGS.md@test_refactoring_32],
  [`docs/cli/CLI.md`][ref: repo:docs/cli/CLI.md@test_refactoring_32] и
  [`docs/qc/QA_QC.md`][ref: repo:docs/qc/QA_QC.md@test_refactoring_32].

- Обновлён README с кратким описанием, быстрым стартом и статусом источников ([ref: repo:README.md@test_refactoring_32]).

- Все внутренние ссылки на код унифицированы в формате `[ref: repo:path@test_refactoring_32]`.

- Расширены примеры команд в CLI-документации для всех пайплайнов.

### Changed

- Материалы из [`docs/architecture/refactoring/`][ref: repo:docs/architecture/refactoring/@test_refactoring_32] консолидированы в [`docs/requirements/`][ref: repo:docs/requirements/@test_refactoring_32];
  файлы в `refactoring/` оставлены как указатели для обратной совместимости линк-чекера.

- Обновлены ссылки в README на новые разделы `requirements/` вместо `architecture/`.

### Quality

- Документация проходит markdownlint и link-check локально, отчёты без ошибок.
