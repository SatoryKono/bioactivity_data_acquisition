# Changelog

## [Unreleased]

### Added

- Полное обновление документации: навигация, архитектура и спецификации данных в

  [`docs/INDEX.md`][ref: repo:docs/INDEX.md@test_refactoring_32],
  [`docs/requirements/00-architecture-overview.md`][ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32],
  [`docs/requirements/03-data-sources-and-spec.md`][ref: repo:docs/requirements/03-data-sources-and-spec.md@test_refactoring_32],
  [`docs/pipelines/PIPELINES.md`][ref: repo:docs/pipelines/PIPELINES.md@test_refactoring_32],
  [`docs/configs/CONFIGS.md`][ref: repo:docs/configs/CONFIGS.md@test_refactoring_32],
  [`docs/cli/CLI.md`][ref: repo:docs/cli/CLI.md@test_refactoring_32] и
  [`docs/qc/QA_QC.md`][ref: repo:docs/qc/QA_QC.md@test_refactoring_32].

- Обновлён README с кратким описанием, быстрым стартом и статусом источников

  ([ref: repo:README.md@test_refactoring_32]).

- Добавлен `.pages` для генератора сайта и обновлён `.lychee.toml` для нового

  документационного набора.

- Включены pre-commit хуки markdownlint и lychee link-check

  ([ref: repo:.pre-commit-config.yaml@test_refactoring_32]).

- Описаны публичные контракты пайплайнов и обновлены правила для CONTRIBUTORS

  (PROJECT_RULES.md, USER_RULES.md).

### Changed

- Материалы из `refactoring/` заменены ссылками на актуальные файлы в `docs/`;

  файлы оставлены как указатели для обратной совместимости.

### Quality

- Документация проходит markdownlint и link-check локально, отчёты без ошибок.
