# 11 Naming Policy

This document codifies the canonical naming policy for the `bioetl` repository.
It extends the [Naming Conventions](00-naming-conventions.md) guide with
mandatory rules that cover the full project surface, including packages,
modules, tests, documentation, and registry keys.

## Top-Level Directory Layout

The source tree **MUST** follow the `src/<package>/` structure with the expected
domain partitions:

- `api/`, `cli/`, `clients/`, `core/`, `pipelines/`, `schemas/`, `utils/`,
  `tools/`, `config/`, `qc/`.

Inside `pipelines/`, the hierarchy **MUST** remain three levels deep using
`<provider>/<entity>/<stage>.py`, where:

- `provider` ∈ {`chembl`, `pubchem`, `uniprot`, `iuphar`, `pubmed`, `openalex`,
  `crossref`, `semanticscholar`}
- `entity` ∈ {`assay`, `activity`, `target`, `document`, `testitem`}
- `stage` ∈ {`extract`, `transform`, `validate`, `write`, `run`}

Tests **MUST** mirror the pipeline tree:
`tests/bioetl/<package>/.../test_<module>.py`.

## File Naming Policy

### Общие правила

**Формат по умолчанию**: `<layer>_<domain>_<action>[_<provider>].py`

- `layer` ∈ {`api`, `cli`, `client`, `service`, `repo`, `schema`, `model`,
  `utils`, `pipeline`}
- `domain` — каноническое имя сущности (единственное число, `snake_case`)
- `action` ∈ {`extract`, `transform`, `validate`, `write`, `run`, `sync`,
  `normalize`, `resolve`, `map`}
- `provider` — дополнительный суффикс в нижнем регистре

### Специализация для схем

- Публичные схемы источников **MUST** называться
  `<provider>_<entity>_schema.py`, где `provider` соответствует источнику
  (например, `chembl`), а `entity` ∈ {`activity`, `assay`, `document`, `target`,
  `testitem`}.
- Энричмент-схемы и вспомогательные валидаторы **MUST** использовать
  `<entity>_enrichment_schema.py` и располагаться в `src/bioetl/schemas/`.
- Любые новые схемы **MUST** документировать отклонения в `rename_map.json` и
  `docs/styleguide/VIOLATIONS_TABLE.md`.

### Ограничения

Запрещены пробелы, дефисы и CamelCase в именах файлов. Документация **MUST**
использовать `NN-topic-name.md`, например `09-document-chembl-extraction.md`.

## Object Naming Policy

- Classes — `PascalCase`; suffixes: `Client`, `Service`, `Repository`,
  `Validator`, `Normalizer`, `Writer`
- Base classes and interfaces — `*Base`, `*ABC`; exceptions — `*Error`
- Data structures — suffix `*Model`, `*Record`, `*Config`
- Functions/methods — verb-based `snake_case`, e.g. `load_config`,
  `create_session`, `normalize_row`, `validate_payload`
- Predicates use `is_`, `has_`, `should_`
- Factories use `build_`, `create_`, `make_`
- I/O helpers use `load_`, `save_`
- Private helpers use `_name`

## Regular Expressions

```regex
PACKAGE/MODULE: ^[a-z][a-z0-9_]*$
TEST MODULE:    ^test_[a-z0-9_]+\.py$
CLASS:          ^[A-Z][A-Za-z0-9]*$
FUNCTION:       ^[a-z][a-z0-9_]*$
PRIVATE NAME:   ^_[a-z][a-z0-9_]*$
CONST:          ^[A-Z][A-Z0-9_]*$
```

## Directory → File Type Matrix

| Directory | File Types               | Expected Patterns                                                                      |
| --------- | ------------------------ | -------------------------------------------------------------------------------------- |
| api       | `.py`                    | `^api_.*\.py$`                                                                         |
| cli       | `.py`                    | `^cli_.*\.py$`                                                                         |
| clients   | `.py`                    | `^.*Client\.py$` for classes, `client_.*\.py` for modules                              |
| core      | `.py`                    | No prefixes; base classes use `*Base`, `*ABC`                                          |
| pipelines | `.py`                    | `<provider>/<entity>/<stage>.py`                                                       |
| schemas   | `.py`, `.yaml`, `.yml`   | `<provider>_<entity>_schema.py`, `<entity>_enrichment_schema.py`, `.*_schema\.(ya?ml)` |
| utils     | `.py`                    | `.*_utils\.py`                                                                         |
| tests     | `.py`                    | `test_.*\.py`                                                                          |
| config    | `.yaml`, `.yml`, `.json` | `^[a-z0-9_]+\.(yaml&#124;yml&#124;json)$`                                              |
| docs      | `.md`                    | `\d{2}-.*\.md`                                                                         |

## Enforcement

- Конфигурация `ruff` и `flake8` включает правило `N` (`pep8-naming`), а
  whitelist для dunder-хуков и защищённых структур (`_ClassName`, `_helper`)
  оформлен через `ignore-names` и `ignore-class-names`.
- CLI-команда `bioetl-validate-naming-violations` проверяет, что
  `docs/styleguide/VIOLATIONS_TABLE.md` пуст и тем самым блокирует попадание
  новых нарушений.
- Любое подтверждённое исключение описывается в таблице нарушений и в
  `12-naming-normalization-plan.md` с указанием владельца и срока устранения.

## DRY and Deduplication

- Shared utilities belong in `utils/` (e.g., `io_utils.py`, `string_utils.py`,
  `retry_utils.py`)
- Unified HTTP/DB clients reside in `clients/` with the shared `HttpClientBase`
- Validation schemas live in `schemas/` and reuse the unified validator
- Before creating a new helper, search for existing alternatives; justify any
  unavoidable duplication in the PR description
- Standard argument names: `session`, `logger`, `config`, `run_id`, `limit`,
  `offset`, `timeout`

## Registry Key Naming

Strategy registry keys **MUST** use `<provider>:<entity>:<stage>` pointing to
the concrete class implementation, e.g. `chembl:assay:extract`.
