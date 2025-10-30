# De-duplication Plan

Этот документ описывает план детерминированной дедупликации для всего пайплайна.

## Guiding Principles

- Опора на канонические идентификаторы (ChEMBL ID, UniProt, PubChem)
- Чёткие бизнес-ключи и хеши BLAKE2 для производных
- Идемпотентность и трассируемость

## Strategy Overview

- Вводим слой нормализации идентификаторов
- Детерминированная сортировка строк перед операциями слияния
- Явные политики конфликтов

## Client Initialization

- Shared клиент ChEMBL и адаптеры с TTL‑кэшем и троттлингом
- Callers must persist the returned client alongside the resolved batch and

  limit metadata to honour the shared runtime contract, and tests should
  monkeypatch `_init_chembl_client` to intercept client creation in a single
  location.

## Clone Remediation Roadmap

| Clone Group | Description | Planned Action | Risks | Checks | Artifacts |
| --- | --- | --- | --- | --- | --- |
| Token-1 | `bioetl.pipelines.activity._derive_exact_data_citation` mirrors `_derive_rounded_data_citation` per `reports/token_clones.csv`. | Fold both helpers into a parameterised citation formatter to remove duplicated token sequences while preserving branch coverage. | Formatting regressions for edge rounding rules. | Extend `tests/unit/pipelines/test_activity_pipeline.py` to cover both citation paths and rerun `pytest`. | `reports/token_clones.csv`, `artifacts/module_map.json` |
| Token-2 | Identifier normaliser tests replicate fixtures according to `reports/token_clones.csv`. | Consolidate repetitive test cases into parametrised pytest fixtures to centralise canonical identifier expectations. | Reduced readability if parametrisation hides intent. | Validate with `pytest -k normalizer` and snapshot the updated expectation tables. | `reports/token_clones.csv`, `reports/semantic_clones.csv` |
| AST-1 | Config loader env-var assertions duplicated (see `reports/ast_clones.csv`). | Extract shared assertion helper for environment validation within `tests/unit/test_config_loader.py`. | Helper may mask subtly different semantics between API key and header checks. | Add regression tests that simulate missing env vars for both call sites. | `reports/ast_clones.csv`, `artifacts/import_graph.mmd` |
| Semantic-1 | Pipeline `__dir__` implementations share structure (`reports/semantic_clones.csv`). | Replace bespoke `__dir__` overrides with shared mixin that injects deterministic ordering. | Downstream tooling might rely on module-local overrides; ensure mixin export path is stable. | Run `pytest -k pipelines` and confirm lint (`ruff`) stays clean. | `reports/semantic_clones.csv`, `artifacts/module_map.json` |
| Config-1 | Pipeline YAML skeletons reuse identical empty sections per `reports/config_duplicates.csv`. | Extract shared include for blank pipeline sections or document intentional duplication to avoid drift. | YAML anchors/includes must respect existing `!include` semantics. | Render configs via existing loader smoke tests to ensure includes resolve. | `reports/config_duplicates.csv`, `artifacts/module_map.json` |
