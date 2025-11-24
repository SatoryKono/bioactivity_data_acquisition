# Code Cleanliness Audit

## Unused or Unreferenced Components (from `vulture`)
| Path | Symbol | Notes |
| --- | --- | --- |
| `src/application/pipelines/__init__.py:L64-L71` | `__getattr__` | Lazy resolver not referenced elsewhere. |
| `src/application/pipelines/specs/__init__.py:L50-L67` | `__getattr__` | Secondary lazy export helper unused by imports. |
| `src/application/pipelines/specs/chembl/__init__.py:L17-L24` | `__getattr__` | Lazy ChEMBL export resolver never called directly. |
| `src/application/pipelines/specs/chembl/activity/run.py:L599-L620` | `_extract_data_validity_descriptions` | Helper method unused in pipeline runtime. |
| `src/application/pipelines/specs/chembl/assay/normalize.py:L39-L55` | `_should_nullify_string_value`, `_stringify_records` | Internal helpers not invoked. |
| `src/application/pipelines/specs/chembl/helpers.py:L21-L30` | `extract_fields` | Unreferenced extraction helper. |
| `src/application/pipelines/specs/chembl/stage_runner.py:L43-L100` | `_PipelineResolver` | Lazy stage resolver class not instantiated. |
| `src/application/pipelines/common.py:L1478-L1598` | `finalize_with_standard_metadata`, `set_export_metadata_from_dataframe`, `record_validation_issue`, `reset_stage_context` | Unused lifecycle hooks. |

## Deprecated / Commented Markers
| Path | Marker | Context |
| --- | --- | --- |
| `src/infrastructure/clients/client_chembl.py:L422-L440` | `# Assay class map fetching (DEPRECATED)` | Deprecated endpoint shim retained in client code. |
| `src/application/pipelines/common.py:L2063-L2066` | `deprecated` string | Pipeline warning about deprecated write path. |
| `src/bioetl/core/__init__.py:L145-L210` | `# Deprecated exports` | Backwards-compatible exports slated for removal. |

## Duplicate or Near-Duplicate Structures
| Paths | Duplication Notes |
| --- | --- |
| `src/application/pipelines/specs/chembl/activity/validate.py` and `src/application/pipelines/specs/chembl/assay/validate.py` | Identical stage wrapper templates building `PIPELINE, _STAGES = build_stage_functions(...)` for single-stage validate functions. |
| `src/application/pipelines/specs/__init__.py` and `src/application/pipelines/__init__.py` | Similar lazy `__getattr__` resolvers duplicating import-and-cache pattern. |
| `src/application/pipelines/specs/chembl/*/(extract|transform|validate|write).py` | Repeated `PIPELINE, _STAGES = build_stage_functions(...)` scaffolding across stage modules. |

## Candidates for Removal / Simplification
| Item | Rationale |
| --- | --- |
| Unused lazy `__getattr__` functions in pipeline `__init__` modules | Not referenced; can remove to simplify namespace exports. |
| Unused helper methods in `application.pipelines.common` and ChEMBL stage modules | `vulture` reports no call sites; removing reduces maintenance surface. |
| Deprecated client code paths | Marked as deprecated and unused in tests; can be pruned after confirming external callers absent. |
| Repeated stage wrapper templates | Consolidate into shared factory or reduce duplication via helper generator. |

## Safe Cleanup Plan
1. Confirm `vulture` findings by grepping for call sites; add runtime telemetry if uncertain.
2. Remove or comment-guard unused helpers (`finalize_with_standard_metadata`, `_PipelineResolver`, stage-specific `PIPELINE` constants) after validating no dynamic references.
3. Eliminate deprecated ChEMBL client shims; provide migration notes for downstream consumers.
4. Refactor duplicate stage wrappers to a shared helper to cut repetition and reduce drift.
5. Run full test suite and pipeline smoke tests to ensure functional parity after cleanup.
