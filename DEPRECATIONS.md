# Deprecations Register

| Symbol | Module | Date Announced | Removal Plan | Replacement |
| --- | --- | --- | --- | --- |
| `bioetl.configs` | `bioetl.configs` | 2025-01-XX | 1.2.0 | `bioetl.config` |
| `bioetl.configs.models` | `bioetl.configs.models` | 2025-01-XX | 1.2.0 | `bioetl.config.models` |
| `bioetl.schemas.registry` | `bioetl.schemas.registry` | 2025-01-XX | 1.2.0 | `bioetl.core.unified_schema` |
| `bioetl.transform.adapters.*` | `bioetl.transform.adapters` | 2025-01-XX | 1.2.0 | `bioetl.adapters.*` |
| `bioetl.utils.io.load_input_frame` | `bioetl.utils.io` | 2025-01-XX | 1.2.0 | `bioetl.io.readers.load_input_frame` |
| `bioetl.utils.io.resolve_input_path` | `bioetl.utils.io` | 2025-01-XX | 1.2.0 | `bioetl.io.readers.resolve_input_path` |

## Реэкспорты с Deprecation Warning

### `bioetl.configs` → `bioetl.config`

**Объявление**: 2025-01-XX  
**Планируемое удаление**: 1.2.0 (2 MINOR версии после объявления)  
**Замена**: Использовать `bioetl.config` вместо `bioetl.configs`

**Причина**: Консолидация модулей `config/` и `configs/` в единый `config/` для устранения дублирования.

**Реэкспорт**:
```python
# src/bioetl/configs/__init__.py
from bioetl.core.deprecation import deprecation_warning

deprecation_warning(
    "bioetl.configs",
    replacement="bioetl.config",
    version="1.2.0",
)

from bioetl.config import *  # Реэкспорт всех символов
```

### `bioetl.schemas.registry` → `bioetl.core.unified_schema`

**Объявление**: 2025-01-XX  
**Планируемое удаление**: 1.2.0 (2 MINOR версии после объявления)  
**Замена**: Использовать `bioetl.core.unified_schema` вместо `bioetl.schemas.registry`

**Причина**: Объединение логики реестра схем в `core/unified_schema.py` для централизации.

**Реэкспорт**:
```python
# src/bioetl/schemas/registry.py
from bioetl.core.deprecation import deprecation_warning

deprecation_warning(
    "bioetl.schemas.registry",
    replacement="bioetl.core.unified_schema",
    version="1.2.0",
)

from bioetl.core.unified_schema import (
    SchemaRegistry,
    SchemaRegistration,
    schema_registry,
    get_schema,
    get_schema_metadata,
    register_schema,
    get_registry,
)
```

### `bioetl.transform.adapters.*` → `bioetl.adapters.*`

**Объявление**: 2025-01-XX  
**Планируемое удаление**: 1.2.0 (2 MINOR версии после объявления)  
**Замена**: Использовать `bioetl.adapters.chembl_activity` вместо `bioetl.transform.adapters.chembl_activity`

**Причина**: Консолидация адаптеров в единый модуль `adapters/` для устранения дублирования.

**Реэкспорты**:
```python
# src/bioetl/transform/adapters/__init__.py
from bioetl.core.deprecation import deprecation_warning

deprecation_warning(
    "bioetl.transform.adapters",
    replacement="bioetl.adapters",
    version="1.2.0",
)

from bioetl.adapters.chembl_activity import ActivityNormalizer
from bioetl.adapters.chembl_assay import AssayNormalizer
```

### `bioetl.utils.io.*` → `bioetl.io.readers.*`

**Объявление**: 2025-01-XX  
**Планируемое удаление**: 1.2.0 (2 MINOR версии после объявления)  
**Замена**: Использовать `bioetl.io.readers.load_input_frame` вместо `bioetl.utils.io.load_input_frame`

**Причина**: Выделение IO операций в отдельный модуль `io/` для лучшей организации.

**Реэкспорт**:
```python
# src/bioetl/utils/io.py
from bioetl.core.deprecation import deprecation_warning

deprecation_warning(
    "bioetl.utils.io.load_input_frame",
    replacement="bioetl.io.readers.load_input_frame",
    version="1.2.0",
)

from bioetl.io.readers import load_input_frame, resolve_input_path
```

## Removed [2025-11-01]

The following items were removed during repository cleanup:

### Temporary Files
- `md_scan*.txt` (7 files) — Markdown linter output files
- `md_report.txt` — Temporary lint report
- `coverage.xml` — Generated coverage artifact (should not be tracked in git)

### Archived Reports
The following reports have been moved to `docs/reports/archived/2025-11-01/` as historical artifacts:
- `acceptance-criteria-document.md`, `acceptance-criteria.md` — Acceptance criteria
- `assessment.md`, `gaps.md`, `pr-plan.md`, `test-plan.md` — Planning documents
- `COMPLETED_IMPLEMENTATION.md`, `implementation-status.md`, `implementation-examples.md` — Implementation reports
- `DOCUMENT_PIPELINE_VERIFICATION.md` — Pipeline verification report
- `FINAL_100_ROWS_REPORT.md`, `FINAL_100_ROWS_SUCCESS.md`, `FINAL_RUN_RESULTS.md`, `FINAL_STATUS.md`, `FINAL_VALIDATION_REPORT.md` — Final status reports
- `PROGRESS_SUMMARY.md`, `RUN_RESULTS_SUMMARY.md` — Progress summaries
- `REQUIREMENTS_AUDIT.md`, `REQUIREMENTS_UPDATED.md` — Requirements audits
- `RISK_REGISTER.md` — Risk register
- `SCHEMA_COMPLIANCE_REPORT.md`, `SCHEMA_GAP_ANALYSIS.md` — Schema analysis reports
- `SCHEMA_SYNC_*.md` (8 files) — Schema synchronization reports

### Archived Patches
Historical patch files have been moved to `docs/reports/archived/patches/`:
- `0001-chembl-helper.patch`
- `0002-pipeline-refactor.patch`
- `0003-test-docs.patch`
- `001-determinism-defaults.patch`
- `cli_registry_refactor.patch`

## Update Policy

All deprecations MUST be recorded in this table when the `DeprecationWarning` is introduced. Each entry MUST specify the earliest
release where the removal is planned (SemVer `MAJOR.MINOR`), and the date the warning was announced. When the removal is executed,
update the table to reflect the outcome and link to the corresponding changelog entry.

Changes to public APIs MUST follow Semantic Versioning 2.0.0. Any incompatible change is deferred to the next MAJOR release; while
a warning is active, the MINOR version MUST increment on releases that introduce or update the deprecation plan.

## Миграционный путь

### Для разработчиков

1. **Обновить импорты конфигурации**:
   ```python
   # Старый код
   from bioetl.configs import PipelineConfig
   
   # Новый код
   from bioetl.config import PipelineConfig
   ```

2. **Обновить импорты схем**:
   ```python
   # Старый код
   from bioetl.schemas.registry import schema_registry
   
   # Новый код
   from bioetl.core.unified_schema import schema_registry
   ```

3. **Обновить импорты адаптеров**:
   ```python
   # Старый код
   from bioetl.transform.adapters.chembl_activity import ActivityNormalizer
   
   # Новый код
   from bioetl.adapters.chembl_activity import ActivityNormalizer
   ```

4. **Обновить импорты IO**:
   ```python
   # Старый код
   from bioetl.utils.io import load_input_frame
   
   # Новый код
   from bioetl.io.readers import load_input_frame
   ```

### Автоматическая проверка

Для автоматического поиска устаревших импортов используйте:

```bash
# Поиск использования bioetl.configs
rg -n "from bioetl\.configs|import bioetl\.configs"

# Поиск использования bioetl.schemas.registry
rg -n "from bioetl\.schemas\.registry|import bioetl\.schemas\.registry"

# Поиск использования bioetl.transform.adapters
rg -n "from bioetl\.transform\.adapters|import bioetl\.transform\.adapters"
```
