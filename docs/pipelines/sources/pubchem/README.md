# PubChem enrichment

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Maintainers

- `@SatoryKono` — глобальный ревьюер и владелец документации, отслеживает изменения по источнику PubChem.【F:.github/CODEOWNERS†L5-L41】

## Требования к содержанию карточки источника

- `MODULE_RULES.md` требует, чтобы карточка источника фиксировала публичный API, ключи конфигурации и merge-политику в соответствии со структурой модулей источника.【F:refactoring/MODULE_RULES.md†L37-L42】【F:refactoring/MODULE_RULES.md†L146-L149】
- `DATA_SOURCES.md` закрепляет обязательные поля карточки (`source`, `public_api`, `config_keys`, `merge_policy`) для внешних энрихеров, включая PubChem.【F:refactoring/DATA_SOURCES.md†L63-L76】

## Public API

- `bioetl.pipelines.pubchem.PubChemPipeline` — самостоятельный пайплайн, обогащающий список ChEMBL-молекул по `molecule_chembl_id` и `standard_inchi_key`, с подсчётом QC-метрик по покрытию и доле обогащений.【F:src/bioetl/pipelines/pubchem.py†L1-L170】
- `bioetl.sources.pubchem.pipeline.PubChemPipeline` — совместимый shim, реэкспортирующий standalone-пайплайн под пространством `sources` для соответствия регистру источников.【F:src/bioetl/sources/pubchem/pipeline.py†L1-L5】

## Module layout

- Пайплайн использует адаптер `bioetl.adapters.PubChemAdapter` и вспомогательные константы `_LOOKUP_COLUMNS`/`_PUBCHEM_COLUMNS`, обеспечивая слоистую архитектуру (pipeline → adapter → HTTP).【F:src/bioetl/pipelines/pubchem.py†L11-L138】

## Configuration keys

- `configs/pipelines/pubchem.yaml` определяет базовый URL PUG REST, параметры rate limit (`rate_limit_max_calls`, `rate_limit_period`), `batch_size`, заголовок `User-Agent`, а также путь к lookup-файлу в `postprocess.enrichment.pubchem_lookup_input`.【F:src/bioetl/configs/pipelines/pubchem.yaml†L1-L72】
- QC секция в том же профиле контролирует метрики `pubchem.min_inchikey_coverage` и `pubchem.min_enrichment_rate`, что синхронизировано с проверками в коде пайплайна.【F:src/bioetl/configs/pipelines/pubchem.yaml†L67-L72】【F:src/bioetl/pipelines/pubchem.py†L80-L137】

## Merge policy

- Пайплайн использует lookup-таблицу с колонками `molecule_chembl_id` и `standard_inchi_key`; обогащённые поля `pubchem_*` добавляются к этим ключам и сортируются детерминированно перед экспортом, обеспечивая совместимость с testitem-пайплайном ChEMBL.【F:src/bioetl/pipelines/pubchem.py†L32-L158】
- Матрица источников фиксирует, что в финальной выдаче test items имена и синонимы берутся из PubChem с приоритетом над ChEMBL, конфликтные случаи помечаются в QC.【F:refactoring/DATA_SOURCES.md†L38-L39】

## Tests

- `tests/unit/test_pubchem_pipeline.py` покрывает полный цикл `extract → transform → validate → export`, используя мок-адаптер PubChem.【F:tests/unit/test_pubchem_pipeline.py†L1-L60】
- Модульные тесты в `tests/sources/pubchem/` проверяют клиент и нормализацию адаптера, что поддерживает слои, требуемые `MODULE_RULES.md`.【F:tests/sources/pubchem/test_client.py†L1-L40】【F:tests/sources/pubchem/test_normalizer.py†L1-L40】
