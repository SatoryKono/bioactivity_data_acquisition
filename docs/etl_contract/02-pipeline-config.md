# 2. Pipeline Configuration

> **Note**: Implementation status: **implemented**. The behaviors described
> below are enforced today by the Pydantic models in
> `src/bioetl/config/models/`, with `src/bioetl/config/models/base.py` (and its
> sibling modules such as `http.py`, `cache.py`, and `paths.py`) serving as the
> authoritative source of truth for the runtime contract.

## Overview

Every pipeline in the `bioetl` framework is driven by a declarative YAML
configuration file. This approach separates the pipeline's logic (defined in its
Python class) from its behavior (defined in the YAML), making pipelines
flexible, reusable, and easy to manage.

All configuration files are validated at runtime against a set of strongly-typed
Pydantic models located in `src/bioetl/config/models/`. The models are organized
into logical modules (base, http, cache, paths, determinism, validation,
transform, postprocess, source, cli, fallbacks) and are imported directly from
`bioetl.config.models.models` and `bioetl.config.models.policies`. The legacy
`__init__.py` shim has been removed entirely, so every import must reference the
concrete module. These modules (especially `base.py`, which stitches together
the full `PipelineConfig` model) form the canonical specification for what the
YAML must contain, ensuring that all configurations are well-formed and contain
all necessary parameters before the pipeline begins execution.

## Configuration Skeleton

`PipelineConfig` now separates domain settings (schemas, transforms, enrichment)
from infrastructure settings (IO, HTTP, logging, determinism). A representative
YAML snippet:

```yaml
# src/bioetl/configs/pipelines/<provider>/<pipeline>.yaml
version: 1
extends:
  - ../../defaults/base.yaml
  - ../../defaults/determinism.yaml

pipeline:
  name: activity
  version: "1.2.3"
  owner: "activity@bioetl"

domain:
  validation:
    schema_out: "bioetl.schemas.chembl.activity.ActivityOutput"
    strict: true
  transform:
    flatten_objects:
      activity_properties:
        - pref_name
        - value
  postprocess:
    hash_row_columns:
      - activity_id
      - assay_id
  fallbacks:
    enabled: true
    policy: ordered
    sources:
      - cache
      - semantic_scholar.title_search
  sources:
    chembl:
      enabled: true
      description: "Primary ChemBL snapshot"
  chembl:
    enrich_descriptors: true

infrastructure:
  runtime:
    parallelism: 4
  io:
    read_batch_size: 10000
  http:
    default:
      base_url: "https://www.ebi.ac.uk"
      timeout_seconds: 30
  cache:
    ttl_seconds: 86400
  paths:
    input_root: "data/input"
    output_root: "data/output"
  determinism:
    column_order:
      - activity_id
      - assay_id
  materialization:
    default_format: "parquet"
  logging:
    level: "INFO"
  telemetry:
    exporters: []
  cli:
    limit: null
```

## Section Details

- **`extends`**: Allows for the composition of configurations. Common settings
  can be placed in base files (like `base.yaml`) and included in multiple
  pipeline configurations to avoid repetition.
- **`domain.*`**: Business-facing controls. `validation` binds the pipeline to
  Pandera schemas, `transform` describes flattening/serialization rules,
  `postprocess` defines hashing/QC enrichments (see the
  [postprocess configuration section](../configs/00-typed-configs-and-profiles.md#27-postprocess)
  for the list of keys and defaults, including the optional correlation report),
  `fallbacks` centralizes resilience policies, and `sources` enumerates provider overrides (ChemBL,
  `postprocess` defines hashing/QC enrichments, `fallbacks` centralizes
  resilience policies through `fallbacks.policy` (`ordered`, `best_effort`,
  `strict`) and the prioritized `fallbacks.sources` list, and `sources` enumerates provider overrides (ChemBL,
  UniProt, PubChem, etc.). The optional `chembl` block captures enrichment knobs
  specific to the ChemBL adapters.
- **`infrastructure.*`**: Cross-cutting platform controls. `runtime` sets
  execution parameters, `io` governs buffering and formats, `http` consolidates
  retry/backoff policies, `cache` defines TTL-based reuse, `paths` anchors input
  and output directories, `determinism` enforces sorting/column ordering,
  `materialization` covers file formats and partitioning, `logging` binds to
  UnifiedLogger, `telemetry` wires tracing/metrics exporters, and `cli`
  surfaces operator overrides (limit, sample, dry-run, etc.).

## Lifecycle and precedence

`bioetl.config` придерживается фиксированного жизненного цикла конфигурации. Все
поля проходят одни и те же этапы, что гарантирует воспроизводимость и явные
контракты:

1. **Raw YAML** — основной файл пайплайна читается с поддержкой `extends` и
   `!include`. На этом шаге допускается только чтение с диска.
2. **Profiles** — к базовому содержимому последовательно применяются профили:
   сначала `configs/defaults/*.yaml` (если включены), затем профили, запрошенные
   через CLI (`--profile`) или указанные в конфиге. Каждый профиль может
   расширяться через `extends`, порядок применения является детерминированным.
3. **Environment layers** — после профилей подмешиваются файлы из
   `configs/env/<BIOETL_ENV>/` и короткие переменные из `.env`. Модуль
   `bioetl.config.environment` отвечает за поиск `.env`, валидацию `BIOETL_ENV`
   (`dev`, `stage`, `prod`) и построение словаря `BIOETL__...`-overrides без
   побочных эффектов.
4. **CLI overrides** — последним слоем идут значения из `CLIConfig`. Флаги
   `--set key=value` попадают в `CLIConfig.set_overrides`, разбиваются на
   вложенные структуры и детерминированно перекрывают предыдущие слои.
5. **Finalization** — объединённый маппинг валидируется через
   `PipelineConfig`. Полученная модель неизменяемо передаётся пайплайнам;
   дальнейшие изменения запрещены, чтобы потребители всегда работали с
   финализированным состоянием.

Приоритет слоёв: `raw YAML < profiles < env < CLI`. Любой новый уровень обязан
явно документировать своё место в цепочке и использовать те же правила детерминизма
(стабильный порядок ключей, UTC, отсутствие скрытых побочных эффектов).

`CLIConfig.profiles`, `CLIConfig.environment_profiles` и `CLIConfig.environment`
фиксируют, какие профили и env-слои были применены, что упрощает отладку и
аудит overrides.
