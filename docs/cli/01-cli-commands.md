# CLI Commands

> **Note**: The Typer CLI shipped in `bioetl.cli.cli_app` is available today.
> All paths referencing `src/bioetl/` correspond to the editable source tree
> that backs the installed package.

This reference drills into every command exposed by
`python -m bioetl.cli.cli_app`, capturing its signature, required and optional
options, default configuration bundles, runnable examples, and determinism
guarantees.

## Invocation pattern

All pipeline entry points share the same Typer invocation form:

```bash
python -m bioetl.cli.cli_app <command> [OPTIONS]
```

После установки пакета доступен идентичный консольный скрипт:

```bash
bioetl <command> [OPTIONS]
```

The CLI loads configuration layers in a fixed precedence: profiles declared via
`extends` (typically `configs/defaults/base.yaml`,
`configs/defaults/network.yaml`, `configs/defaults/determinism.yaml`, and the
post-processing defaults in `configs/defaults/postprocess.yaml`), then the
pipeline YAML passed with `--config`, then any `--set` overrides, and finally
environment variables. This merge order keeps defaults predictable while still
allowing per-run overrides. The postprocess profile simply injects the
[`postprocess.correlation.enabled` defaults documented in the spec](../configs/00-typed-configs-and-profiles.md#27-postprocess)
so that the optional correlation report is opt-in rather than surprising.

## Global options

Эти флаги объявлены в `bioetl.cli.cli_command.create_pipeline_command` и
подключаются ко всем пайплайнам:

| Flag                                            | Purpose                                                                      | Default                     |
| ----------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------- |
| `--config, -c`                                  | Путь к YAML конфигурации пайплайна.                                          | Required                    |
| `--output-dir, -o`                              | Каталог назначения для артефактов и QC.                                     | Required                    |
| `--dry-run, -d`                                 | Загрузить и провалидировать конфиг без запуска стадий.                       | `False`                     |
| `--verbose, -v`                                 | Включить подробное (DEBUG) логирование.                                      | `False`                     |
| `--set, -S`                                     | Повторяемые `KEY=VALUE` оверрайды после merge профилей.                      | `[]`                        |
| `--sample`                                      | Детминированно выбрать `N` строк входного набора.                            | `None`                      |
| `--limit`                                       | Обработать не более `N` строк (smoke-run).                                   | `None`                      |
| `--extended`                                    | Включить расширенную отчётность QC.                                          | `False`                     |
| `--fail-on-schema-drift/--allow-schema-drift`   | Влияет на ошибку при дрейфе схемы (по умолчанию — аварийный выход).          | `--fail-on-schema-drift`    |
| `--validate-columns/--no-validate-columns`      | Жёсткая проверка колонок и порядка перед экспортом.                          | `--validate-columns`        |
| `--golden`                                      | Путь к golden-файлу для битовой проверки.                                    | `None`                      |
| `--input-file, -i`                              | Доп. CSV/Parquet с seed-идентификаторами для выборочного извлечения.         | `None`                      |


### Correlation report toggle

Все команды автоматически подмешивают профиль
[`configs/defaults/postprocess.yaml`](../../configs/defaults/postprocess.yaml),
поэтому `postprocess.correlation.enabled` остаётся `false`, пока оператор
явно его не переключит. Чтобы добавить корреляционный отчёт
(`*_correlation_report.csv`) в набор QC-артефактов, передайте CLI override:

```bash
python -m bioetl.cli.cli_app activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir ./data/output/activity \
  --set postprocess.correlation.enabled=true
```

Любой командой можно принудительно отключить отчёт даже при наличии
пользовательских профилей (`--set postprocess.correlation.enabled=false`).
Полное описание поведения приведено в разделе
[`postprocess`](../configs/00-typed-configs-and-profiles.md#27-postprocess).

## Доступные команды (актуально)

Список синхронизирован с `bioetl.cli.cli_registry.COMMAND_REGISTRY` и отражает
только активные команды CLI.

- `activity_chembl` — ChEMBL activity fact pipeline.
- `assay_chembl` — ChEMBL assay dimension pipeline.
- `target_chembl` — ChEMBL target dimension pipeline.
- `document_chembl` — ChEMBL document pipeline.
- `testitem_chembl` — ChEMBL molecule/test item pipeline.
- `run-chembl-all` — групповой запуск всех ChEMBL-пайплайнов с едиными флагами.

## Утилиты конфигурации

- `config inspect` — вспомогательный сабкоманд `bioetl config inspect`, который
  загружает, мерджит и печатает `PipelineConfig` без запуска пайплайна. Команда
  принимает те же `--set/--limit/--sample` флаги, что и основные entrypoints, и
  позволяет быстро проверить новые секции (`postprocess`, `fallbacks`) или
  влияние env-переменных. Полная справка в
  [`docs/cli/config-inspect.md`](config-inspect.md).

## Групповые команды

### `run-chembl-all`

- **Signature**: `bioetl run-chembl-all --output-root PATH [OPTIONS]`
- **Purpose**: последовательно запускает все ChEMBL-пайплайны, перечисленные в
  `PIPELINE_REGISTRY`, и собирает артефакты в указанной директории. Команда
  повторно использует тот же `Typer`-контекст, что и отдельные пайплайны, и
  передаёт общие флаги (`--limit`, `--extended`, `--golden`, `--verbose`,
  `--dry-run`).
- **Options**:
  - `--output-root, -o PATH` — обязательный путь, куда будут размещены
    артефакты всех пайплайнов (`activity`, `assay`, `target`, `document`,
    `testitem`). Для каждого пайплайна создаётся подпапка вида
    `<output-root>/<pipeline_code>`.
  - `--configs-dir PATH` — базовый каталог конфигураций (по умолчанию `configs`).
    Используется для автопоиска YAML-файлов, ожидаемых конкретным пайплайном.
  - `--limit INTEGER` — soft-ограничение количества строк в каждом пайплайне для
    smoke-тестов.
  - `--extended` — включает расширенные QC-артефакты (`quality_report`,
    correlation report).
  - `--golden PATH` — опциональный путь к golden-набору; проверка выполняется
    после завершения каждого пайплайна.
  - `--verbose / --dry-run` — передаются напрямую в каждый пайплайн, сохраняя
    поведение одиночных команд.
- **Output**: единый лог и набор подпапок с артефактами. Ошибка на любом
  пайплайне прерывает дальнейший запуск, сохраняя уже сгенерированные результаты
  для анализа.

## Roadmap

Описание нереализованных пайплайнов перенесено в
[`docs/roadmap.md`](../roadmap.md). CLI не содержит заглушек — каждая команда
ниже соответствует исполняемому пайплайну и валидной конфигурации.
## Determinism building blocks

Every command inherits the determinism policy enforced by `PipelineBase`: stable
sorting, canonicalised values, SHA256 row and business-key hashes, and atomic
writes. The shared `configs/defaults/determinism.yaml` profile captures these
guarantees, while pipeline-specific configs define the concrete sort keys.

## Determinism and `--golden`

The `--golden` option attaches a reference dataset that the pipeline must match
byte-for-byte. After the write stage completes, the resulting artifact is
compared against the supplied golden file using the deterministic serialization
rules defined in `configs/defaults/determinism.yaml`. Any drift in row order,
column order, canonicalised values, or hash columns is treated as a determinism
failure and surfaces as a non-zero exit code together with structured log
records that describe the mismatch.

Golden checks pair naturally with the `--extended` flag: enabling extended QC
emits the full set of sidecar reports (`quality_report`, correlation metrics,
and `meta.yaml`) that the determinism policy expects. When both flags are
present, the CLI writes QC artefacts and then validates that every emitted file
matches the golden snapshots tracked in source control.

Reference: see `docs/determinism/00-determinism-policy.md` for the full contract
that governs deterministic outputs and golden artefact maintenance.

### Example

```bash
python -m bioetl.cli.cli_app activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir data/output/activity \
  --golden tests/golden/activity_chembl/v1/dataset/activity_chembl_extended_20240101.csv \
  --extended
# Exit code: 0 (outputs identical to the golden snapshot)
```

If the comparison detects any byte-level difference, the command exits with a
non-zero status, leaving the produced artefacts in place for inspection and
emitting structured diagnostics that pinpoint the drift.

## Command reference

### `qc` (runtime configuration inspector)

- **Signature**: `python -m bioetl.cli.cli_app qc [OPTIONS]`
- **Purpose**: Inspect the merged runtime configuration (thresholds, QC report
  templates, and `fail_on_threshold_violation`) without running any pipelines.
- **Options**:
  - `--runtime-config PATH` (default: `configs/default.yml`) — points to the
    runtime configuration bundle that layers environment overrides and
    short-form variables.
  - `--pipeline, -p` — when specified, prints pipeline-specific thresholds and
    QC report templates after the base section.
  - `--materialization-root, -m` — overrides the root directory used to expand
    QC templates; defaults to `data/output/<pipeline>` when `--pipeline` is
    passed.
- **Output**: emits a "QC configuration summary" section that lists base
  thresholds, the `fail_on_threshold_violation` flag, and pipeline-specific
  thresholds/templates, allowing operators to verify that `.env` overrides and
  CLI `--set` values have been merged correctly.

#### Example: verify merged runtime configuration

```bash
BIOETL_ENV=dev bioetl qc --runtime-config configs/default.yml --pipeline activity_chembl
```

The command exits with `0` after printing the merged configuration layers,
helping you confirm that environment variables and runtime YAML edits were
applied as expected. A non-zero exit code indicates that the runtime
configuration file could not be located or failed validation.

### `activity_chembl`

- **Signature**: `python -m bioetl.cli.cli_app activity_chembl [OPTIONS]`
- **Purpose**: Extract biological activity records from ChEMBL `/activity.json`
  and normalise them to the project schema.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, and any
  applicable `--set` overrides.
- **Default profiles**: Always merges `configs/defaults/base.yaml`,
  `configs/defaults/determinism.yaml`, and `configs/defaults/postprocess.yaml`;
  network defaults can be layered when referenced in the pipeline YAML.
- **Deterministic output**: Rows are sorted by `assay_id`, `testitem_id`, then
  `activity_id`; `hash_row` and `hash_business_key` are produced with SHA256
  using the canonicalisation rules from the determinism profile. The run emits a
  `meta.yaml` snapshot with the fingerprint of both configuration and outputs.

#### Пример запуска: activity_chembl

```bash
python -m bioetl.cli.cli_app activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir ./data/output/activity \
  --sample 5
```

### `assay_chembl`

- **Signature**: `python -m bioetl.cli.cli_app assay_chembl [OPTIONS]`
- **Purpose**: Retrieve and normalise assay metadata from ChEMBL `/assay.json`.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml`, `determinism.yaml`, and
  `postprocess.yaml`, with optional network profile via the pipeline config.
- **Deterministic output**: Sorted by `assay_id` before export; SHA256 hashes
  cover the business key and entire row, ensuring reproducible QC and golden
  comparisons.

#### Пример запуска: assay_chembl

```bash
python -m bioetl.cli.cli_app assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir ./data/output/assay \
  --sample 5
```

### `target_chembl`

- **Signature**: `python -m bioetl.cli.cli_app target_chembl [OPTIONS]`
- **Purpose**: Build the enriched target dimension by combining ChEMBL
  `/target.json` data with UniProt and IUPHAR classifications.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`
  (for example, to toggle enrichment services).
- **Default profiles**: `base.yaml` + `determinism.yaml` + `postprocess.yaml`;
  additional network-specific overrides come from the pipeline YAML, including
  dedicated HTTP profiles for external enrichers.
- **Deterministic output**: Sorted by `target_id` and hashed with SHA256; the
  determinism profile guarantees stable canonicalisation, while the pipeline
  config fixes enrichment thresholds and QC expectations.

#### Пример запуска: target_chembl

```bash
python -m bioetl.cli.cli_app target_chembl \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir ./data/output/target \
  --sample 5
```

### `document_chembl`

- **Signature**: `python -m bioetl.cli.cli_app document_chembl [OPTIONS]`
- **Purpose**: Extract ChEMBL documents and optionally enrich them with PubMed,
  Crossref, OpenAlex, and Semantic Scholar metadata, depending on the configured
  mode.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode` (for example `chembl` vs `all`),
  `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml`, `determinism.yaml`, и `postprocess.yaml`;
  enrichment adapters inherit network defaults specified in the document
  pipeline config.
- **Deterministic output**: Sorted by `year` and `document_id`, with SHA256
  hashes covering both the full row and business keys. The adapter settings
  ensure canonical source precedence while still producing deterministic outputs
  and metadata.

#### Пример запуска: document_chembl

```bash
python -m bioetl.cli.cli_app document_chembl \
  --config configs/pipelines/document/document_chembl.yaml \
  --output-dir ./data/output/document \
  --sample 5
```

### `testitem_chembl`

- **Signature**: `python -m bioetl.cli.cli_app testitem_chembl [OPTIONS]`
- **Purpose**: Produce the molecule (test item) dimension from ChEMBL
  `/molecule.json`, optionally blending in PubChem enrichment.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`
  (for example, toggling PubChem enrichment).
- **Default profiles**: `base.yaml`, `determinism.yaml`, `postprocess.yaml`,
  plus any network overrides included in the pipeline config.
- **Deterministic output**: Sorted by `testitem_id` and hashed
  deterministically; outputs and QC sidecars inherit the shared determinism
  policy.

#### Пример запуска: testitem_chembl

```bash
python -m bioetl.cli.cli_app testitem_chembl \
  --config configs/pipelines/testitem/testitem_chembl.yaml \
  --output-dir ./data/output/testitem \
  --sample 5
```

## Summary matrix

| Command           | Data domain                                    | Primary configuration                             | Default profiles applied                                                                                    |
| ----------------- | ---------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `activity_chembl` | ChEMBL activity fact table                     | `configs/pipelines/activity/activity_chembl.yaml` | `configs/defaults/base.yaml`, `configs/defaults/determinism.yaml`, `configs/defaults/postprocess.yaml`, optional `configs/defaults/network.yaml` |
| `assay_chembl`    | ChEMBL assay dimension                         | `configs/pipelines/assay/assay_chembl.yaml`       | `base.yaml`, `determinism.yaml`, `postprocess.yaml`, optional `network.yaml`                                                                        |
| `target_chembl`   | ChEMBL target dimension                        | `configs/pipelines/target/target_chembl.yaml`     | `base.yaml`, `determinism.yaml`, `postprocess.yaml`, optional `network.yaml`                                                                        |
| `document_chembl` | ChEMBL documents with optional adapters        | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, `postprocess.yaml`, optional `network.yaml`                                                                        |
| `testitem_chembl` | ChEMBL molecules with PubChem enrichment hooks | `configs/pipelines/testitem/testitem_chembl.yaml` | `base.yaml`, `determinism.yaml`, `postprocess.yaml`, optional `network.yaml`                                                                        |

Each matrix entry links the CLI command to its authoritative configuration
bundle, making it easy to trace which YAML files—and therefore which typed
models—govern a run.
