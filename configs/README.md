# Configuration Layering

The `configs/` tree is split into deterministic layers so that every pipeline
run resolves to the same configuration on every machine.

## Directory layout

- `defaults/` — canonical profile fragments shared by all pipelines. Files are
  merged in lexicographic order by filename.
- `env/<name>/` — environment-specific overrides applied when
  `BIOETL_ENV=<name>` (supported values: `dev`, `stage`, `prod`).
- `pipelines/` — pipeline entry-points referencing defaults via `extends`.

## Merge precedence

The loader in `src/bioetl/config/loader.py` materialises the configuration in
the following order:

1. `defaults/*.yaml` (opt-in via `include_default_profiles=True`).
2. Profiles supplied through the `profiles` argument and any `extends`
   references within those files.
3. The primary pipeline YAML passed via `config_path` (recursively resolving
   its own `extends`).
4. `env/<BIOETL_ENV>/*.yaml`, if `BIOETL_ENV` is defined.
5. CLI overrides (`--set key=value`).
6. Environment variables with `BIOETL__`/`BIOACTIVITY__` prefixes.

Environment directories must exist when `BIOETL_ENV` is provided. Missing
directories cause the loader to fail fast instead of silently skipping
overrides.

## Authoring guidelines

- Keep defaults immutable; changes affect every pipeline.
- Sort keys and values deterministically and avoid relying on implicit merge
  order.
- New environment layers should mirror the defaults they override and stay
  minimal to simplify diffs.

## Pipeline configuration registry

The CLI registry at `src/bioetl/cli/cli_registry.py` defines the canonical
mapping between pipeline commands and their default configuration YAML. The
current matrix ensures there are no сиротские конфиги:

| CLI command        | Provider | Entity    | Default config path                                   |
| ------------------ | -------- | --------- | ----------------------------------------------------- |
| `activity_chembl`  | `chembl` | `activity`| `configs/pipelines/activity/activity_chembl.yaml`     |
| `assay_chembl`     | `chembl` | `assay`   | `configs/pipelines/assay/assay_chembl.yaml`           |
| `document_chembl`  | `chembl` | `document`| `configs/pipelines/document/document_chembl.yaml`     |
| `target_chembl`    | `chembl` | `target`  | `configs/pipelines/target/target_chembl.yaml`         |
| `testitem_chembl`  | `chembl` | `testitem`| `configs/pipelines/testitem/testitem_chembl.yaml`     |

Any новый конфиг должен появляться в этой таблице и в CLI-реестре; удаления
без ссылок допускаются только вместе с записью в
`configs/naming_exceptions.yaml`.
