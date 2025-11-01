# Semantic Clone Candidates

## CLI command factories
- Modules `bioetl.cli.commands.chembl_activity`, `bioetl.cli.commands.chembl_assay`, and `bioetl.cli.commands.chembl_testitem` each define `build_command_config` with identical orchestration logic differing only in default literals (pipeline name, default paths, description). They all wrap `PipelineCommandConfig`, call `get_config_path` with a pipeline-specific YAML file, and return a lambda that yields the corresponding pipeline class. [ref: repo:src/bioetl/cli/commands/chembl_activity.py@test_refactoring_32] [ref: repo:src/bioetl/cli/commands/chembl_assay.py@test_refactoring_32] [ref: repo:src/bioetl/cli/commands/chembl_testitem.py@test_refactoring_32]

- Non-ChEMBL command modules (`openalex`, `pubmed`, `crossref`, `semantic_scholar`, `uniprot_protein`, etc.) repeat the same factory pattern with shared constant defaults and only the pipeline identifier changing. [ref: repo:src/bioetl/cli/commands/openalex.py@test_refactoring_32] [ref: repo:src/bioetl/cli/commands/pubmed.py@test_refactoring_32] [ref: repo:src/bioetl/cli/commands/crossref.py@test_refactoring_32] [ref: repo:src/bioetl/cli/commands/semantic_scholar.py@test_refactoring_32]

## Lazy export facades
- `bioetl.pipelines.__getattr__` and `bioetl.schemas.__getattr__` implement the same lazy import pattern: map attribute names to module strings, import on demand, and rethrow `AttributeError` with identical messaging. Both also duplicate a sorted `__dir__` implementation. [ref: repo:src/bioetl/pipelines/__init__.py@test_refactoring_32] [ref: repo:src/bioetl/schemas/__init__.py@test_refactoring_32]

## Resource finalizers
- Many pipeline classes (`chembl_activity`, `chembl_assay`, `chembl_document`, `chembl_target`, `chembl_testitem`, `sources.uniprot.pipeline`, `sources.pubchem.pipeline`, etc.) override `close_resources` with near-identical clean-up behaviour (either no-op with docstring or resetting cached attributes). Tests define matching stub implementations. [ref: repo:src/bioetl/pipelines/chembl_activity.py@test_refactoring_32] [ref: repo:src/bioetl/pipelines/chembl_assay.py@test_refactoring_32] [ref: repo:src/bioetl/sources/uniprot/pipeline.py@test_refactoring_32]

## Schema configuration blocks
- Pandera schema modules under `bioetl.sources.*.schema` repeatedly declare nested `Config` classes that set the same trio of flags (`strict`, `coerce`, `ordered`). The pattern appears dozens of times without variation. [ref: repo:src/bioetl/sources/openalex/schema/__init__.py@test_refactoring_32] [ref: repo:src/bioetl/sources/crossref/schema/__init__.py@test_refactoring_32] [ref: repo:src/bioetl/sources/pubmed/schema/__init__.py@test_refactoring_32]

## Pipeline YAML structure
- Pipeline configuration files in `src/bioetl/configs/pipelines/` share identical `pipeline` blocks with keys `name`, `module`, and `class`. Many also repeat `extends` and `materialization` sections. [ref: repo:src/bioetl/configs/pipelines/openalex.yaml@test_refactoring_32] [ref: repo:src/bioetl/configs/pipelines/semantic_scholar.yaml@test_refactoring_32] [ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@test_refactoring_32]
