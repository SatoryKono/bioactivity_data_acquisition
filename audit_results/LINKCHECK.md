# Link Check Report

## Missing Files from .lychee.toml

| Source | File | Type | Criticality |
|--------|------|------|-------------|
| .lychee.toml | docs/architecture/00-architecture-overview.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/architecture/03-data-sources-and-spec.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/pipelines/PIPELINES.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/configs/CONFIGS.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/cli/CLI.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/qc/QA_QC.md | declared_but_missing | CRITICAL |

## Broken Internal Links

| Source | Link Text | Link Path | Type | Criticality |
|--------|-----------|-----------|------|-------------|
| docs\INDEX.md | Prompt Documentation | 00-promt/ | broken_internal_link | MEDIUM |
| docs\INDEX.md | crossref/README.md | pipelines/sources/crossref/README.md | broken_internal_link | MEDIUM |
| docs\INDEX.md | openalex/README.md | pipelines/sources/openalex/README.md | broken_internal_link | MEDIUM |
| docs\INDEX.md | pubmed/README.md | pipelines/sources/pubmed/README.md | broken_internal_link | MEDIUM |
| docs\INDEX.md | semantic_scholar/README.md | pipelines/sources/semantic_scholar/README.md | broken_internal_link | MEDIUM |
| docs\pipelines\10-document-chembl-transformation.md | 09-document-chembl-extraction. | 09-document-chembl-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\10-document-chembl-transformation.md | 11-document-chembl-validation. | 11-document-chembl-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\10-document-chembl-transformation.md | 00-document-chembl-overview.md | 00-document-chembl-overview.md | broken_internal_link | MEDIUM |
| docs\pipelines\17-document-chembl-config.md | 16-document-chembl-cli.md | 16-document-chembl-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\17-document-chembl-config.md | 00-document-chembl-overview.md | 00-document-chembl-overview.md | broken_internal_link | MEDIUM |
| docs\pipelines\28-chembl2uniprot-mapping.md | 0-9 | [A-Z][A-Z, 0-9][A-Z, 0-9][0-9] | broken_internal_link | MEDIUM |
| docs\styleguide\02-logging-guidelines.md | `docs/logging/` | ../logging/ | broken_internal_link | MEDIUM |
| docs\styleguide\04-deterministic-io.md | `docs/determinism/` | ../determinism/ | broken_internal_link | MEDIUM |
| docs\styleguide\06-cli-contracts.md | `docs/cli/` | ../cli/ | broken_internal_link | MEDIUM |
| docs\styleguide\08-etl-architecture.md | `docs/etl_contract/` | ../etl_contract/ | broken_internal_link | MEDIUM |
| docs\styleguide\09-secrets-config.md | `docs/configs/` | ../configs/ | broken_internal_link | MEDIUM |
| docs\pipelines\document-chembl\00-document-chembl-overview.md | 09-document-pubmed-extraction. | 09-document-pubmed-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-chembl\00-document-chembl-overview.md | 09-document-crossref-extractio | 09-document-crossref-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-chembl\00-document-chembl-overview.md | 09-document-openalex-extractio | 09-document-openalex-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-chembl\00-document-chembl-overview.md | 09-document-semantic-scholar-e | 09-document-semantic-scholar-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 10-document-crossref-transform | 10-document-crossref-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 11-document-crossref-validatio | 11-document-crossref-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 12-document-crossref-io.md | 12-document-crossref-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 13-document-crossref-determini | 13-document-crossref-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 14-document-crossref-qc.md | 14-document-crossref-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 15-document-crossref-logging.m | 15-document-crossref-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 16-document-crossref-cli.md | 16-document-crossref-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 17-document-crossref-config.md | 17-document-crossref-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\00-document-crossref-overview.md | 17-document-crossref-config.md | 17-document-crossref-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-crossref\09-document-crossref-extraction.md | `docs/pipelines/sources/crossr | sources/crossref/00-configuration.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 10-document-openalex-transform | 10-document-openalex-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 11-document-openalex-validatio | 11-document-openalex-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 12-document-openalex-io.md | 12-document-openalex-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 13-document-openalex-determini | 13-document-openalex-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 14-document-openalex-qc.md | 14-document-openalex-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 15-document-openalex-logging.m | 15-document-openalex-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 16-document-openalex-cli.md | 16-document-openalex-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 17-document-openalex-config.md | 17-document-openalex-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\00-document-openalex-overview.md | 17-document-openalex-config.md | 17-document-openalex-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-openalex\09-document-openalex-extraction.md | `docs/pipelines/sources/openal | sources/openalex/00-configuration.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 10-document-pubmed-transformat | 10-document-pubmed-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 11-document-pubmed-validation. | 11-document-pubmed-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 12-document-pubmed-io.md | 12-document-pubmed-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 13-document-pubmed-determinism | 13-document-pubmed-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 14-document-pubmed-qc.md | 14-document-pubmed-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 15-document-pubmed-logging.md | 15-document-pubmed-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 16-document-pubmed-cli.md | 16-document-pubmed-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 17-document-pubmed-config.md | 17-document-pubmed-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\00-document-pubmed-overview.md | 17-document-pubmed-config.md | 17-document-pubmed-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-pubmed\09-document-pubmed-extraction.md | `docs/pipelines/sources/pubmed | sources/pubmed/00-configuration.md | broken_internal_link | MEDIUM |
