# Link Check Report

## Missing Files from .lychee.toml

| Source | File | Type | Criticality |
|--------|------|------|-------------|
| .lychee.toml | docs/architecture/00-architecture-overview.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/architecture/03-data-sources-and-spec.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/cli/CLI.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/configs/CONFIGS.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/pipelines/PIPELINES.md | declared_but_missing | CRITICAL |
| .lychee.toml | docs/qc/QA_QC.md | declared_but_missing | CRITICAL |

## Broken Internal Links

| Source | Link Text | Link Path | Type | Criticality |
|--------|-----------|-----------|------|-------------|
| docs\INDEX.md | Architecture Decision Records | adr/ | broken_internal_link | MEDIUM |
| docs\pipelines\28-chembl2uniprot-mapping.md | 0-9 | [A-Z][A-Z, 0-9][A-Z, 0-9][0-9] | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 10-document-semantic-scholar-t | 10-document-semantic-scholar-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 11-document-semantic-scholar-v | 11-document-semantic-scholar-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 12-document-semantic-scholar-i | 12-document-semantic-scholar-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 13-document-semantic-scholar-d | 13-document-semantic-scholar-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 14-document-semantic-scholar-q | 14-document-semantic-scholar-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 15-document-semantic-scholar-l | 15-document-semantic-scholar-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 16-document-semantic-scholar-c | 16-document-semantic-scholar-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 17-document-semantic-scholar-c | 17-document-semantic-scholar-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\00-document-semantic-scholar-overview.md | 17-document-semantic-scholar-c | 17-document-semantic-scholar-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\document-semantic-scholar\09-document-semantic-scholar-extraction.md | `docs/pipelines/sources/semant | sources/semantic-scholar/00-configuration.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-chembl\00-target-chembl-overview.md | 09-target-iuphar-extraction.md | 09-target-iuphar-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-chembl\00-target-chembl-overview.md | 09-target-uniprot-extraction.m | 09-target-uniprot-extraction.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 10-target-iuphar-transformatio | 10-target-iuphar-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 11-target-iuphar-validation.md | 11-target-iuphar-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 12-target-iuphar-io.md | 12-target-iuphar-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 13-target-iuphar-determinism.m | 13-target-iuphar-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 14-target-iuphar-qc.md | 14-target-iuphar-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 15-target-iuphar-logging.md | 15-target-iuphar-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 16-target-iuphar-cli.md | 16-target-iuphar-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 17-target-iuphar-config.md | 17-target-iuphar-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-iuphar\00-target-iuphar-overview.md | 17-target-iuphar-config.md | 17-target-iuphar-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 10-target-uniprot-transformati | 10-target-uniprot-transformation.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 11-target-uniprot-validation.m | 11-target-uniprot-validation.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 12-target-uniprot-io.md | 12-target-uniprot-io.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 13-target-uniprot-determinism. | 13-target-uniprot-determinism.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 14-target-uniprot-qc.md | 14-target-uniprot-qc.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 15-target-uniprot-logging.md | 15-target-uniprot-logging.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 16-target-uniprot-cli.md | 16-target-uniprot-cli.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 17-target-uniprot-config.md | 17-target-uniprot-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\00-target-uniprot-overview.md | 17-target-uniprot-config.md | 17-target-uniprot-config.md | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\09-target-uniprot-extraction.md | 0-9 | [A-Z][A-Z, 0-9][A-Z, 0-9][0-9] | broken_internal_link | MEDIUM |
| docs\pipelines\target-uniprot\09-target-uniprot-extraction.md | 0-9 | [A-Z][A-Z, 0-9][A-Z, 0-9][0-9] | broken_internal_link | MEDIUM |
| docs\pipelines\testitem-chembl\00-testitem-chembl-overview.md | 09-testitem-pubchem-extraction | 09-testitem-pubchem-extraction.md | broken_internal_link | MEDIUM |
| docs\styleguide\02-logging-guidelines.md | `docs/logging/` | ../logging/ | broken_internal_link | MEDIUM |
| docs\styleguide\04-deterministic-io.md | `docs/determinism/` | ../determinism/ | broken_internal_link | MEDIUM |
| docs\styleguide\06-cli-contracts.md | `docs/cli/` | ../cli/ | broken_internal_link | MEDIUM |
| docs\styleguide\08-etl-architecture.md | `docs/etl_contract/` | ../etl_contract/ | broken_internal_link | MEDIUM |
| docs\styleguide\09-secrets-config.md | `docs/configs/` | ../configs/ | broken_internal_link | MEDIUM |
