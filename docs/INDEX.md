# BioETL Documentation

This directory contains the human-readable documentation for the BioETL
project. It describes the architecture, pipelines, data schemas, quality
checks, CLI usage, and development guidelines.

## Main Sections

### Core Documentation

- **00-project-overview.md** - Project overview and quick start
- **01-architecture-overview.md** - Architecture overview and layers
- **02-pipelines-overview.md** - Pipelines overview and lifecycle
- **03-docs-structure-and-style.md** - Documentation structure and style guidelines
- **04-docs-and-ci-sync.md** - Documentation and CI synchronization
- **05-development-and-testing.md** - Development and testing guidelines

### Client Architecture

- **06-rest-clients-yaml-migration.md** - REST client YAML configuration plan
- **07-chembl-transport-examples.md** - ChEMBL transport layer and client protocols
- **08-client-abstractions-plan.md** - Plan for eliminating duplication and aligning clients
- **09-clients-contract.md** - Unified client layer contract
- **10-clients-structure-plan.md** - Plan for aligning `src/bioetl/clients` file structure

### Code Analysis & Refactoring

- **11-dead-base-class-review.md** - Review of base classes and candidates for removal
- **12-duplication-analysis.md** - Duplication analysis map
- **13-pyarch003-refactor-plan.md** - Refactoring plan

### Unified Client Interface

- **14-unified-client-interface.md** - Unification of client interface for external data sources
- **15-unified-data-clients.md** - Unified data clients
- **16-unified-provider-contract.md** - Unified provider contract

### Entity Policies

- **17-new-entity-implementation-policy.md** - Policy for creating and locating new objects (ABC / Default / Impl)
- **18-new-entity-naming-policy.md** - Complete naming policy for BioETL

## Subdirectories

- **cli/** - CLI commands and configuration (see `cli/INDEX.md`)
- **clients/** - Client architecture documentation (see `clients/00-clients-overview.md`)
- **pipelines/** - Pipeline-specific documentation (see `pipelines/INDEX.md`)
- **qc/** - Quality control artifacts (see `qc/INDEX.md`)
- **schemas/** - Data schemas registry (see `schemas/INDEX.md`)
