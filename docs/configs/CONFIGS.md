# BioETL Configuration System

This directory contains the configuration files and documentation for the `bioetl` pipeline framework.

## System Specification

The `bioetl` configuration system is a typed, layered system built on Pydantic for validation and robustness. It supports reusable profiles to ensure consistency across pipelines.

**For a complete technical reference, including the full `PipelineConfig` data model, the layer merging algorithm, and usage examples, please see the official specification document:**

- **[Specification: Typed Configurations and Profiles](./00-typed-configs-and-profiles.md)**

## Configuration Profiles

Reusable configuration profiles are stored in the `configs/profiles/` directory. These files provide baseline settings that can be extended by individual pipeline configurations.

- **[`base.yaml`](../../configs/profiles/base.yaml):** Provides default settings for HTTP clients, caching, and pathing.
- **[`determinism.yaml`](../../configs/profiles/determinism.yaml):** Provides settings to ensure reproducible, deterministic outputs.
