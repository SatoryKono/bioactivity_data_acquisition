# 0. BioETL Framework Overview

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Introduction

The `bioetl` framework is a specialized system designed for building robust, maintainable, and verifiable ETL (Extract, Transform, Load) pipelines. Its primary goal is to provide a standardized, opinionated architecture that guarantees data quality, consistency, and reproducibility across all data acquisition processes.

This document provides a high-level overview of the framework's architecture, its core principles, and the goals it aims to achieve.

## Core Principles

The entire framework is built upon a set of fundamental principles that ensure the reliability and integrity of every pipeline:

- **Determinism**: Every pipeline run must be deterministic. Given the same input data and configuration, the framework will always produce a bit-for-bit identical output. This is crucial for debugging, auditing, and ensuring trust in the data.
- **Reproducibility**: Beyond being deterministic, pipeline runs are fully reproducible. Each output artifact is accompanied by comprehensive metadata that captures the exact conditions of the run, including configuration, source versions, and data hashes, allowing for full lineage tracking.
- **Strict Validation**: Data quality is not an afterthought; it is enforced. The framework uses the Pandera library to perform strict schema validation on all data flowing through the system. Any deviation from the defined schema results in an immediate pipeline failure, preventing corrupt data from ever being written.
- **Atomicity**: The framework guarantees that all output artifacts—the dataset and its metadata—are written atomically. A pipeline run either succeeds completely or it fails cleanly, leaving no partial or corrupt files behind.
- **Clarity and Convention**: Pipelines are configured through clear, declarative YAML files, which are validated against strongly-typed Pydantic models. This convention-over-configuration approach simplifies development and makes pipelines self-documenting.

## High-Level Architecture

The `bioetl` framework consists of several key components that work together to provide a cohesive development and execution environment:

1.  **Pipeline Interface (`PipelineBase`)**: At the heart of the framework is the abstract `PipelineBase` class. Every pipeline must inherit from this class, which defines a standardized lifecycle and a clear contract for developers to implement.

2.  **Four-Stage Pipeline Lifecycle**: Each pipeline follows a strict, sequential lifecycle composed of four distinct stages, orchestrated by the `run()` method:
    - **`extract`**: Retrieves data from the source system.
    - **`transform`**: Applies business logic, normalization, and enrichment to the data.
    - **`validate`**: Verifies the transformed data against a strict Pandera schema.
    - **`write`**: Atomically writes the final dataset and its metadata.
    
    The **`run()`** method orchestrates the execution of these stages in sequence, handling logging, timing, and error management. It is not a stage itself, but rather the orchestrator that ensures the stages execute in the correct order.

3.  **Declarative Configuration**: Pipeline behavior is driven by YAML configuration files. This allows developers to define sources, extraction parameters, transformation rules, and output settings in a declarative manner, separating logic from configuration.

4.  **Schema-Driven Validation**: The framework leverages Pandera for rigorous, schema-driven data validation. Schemas define not just data types but also constraints, column order, and other quality checks, ensuring that all data conforms to its expected structure.

5.  **Command-Line Interface (CLI)**: A Typer-based CLI provides a unified entry point for running and managing pipelines. It allows users to list available pipelines, execute them with custom parameters, and perform dry runs to validate configurations without writing data.

## Goals of the Framework

The primary objective of the `bioetl` framework is to empower developers and analysts to:

- **Build Reliable Pipelines Faster**: By providing a clear contract and handling the boilerplate of logging, error management, and file I/O, the framework allows developers to focus solely on the business logic of their pipelines.
- **Ensure High Data Quality**: Strict, non-negotiable validation at the core of the framework prevents data quality issues at the source, rather than trying to fix them downstream.
- **Guarantee Reproducibility and Trust**: The focus on determinism and detailed metadata ensures that every dataset is trustworthy and its origin can be traced and verified at any time.
