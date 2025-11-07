# 8. New Pipeline Implementation Guide

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Overview

This guide provides a step-by-step walkthrough for creating a new ETL pipeline within the `bioetl` framework. It brings together all the concepts from the previous documents—the `PipelineBase` contract, configuration, schema validation, and CLI integration—into a practical workflow.

By following these steps, you can ensure that your new pipeline is robust, maintainable, and fully compliant with the framework's architecture.

## Step 1: Create the Pipeline Class

The first step is to create a new Python file for your pipeline in the `src/bioetl/pipelines/` directory. The file should contain a new class that inherits from `PipelineBase`.

- **Location**: `src/bioetl/pipelines/<source>/<pipeline_name>.py`
- **Example**: `src/bioetl/pipelines/uniprot/protein.py`

```python
# file: src/bioetl/pipelines/uniprot/protein.py
import pandas as pd
from bioetl.pipelines.base import PipelineBase
from bioetl.config import PipelineConfig

class UniprotProteinPipeline(PipelineBase):
    """
    ETL pipeline for fetching protein data from the UniProt API.
    """
    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        # TODO: Initialize your API client here.
        self.api_client = None

    def extract(self) -> pd.DataFrame:
        # TODO: Implement data extraction logic.
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO: Implement data transformation logic.
        raise NotImplementedError
```

## Step 2: Create the Pandera Schema

Define the schema for your pipeline's **output**. This is a critical step that dictates the final structure of your data.

- **Location**: `src/bioetl/schemas/<source>/<pipeline_name>_out.py`
- **Example**: `src/bioetl/schemas/uniprot/protein_out.py`

```python
# file: src/bioetl/schemas/uniprot/protein_out.py
import pandera as pa
from pandera.typing import Series, String, Int64

class UniprotProteinSchema(pa.SchemaModel):
    """
    Pandera schema for the final, transformed UniProt protein data.
    """
    uniprot_id: Series[String] = pa.Field(nullable=False, unique=True)
    protein_name: Series[String] = pa.Field(nullable=False)
    gene_name: Series[String] = pa.Field(nullable=True)
    organism_id: Series[Int64] = pa.Field(nullable=False)

    class Config:
        ordered = True
        lazy = True
```

## Step 3: Create the YAML Configuration

Create the YAML configuration file that will drive your pipeline. Start by copying the skeleton from the configuration guide (`02-pipeline-config.md`) and filling in the details.

- **Location**: `src/bioetl/configs/pipelines/<source>/<pipeline_name>.yaml`
- **Example**: `src/bioetl/configs/pipelines/uniprot/protein.yaml`

```yaml
# file: src/bioetl/configs/pipelines/uniprot/protein.yaml
extends:
  - ../../defaults/base.yaml
  - ../../defaults/determinism.yaml

source:
  endpoint: "https://www.uniprot.org/uniprot/"
  rps_limit: 15

extract:
  pagination: { type: "cursor", cursor_key: "Link", page_size: 500 }

validate:
  schema_out: "schemas/uniprot/protein_out.py"
  enforce_column_order: true

write:
  format: "parquet"
  sort_by: ["uniprot_id"]
  hash_row: ["uniprot_id", "protein_name", "gene_name", "organism_id"]
  hash_business_key: ["uniprot_id"]
  output_dir: "data/output/uniprot_protein"

# ... and other sections as needed ...
```

## Step 4: Implement the `extract()` Method

Flesh out the `extract()` method in your pipeline class. This involves:
1. Initializing a robust API client (as described in `03-extraction.md`).
2. Implementing the logic to fetch all pages of data from the source.
3. Returning the raw data as a DataFrame.

## Step 5: Implement the `transform()` Method

Flesh out the `transform()` method. This is typically an iterative process:
1. Implement the core transformation logic (renaming, normalization, type casting).
2. Run a `--dry-run` to see the validation errors from Pandera.
3. Refine the transformation logic to fix the errors.
4. Repeat until the `--dry-run` completes successfully.

**Workflow using the CLI:**

```bash
# Use --limit to test with a small number of records for faster iteration.
python -m bioetl.cli.main uniprot_protein \
    --output-dir /tmp/uniprot-test \
    --limit 50 \
    --dry-run
```

## Step 6: Full Test Run

Once the `--dry-run` is successful, perform a full test run without the `--dry-run` or `--limit` flags.

```bash
python -m bioetl.cli.main uniprot_protein --output-dir /data/output/uniprot/protein-latest
```

Inspect the output artifacts:
- Check the Parquet file to ensure the data looks correct.
- Review the `meta.yaml` file to ensure all the fields (run ID, hashes, row count) have been populated correctly.

## Step 7: Automatic CLI Registration

That's it! Because you followed the framework's conventions, your new `uniprot_protein` pipeline is automatically registered with the CLI. You can confirm this by running:

```bash
python -m bioetl.cli.main list
```

Your new pipeline should appear in the list of available commands, ready for production use.
