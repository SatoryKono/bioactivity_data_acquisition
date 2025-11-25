# Chembl batch pipeline scaffold

This document outlines the lightweight Chembl batch pipelines that implement the cycle **io → extract → normalize → validate → save** for Activity, Assay, Document, Target, and TestItem tables. The implementation favors composability: you can swap normalizers or validators without changing the pipeline core, and most behavior is configured through `PipelineConfig`.

```python
from bioetl.pipelines.chembl.batch import (
    ActivityPipeline,
    CommonNormalizer,
    CommonValidator,
    DummyChemblDbClient,
    PipelineConfig,
)

# Input data stub for the example
raw_activity_rows = {
    1001: {"assay_id": 10, "standard_value": 5.0},
    1002: {"assay_id": 11, "standard_value": 7.5},
}

config = PipelineConfig(
    table_name="activity",
    id_field="activity_id",
    batch_size=500,
    save_mode="return",
    raise_on_validation_error=False,
)

pipeline = ActivityPipeline(
    db_client=DummyChemblDbClient(raw_activity_rows),
    normalizer=CommonNormalizer(),
    validator=CommonValidator(required_fields=["activity_id", "assay_id"]),
    config=config,
)

result = pipeline.run(ids=[1001, 1002, 1003])
print(result.valid_records)  # validated activity rows
print(result.errors)         # structured validation errors (e.g., missing IDs)
print(result.missing_ids)    # ids not found in the data source
```

Concrete pipelines only fix the `table_name`/`id_field` defaults; you can override any stage by subclassing or by swapping the supplied normalizer/validator implementations. The `DummyChemblDbClient` stub makes it easy to run the pipelines in unit tests without a real database.
