# Assay ChEMBL Pipeline

The assay_chembl pipeline focuses on assay-level metadata derived from
ChEMBL.

## Purpose

- Capture assay identifiers and basic metadata.
- Describe assay types, formats, and biological context.
- Provide a link between activities, targets, and test items.

## Inputs and sources

- Primary source: ChEMBL assay tables and related REST endpoints.
- Cross-links to activity, target, and test item records.

## Outputs and schemas

The pipeline writes one or more assay datasets that are validated using
Pandera schemas from the bioetl.schemas registry.

Field-level descriptions for the assay table are documented in
../../datatypes/assay.md.

## Notes

CLI examples and configuration options for this pipeline are intended to
follow the unified CLI documentation, so they are not duplicated here.

Автоматического enrichment нет. Пример ручного вызова клиента:

```python
from bioetl.clients import EnricherClientFactory

# Получаем фабрику из config
factory = EnricherClientFactory.from_config(config.get("clients", {}).get("enrichers"))
pubchem = factory.create("pubchem")

# Внутри pipeline stage (явно)
def enrich_pubchem_column(df):
    if df.empty or "inchi_key" not in df.columns:
        return df
    df = df.copy()
    df["pubchem_enrichment"] = df["inchi_key"].apply(
        lambda v: None if pd.isna(v) else pubchem.lookup(v)
    )
    return df
```
