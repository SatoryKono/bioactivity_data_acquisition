# Test Item ChEMBL Pipeline

The testitem_chembl pipeline describes tested items in ChEMBL, such as
compounds or batches used in experiments.

## Purpose

- Represent test items that participate in assays and activities.
- Link test items to underlying molecules or molecule forms.
- Provide additional experimental context where available.

## Inputs and sources

- Primary source: ChEMBL test item or related tables.
- Cross-links to molecule, assay, and activity records.

## Outputs and schemas

The pipeline produces test item datasets that are validated using
Pandera schemas from the bioetl.schemas registry.

Field-level descriptions for related entities can be found in
../../datatypes/molecule.md and other datatype files under
../../datatypes/.

## Notes

This documentation focuses on the domain model. CLI commands and config
options for running this pipeline should be documented centrally in the
CLI and configuration documentation.

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
