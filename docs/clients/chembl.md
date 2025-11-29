# ChEMBL Clients

Enricher configuration for ChEMBL pipelines now lives under `clients.enrichers`
in the merged configuration. The former top-level `enrichers` section is no
longer used; keep client settings in one place and reference them from the
pipeline config.

## Manual enrichment pattern

Automatic enrichment inside pipelines is intentionally disabled. Pipelines that
need enrichment should instantiate clients explicitly, for example:

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
