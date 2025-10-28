# 8d. Ортологи и изоформы: дополнительные источники

## Обзор

Orthologs (ортологи) и isoforms (изоформы) являются дополнительными источниками данных для targets, обеспечивающими cross-species mapping и alternative protein variants.

---

## 5.1 Orthologs Fetching

### Через UniProt

Orthologs извлекаются через UniProt search/stream с organism filter:

```python
def fetch_orthologs(
    accession: str,
    organism: str,
    cfg: UniprotCfg
) -> list[dict[str, str]]:
    """Fetch orthologs for target across species."""

    # Query: homology search

    query = f"gene_exact:{organism} AND reviewed:true"

    # Search через UniProt API

    url = f"{cfg.base_url}/uniprotkb/search?query={query}&format=json"
    response = requests.get(url, timeout=cfg.timeout_sec)
    response.raise_for_status()

    data = response.json()
    orthologs = []

    for result in data.get("results", []):
        ortholog = {
            "accession": result.get("primaryAccession"),
            "organism": result.get("organism", {}).get("scientificName"),
            "taxon_id": extract_taxon_id(result),
            "gene_symbol": extract_gene_name(result),
        }
        orthologs.append(ortholog)

    return orthologs

```

### ID Mapping для ортологов

UniProt ID Mapping поддерживает cross-species mapping через источники:

- **Ensembl**: cross-species gene orthologs

- **OMA** (Orthologous Matrix): orthology relationships

- **OrthoDB**: ortholog groups across species

```python
def map_orthologs_via_id_mapping(
    accessions: Sequence[str],
    from_organism: str,
    to_organism: str,
    cfg: UniprotCfg
) -> pd.DataFrame:
    """Map orthologs via UniProt ID Mapping."""

    # Submit ID mapping job

    job_id = submit_id_mapping_job(
        accessions,
        from_type=f"UniProtKB_{from_organism}",
        to_type=f"UniProtKB_{to_organism}",
        cfg=cfg
    )

    # Poll и stream results

    poll_mapping_status(job_id, cfg)
    results = stream_mapping_results(job_id, cfg)

    return results

```

### Приоритеты

При наличии множественных orthologs применяется приоритет:

**human > mouse > rat > остальные**

**Обоснование:**

- Human: наиболее изученный организм

- Mouse: стандартная модель для in vivo

- Rat: дополнительная модель

- Другие: fallback только

---

## 5.2 Isoforms Resolution

### UniProt isoform entries

UniProt различает canonical и isoform entries:

**Canonical accession:**

- Формат: `P12345`

- Представляет основной белок

- Reference sequence для большинства случаев

**Isoform accession:**

- Формат: `P12345-1`, `P12345-2`

- Альтернативные варианты сплайсинга

- Вариации последовательности

### Extracting isoform data

```python
def extract_isoforms_from_uniprot(
    accession: str,
    cfg: UniprotCfg
) -> list[dict[str, str]]:
    """Extract isoforms for canonical accession."""

    # Fetch UniProt entry

    entry = fetch_uniprot(accession, cfg=cfg)

    isoforms = []

    # Extract from comments.ALTERNATIVE_PRODUCTS

    comments = entry.get("comments", [])
    for comment in comments:
        if comment.get("commentType") == "ALTERNATIVE_PRODUCTS":
            isoforms_list = comment.get("isoforms", [])

            for iso in isoforms_list:
                if not isinstance(iso, dict):
                    continue

                iso_data = {
                    "isoform_id": iso.get("id"),  # P12345-1

                    "isoform_name": iso.get("name"),
                    "isoform_sequence": iso.get("sequence"),
                    "isoform_note": iso.get("note"),
                }
                isoforms.append(iso_data)

    return isoforms

```

### Alternative splicing

Isoforms возникают из alternative splicing:

- **Exon skipping**: пропуск экзонов

- **Alternative splice sites**: разные границы экзонов

- **Mutually exclusive exons**: взаимоисключающие экзоны

**Sequence differences:**

- Сохраняются только отличающиеся участки

- Canonical sequence + differences → isoform sequence

### Secondary accessions

UniProt содержит secondary accessions для:

- **Merged entries**: устаревшие IDs, объединенные в canonical

- **Replaced entries**: замененные из-за некорректности

**Mapping таблица:**

```python
def get_secondary_accession_mapping(
    accession: str,
    cfg: UniprotCfg
) -> list[str]:
    """Get secondary accessions for primary accession."""

    entry = fetch_uniprot(accession, cfg=cfg)

    # Extract secondaryAccessions

    secondary = entry.get("secondaryAccessions", [])

    return secondary if isinstance(secondary, list) else []

```

---

## 5.3 Fallback Strategies

### Priority order

Когда данных нет в primary источнике, применяется fallback стратегия:

**1. UniProt → ChEMBL accession**

```python
if not uniprot_accession:
    # Fallback на ChEMBL xref

    uniprot_accession = extract_uniprot_from_chembl_xrefs(target)

```

**2. IUPHAR → gene symbol fuzzy**

```python
if not iuphar_mapping:
    # Fuzzy match по gene symbol

    iuphar_mapping = fuzzy_gene_symbol_match(
        gene_symbol,
        iuphar_targets
    )

```

**3. Orthologs → closest species**

```python
if not human_ortholog:
    # Fallback на mouse

    human_ortholog = get_mouse_ortholog(accession)

    if not human_ortholog:
        # Fallback на closest species

        human_ortholog = get_closest_species_ortholog(accession)

```

### Graceful degradation

Pipeline продолжает работу даже при отсутствии некоторых данных:

- **Orthologs**: optional enrichment

- **Isoforms**: optional enrichment

- **Missing data**: заполнение "-" или NULL

- **Logging**: детальное логирование пропусков

---

## Примеры использования

### Complete enrichment pipeline

```python
from library.integration.uniprot_library import fetch_uniprot, extract_isoforms
from library.integration import ortholog_mapping

def enrich_target_with_orthologs_and_isoforms(
    target_row: pd.Series,
    cfg: UniprotCfg
) -> pd.Series:
    """Enrich single target with orthologs and isoforms."""

    accession = target_row.get("uniprot_id_primary")
    organism = target_row.get("organism")

    # 1. Extract isoforms

    isoforms = extract_isoforms_from_uniprot(accession, cfg)
    target_row["isoform_ids"] = "|".join([iso["isoform_id"] for iso in isoforms])
    target_row["isoform_names"] = "|".join([iso["isoform_name"] for iso in isoforms])

    # 2. Fetch orthologs

    orthologs = fetch_orthologs(accession, organism, cfg)

    # Prioritize human > mouse > rat

    human_ortholog = next((o for o in orthologs if "Homo sapiens" in o.get("organism", "")), None)
    mouse_ortholog = next((o for o in orthologs if "Mus musculus" in o.get("organism", "")), None)
    rat_ortholog = next((o for o in orthologs if "Rattus norvegicus" in o.get("organism", "")), None)

    if human_ortholog:
        target_row["human_ortholog"] = human_ortholog["accession"]
        target_row["human_gene_symbol"] = human_ortholog["gene_symbol"]
    elif mouse_ortholog:
        target_row["human_ortholog"] = mouse_ortholog["accession"]  # Fallback

        target_row["human_gene_symbol"] = mouse_ortholog["gene_symbol"]
    elif rat_ortholog:
        target_row["human_ortholog"] = rat_ortholog["accession"]
        target_row["human_gene_symbol"] = rat_ortholog["gene_symbol"]

    return target_row

```

---

## Ссылки

- [UniProt Alternative Products](https://www.uniprot.org/help/alternative_products)

- [UniProt Orthologs](https://www.uniprot.org/help/orthology)

- [Ensembl Ortholog Mapping](https://www.ensembl.org/info/genome/compara/orthologs.html)

- [OMA Orthologous Matrix](https://omabrowser.org/)

