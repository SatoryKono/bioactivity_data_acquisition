# 8b. UniProt источник: обогащение protein данных

## Обзор

UniProt служит источником обогащения для protein targets, предоставляя детальную информацию о белках: имена, gene symbols, таксономию, изоформы, PTM, features и cross-references на другие базы данных.

**Официальная документация**: [UniProt Website API](https://academic.oup.com/bioinformatics/article/38/Supplement_2/ii29/6702002)

---

## 3.1 UniProt REST API

### Базовая конфигурация

- **Base URL**: `https://rest.uniprot.org/`

- **API version**: v2 (REST)

- **Форматы**: JSON (рекомендуется)

### Режимы работы

#### entry - Одиночный запрос

Получение детальной информации об одном белке.

**Пример:**

```bash
GET https://rest.uniprot.org/uniprotkb/P29274?fields=accession,protein_name,gene_names,organism,taxonomy

```

**Использование:**

- Малое количество IDs (<100)

- Детальная информация

- Простая реализация

#### search - Поиск с пагинацией

Поиск белков по query с возвратом результатов постранично.

**Пример:**

```bash
GET https://rest.uniprot.org/uniprotkb/search?query=organism_id:9606&size=25&fields=accession,protein_name

```

**Параметры:**

- `query`: UniProt query syntax (например, organism_id, reviewed, length)

- `size`: количество результатов на страницу (default: 25)

- `fields`: список полей для возврата (минимизация трафика)

**Пагинация:**

```bash

# Получение следующей страницы

GET {next_page_url_from_response}

```

#### stream - Массовая выгрузка

Массовая выгрузка без пагинации (экономия циклов).

**Пример:**

```bash
GET https://rest.uniprot.org/uniprotkb/stream?query=accession:P29274&compressed=false&format=json

```

**Преимущества:**

- Нет необходимости отслеживать пагинацию

- Подходит для ночных прогонов

- Stream обработка

**Ограничения:**

- Нельзя возобновить после сбоя

- Таймаут 5 минут на запрос

### ID Mapping

UniProt ID Mapping позволяет массово конвертировать идентификаторы между базами данных.

**Endpoint:** `/idmapping/run`

**Лимит:** 100,000 IDs на задачу

**Workflow:**

1. **Submit job**: POST с IDs и from/to форматами

2. **Poll status**: GET `/idmapping/status/{jobId}` до completion

3. **Stream results**: GET `/idmapping/stream/{jobId}`

**Пример:**

```bash

# 1. Submit

POST https://rest.uniprot.org/idmapping/run
Content-Type: application/json
{
  "ids": ["P29274", "P12345", "Q98765"],
  "from": "UniProtKB_AC-ID",
  "to": "UniProtKB"
}

# Response: {"jobId": "abc123..."}

# 2. Poll

GET https://rest.uniprot.org/idmapping/status/abc123

# Response: {"status": "RUNNING" | "FINISHED" | "FAILED"}

# 3. Stream results

GET https://rest.uniprot.org/idmapping/stream/abc123?format=tsv

```

**Best practice:**

- Для больших наборов (>100k): партиционирование на батчи

- Использование для ночных прогонов

- Кэширование результатов mapping

### Фильтрация полей

UniProt поддерживает выбор полей через параметр `fields`:

```bash
?fields=accession,protein_name,gene_names,organism,taxonomy,sequence,ft_signal,ft_topo_dom

```

**Преимущества:**

- Минимизация размера ответа

- Ускорение обработки

- Снижение трафика

**Обязательные поля для target enrichment:**

- `accession`, `gene_names`, `protein_name`

- `organism`, `taxonomy` (lineage)

- `ft_signal`, `ft_topo_dom`, `ft_transmem` (features)

- `cc_ptm` (post-translational modifications)

### Rate limiting

UniProt автоматически throttle запросы:

- **Рекомендуемая скорость**: 5 requests/second

- **Connection pooling**: переиспользование соединений

- **Backoff**: экспоненциальный при 429 Too Many Requests

---

## 3.2 ID Mapping стратегия

### Шаг 1: Submit job

```python
def submit_id_mapping_job(
    ids: Sequence[str], from_type: str, to_type: str, cfg: UniprotCfg
) -> str:
    """Submit ID mapping job and return jobId."""
    url = f"{cfg.base_url}/idmapping/run"
    payload = {
        "ids": list(ids),
        "from": from_type,  # "UniProtKB_AC-ID"

        "to": to_type,      # "UniProtKB"

    }
    response = requests.post(url, json=payload, timeout=cfg.timeout_sec)
    response.raise_for_status()
    return response.json()["jobId"]

```

### Шаг 2: Poll status

```python
def poll_mapping_status(job_id: str, cfg: UniprotCfg) -> str:
    """Poll ID mapping status until completion."""
    url = f"{cfg.base_url}/idmapping/status/{job_id}"

    while True:
        response = requests.get(url, timeout=cfg.timeout_sec)
        response.raise_for_status()
        status = response.json()["status"]

        if status == "FINISHED":
            return status
        elif status == "FAILED":
            raise RuntimeError(f"ID mapping failed for job {job_id}")

        time.sleep(2)  # Poll interval

```

### Шаг 3: Stream результаты

```python
def stream_mapping_results(job_id: str, cfg: UniprotCfg) -> pd.DataFrame:
    """Stream ID mapping results as DataFrame."""
    url = f"{cfg.base_url}/idmapping/stream/{job_id}?format=tsv"
    response = requests.get(url, timeout=cfg.timeout_sec, stream=True)
    response.raise_for_status()

    # Parse TSV

    return pd.read_csv(response.raw, sep="\t")

```

### Валидация/обновление Accessions

ChEMBL предоставляет accessions через `/target_component`. ID Mapping:

1. Валидирует формат accession

2. Проверяет актуальность (merged entries → current accession)

3. Обновляет устаревшие IDs

### Партиционирование

Для больших наборов (>100k):

```python
def partition_accessions(accessions: Sequence[str], max_size: int = 100000) -> Iterator[Sequence[str]]:
    """Partition accessions into batches of max_size."""
    for i in range(0, len(accessions), max_size):
        yield accessions[i:i + max_size]

```

---

## 3.3 Protein Names

### extract_names функция

Извлечение всех protein и gene names из UniProt entry:

```python
def extract_names(data: Any) -> set[str]:
    """Return all protein and gene names found in data."""
    names: set[str] = set()

    # Поддержка разных форматов ответа

    if isinstance(data, dict) and "results" in data:
        entries = data["results"]
    elif isinstance(data, list):
        entries = data
    else:
        entries = [data]

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # Извлечение protein names

        names.update(_extract_protein_names(entry.get("proteinDescription", {})))
        # Извлечение gene names

        names.update(_extract_gene_names(entry))

    return names

```

### recommendedName vs alternativeNames

UniProt различает:

- **recommendedName**: каноническое название

- **alternativeNames**: синонимы и альтернативы

```python
def _extract_protein_names(protein_description: dict) -> set[str]:
    """Extract protein names from proteinDescription."""
    names: set[str] = set()

    # Recommended name

    recommended = protein_description.get("recommendedName")
    if recommended and isinstance(recommended, dict):
        # Short name

        short_name = recommended.get("shortName", {}).get("value")
        if short_name:
            names.add(short_name)
        # Full name

        full_name = recommended.get("fullName", {}).get("value")
        if full_name:
            names.add(full_name)
        # EC number

        ec_number = recommended.get("ecNumber")
        if ec_number:
            names.add(f"EC:{ec_number}")

    # Alternative names

    alternatives = protein_description.get("alternativeNames", [])
    for alt in alternatives:
        if isinstance(alt, dict):
            short_name = alt.get("shortName", {}).get("value")
            if short_name:
                names.add(short_name)
            full_name = alt.get("fullName", {}).get("value")
            if full_name:
                names.add(full_name)

    return names

```

### Gene names

```python
def _extract_gene_names(entry: dict) -> set[str]:
    """Extract gene names from entry."""
    names: set[str] = set()

    genes = entry.get("genes", [])
    for gene in genes:
        if not isinstance(gene, dict):
            continue

        # Gene name

        gene_name = gene.get("geneName", {}).get("value")
        if gene_name:
            names.add(gene_name)

        # Synonyms

        synonyms = gene.get("synonyms", [])
        for syn in synonyms:
            if isinstance(syn, dict):
                value = syn.get("value")
                if value:
                    names.add(value)

        # Ordered locus names

        orfs = gene.get("orderedLocusNames", [])
        for orf in orfs:
            if isinstance(orf, dict):
                value = orf.get("value")
                if value:
                    names.add(value)

    return names

```

### Нормализация

Извлеченные names нормализуются:

- **Trim whitespace**: `name.strip()`

- **Lowercase**: для consistent comparison

- **Pipe-delimited**: объединение в строку `"name1|name2|name3"`

---

## 3.4 Organism Taxonomy

### extract_organism функция

Извлечение таксономии из UniProt entry:

```python
def extract_organism(data: Any) -> dict[str, str]:
    """Return organism taxonomy information."""
    result = {
        "genus": "",
        "superkingdom": "",
        "phylum": "",
        "lineage_class": "",
        "taxon_id": "",
    }

    # Поддержка разных форматов

    if isinstance(data, dict) and "results" in data:
        entries = data["results"]
    elif isinstance(data, list):
        entries = data
    else:
        entries = [data]

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        org = entry.get("organism", {})
        if not isinstance(org, dict):
            continue

        # Scientific name

        scientific_name = org.get("scientificName")
        if scientific_name and not result["genus"]:
            result["genus"] = scientific_name.split()[0]  # First token

        # Taxonomy

        taxonomy = org.get("taxonomy")
        if isinstance(taxonomy, list):
            for level in taxonomy:
                # Mapping lineage levels

                if level == "Eukaryota":
                    result["superkingdom"] = "Eukaryota"
                elif level in {"Bacteria", "Archaea"}:
                    result["superkingdom"] = level
                # ... additional mapping logic

        # Taxon ID

        db_references = org.get("dbReferences", [])
        for ref in db_references:
            if isinstance(ref, dict) and ref.get("type") == "NCBI Taxonomy":
                result["taxon_id"] = ref.get("id", "")

    return result

```

### Lineage extraction

UniProt `organism.taxonomy` содержит lineage в виде списка:

```json
{
  "organism": {
    "taxonomy": ["Eukaryota", "Metazoa", "Chordata", "Mammalia", ...]
  }
}

```

**Derivation genus:**

- Первый токен от `organism.scientificName`

- Пример: "Homo sapiens" → genus = "Homo"

**Cellularity classification:**

- Используется в postprocessing для вывода типа клетки

- Superkingdom: Eukaryota, Bacteria, Archaea

- Определяет whether target is human-readable

---

## 3.5 Isoforms

### extract_isoform функция

Извлечение информации об изоформах:

```python
def extract_isoform(data: Any) -> dict[str, str]:
    """Extract isoform information from UniProt entry."""
    result = {
        "isoform_ids": "",
        "isoform_names": "",
        "isoform_synonyms": "",
    }

    # Canonical vs isoform accessions

    accession = data.get("primaryAccession")
    if "-" in accession:
        # Isoform accession: P12345-1

        result["isoform_ids"] = accession
    else:
        # Canonical accession: P12345

        result["isoform_ids"] = accession

    # Alternative products (isoforms)

    comments = data.get("comments", [])
    for comment in comments:
        if isinstance(comment, dict) and comment.get("commentType") == "ALTERNATIVE_PRODUCTS":
            isoforms = comment.get("isoforms", [])
            if isinstance(isoforms, list):
                isoform_ids = [iso.get("id") for iso in isoforms if isinstance(iso, dict)]
                result["isoform_ids"] = "|".join(isoform_ids)

                isoform_names = []
                for iso in isoforms:
                    if isinstance(iso, dict):
                        names = iso.get("name")
                        if names:
                            isoform_names.append(names)
                result["isoform_names"] = "|".join(isoform_names)

    return result

```

### Canonical vs isoform accessions

- **Canonical**: `P12345` (основной белок)

- **Isoform**: `P12345-1`, `P12345-2` (альтернативные варианты)

**Secondary accessions:**

- Устаревшие или merged entries

- Сохраняются в `secondaryAccessions`

- Mapping к текущему accession через ID Mapping

---

## 3.6 PTM и Features

### extract_ptm функция

Извлечение post-translational modifications:

```python
def extract_ptm(data: Any) -> dict[str, str]:
    """Extract PTM information."""
    result = {}

    comments = data.get("comments", [])
    for comment in comments:
        if not isinstance(comment, dict):
            continue

        comment_type = comment.get("commentType")

        # PTM modifications

        if comment_type == "PTM":
            ptm_texts = comment.get("texts", [])
            if isinstance(ptm_texts, list):
                for text in ptm_texts:
                    if isinstance(text, dict):
                        ptm_type = text.get("evidences", [{}])[0].get("note")
                        value = text.get("value")
                        if ptm_type and value:
                            result[f"ptm_{ptm_type.lower()}"] = value

        # Features

        features = data.get("features", [])
        for feature in features:
            if not isinstance(feature, dict):
                continue

            feature_type = feature.get("type")
            if feature_type:
                # Boolean flags

                result[feature_type] = "true"

                # Location and description

                location = feature.get("location", {})
                description = feature.get("description")
                if description:
                    result[f"{feature_type}_description"] = description

    return result

```

### PTM типы

- **Glycosylation**: `cc_ptm` с типом "glycosylation"

- **Lipidation**: `cc_ptm` с типом "lipidation"

- **Disulfide bonds**: `features.type == "DISULFID"`

- **Modified residues**: `features.type == "MOD_RES"`

- **Phosphorylation**: `features.type == "PHOSPHO"`

- **Acetylation**: `features.type == "METHYL"`

- **Ubiquitination**: `features.type == "UBIQUITIN"`

### Features

**Трансмембранные и мембраны:**

- `ft_signal`: signal peptide

- `ft_propep`: propeptide

- `ft_transmem`: transmembrane region

- `ft_topo_dom`: topological domain

- `ft_intramem`: intramembrane region

**Boolean flags:**

- Преобразование в boolean столбцы

- "true" / "false" для visibility

---

## 3.7 Cross-references

### extract_crossrefs функция

Извлечение cross-references на другие базы данных:

```python
def extract_crossrefs(data: Any) -> dict[str, str]:
    """Extract cross-references."""
    result = {}

    uni_prot_kb_cross_references = data.get("uniProtKBCrossReferences", [])
    for xref in uni_prot_kb_cross_references:
        if not isinstance(xref, dict):
            continue

        database = xref.get("database")
        xref_id = xref.get("id")

        if not database or not xref_id:
            continue

        # Mapping к стандартным именам колонок

        column_map = {
            "Ensembl": "xref_ensembl",
            "PDB": "xref_pdb",
            "AlphaFold": "xref_alphafold",
            "Pfam": "xref_pfam",
            "InterPro": "xref_interpro",
            "PROSITE": "xref_prosite",
            "GuidetoPHARMACOLOGY": "xref_iuphar",
        }

        column_name = column_map.get(database)
        if column_name:
            # Append to existing value

            if column_name in result:
                result[column_name] += f"|{xref_id}"
            else:
                result[column_name] = xref_id

    return result

```

### Databases

**Genomics:**

- **Ensembl**: IDs для genome browsers

**Structure:**

- **PDB**: структурные координаты

- **AlphaFold**: predicted structures

**Families:**

- **Pfam**: domain families

- **InterPro**: integrated signatures

- **PROSITE**: patterns and profiles

- **PRINTS**: fingerprints

- **SUPFAM**: structural similarities

- **TCDB**: transporter classification

**Pharmacology:**

- **GuidetoPHARMACOLOGY** (GtoPdb): pharmacological targets

---

## 3.8 Batch Processing

### process функция

Batch processing для множества accessions:

```python
def process(
    accessions: Sequence[str], cfg: UniprotCfg, batch_size: int = 500
) -> pd.DataFrame:
    """Batch process accessions through UniProt API."""

    results = []

    for i in range(0, len(accessions), batch_size):
        batch = accessions[i:i + batch_size]

        # Stream API для batch

        query = "+OR+".join([f"accession:{acc}" for acc in batch])
        url = f"{cfg.base_url}/uniprotkb/stream?query={query}&format=json"

        try:
            response = requests.get(url, timeout=cfg.stream_timeout)
            response.raise_for_status()
            data = response.json()

            # Process each entry

            for entry in data.get("results", []):
                enriched = {
                    "accession": entry.get("primaryAccession"),
                    **extract_names(entry),
                    **extract_organism(entry),
                    **extract_isoform(entry),
                    **extract_ptm(entry),
                    **extract_crossrefs(entry),
                }
                results.append(enriched)

        except Exception as exc:
            logger.warning(f"UniProt batch failed: {exc}")
            continue

    return pd.DataFrame(results)

```

### Error handling

- **Retry**: автоматический retry при 5xx

- **Skip**: пропуск при недоступности

- **Log**: детальное логирование ошибок

### Landing zone

Сырые JSON ответы сохраняются в `data/landing/uniprot/{accession}.json` для:

- Аудита запросов

- Отладки парсинга

- Regression testing

---

## 3.9 Код примеры

### Полный пример enrichment pipeline

```python
from library.integration.uniprot_library import (
    submit_id_mapping_job,
    poll_mapping_status,
    stream_mapping_results,
    fetch_uniprot,
    extract_names,
    extract_organism,
)

# 1. ID Mapping для валидации accessions

accessions = ["P29274", "P12345", "Q98765"]
job_id = submit_id_mapping_job(accessions, "UniProtKB_AC-ID", "UniProtKB", cfg)
status = poll_mapping_status(job_id, cfg)
mapping_df = stream_mapping_results(job_id, cfg)

# 2. Fetch enriched data

for accession in accessions:
    entry = fetch_uniprot(accession, cfg=cfg)

    # Extract fields

    names = extract_names(entry)
    organism = extract_organism(entry)
    isoforms = extract_isoform(entry)
    ptm = extract_ptm(entry)
    xrefs = extract_crossrefs(entry)

    # Merge с ChEMBL data

    enriched = {
        "target_chembl_id": chembl_id,
        "uniprot_accession": accession,
        "protein_names": "|".join(names),
        **organism,
        **isoforms,
        **ptm,
        **xrefs,
    }

```

---

## Ссылки

- [UniProt Website API](https://academic.oup.com/bioinformatics/article/38/Supplement_2/ii29/6702002)

- [UniProt REST API Documentation](https://www.uniprot.org/help/api)

- [UniProt ID Mapping](https://www.uniprot.org/id-mapping)

- Код: `e:\github\ChEMBL_data_acquisition6\library\integration\uniprot_library.py`
