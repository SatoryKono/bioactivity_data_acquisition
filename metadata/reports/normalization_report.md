# Отчет по проверке нормализации

Сгенерировано: 2025-10-23 22:48:22

## Сводка по сущностям

| Entity | Columns Checked | Total Issues |
|--------|-----------------|--------------|
| activity | 6 | 1 |
| assay | 3 | 1 |
| document | 29 | 17 |
| target | 11 | 9 |
| testitem | 9 | 2 |

## Activity - Детали нормализации

### extracted_at
- **Total Records**: 20
- **Valid**: 0
- **Invalid**: 20

- 20 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23T19:06:58.750152Z`


## Assay - Детали нормализации

### extracted_at
- **Total Records**: 100
- **Valid**: 0
- **Invalid**: 100

- 100 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23T17:43:34.616610`
- `2025-10-23T17:43:34.725220`
- `2025-10-23T17:43:34.815528`
- `2025-10-23T17:43:34.911217`
- `2025-10-23T17:43:35.001771`


## Document - Детали нормализации

### chembl_pmid
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 ChEMBL ID не соответствуют формату CHEMBL\d+
- 2 ChEMBL ID не в верхнем регистре

**Examples of invalid values:**

- `17827018`
- `18578478`

### pubmed_abstract
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_authors
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 PMID не являются числовыми

**Examples of invalid values:**

- `['Gmeiner P', 'Rodriguez Loaiza P', 'Löber S', 'Hübner H']`
- `['Sagot E', 'Nielsen B', 'Bolte J', 'Gefflaut T', 'Umberti M', 'Pickering DS', 'Stensbøl TB', 'Chapelet M', 'Bunch L', 'Pu X']`

### openalex_doi
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_doi
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### semantic_scholar_doi
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_doc_type
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 PMID не являются числовыми

**Examples of invalid values:**

- `JOURNAL ARTICLE`

### pubmed_issn
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 PMID не являются числовыми

**Examples of invalid values:**

- `0968-0896`
- `0022-2623`

### pubmed_journal
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 PMID не являются числовыми

**Examples of invalid values:**

- `bioorg medicine chemical`
- `journal medicine chemical`

### pubmed_year
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_error
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_year_completed
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_month_completed
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### pubmed_day_completed
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### publication_date
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### extracted_at
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23T19:02:19.183881Z`


## Target - Детали нормализации

### uniprot_id_primary
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### uniprot_ids_all
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### uniProtkbId
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### uniprot_last_update
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### timestamp_utc
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### iuphar_uniprot_id_primary
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 0

- No data to validate

### multi_source_validated
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 дат не в ISO 8601 формате
- 2 дат не парсятся как datetime

**Examples of invalid values:**

- `False`

### extracted_at
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23 19:32:18.624220`


## Testitem - Детали нормализации

### extracted_at
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23T19:04:49.634754Z`

### extracted_at.1
- **Total Records**: 2
- **Valid**: 0
- **Invalid**: 2

- 2 дат не в ISO 8601 формате

**Examples of invalid values:**

- `2025-10-23T19:04:49.634754Z`
