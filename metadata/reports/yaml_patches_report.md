# Отчет по YAML-патчам

Сгенерировано: 2025-10-23 22:49:58

## Сводка по сущностям

| Entity | Missing Columns | Extra Columns | Order Issues |
|--------|-----------------|---------------|--------------|
| activity | 0 | 1 | 0 |
| assay | 19 | 10 | 0 |
| document | 29 | 0 | 0 |
| target | 0 | 5 | 0 |
| testitem | 19 | 7 | 0 |

## Activity - Детали патчей

### Extra Columns (1):

- `saltform_id`

### Рекомендации:

- **remove_extra_columns**: Удалить 1 лишних колонок из выхода или добавить в YAML


## Assay - Детали патчей

### Missing Columns (19):

- `bao_assay_format`
- `bao_assay_format_label`
- `bao_assay_format_uri`
- `bao_assay_type`
- `bao_assay_type_label`
- `bao_assay_type_uri`
- `bao_endpoint`
- `bao_endpoint_label`
- `bao_endpoint_uri`
- `chembl_release`
- `index`
- `is_variant`
- `pipeline_version`
- `target_isoform`
- `target_organism`
- `target_tax_id`
- `target_uniprot_accession`
- `variant_mutations`
- `variant_sequence`

### Extra Columns (10):

- `assay_classifications`
- `assay_type_description`
- `confidence_score`
- `isoform`
- `mutation`
- `relationship_type`
- `sequence`
- `src_assay_id`
- `src_id`
- `src_name`

### Рекомендации:

- **add_missing_columns**: Добавить 19 отсутствующих колонок в column_order

- **remove_extra_columns**: Удалить 10 лишних колонок из выхода или добавить в YAML


## Document - Детали патчей

### Missing Columns (29):

- `chembl_error`
- `chembl_issn`
- `classification`
- `crossref_abstract`
- `crossref_issn`
- `crossref_journal`
- `crossref_pmid`
- `document_contains_external_links`
- `is_experimental_doc`
- `openalex_abstract`
- `openalex_authors`
- `openalex_crossref_doc_type`
- `openalex_doc_type`
- `openalex_first_page`
- `openalex_issn`
- `openalex_issue`
- `openalex_journal`
- `openalex_last_page`
- `openalex_pmid`
- `openalex_volume`
- `openalex_year`
- `pubmed_article_title`
- `pubmed_chemical_list`
- `pubmed_id`
- `pubmed_mesh_descriptors`
- `pubmed_mesh_qualifiers`
- `semantic_scholar_doc_type`
- `semantic_scholar_issn`
- `semantic_scholar_journal`

### Рекомендации:

- **add_missing_columns**: Добавить 29 отсутствующих колонок в column_order


## Target - Детали патчей

### Extra Columns (5):

- `extraction_status`
- `ptm_disulfide_bond`
- `ptm_glycosylation`
- `ptm_lipidation`
- `ptm_modified_residue`

### Рекомендации:

- **remove_extra_columns**: Удалить 5 лишних колонок из выхода или добавить в YAML


## Testitem - Детали патчей

### Missing Columns (19):

- `drug_antibacterial_flag`
- `drug_antifungal_flag`
- `drug_antiinflammatory_flag`
- `drug_antineoplastic_flag`
- `drug_antiparasitic_flag`
- `drug_antiviral_flag`
- `drug_chembl_id`
- `drug_immunosuppressant_flag`
- `drug_indication_flag`
- `drug_name`
- `drug_substance_flag`
- `drug_type`
- `indication_class`
- `molregno`
- `pubchem_isomeric_smiles`
- `salt_chembl_id`
- `withdrawn_country`
- `withdrawn_reason`
- `withdrawn_year`

### Extra Columns (7):

- `chembl_release.1`
- `extracted_at.1`
- `hash_business_key.1`
- `hash_row.1`
- `index.1`
- `pipeline_version.1`
- `source_system.1`

### Рекомендации:

- **add_missing_columns**: Добавить 19 отсутствующих колонок в column_order

- **remove_extra_columns**: Удалить 7 лишних колонок из выхода или добавить в YAML
