## Quality Filters (QC)

### Метрики
- row_count, enabled_sources, per-source counts
- Для testitem: pubchem_enriched_records, records_with_errors

### Пороги
- Общие пороги задаются в конфиге `validation.qc.*` (если применимо).

### Артефакты
- documents: `documents_{date_tag}_qc.csv`, каталог `documents_correlation_report_{date_tag}`
- testitem: `testitems_{date_tag}_qc.csv`, каталог `testitems_correlation_report_{date_tag}`
- assay: `assays_{date_tag}_qc.csv`, корреляционные отчеты при включении

