## Enrichment (UniProt, IUPHAR)

### UniProt (target)
- Источники полей: `uniprotkb_Id`, `secondaryAccessions`, `recommendedName`, `geneName`.
- Приоритеты/резолвинг: первичный ID → fallback из `uniProtkbIdFallback`/`uniprot_id`.

### IUPHAR (target)
- CSV словари: `configs/dictionary/_target/` (`_IUPHAR_target.csv`, `_IUPHAR_family.csv`).
- Join ключи: UniProt accession → target_id; фолбэки: HGNC, gene, pref_name, EC.
- Конфликты: приоритет CSV; fallback REST API `guidetopharmacology` с фильтрами и ретраями (см. `library.pipelines.target.iuphar_target`).

