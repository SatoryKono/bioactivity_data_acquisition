# Нормализация данных в ETL-пайплайнах биоактивности

## Обзор

Документ описывает систему нормализации данных для пяти ETL-пайплайнов: Documents, Targets, Assays, Activities, Testitems. Нормализация обеспечивает консистентность данных, валидацию по схемам Pandera и детерминизм результатов.

## 1. Нормализация стандартных типов данных

| Тип | Функция | Описание | Обязательность | Вход/Выход | Примеры |
|-----|---------|----------|----------------|-----------|---------|
| **string** | `normalize_string_strip` | Удаление ведущих/завершающих пробелов | Обязательная | `"  text  "` → `"text"` | `"  CHEMBL123  "` → `"CHEMBL123"` |
| | `normalize_string_upper` | Приведение к верхнему регистру | Обязательная | `"chembl123"` → `"CHEMBL123"` | `"chembl123"` → `"CHEMBL123"` |
| | `normalize_string_lower` | Приведение к нижнему регистру | Обязательная | `"DOI"` → `"doi"` | `"DOI"` → `"doi"` |
| | `normalize_string_nfc` | Unicode NFC нормализация | Обязательная | `"café"` → `"café"` | `"café"` → `"café"` |
| | `normalize_string_whitespace` | Свёртка внутренних пробелов | Обязательная | `"text   with   spaces"` → `"text with spaces"` | `"text   with   spaces"` → `"text with spaces"` |
| | `normalize_string_titlecase` | Title case для названий | Опциональная | `"journal name"` → `"Journal Name"` | `"journal name"` → `"Journal Name"` |
| **int** | `normalize_int` | Строгая коэрция к целому | Обязательная | `"123.45"` → `123` | `"123.45"` → `123` |
| | `normalize_int_positive` | Положительные целые | Обязательная | `"-5"` → `None`, `"5"` → `5` | `"-5"` → `None`, `"5"` → `5` |
| | `normalize_int_range` | Целые в диапазоне | Опциональная | `"15"` (1-12) → `None` | `"15"` (1-12) → `None` |
| **float** | `normalize_float` | Коэрция к float с проверкой NaN/∞ | Обязательная | `"123.45"` → `123.45` | `"123.45"` → `123.45` |
| | `normalize_float_precision` | Округление до N знаков | Опциональная | `123.456789` (3) → `123.457` | `123.456789` (3) → `123.457` |
| **boolean** | `normalize_boolean` | Канонизация булевых значений | Обязательная | `"true"/"1"/"yes"` → `True` | `"true"/"1"/"yes"` → `True` |
| | `normalize_boolean_strict` | Строгая проверка | Опциональная | `"1"` → `True`, `"2"` → `None` | `"1"` → `True`, `"2"` → `None` |
| **datetime** | `normalize_datetime_iso8601` | ISO 8601 в UTC | Обязательная | `"2023-01-01"` → `"2023-01-01T00:00:00Z"` | `"2023-01-01"` → `"2023-01-01T00:00:00Z"` |
| | `normalize_datetime_validate` | Валидация дат | Опциональная | `"invalid"` → `None` | `"invalid"` → `None` |

## 2. Нормализация доменных типов

| Тип | Функция | Описание | Обязательность | Вход/Выход | Примеры |
|-----|---------|----------|----------------|-----------|---------|
| **DOI** | `normalize_doi` | Нормализация DOI по спецификации | Обязательная | `"https://doi.org/10.1234/example"` → `"10.1234/example"` | `"https://doi.org/10.1234/example"` → `"10.1234/example"` |
| **ChEMBL ID** | `normalize_chembl_id` | Валидация и нормализация ChEMBL ID | Обязательная | `"chembl123"` → `"CHEMBL123"` | `"chembl123"` → `"CHEMBL123"` |
| **UniProt ID** | `normalize_uniprot_id` | Валидация формата UniProt | Обязательная | `"P12345"` → `"P12345"` | `"P12345"` → `"P12345"` |
| **PubChem CID** | `normalize_pubchem_cid` | Валидация PubChem CID | Обязательная | `"12345"` → `12345` | `"12345"` → `12345` |
| **SMILES** | `normalize_smiles` | Базовая валидация SMILES | Обязательная | `"CCO"` → `"CCO"` | `"CCO"` → `"CCO"` |
| **InChI** | `normalize_inchi` | Валидация InChI | Обязательная | `"InChI=1S/C2H6O/c1-2-3/h2-3H,1H3"` → `"InChI=1S/C2H6O/c1-2-3/h2-3H,1H3"` | `"InChI=1S/C2H6O/c1-2-3/h2-3H,1H3"` → `"InChI=1S/C2H6O/c1-2-3/h2-3H,1H3"` |
| **InChI Key** | `normalize_inchi_key` | Валидация InChI Key | Обязательная | `"LFQSCWFLJHTTHZ-UHFFFAOYSA-N"` → `"LFQSCWFLJHTTHZ-UHFFFAOYSA-N"` | `"LFQSCWFLJHTTHZ-UHFFFAOYSA-N"` → `"LFQSCWFLJHTTHZ-UHFFFAOYSA-N"` |
| **BAO ID** | `normalize_bao_id` | Нормализация BAO идентификаторов | Обязательная | `"bao_000001"` → `"BAO_000001"` | `"bao_000001"` → `"BAO_000001"` |
| **Единицы** | `normalize_units` | Стандартизация единиц измерения | Обязательная | `"nM"` → `"nm"`, `"μM"` → `"μm"` | `"nM"` → `"nm"`, `"μM"` → `"μm"` |
| **pChEMBL** | `normalize_pchembl` | Валидация pChEMBL значений | Обязательная | `"8.5"` → `8.5` (3.0-12.0) | `"8.5"` → `8.5` (3.0-12.0) |

## 3. Пайплайны ETL

### 3.1 Documents Pipeline

**Процесс:** extract → transform → validate → load

**Модули:**
- Extract: `src/library/documents/extract.py` (Crossref, OpenAlex, PubMed, Semantic Scholar)
- Transform: `src/library/documents/normalize.py`
- Validate: `src/library/schemas/document_schema_normalized.py`
- Load: `src/library/etl/load.py`

**Источники:** ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar

**Таблица колонок:**

| Таблица | Колонка | Тип | Нормализация | Обязательность | Примечание |
|---------|---------|-----|---------------|-----------------|------------|
| documents | document_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | NOT NULL | PK документа |
| | doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI документа |
| | pubmed_id | STRING | normalize_string_strip, normalize_pmid | nullable | PubMed ID |
| | chembl_title | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название из ChEMBL |
| | crossref_title | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название из Crossref |
| | openalex_title | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название из OpenAlex |
| | pubmed_article_title | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название из PubMed |
| | semantic_scholar_title | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название из Semantic Scholar |
| | chembl_abstract | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Аннотация из ChEMBL |
| | crossref_abstract | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Аннотация из Crossref |
| | openalex_abstract | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Аннотация из OpenAlex |
| | pubmed_abstract | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Аннотация из PubMed |
| | chembl_authors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Авторы из ChEMBL |
| | crossref_authors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Авторы из Crossref |
| | openalex_authors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Авторы из OpenAlex |
| | pubmed_authors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Авторы из PubMed |
| | semantic_scholar_authors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Авторы из Semantic Scholar |
| | chembl_doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI из ChEMBL |
| | crossref_doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI из Crossref |
| | openalex_doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI из OpenAlex |
| | pubmed_doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI из PubMed |
| | semantic_scholar_doi | STRING | normalize_string_strip, normalize_string_lower, normalize_doi | nullable | DOI из Semantic Scholar |
| | chembl_doc_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип документа из ChEMBL |
| | crossref_doc_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип документа из Crossref |
| | openalex_doc_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип документа из OpenAlex |
| | pubmed_doc_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип публикации из PubMed |
| | semantic_scholar_doc_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Типы публикации из Semantic Scholar |
| | chembl_issn | STRING | normalize_string_strip, normalize_string_upper | nullable | ISSN из ChEMBL |
| | crossref_issn | STRING | normalize_string_strip, normalize_string_upper | nullable | ISSN из Crossref |
| | openalex_issn | STRING | normalize_string_strip, normalize_string_upper | nullable | ISSN из OpenAlex |
| | pubmed_issn | STRING | normalize_string_strip, normalize_string_upper | nullable | ISSN из PubMed |
| | semantic_scholar_issn | STRING | normalize_string_strip, normalize_string_upper | nullable | ISSN из Semantic Scholar |
| | chembl_journal | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Название журнала из ChEMBL |
| | crossref_journal | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Название журнала из Crossref |
| | openalex_journal | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Название журнала из OpenAlex |
| | pubmed_journal | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Название журнала из PubMed |
| | semantic_scholar_journal | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Название журнала из Semantic Scholar |
| | chembl_year | INT | normalize_int, normalize_year | nullable | Год из ChEMBL |
| | crossref_year | INT | normalize_int, normalize_year | nullable | Год из Crossref |
| | openalex_year | INT | normalize_int, normalize_year | nullable | Год из OpenAlex |
| | pubmed_year | INT | normalize_int, normalize_year | nullable | Год из PubMed |
| | chembl_volume | STRING | normalize_string_strip | nullable | Том из ChEMBL |
| | crossref_volume | STRING | normalize_string_strip | nullable | Том из Crossref |
| | openalex_volume | STRING | normalize_string_strip | nullable | Том из OpenAlex |
| | pubmed_volume | STRING | normalize_string_strip | nullable | Том из PubMed |
| | chembl_issue | STRING | normalize_string_strip | nullable | Номер выпуска из ChEMBL |
| | crossref_issue | STRING | normalize_string_strip | nullable | Номер выпуска из Crossref |
| | openalex_issue | STRING | normalize_string_strip | nullable | Номер выпуска из OpenAlex |
| | pubmed_issue | STRING | normalize_string_strip | nullable | Номер выпуска из PubMed |
| | crossref_first_page | STRING | normalize_string_strip | nullable | Первая страница из Crossref |
| | openalex_first_page | STRING | normalize_string_strip | nullable | Первая страница из OpenAlex |
| | pubmed_first_page | STRING | normalize_string_strip | nullable | Начальная страница из PubMed |
| | crossref_last_page | STRING | normalize_string_strip | nullable | Последняя страница из Crossref |
| | openalex_last_page | STRING | normalize_string_strip | nullable | Последняя страница из OpenAlex |
| | pubmed_last_page | STRING | normalize_string_strip | nullable | Конечная страница из PubMed |
| | chembl_error | STRING | normalize_string_strip | nullable | Ошибка из ChEMBL |
| | crossref_error | STRING | normalize_string_strip | nullable | Ошибка из Crossref |
| | openalex_error | STRING | normalize_string_strip | nullable | Ошибка из OpenAlex |
| | pubmed_error | STRING | normalize_string_strip | nullable | Ошибка из PubMed |
| | semantic_scholar_error | STRING | normalize_string_strip | nullable | Ошибка из Semantic Scholar |
| | pubmed_year_completed | INT | normalize_int, normalize_year | nullable | Год завершения из PubMed |
| | pubmed_month_completed | INT | normalize_int, normalize_month | nullable | Месяц завершения из PubMed |
| | pubmed_day_completed | INT | normalize_int, normalize_day | nullable | День завершения из PubMed |
| | pubmed_year_revised | INT | normalize_int, normalize_year | nullable | Год пересмотра из PubMed |
| | pubmed_month_revised | INT | normalize_int, normalize_month | nullable | Месяц пересмотра из PubMed |
| | pubmed_day_revised | INT | normalize_int, normalize_day | nullable | День пересмотра из PubMed |
| | publication_date | STRING | normalize_datetime_iso8601 | nullable | Дата публикации |
| | document_sortorder | INT | normalize_int, normalize_int_positive | NOT NULL | Порядок сортировки документов |
| | index | INT | normalize_int, normalize_int_positive | NOT NULL | Порядковый номер записи |
| | pipeline_version | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Версия пайплайна |
| | source_system | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Система-источник |
| | chembl_release | STRING | normalize_string_strip, normalize_string_upper | nullable | Версия ChEMBL |
| | extracted_at | STRING | normalize_datetime_iso8601 | NOT NULL | Время извлечения данных |
| | hash_row | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш строки |
| | hash_business_key | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш бизнес-ключа |

### 3.2 Targets Pipeline

**Процесс:** extract → transform → validate → load

**Модули:**

- Extract: `src/library/targets/extract.py` (ChEMBL, UniProt, IUPHAR)
- Transform: `src/library/targets/normalize.py`
- Validate: `src/library/schemas/target_schema_normalized.py`
- Load: `src/library/etl/load.py`

**Источники:** ChEMBL, UniProt, IUPHAR, GtoPdb

**Таблица колонок:**

| Таблица | Колонка | Тип | Нормализация | Обязательность | Примечание |
|---------|---------|-----|---------------|-----------------|------------|
| targets | target_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | NOT NULL | PK таргета |
| | pref_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Предпочтительное название |
| | component_description | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Описание компонента |
| | component_id | INT | normalize_int, normalize_int_positive | nullable | ID компонента |
| | relationship | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Отношение компонента |
| | gene | STRING | normalize_string_strip, normalize_string_upper | nullable | Символ гена |
| | uniprot_id | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | UniProt ID из ChEMBL |
| | mapping_uniprot_id | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | Маппинг UniProt ID |
| | chembl_alternative_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Альтернативное название ChEMBL |
| | ec_code | STRING | normalize_string_strip, normalize_string_upper | nullable | EC код фермента |
| | hgnc_name | STRING | normalize_string_strip, normalize_string_upper | nullable | Название по HGNC |
| | hgnc_id | STRING | normalize_string_strip, normalize_hgnc_id | nullable | HGNC идентификатор |
| | target_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип таргета |
| | tax_id | INT | normalize_int, normalize_int_positive | nullable | Таксономический ID |
| | species_group_flag | BOOL | normalize_boolean | nullable | Флаг группировки по видам |
| | target_components | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Компоненты таргета |
| | protein_classifications | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Классификация белка |
| | cross_references | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Перекрестные ссылки |
| | reaction_ec_numbers | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | EC номера реакций |
| | uniprot_id_primary | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | Первичный UniProt ID |
| | uniprot_ids_all | TEXT | normalize_string_strip, normalize_string_upper | nullable | Все UniProt ID |
| | uniProtkbId | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | UniProtKB ID |
| | secondaryAccessions | TEXT | normalize_string_strip, normalize_string_upper | nullable | Вторичные accession номера |
| | secondaryAccessionNames | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Названия вторичных accession |
| | isoform_ids | TEXT | normalize_string_strip, normalize_string_upper | nullable | ID изоформ |
| | isoform_names | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Названия изоформ |
| | isoform_synonyms | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Синонимы изоформ |
| | recommendedName | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Рекомендуемое название |
| | geneName | STRING | normalize_string_strip, normalize_string_upper | nullable | Название гена |
| | protein_name_canonical | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Каноническое название белка |
| | protein_name_alt | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Альтернативное название белка |
| | protein_synonym_list | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Список синонимов белка |
| | taxon_id | INT | normalize_int, normalize_int_positive | nullable | Таксономический ID |
| | lineage_superkingdom | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Суперцарство в линии |
| | lineage_phylum | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Тип в линии |
| | lineage_class | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Класс в линии |
| | sequence_length | INT | normalize_int, normalize_int_positive | nullable | Длина последовательности |
| | features_signal_peptide | BOOL | normalize_boolean | nullable | Сигнальный пептид |
| | features_transmembrane | BOOL | normalize_boolean | nullable | Трансмембранный |
| | features_topology | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Топология |
| | molecular_function | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Молекулярная функция |
| | cellular_component | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Клеточный компонент |
| | subcellular_location | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Субклеточная локализация |
| | topology | TEXT | normalize_string_strip, normalize_string_upper | nullable | Топология |
| | transmembrane | BOOL | normalize_boolean | nullable | Трансмембранный |
| | intramembrane | BOOL | normalize_boolean | nullable | Внутримембранный |
| | glycosylation | BOOL | normalize_boolean | nullable | Гликозилирование |
| | lipidation | BOOL | normalize_boolean | nullable | Липидирование |
| | disulfide_bond | BOOL | normalize_boolean | nullable | Дисульфидная связь |
| | modified_residue | BOOL | normalize_boolean | nullable | Модифицированный остаток |
| | phosphorylation | BOOL | normalize_boolean | nullable | Фосфорилирование |
| | acetylation | BOOL | normalize_boolean | nullable | Ацетилирование |
| | ubiquitination | BOOL | normalize_boolean | nullable | Убиквитинирование |
| | signal_peptide | BOOL | normalize_boolean | nullable | Сигнальный пептид |
| | propeptide | BOOL | normalize_boolean | nullable | Пропептид |
| | xref_chembl | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на ChEMBL |
| | xref_uniprot | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на UniProt |
| | xref_ensembl | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на Ensembl |
| | xref_iuphar | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на IUPHAR |
| | xref_pdb | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на PDB |
| | xref_alphafold | TEXT | normalize_string_strip, normalize_string_upper | nullable | Ссылки на AlphaFold |
| | GuidetoPHARMACOLOGY | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Guide to Pharmacology классификация |
| | family | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Семейство |
| | SUPFAM | TEXT | normalize_string_strip, normalize_string_upper | nullable | SUPFAM классификация |
| | PROSITE | TEXT | normalize_string_strip, normalize_string_upper | nullable | PROSITE классификация |
| | InterPro | TEXT | normalize_string_strip, normalize_string_upper | nullable | InterPro классификация |
| | Pfam | TEXT | normalize_string_strip, normalize_string_upper | nullable | Pfam классификация |
| | PRINTS | TEXT | normalize_string_strip, normalize_string_upper | nullable | PRINTS классификация |
| | TCDB | TEXT | normalize_string_strip, normalize_string_upper | nullable | TCDB классификация |
| | pfam | TEXT | normalize_string_strip, normalize_string_upper | nullable | Pfam классификация (lowercase) |
| | interpro | TEXT | normalize_string_strip, normalize_string_upper | nullable | InterPro классификация (lowercase) |
| | reactions | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Реакции |
| | reaction_ec_numbers_uniprot | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | EC номера реакций UniProt |
| | uniprot_last_update | STRING | normalize_datetime_iso8601 | nullable | Последнее обновление UniProt |
| | uniprot_version | INT | normalize_int, normalize_int_positive | nullable | Версия UniProt |
| | gene_symbol | STRING | normalize_string_strip, normalize_string_upper | nullable | Символ гена |
| | gene_symbol_list | TEXT | normalize_string_strip, normalize_string_upper | nullable | Список символов генов |
| | organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Организм |
| | iuphar_target_id | INT | normalize_int, normalize_int_positive | nullable | IUPHAR ID таргета |
| | iuphar_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название по IUPHAR |
| | iuphar_family_id | INT | normalize_int, normalize_int_positive | nullable | IUPHAR ID семейства |
| | iuphar_full_id_path | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Полный путь ID по IUPHAR |
| | iuphar_full_name_path | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Полный путь названий по IUPHAR |
| | iuphar_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип по IUPHAR |
| | iuphar_class | STRING | normalize_string_strip, normalize_string_upper | nullable | Класс по IUPHAR |
| | iuphar_subclass | STRING | normalize_string_strip, normalize_string_upper | nullable | Подкласс по IUPHAR |
| | iuphar_chain | STRING | normalize_string_strip, normalize_string_upper | nullable | Цепь по IUPHAR |
| | iuphar_gene_symbol | STRING | normalize_string_strip, normalize_string_upper | nullable | Символ гена IUPHAR |
| | iuphar_hgnc_id | STRING | normalize_string_strip, normalize_hgnc_id | nullable | HGNC ID IUPHAR |
| | iuphar_hgnc_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | HGNC название IUPHAR |
| | iuphar_uniprot_id_primary | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | Первичный UniProt ID IUPHAR |
| | iuphar_uniprot_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | UniProt название IUPHAR |
| | iuphar_organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Организм IUPHAR |
| | iuphar_taxon_id | INT | normalize_int, normalize_int_positive | nullable | Таксономический ID IUPHAR |
| | iuphar_description | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Описание IUPHAR |
| | iuphar_abbreviation | STRING | normalize_string_strip, normalize_string_upper | nullable | Аббревиатура IUPHAR |
| | gtop_target_id | INT | normalize_int, normalize_int_positive | nullable | GtoP ID таргета |
| | gtop_synonyms | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Синонимы по GtoP |
| | gtop_natural_ligands_n | INT | normalize_int, normalize_int_positive | nullable | Количество природных лигандов |
| | gtop_interactions_n | INT | normalize_int, normalize_int_positive | nullable | Количество взаимодействий |
| | gtop_function_text_short | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Краткое описание функции |
| | unified_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Унифицированное название |
| | unified_organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Унифицированный организм |
| | unified_tax_id | INT | normalize_int, normalize_int_positive | nullable | Унифицированный таксономический ID |
| | unified_target_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Унифицированный тип таргета |
| | has_chembl_data | BOOL | normalize_boolean | nullable | Есть данные ChEMBL |
| | has_uniprot_data | BOOL | normalize_boolean | nullable | Есть данные UniProt |
| | has_iuphar_data | BOOL | normalize_boolean | nullable | Есть данные IUPHAR |
| | has_gtopdb_data | BOOL | normalize_boolean | nullable | Есть данные GtoPdb |
| | has_name | BOOL | normalize_boolean | nullable | Есть название |
| | has_organism | BOOL | normalize_boolean | nullable | Есть организм |
| | has_tax_id | BOOL | normalize_boolean | nullable | Есть таксономический ID |
| | has_target_type | BOOL | normalize_boolean | nullable | Есть тип таргета |
| | multi_source_validated | BOOL | normalize_boolean | nullable | Валидировано несколькими источниками |
| | protein_class_pred_L1 | STRING | normalize_string_strip, normalize_string_upper | nullable | Предсказание класса белка L1 |
| | protein_class_pred_L2 | STRING | normalize_string_strip, normalize_string_upper | nullable | Предсказание класса белка L2 |
| | protein_class_pred_L3 | STRING | normalize_string_strip, normalize_string_upper | nullable | Предсказание класса белка L3 |
| | protein_class_pred_confidence | FLOAT | normalize_float, normalize_float_precision | nullable | Уверенность предсказания класса |
| | protein_class_pred_evidence | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Доказательства предсказания класса |
| | protein_class_pred_rule_id | STRING | normalize_string_strip, normalize_string_upper | nullable | ID правила предсказания класса |
| | validation_errors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Ошибки валидации |
| | extraction_errors | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Ошибки извлечения |
| | index | INT | normalize_int, normalize_int_positive | NOT NULL | Порядковый номер записи |
| | pipeline_version | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Версия пайплайна |
| | source_system | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Система-источник |
| | chembl_release | STRING | normalize_string_strip, normalize_string_upper | nullable | Версия ChEMBL |
| | extracted_at | STRING | normalize_datetime_iso8601 | NOT NULL | Время извлечения данных |
| | hash_row | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш строки |
| | hash_business_key | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш бизнес-ключа |

### 3.3 Assays Pipeline

**Процесс:** extract → transform → validate → load

**Модули:**

- Extract: `src/library/assay/extract.py` (ChEMBL)
- Transform: `src/library/assay/normalize.py`
- Validate: `src/library/schemas/assay_schema_normalized.py`
- Load: `src/library/etl/load.py`

**Источники:** ChEMBL

**Таблица колонок:**

| Таблица | Колонка | Тип | Нормализация | Обязательность | Примечание |
|---------|---------|-----|---------------|-----------------|------------|
| assays | assay_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | NOT NULL | PK ассая |
| | assay_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип ассая (B, F, A, P, T, U) |
| | assay_category | STRING | normalize_string_strip, normalize_string_upper | nullable | Категория ассая |
| | target_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | FK к target_dim |
| | target_organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Организм мишени |
| | target_tax_id | INT | normalize_int, normalize_int_positive | nullable | Таксономический ID мишени |
| | bao_format | STRING | normalize_string_strip, normalize_string_upper, normalize_bao_id | nullable | BAO format классификация |
| | bao_label | STRING | normalize_string_strip, normalize_string_titlecase | nullable | BAO label классификация |
| | bao_endpoint | STRING | normalize_string_strip, normalize_string_upper | nullable | BAO endpoint классификация |
| | bao_assay_format | STRING | normalize_string_strip, normalize_string_upper | nullable | BAO assay format |
| | bao_assay_type | STRING | normalize_string_strip, normalize_string_upper | nullable | BAO assay type |
| | bao_assay_type_label | STRING | normalize_string_strip, normalize_string_titlecase | nullable | BAO assay type label |
| | bao_assay_type_uri | STRING | normalize_string_strip, normalize_string_lower | nullable | BAO assay type URI |
| | bao_assay_format_uri | STRING | normalize_string_strip, normalize_string_lower | nullable | BAO assay format URI |
| | bao_assay_format_label | STRING | normalize_string_strip, normalize_string_titlecase | nullable | BAO assay format label |
| | bao_endpoint_uri | STRING | normalize_string_strip, normalize_string_lower | nullable | BAO endpoint URI |
| | bao_endpoint_label | STRING | normalize_string_strip, normalize_string_titlecase | nullable | BAO endpoint label |
| | variant_id | INT | normalize_int, normalize_int_positive | nullable | ID варианта |
| | is_variant | BOOL | normalize_boolean | nullable | Флаг наличия вариантных данных |
| | variant_accession | STRING | normalize_string_strip, normalize_string_upper | nullable | Accession варианта |
| | variant_sequence_accession | STRING | normalize_string_strip, normalize_string_upper | nullable | Accession последовательности варианта |
| | variant_sequence_mutation | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Мутация в последовательности варианта |
| | variant_mutations | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Описание мутаций варианта |
| | variant_sequence | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Последовательность варианта |
| | variant_text | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Текстовое описание варианта |
| | variant_sequence_id | STRING | normalize_string_strip, normalize_string_upper | nullable | ID последовательности варианта |
| | variant_organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Организм варианта |
| | target_uniprot_accession | STRING | normalize_string_strip, normalize_string_upper, normalize_uniprot_id | nullable | UniProt accession таргета |
| | target_isoform | STRING | normalize_string_strip, normalize_string_upper | nullable | Изоформа таргета |
| | assay_organism | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Организм для ассая |
| | assay_tax_id | INT | normalize_int, normalize_int_positive | nullable | Таксономический ID организма |
| | assay_strain | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Штамм организма |
| | assay_tissue | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Ткань для ассая |
| | assay_cell_type | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Тип клеток для ассая |
| | assay_subcellular_fraction | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Субклеточная фракция |
| | description | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Описание ассая |
| | assay_parameters | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Параметры ассая (legacy) |
| | assay_parameters_json | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Параметры ассая (нормализованный JSON) |
| | assay_format | STRING | normalize_string_strip, normalize_string_upper | nullable | Формат ассая |
| | index | INT | normalize_int, normalize_int_positive | NOT NULL | Порядковый номер записи |
| | pipeline_version | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Версия пайплайна |
| | source_system | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Система-источник |
| | chembl_release | STRING | normalize_string_strip, normalize_string_upper | nullable | Версия ChEMBL |
| | extracted_at | STRING | normalize_datetime_iso8601 | NOT NULL | Время извлечения данных |
| | hash_row | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш строки |
| | hash_business_key | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш бизнес-ключа |

### 3.4 Activities Pipeline

**Процесс:** extract → transform → validate → load

**Модули:**

- Extract: `src/library/activity/extract.py` (ChEMBL)
- Transform: `src/library/activity/normalize.py`
- Validate: `src/library/schemas/activity_schema_normalized.py`
- Load: `src/library/etl/load.py`

**Источники:** ChEMBL

**Таблица колонок:**

| Таблица | Колонка | Тип | Нормализация | Обязательность | Примечание |
|---------|---------|-----|---------------|-----------------|------------|
| activities | activity_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | NOT NULL | PK активности |
| | assay_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | FK к assay_dim |
| | document_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | FK к document_dim |
| | target_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | FK к target_dim |
| | molecule_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | FK к testitem_dim |
| | activity_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип активности (IC50, EC50, Ki, Kd) |
| | activity_value | DECIMAL | normalize_float, normalize_activity_value | nullable | Значение активности |
| | activity_unit | STRING | normalize_string_strip, normalize_units | nullable | Единицы измерения |
| | pchembl_value | DECIMAL | normalize_float, normalize_pchembl, normalize_pchembl_range | nullable | -log10(стандартизованное значение) |
| | data_validity_comment | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Комментарий о валидности данных |
| | activity_comment | TEXT | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Комментарий к активности |
| | lower_bound | DECIMAL | normalize_float, normalize_activity_value | nullable | Нижняя граница для цензурированных данных |
| | upper_bound | DECIMAL | normalize_float, normalize_activity_value | nullable | Верхняя граница для цензурированных данных |
| | is_censored | BOOL | normalize_boolean | nullable | Флаг цензурированных данных |
| | published_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Оригинальный тип опубликованной активности |
| | published_relation | STRING | normalize_string_strip, normalize_string_upper | nullable | Оригинальное отношение (=, >, <, >=, <=) |
| | published_value | DECIMAL | normalize_float, normalize_activity_value | nullable | Оригинальное опубликованное значение |
| | published_units | STRING | normalize_string_strip, normalize_units | nullable | Оригинальные единицы |
| | standard_type | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Стандартизованный тип активности |
| | standard_relation | STRING | normalize_string_strip, normalize_string_upper | nullable | Стандартизованное отношение |
| | standard_value | DECIMAL | normalize_float, normalize_activity_value | NOT NULL | Стандартизованное значение |
| | standard_units | STRING | normalize_string_strip, normalize_units | NOT NULL | Стандартизованные единицы |
| | standard_flag | BOOL | normalize_boolean | nullable | Флаг стандартизации |
| | bao_endpoint | STRING | normalize_string_strip, normalize_string_upper | nullable | BAO endpoint классификация |
| | bao_format | STRING | normalize_string_strip, normalize_string_upper, normalize_bao_id | nullable | BAO format классификация |
| | bao_label | STRING | normalize_string_strip, normalize_string_titlecase | nullable | BAO label классификация |
| | index | INT | normalize_int, normalize_int_positive | NOT NULL | Порядковый номер записи |
| | pipeline_version | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Версия пайплайна |
| | source_system | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Система-источник |
| | chembl_release | STRING | normalize_string_strip, normalize_string_upper | nullable | Версия ChEMBL |
| | extracted_at | STRING | normalize_datetime_iso8601 | NOT NULL | Время извлечения данных |
| | hash_row | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш строки |
| | hash_business_key | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш бизнес-ключа |

### 3.5 Testitems Pipeline

**Процесс:** extract → transform → validate → load

**Модули:**

- Extract: `src/library/testitem/extract.py` (ChEMBL, PubChem)
- Transform: `src/library/testitem/normalize.py`
- Validate: `src/library/schemas/testitem_schema_normalized.py`
- Load: `src/library/etl/load.py`

**Источники:** ChEMBL, PubChem

**Таблица колонок:**

| Таблица | Колонка | Тип | Нормализация | Обязательность | Примечание |
|---------|---------|-----|---------------|-----------------|------------|
| testitems | molecule_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | NOT NULL | PK молекулы |
| | molregno | INT | normalize_int, normalize_int_positive | nullable | Номер регистрации молекулы |
| | pref_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Предпочтительное название молекулы |
| | pref_name_key | STRING | normalize_string_strip, normalize_string_upper | nullable | Ключ предпочтительного названия |
| | parent_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | ID родительской молекулы |
| | parent_molregno | INT | normalize_int, normalize_int_positive | nullable | Номер регистрации родительской молекулы |
| | max_phase | DECIMAL | normalize_float, normalize_float_precision | nullable | Максимальная фаза разработки |
| | therapeutic_flag | BOOL | normalize_boolean | nullable | Флаг терапевтического применения |
| | dosed_ingredient | BOOL | normalize_boolean | nullable | Флаг дозируемого ингредиента |
| | first_approval | STRING | normalize_datetime_iso8601 | nullable | Дата первого одобрения |
| | structure_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип структуры |
| | molecule_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип молекулы |
| | mw_freebase | DECIMAL | normalize_float, normalize_molecular_weight | nullable | Молекулярная масса freebase |
| | alogp | DECIMAL | normalize_float, normalize_float_precision | nullable | ALogP значение |
| | hba | INT | normalize_int, normalize_int_positive | nullable | Количество акцепторов водорода |
| | hbd | INT | normalize_int, normalize_int_positive | nullable | Количество доноров водорода |
| | psa | DECIMAL | normalize_float, normalize_float_precision | nullable | Полярная площадь поверхности |
| | rtb | INT | normalize_int, normalize_int_positive | nullable | Количество вращающихся связей |
| | ro3_pass | BOOL | normalize_boolean | nullable | Проходит ли Rule of 3 |
| | num_ro5_violations | INT | normalize_int, normalize_int_positive | nullable | Количество нарушений Rule of 5 |
| | acd_most_apka | DECIMAL | normalize_float, normalize_float_precision | nullable | Наиболее кислотный pKa |
| | acd_most_bpka | DECIMAL | normalize_float, normalize_float_precision | nullable | Наиболее основной pKa |
| | acd_logp | DECIMAL | normalize_float, normalize_float_precision | nullable | ACD LogP |
| | acd_logd | DECIMAL | normalize_float, normalize_float_precision | nullable | ACD LogD |
| | molecular_species | STRING | normalize_string_strip, normalize_string_upper | nullable | Молекулярный вид |
| | full_mwt | DECIMAL | normalize_float, normalize_molecular_weight | nullable | Полная молекулярная масса |
| | aromatic_rings | DECIMAL | normalize_float, normalize_float_precision | nullable | Количество ароматических колец |
| | heavy_atoms | DECIMAL | normalize_float, normalize_float_precision | nullable | Количество тяжелых атомов |
| | qed_weighted | DECIMAL | normalize_float, normalize_float_precision | nullable | Weighted QED значение |
| | mw_monoisotopic | DECIMAL | normalize_float, normalize_molecular_weight | nullable | Моноизотопная молекулярная масса |
| | full_molformula | STRING | normalize_string_strip, normalize_string_upper | nullable | Полная молекулярная формула |
| | hba_lipinski | INT | normalize_int, normalize_int_positive | nullable | HBA по Lipinski |
| | hbd_lipinski | INT | normalize_int, normalize_int_positive | nullable | HBD по Lipinski |
| | num_lipinski_ro5_violations | INT | normalize_int, normalize_int_positive | nullable | Количество нарушений Lipinski Ro5 |
| | oral | BOOL | normalize_boolean | nullable | Оральный путь введения |
| | parenteral | BOOL | normalize_boolean | nullable | Парентеральный путь введения |
| | topical | BOOL | normalize_boolean | nullable | Топический путь введения |
| | black_box_warning | BOOL | normalize_boolean | nullable | Предупреждение черного ящика |
| | natural_product | BOOL | normalize_boolean | nullable | Природный продукт |
| | first_in_class | BOOL | normalize_boolean | nullable | Первый в классе |
| | chirality | INT | normalize_int, normalize_int_range | nullable | Хиральность |
| | prodrug | BOOL | normalize_boolean | nullable | Пролекарство |
| | inorganic_flag | BOOL | normalize_boolean | nullable | Неорганическое соединение |
| | polymer_flag | BOOL | normalize_boolean | nullable | Полимер |
| | usan_year | INT | normalize_int, normalize_year | nullable | Год USAN регистрации |
| | availability_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип доступности |
| | usan_stem | STRING | normalize_string_strip, normalize_string_upper | nullable | USAN stem |
| | usan_substem | STRING | normalize_string_strip, normalize_string_upper | nullable | USAN substem |
| | usan_stem_definition | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Определение USAN stem |
| | indication_class | STRING | normalize_string_strip, normalize_string_upper | nullable | Класс показаний |
| | withdrawn_flag | BOOL | normalize_boolean | nullable | Отозванное лекарство |
| | withdrawn_year | INT | normalize_int, normalize_year | nullable | Год отзыва |
| | withdrawn_country | STRING | normalize_string_strip, normalize_string_titlecase | nullable | Страна отзыва |
| | withdrawn_reason | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Причина отзыва |
| | mechanism_of_action | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Механизм действия |
| | direct_interaction | BOOL | normalize_boolean | nullable | Прямое взаимодействие |
| | molecular_mechanism | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Молекулярный механизм |
| | drug_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | ID лекарства в ChEMBL |
| | drug_name | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Название лекарства |
| | drug_type | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип лекарства |
| | drug_substance_flag | BOOL | normalize_boolean | nullable | Флаг лекарственного вещества |
| | drug_indication_flag | BOOL | normalize_boolean | nullable | Флаг показаний |
| | drug_antibacterial_flag | BOOL | normalize_boolean | nullable | Флаг антибактериального действия |
| | drug_antiviral_flag | BOOL | normalize_boolean | nullable | Флаг противовирусного действия |
| | drug_antifungal_flag | BOOL | normalize_boolean | nullable | Флаг противогрибкового действия |
| | drug_antiparasitic_flag | BOOL | normalize_boolean | nullable | Флаг противопаразитарного действия |
| | drug_antineoplastic_flag | BOOL | normalize_boolean | nullable | Флаг противоопухолевого действия |
| | drug_immunosuppressant_flag | BOOL | normalize_boolean | nullable | Флаг иммуносупрессивного действия |
| | drug_antiinflammatory_flag | BOOL | normalize_boolean | nullable | Флаг противовоспалительного действия |
| | pubchem_cid | INT | normalize_int, normalize_int_positive | nullable | PubChem CID |
| | pubchem_molecular_formula | STRING | normalize_string_strip, normalize_string_upper | nullable | Молекулярная формула PubChem |
| | pubchem_molecular_weight | DECIMAL | normalize_float, normalize_molecular_weight | nullable | Молекулярная масса PubChem |
| | pubchem_canonical_smiles | STRING | normalize_string_strip, normalize_smiles | nullable | Канонические SMILES PubChem |
| | pubchem_isomeric_smiles | STRING | normalize_string_strip, normalize_smiles | nullable | Изомерные SMILES PubChem |
| | pubchem_inchi | STRING | normalize_string_strip, normalize_inchi | nullable | InChI PubChem |
| | pubchem_inchi_key | STRING | normalize_string_strip, normalize_inchi_key | nullable | InChI Key PubChem |
| | pubchem_registry_id | STRING | normalize_string_strip, normalize_string_upper | nullable | Registry ID PubChem |
| | pubchem_rn | STRING | normalize_string_strip, normalize_string_upper | nullable | RN PubChem |
| | standardized_inchi | STRING | normalize_string_strip, normalize_inchi | nullable | Стандартизированный InChI |
| | standardized_inchi_key | STRING | normalize_string_strip, normalize_inchi_key | nullable | Стандартизированный InChI Key |
| | standardized_smiles | STRING | normalize_string_strip, normalize_smiles | nullable | Стандартизированные SMILES |
| | atc_classifications | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | ATC классификации (JSON) |
| | biotherapeutic | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Биотерапевтическое соединение (JSON) |
| | chemical_probe | BOOL | normalize_boolean | nullable | Химический зонд |
| | cross_references | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Перекрестные ссылки (JSON) |
| | helm_notation | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | HELM нотация |
| | molecule_hierarchy | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Иерархия молекулы (JSON) |
| | molecule_properties | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Свойства молекулы (JSON) |
| | molecule_structures | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Структуры молекулы (JSON) |
| | molecule_synonyms | STRING | normalize_string_strip, normalize_string_nfc, normalize_string_whitespace | nullable | Синонимы молекулы (JSON) |
| | orphan | BOOL | normalize_boolean | nullable | Орфанное лекарство |
| | veterinary | BOOL | normalize_boolean | nullable | Ветеринарное лекарство |
| | standard_inchi | STRING | normalize_string_strip, normalize_inchi | nullable | Стандартный InChI |
| | chirality_chembl | STRING | normalize_string_strip, normalize_string_upper | nullable | Хиральность из ChEMBL |
| | molecule_type_chembl | STRING | normalize_string_strip, normalize_string_upper | nullable | Тип молекулы из ChEMBL |
| | nstereo | INT | normalize_int, normalize_int_positive | nullable | Количество стереоизомеров |
| | salt_chembl_id | STRING | normalize_string_strip, normalize_string_upper, normalize_chembl_id | nullable | ChEMBL ID соли |
| | index | INT | normalize_int, normalize_int_positive | NOT NULL | Порядковый номер записи |
| | pipeline_version | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Версия пайплайна |
| | source_system | STRING | normalize_string_strip, normalize_string_upper | NOT NULL | Система-источник |
| | chembl_release | STRING | normalize_string_strip, normalize_string_upper | nullable | Версия ChEMBL |
| | extracted_at | STRING | normalize_datetime_iso8601 | NOT NULL | Время извлечения данных |
| | hash_row | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш строки |
| | hash_business_key | STRING | normalize_string_strip, normalize_string_lower | NOT NULL | Хеш бизнес-ключа |

## 4. Ключевые проблемы нормализации и подходы

### 4.1 Конфликты источников (Documents/Targets)

**Проблема:** Различные источники предоставляют разные значения для одних и тех же полей.

**Решение:**
- Приоритетный источник + merge-стратегия
- Поля `*_source` для отслеживания источника данных
- Audit-лог изменений
- Поля `*_error` для ошибок извлечения

**Примеры:**
- DOI из Crossref vs DOI из OpenAlex
- Названия статей из разных источников
- Авторы в разных форматах

### 4.2 Регистры идентификаторов

**Проблема:** ChEMBL/UniProt/DOI имеют фиксированные регистры.

**Решение:**
- ChEMBL ID: всегда UPPERCASE (`CHEMBL123`)
- UniProt ID: всегда UPPERCASE (`P12345`)
- DOI: всегда lowercase (`10.1234/example`)
- Оригинал в `*_raw` полях

### 4.3 Единицы измерения и цензурирование (Activities)

**Проблема:** Различные единицы измерения и цензурированные данные.

**Решение:**
- Нормализация `standard_units` к стандартным единицам
- Нормализация `relation` к стандартным операторам
- Расчёт границ для цензурированных данных
- Хранение исходных значений в `published_*` полях
- Отказ от «глухого» округления

### 4.4 Пустые строки vs NULL

**Проблема:** Различные представления пустых значений.

**Решение:**
- Единые правила: `""`, `" "`, `"null"`, `"NULL"` → `NULL`
- Функция `normalize_empty_to_null`
- Pandera схемы с `nullable=True/False`

### 4.5 Детерминизм

**Проблема:** Обеспечение воспроизводимости результатов.

**Решение:**
- Фиксированный порядок колонок из `column_order` в конфигах
- Сортировка по стабильным ключам: `["document_chembl_id", "doi", "pmid"]`
- Функция `write_deterministic_csv`
- Контрольные суммы в `hash_row` и `hash_business_key`

### 4.6 Валидация Pandera

**Проблема:** Обеспечение качества данных.

**Решение:**
- Pandera схемы с метаданными нормализации
- Fail-fast для критичных полей
- QC-отчёты для мягких ошибок
- Валидация по типам, диапазонам, паттернам

### 4.7 Трассировка

**Проблема:** Отслеживание версий и изменений.

**Решение:**
- В `meta.yaml`: версии источников, активные профили нормализации
- Объёмы данных и число исправлений
- Хеши для контроля целостности
- Временные метки извлечения

## 5. Использованные файлы

### Конфигурации

- `configs/config_document.yaml` - конфигурация Documents пайплайна
- `configs/config_target.yaml` - конфигурация Targets пайплайна  
- `configs/config_assay.yaml` - конфигурация Assays пайплайна
- `configs/config_activity.yaml` - конфигурация Activities пайплайна
- `configs/config_testitem.yaml` - конфигурация Testitems пайплайна

### Схемы Pandera

- `src/library/schemas/document_schema_normalized.py` - схемы Documents
- `src/library/schemas/target_schema_normalized.py` - схемы Targets
- `src/library/schemas/assay_schema_normalized.py` - схемы Assays
- `src/library/schemas/activity_schema_normalized.py` - схемы Activities
- `src/library/schemas/testitem_schema_normalized.py` - схемы Testitems

### Модули нормализации

- `src/library/normalizers/string_normalizers.py` - строковые нормализаторы
- `src/library/normalizers/numeric_normalizers.py` - числовые нормализаторы
- `src/library/normalizers/boolean_normalizers.py` - булевые нормализаторы
- `src/library/normalizers/datetime_normalizers.py` - дата/время нормализаторы
- `src/library/normalizers/identifier_normalizers.py` - идентификаторы
- `src/library/normalizers/chemistry_normalizers.py` - химические структуры
- `src/library/normalizers/ontology_normalizers.py` - онтологии (BAO)
- `src/library/normalizers/units_normalizers.py` - единицы измерения

### Пайплайны

- `src/library/documents/pipeline.py` - Documents пайплайн
- `src/library/targets/pipeline.py` - Targets пайплайн
- `src/library/assay/pipeline.py` - Assays пайплайн
- `src/library/activity/pipeline.py` - Activities пайплайн
- `src/library/testitem/pipeline.py` - Testitems пайплайн

### Нормализация

- `src/library/documents/normalize.py` - нормализация Documents
- `src/library/targets/normalize.py` - нормализация Targets
- `src/library/assay/normalize.py` - нормализация Assays
- `src/library/activity/normalize.py` - нормализация Activities
- `src/library/testitem/normalize.py` - нормализация Testitems

### Загрузка

- `src/library/etl/load.py` - детерминистическая загрузка

## 6. Внешние спецификации

### DOI

- [DOI Handbook](https://www.doi.org/handbook_2000.html) - официальная спецификация DOI
- [DataCite DOI display guidelines](https://support.datacite.org/docs/doi-display-guidelines) - рекомендации по отображению DOI

### ChEMBL

- [ChEMBL Web Services](https://chembl.gitbook.io/chembl-interface-documentation/web-services) - API документация
- [ChEMBL Database Schema](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services) - схема базы данных

### UniProt

- [UniProt REST API](https://www.uniprot.org/help/api) - REST API документация
- [UniProt ID mapping](https://www.uniprot.org/help/id_mapping) - маппинг идентификаторов

### PubChem

- [PubChem PUG-REST](https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest) - PUG-REST API
- [PubChem Identifier Exchange](https://pubchem.ncbi.nlm.nih.gov/docs/identifier-exchange) - обмен идентификаторами

### Химические структуры

- [OpenSMILES specification](http://opensmiles.org/opensmiles.html) - спецификация SMILES
- [IUPAC InChI](https://iupac.org/who-we-are/divisions/division-details/inchi/) - спецификация InChI
- [InChI Technical Manual](https://www.inchi-trust.org/technical-faq-2/) - техническое руководство InChI

### Онтологии

- [BioAssay Ontology (BAO)](https://www.bioassayontology.org/) - онтология биоанализов
- [BAO GitHub](https://github.com/BioAssayOntology/BAO) - репозиторий BAO

### Единицы измерения

- [IUPAC Gold Book](https://goldbook.iupac.org/) - стандарты IUPAC
- [ChEMBL Units](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services#units) - единицы измерения в ChEMBL
