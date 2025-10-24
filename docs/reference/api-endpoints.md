# API Endpoints Reference

## Обзор

Детальная документация всех API endpoints, используемых в пайплайнах bioactivity_data_acquisition. Для каждого источника указаны endpoints, параметры запросов, примеры ответов и ограничения.

## ChEMBL API

### Base URL
`https://www.ebi.ac.uk/chembl/api/data`

### Endpoints

#### 1. Document Endpoint
**URL:** `/document/{document_chembl_id}`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `document_chembl_id` (path): ChEMBL ID документа (например, CHEMBL123456)

**Пример запроса:**
```bash
curl "https://www.ebi.ac.uk/chembl/api/data/document/CHEMBL123456"
```

**Пример ответа:**
```json
{
  "document_chembl_id": "CHEMBL123456",
  "document_type": "article",
  "title": "Discovery of novel inhibitors",
  "doi": "10.1021/acs.jmedchem.0c01234",
  "pubmed_id": 12345678,
  "journal": "Journal of Medicinal Chemistry",
  "year": 2020,
  "volume": 63,
  "issue": 12,
  "first_page": 1234,
  "last_page": 1245,
  "abstract": "Abstract text...",
  "authors": "Smith, J.; Doe, A."
}
```

#### 2. Target Endpoint
**URL:** `/target/{target_chembl_id}`  
**Method:** GET  
**Используется в:** targets pipeline

**Параметры:**
- `target_chembl_id` (path): ChEMBL ID мишени (например, CHEMBL240)

**Пример запроса:**
```bash
curl "https://www.ebi.ac.uk/chembl/api/data/target/CHEMBL240"
```

**Пример ответа:**
```json
{
  "target_chembl_id": "CHEMBL240",
  "pref_name": "Adenosine A1 receptor",
  "target_type": "SINGLE PROTEIN",
  "organism": "Homo sapiens",
  "tax_id": 9606,
  "target_components": [
    {
      "component_id": 1,
      "component_type": "PROTEIN",
      "component_description": "Adenosine A1 receptor",
      "uniprot_id": "P30542"
    }
  ]
}
```

#### 3. Assay Endpoint
**URL:** `/assay/{assay_chembl_id}`  
**Method:** GET  
**Используется в:** assays pipeline

**Параметры:**
- `assay_chembl_id` (path): ChEMBL ID ассая (например, CHEMBL1234567)

**Пример ответа:**
```json
{
  "assay_chembl_id": "CHEMBL1234567",
  "assay_type": "B",
  "assay_category": "B",
  "target_chembl_id": "CHEMBL240",
  "description": "Inhibition of human adenosine A1 receptor",
  "assay_format": "Cell-based",
  "bao_format": "BAO_0000357",
  "bao_label": "cell-based format"
}
```

#### 4. Activity Endpoint
**URL:** `/activity`  
**Method:** GET  
**Используется в:** activities pipeline

**Параметры:**
- `limit` (query): Количество записей (по умолчанию 20, максимум 1000)
- `offset` (query): Смещение для пагинации
- `assay_chembl_id` (query): Фильтр по ассаю
- `molecule_chembl_id` (query): Фильтр по молекуле
- `target_chembl_id` (query): Фильтр по мишени

**Пример запроса:**
```bash
curl "https://www.ebi.ac.uk/chembl/api/data/activity?limit=100&offset=0&target_chembl_id=CHEMBL240"
```

**Пример ответа:**
```json
{
  "activities": [
    {
      "activity_chembl_id": "CHEMBL12345678",
      "assay_chembl_id": "CHEMBL1234567",
      "molecule_chembl_id": "CHEMBL123",
      "target_chembl_id": "CHEMBL240",
      "document_chembl_id": "CHEMBL123456",
      "standard_type": "IC50",
      "standard_value": 0.001,
      "standard_units": "nM",
      "pchembl_value": 9.0
    }
  ],
  "page_meta": {
    "limit": 100,
    "offset": 0,
    "total_count": 1500
  }
}
```

#### 5. Molecule Endpoint
**URL:** `/molecule/{molecule_chembl_id}`  
**Method:** GET  
**Используется в:** testitems pipeline

**Параметры:**
- `molecule_chembl_id` (path): ChEMBL ID молекулы (например, CHEMBL123)

**Пример ответа:**
```json
{
  "molecule_chembl_id": "CHEMBL123",
  "pref_name": "Caffeine",
  "max_phase": 4,
  "therapeutic_flag": true,
  "molecule_type": "Small molecule",
  "molecule_properties": {
    "mw_freebase": 194.19,
    "alogp": -0.07,
    "hba": 6,
    "hbd": 0,
    "psa": 61.82
  },
  "molecule_structures": {
    "canonical_smiles": "CN1C=NC2=C1C(=O)N(C(=O)N2C)C",
    "standard_inchi": "InChI=1S/C8H10N4O2/c1-10-4-9-6-5(10)7(13)12(3)8(14)11(6)2/h4H,1-3H3",
    "standard_inchi_key": "RYYVLZVUVIJVGH-UHFFFAOYSA-N"
  }
}
```

### Rate Limits
- **Официальные лимиты:** Не указаны
- **Рекомендуемые:** ≤5 запросов в секунду
- **Retry policy:** 5 попыток с экспоненциальной задержкой

## Crossref API

### Base URL
`https://api.crossref.org`

### Endpoints

#### 1. Works Search
**URL:** `/works`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `query` (query): Поисковый запрос
- `filter` (query): Фильтры (например, `from-pub-date:2020`)
- `select` (query): Поля для возврата (например, `DOI,title,author`)
- `rows` (query): Количество результатов (по умолчанию 20, максимум 1000)
- `cursor` (query): Курсор для пагинации

**Пример запроса:**
```bash
curl "https://api.crossref.org/works?query=chembl&rows=10&select=DOI,title,author"
```

**Пример ответа:**
```json
{
  "message": {
    "total-results": 1500,
    "items": [
      {
        "DOI": "10.1021/acs.jmedchem.0c01234",
        "title": ["Discovery of novel inhibitors"],
        "author": [
          {
            "given": "John",
            "family": "Smith"
          }
        ],
        "published-print": {
          "date-parts": [[2020, 6, 15]]
        },
        "container-title": ["Journal of Medicinal Chemistry"]
      }
    ]
  }
}
```

#### 2. Work by DOI
**URL:** `/works/{DOI}`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `DOI` (path): DOI документа

**Пример запроса:**
```bash
curl "https://api.crossref.org/works/10.1021/acs.jmedchem.0c01234"
```

### Rate Limits
- **Лимиты:** 50 запросов в секунду (с вежливым пулом)
- **Без API ключа:** 50 req/s
- **С API ключом:** 100 req/s

## OpenAlex API

### Base URL
`https://api.openalex.org`

### Endpoints

#### 1. Works Search
**URL:** `/works`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `filter` (query): Фильтры (например, `doi:10.1021/acs.jmedchem.0c01234`)
- `search` (query): Поисковый запрос
- `group_by` (query): Группировка результатов
- `per-page` (query): Количество результатов на страницу (максимум 200)

**Пример запроса:**
```bash
curl "https://api.openalex.org/works?filter=doi:10.1021/acs.jmedchem.0c01234"
```

**Пример ответа:**
```json
{
  "results": [
    {
      "id": "https://openalex.org/W1234567890",
      "doi": "https://doi.org/10.1021/acs.jmedchem.0c01234",
      "title": "Discovery of novel inhibitors",
      "publication_year": 2020,
      "publication_date": "2020-06-15",
      "host_venue": {
        "display_name": "Journal of Medicinal Chemistry",
        "issn": "0022-2623"
      },
      "authorships": [
        {
          "author": {
            "display_name": "John Smith"
          }
        }
      ],
      "abstract_inverted_index": {
        "Abstract": [0, 1, 2, 3]
      }
    }
  ]
}
```

### Rate Limits
- **Лимиты:** 10 запросов в секунду
- **Дневной лимит:** 100,000 запросов без API ключа
- **С API ключом:** 1,000,000 запросов в день

## PubMed API (E-utilities)

### Base URL
`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`

### Endpoints

#### 1. ESearch
**URL:** `/esearch.fcgi`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `db` (query): База данных (pubmed)
- `term` (query): Поисковый запрос
- `retmax` (query): Максимальное количество результатов
- `retmode` (query): Формат ответа (json)
- `api_key` (query): API ключ (опционально)

**Пример запроса:**
```bash
curl "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=chembl&retmax=10&retmode=json"
```

**Пример ответа:**
```json
{
  "esearchresult": {
    "count": "1500",
    "retmax": "10",
    "retstart": "0",
    "idlist": ["12345678", "12345679", "12345680"]
  }
}
```

#### 2. EFetch
**URL:** `/efetch.fcgi`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `db` (query): База данных (pubmed)
- `id` (query): PMID или список PMID
- `retmode` (query): Формат ответа (json)
- `rettype` (query): Тип данных (abstract, summary)
- `api_key` (query): API ключ (опционально)

**Пример запроса:**
```bash
curl "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=12345678&retmode=json&rettype=abstract"
```

**Пример ответа:**
```json
{
  "result": {
    "12345678": {
      "uid": "12345678",
      "title": "Discovery of novel inhibitors",
      "abstract": "Abstract text...",
      "authors": [
        {
          "name": "Smith J",
          "authtype": "Author"
        }
      ],
      "pubdate": "2020 Jun 15",
      "source": "J Med Chem",
      "volume": "63",
      "issue": "12",
      "pages": "1234-45"
    }
  }
}
```

### Rate Limits
- **Без API ключа:** 3 запроса в секунду
- **С API ключом:** 10 запросов в секунду
- **Дневной лимит:** 50,000 запросов

## Semantic Scholar API

### Base URL
`https://api.semanticscholar.org/graph/v1`

### Endpoints

#### 1. Paper Search
**URL:** `/paper/search`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `query` (query): Поисковый запрос
- `fields` (query): Поля для возврата (paperId,title,abstract,venue,year)
- `limit` (query): Количество результатов (максимум 100)
- `offset` (query): Смещение для пагинации

**Пример запроса:**
```bash
curl "https://api.semanticscholar.org/graph/v1/paper/search?query=chembl&fields=paperId,title,abstract&limit=10"
```

**Пример ответа:**
```json
{
  "data": [
    {
      "paperId": "1234567890",
      "title": "Discovery of novel inhibitors",
      "abstract": "Abstract text...",
      "venue": "Journal of Medicinal Chemistry",
      "year": 2020,
      "authors": [
        {
          "name": "John Smith"
        }
      ]
    }
  ],
  "total": 1500
}
```

#### 2. Paper by ID
**URL:** `/paper/{paper_id}`  
**Method:** GET  
**Используется в:** documents pipeline

**Параметры:**
- `paper_id` (path): ID статьи или DOI/PMID с префиксом

**Пример запроса:**
```bash
curl "https://api.semanticscholar.org/graph/v1/paper/PMID:12345678"
```

### Rate Limits
- **Лимиты:** 100 запросов в 5 минут
- **Burst:** до 2 запросов подряд
- **С API ключом:** 1000 запросов в 5 минут

## UniProt API

### Base URL
`https://rest.uniprot.org`

### Endpoints

#### 1. UniProtKB Entry
**URL:** `/uniprotkb/{accession}`  
**Method:** GET  
**Используется в:** targets pipeline

**Параметры:**
- `accession` (path): UniProt accession (например, P30542)
- `format` (query): Формат ответа (json, xml, fasta)
- `fields` (query): Поля для возврата

**Пример запроса:**
```bash
curl "https://rest.uniprot.org/uniprotkb/P30542?format=json"
```

**Пример ответа:**
```json
{
  "entryType": "UniProtKB entry",
  "primaryAccession": "P30542",
  "uniProtkbId": "ADA1_HUMAN",
  "organism": {
    "scientificName": "Homo sapiens",
    "commonName": "Human",
    "taxonId": 9606
  },
  "proteinDescription": {
    "recommendedName": {
      "fullName": {
        "value": "Adenosine A1 receptor"
      }
    }
  },
  "genes": [
    {
      "geneName": {
        "value": "ADORA1"
      }
    }
  ],
  "sequence": {
    "length": 326,
    "molWeight": 36511
  }
}
```

#### 2. UniProtKB Search
**URL:** `/uniprotkb/search`  
**Method:** GET  
**Используется в:** targets pipeline

**Параметры:**
- `query` (query): Поисковый запрос
- `format` (query): Формат ответа
- `size` (query): Количество результатов
- `fields` (query): Поля для возврата

### Rate Limits
- **Лимиты:** ~10 запросов в секунду (политес API)
- **Дневной лимит:** Не ограничен

## PubChem API

### Base URL
`https://pubchem.ncbi.nlm.nih.gov/rest/pug`

### Endpoints

#### 1. Compound Properties
**URL:** `/compound/cid/{cid}/property/{properties}/JSON`  
**Method:** GET  
**Используется в:** testitems pipeline

**Параметры:**
- `cid` (path): PubChem CID
- `properties` (path): Список свойств (MolecularFormula,MolecularWeight,CanonicalSMILES,InChI,InChIKey)

**Пример запроса:**
```bash
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/2244/property/MolecularFormula,MolecularWeight,CanonicalSMILES/JSON"
```

**Пример ответа:**
```json
{
  "PropertyTable": {
    "Properties": [
      {
        "CID": 2244,
        "MolecularFormula": "C8H10N4O2",
        "MolecularWeight": 194.19,
        "CanonicalSMILES": "CN1C=NC2=C1C(=O)N(C(=O)N2C)C"
      }
    ]
  }
}
```

### Rate Limits
- **Лимиты:** 5 запросов в секунду
- **Дневной лимит:** 50,000 запросов

## IUPHAR/GtoPdb

### Источник данных
Локальные CSV файлы в `configs/dictionary/_target/`

### Файлы
- `iuphar_targets.csv` - основные данные о мишенях
- `iuphar_families.csv` - семейства мишеней

**Используется в:** targets pipeline

## Общие принципы работы с API

### Аутентификация
- **ChEMBL:** Опциональный Bearer token
- **Crossref:** Опциональный API ключ
- **PubMed:** Опциональный API ключ
- **Semantic Scholar:** Опциональный API ключ
- **OpenAlex:** Без аутентификации
- **UniProt:** Без аутентификации
- **PubChem:** Без аутентификации

### Обработка ошибок
- **HTTP 429 (Rate Limited):** Экспоненциальная задержка
- **HTTP 5xx:** Повторные попытки с backoff
- **Timeout:** Настраиваемые таймауты для каждого API

### Кэширование
- **TTL:** 24 часа для ChEMBL, 7 дней для остальных
- **Ключи:** SHA256 от URL + параметры
- **Стратегия:** Кэш-first с fallback на API

### Мониторинг
- **Метрики:** Количество запросов, ошибки, время ответа
- **Алерты:** При превышении лимитов или высокой частоте ошибок
- **Логирование:** Структурированные логи для всех API вызовов
