# 8c. IUPHAR/GtoPdb источник: фармакологическая классификация

## Обзор

IUPHAR/BPS Guide to Pharmacology (GtoPdb) предоставляет фармакологическую классификацию таргетов, включая иерархию семейств, natural ligands, interactions и типа target.

**Официальная документация**: [IUPHAR/BPS Guide to PHARMACOLOGY](https://www.guidetopharmacology.org/webServices.jsp)

---

## 4.1 GtoPdb API Overview

### Базовая конфигурация

- **Base URL**: `https://www.guidetopharmacology.org/services/`
- **API version**: v1
- **Форматы**: JSON

### Ключевые сервисы

#### /targets - Основной список таргетов

Получение списка всех таргетов с метаданными.

**Пример:**
```bash
GET https://www.guidetopharmacology.org/services/targets?format=json
```

**Параметры:**
- `type`: фильтр по типу target
- `name`: поиск по названию
- `geneSymbol`: фильтр по gene symbol
- `ecNumber`: фильтр по EC number
- `accession`: фильтр по UniProt accession
- `database`: фильтр по базе данных

#### /targets/families - Иерархия семейств

Получение всех families с иерархической структурой.

**Пример:**
```bash
GET https://www.guidetopharmacology.org/services/targets/families?format=json
```

**Структура response:**
```json
{
  "familyId": 1,
  "familyName": "GPCRs",
  "parentId": null,
  "fullIdPath": "1",
  "fullNamePath": "GPCRs",
  "targetIds": [1, 2, 3, ...]
}
```

#### /targets/families/{id} - Детали семейства

Детальная информация о конкретном семействе.

#### /targets/{id}/subunits - Субъединицы

Информация о субъединицах для комплексов.

#### /complexes - Комплексы

Список protein complexes.

#### /targets/{id}/synonyms - Синонимы

Синонимы таргета.

#### /targets/{id}/geneProteinInformation - Gene/Protein info

Детальная gene и protein информация.

#### /targets/{id}/databaseLinks - Cross-references

Ссылки на внешние базы (UniProt, HGNC, Ensembl, etc.).

---

## 4.2 IUPHARData класс

### Структура данных

```python
@dataclass
class IUPHARData:
    """Container for IUPHAR target and family data."""
    
    target_df: pd.DataFrame
    family_df: pd.DataFrame
    _target_cache: dict[str, pd.Series | None]
    _family_cache: dict[str, pd.Series | None]
```

### Target DataFrame structure

**Ключевые поля:**
- `target_id`: уникальный IUPHAR ID (integer)
- `name`: название target
- `type`: тип (receptor, ion channel, enzyme, transporter)
- `class`: класс (например, "GPCR")
- `subclass`: подкласс (например, "Class A")
- `family_id`: FK к families table
- `hgnc_id`, `hgnc_name`: HGNC gene information
- `uniprot_id_primary`: primary UniProt accession
- `organism`, `taxon_id`: таксономия
- `description`: описание функции

### Family DataFrame structure

**Ключевые поля:**
- `family_id`: уникальный ID family
- `family_name`: название
- `parent_id`: родительское family (для иерархии)
- `full_id_path`: полный путь ID (например, "1.2.3.4")
- `full_name_path`: полный путь названий (например, "GPCRs > Class A > Adenosine")
- `target_ids`: список target IDs (pipe-delimited)

### Lookup caches

Для быстрого доступа:

```python
_target_df_by_id: pd.DataFrame        # Indexed by target_id
_family_df_by_id: pd.DataFrame        # Indexed by family_id
_family_by_target: dict[str, pd.Series]  # Mapping target_id → family row
```

---

## 4.3 Target Classification

### Hierarchical structure

IUPHAR использует иерархическую классификацию:

```
Type
 └── Class (L1)
      └── Subclass (L2)
           └── Chain (L3)
                └── Specific target
```

**Пример:**
```
GPCRs (Type: Receptor, Class: GPCR)
  └── Class A (Subclass)
       └── Adenosine receptors (Chain)
            └── A1 adenosine receptor (Target)
```

### Full ID/Name paths

**Full ID path:**
- Формат: "1.2.3.4" (dot-separated hierarchy levels)
- Строится рекурсивно через parent_id

**Full name path:**
- Формат: "GPCRs > Class A > Adenosine > A1"
- Строится как конкатенация всех ancestor names

**Mapping к target:**
- Через `target_id` в families table
- Join с targets по `target_id`

---

## 4.4 Gene Symbol Mapping

### HGNC Integration

IUPHAR содержит HGNC mapping для gene symbol standardization:

```python
def get_hgnc_mapping(iuphar_data: IUPHARData, target_id: str) -> tuple[str, str]:
    """Extract HGNC name and ID from IUPHAR data."""
    target = iuphar_data.target_df[target_id]
    hgnc_name = target.get("hgnc_name", "")
    hgnc_id = target.get("hgnc_id", "")
    return hgnc_name, hgnc_id
```

**Формат HGNC ID:**
- IUPHAR: "HGNC:123" или plain "123"
- Normalized: "123" (только число)

### UniProt mapping

IUPHAR также содержит primary UniProt IDs для alignment с ChEMBL accessions:

```python
def get_uniprot_mapping(iuphar_data: IUPHARData, target_id: str) -> str:
    """Extract primary UniProt ID from IUPHAR data."""
    target = iuphar_data.target_df[target_id]
    return target.get("uniprot_id_primary", "")
```

---

## 4.5 Enrichment данные

### Natural ligands count

Количество известных natural ligands для target:

```python
def get_natural_ligands_count(iuphar_data: IUPHARData, target_id: str) -> int:
    """Get count of natural ligands for target."""
    # Lookup через databaseLinks или external API call
    # Returns: gtop_natural_ligands_n
```

### Interactions count

Количество protein-protein или protein-ligand interactions:

```python
def get_interactions_count(iuphar_data: IUPHARData, target_id: str) -> int:
    """Get count of interactions for target."""
    # Returns: gtop_interactions_n
```

### Function text

Краткое описание функции target:

```python
def get_function_text(iuphar_data: IUPHARData, target_id: str) -> str:
    """Get function description for target."""
    target = iuphar_data.target_df[target_id]
    return target.get("description", "")[:200]  # Truncate to 200 chars
```

### Synonyms

Pipe-delimited список синонимов:

```python
def get_synonyms(iuphar_data: IUPHARData, target_id: str) -> str:
    """Get pipe-delimited synonyms for target."""
    # Fetch через /targets/{id}/synonyms endpoint
    # Returns: "synonym1|synonym2|synonym3"
```

---

## 4.6 Integration Logic

### Merge стратегия

IUPHAR data merge с ChEMBL/UniProt через join по идентификаторам:

```python
def merge_iuphar_data(
    chembl_df: pd.DataFrame,
    iuphar_data: IUPHARData
) -> pd.DataFrame:
    """Merge IUPHAR classification into ChEMBL data."""
    
    # Primary join: UniProt Accession
    result = chembl_df.merge(
        iuphar_data.target_df,
        left_on="uniprot_id_primary",
        right_on="uniprot_id_primary",
        how="left",
        suffixes=("_chembl", "_iuphar")
    )
    
    # Fallback join: HGNC gene symbol
    missing_mask = result["target_id_iuphar"].isna()
    if missing_mask.any():
        result.loc[missing_mask, :] = result.loc[missing_mask, :].merge(
            iuphar_data.target_df,
            left_on="gene_symbol",
            right_on="hgnc_name",
            how="left",
            suffixes=("_chembl", "_iuphar")
        )
    
    return result
```

### Join priorities

1. **Primary**: `uniprot_id_primary` (самый точный)
2. **Secondary**: `gene_symbol` (fallback при отсутствии UniProt)
3. **Tertiary**: name fuzzy match (последний resort)

### Конфликты

При конфликтах данных применяется правило приоритета:

**ChEMBL priority для core fields:**
- `target_chembl_id`, `pref_name`, `target_type`, `organism`

**IUPHAR priority для classification:**
- `type`, `class`, `subclass`, `full_name_path`
- `natural_ligands_n`, `interactions_n`

### Error handling

IUPHAR API может возвращать:
- **204 No Content**: нет данных для target
- **404 Not Found**: target не существует

**Response:**
- Не паниковать, пропускать target
- Логировать пропуски для аудита
- Continue pipeline без прерывания

---

## Примеры использования

### Загрузка IUPHAR данных

```python
from library.integration.iuphar_library import IUPHARData
from library.clients.iuphar import load_targets, load_families

# Загрузка из CSV files
iuphar_data = IUPHARData.from_files(
    target_path="data/iuphar/targets.csv",
    family_path="data/iuphar/families.csv",
    encoding="utf-8"
)
```

### Enrichment полного pipeline

```python
def enrich_with_iuphar(
    chembl_df: pd.DataFrame,
    iuphar_data: IUPHARData
) -> pd.DataFrame:
    """Enrich ChEMBL data with IUPHAR classification."""
    
    # Merge targets
    enriched = merge_iuphar_data(chembl_df, iuphar_data)
    
    # Extract family paths
    for idx, row in enriched.iterrows():
        target_id = row.get("target_id_iuphar")
        if pd.notna(target_id):
            family = iuphar_data.get_family_by_target(target_id)
            if family is not None:
                enriched.at[idx, "iuphar_full_id_path"] = family.get("full_id_path")
                enriched.at[idx, "iuphar_full_name_path"] = family.get("full_name_path")
    
    return enriched
```

---

## Ссылки

- [IUPHAR/BPS Guide to PHARMACOLOGY](https://www.guidetopharmacology.org/webServices.jsp)
- [GtoPdb API Documentation](https://www.guidetopharmacology.org/services/targets/)
- Код: `e:\github\ChEMBL_data_acquisition6\library\integration\iuphar_library.py`

