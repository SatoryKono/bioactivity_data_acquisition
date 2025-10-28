# 8a. ChEMBL источник: извлечение данных Target

## Обзор

ChEMBL является базовым источником данных о таргетах (targets). Этот документ описывает детали API интеграции, batch retrieval, парсинг компонентов и нормализацию данных.

**Официальная документация**: [ChEMBL Documentation](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services)

---

## 2.1 ChEMBL API Overview

### Базовая конфигурация

- **Base URL**: `https://www.ebi.ac.uk/chembl/api/data/`
- **Форматы**: JSON (рекомендуется), XML, YAML
- **API version**: v1 (REST)

### Ресурсы ChEMBL

#### /target - Объекты таргетов

Основной ресурс для извлечения метаданных таргета.

**Пример запроса:**
```bash
GET https://www.ebi.ac.uk/chembl/api/data/target?target_chembl_id__in=CHEMBL203&format=json&include=protein_classifications,cross_references
```

**Поля:**
- `target_chembl_id`: уникальный ID (формат: CHEMBL\d+)
- `pref_name`: предпочтительное название
- `target_type`: тип (SINGLE PROTEIN, PROTEIN COMPLEX, etc.)
- `organism`: organism name
- `tax_id`: NCBI taxonomy ID
- `species_group_flag`: флаг группировки по видам

#### /target_component - Компоненты таргета

Аминокислотные последовательности и компоненты (для комплексов, мультимеров).

**Пример запроса:**
```bash
GET https://www.ebi.ac.uk/chembl/api/data/target_component?target_chembl_id__in=CHEMBL203&format=json
```

**Поля:**
- `component_id`: уникальный ID компонента
- `component_type`: тип компонента
- `accession`: UniProt accession (если доступно)
- `sequence`: amino acid sequence
- `component_description`: описание

#### /target_relation - Связи между таргетами

Отношения гомологии, эквивалентности между таргетами.

**Пример запроса:**
```bash
GET https://www.ebi.ac.uk/chembl/api/data/target_relation?target_chembl_id__in=CHEMBL203&format=json
```

**Поля:**
- `target_relation_id`: ID связи
- `relationship_type`: тип отношения (homology, equivalence)
- `related_target_chembl_id`: связанный таргет

#### /protein_classification - Иерархия семейств

Protein family classification с иерархией уровней.

**Пример запроса:**
```bash
GET https://www.ebi.ac.uk/chembl/api/data/protein_classification?protein_class_id__in=205&format=json
```

**Поля:**
- `protein_class_id`: уникальный ID
- `class_level`: уровень (1, 2, 3, 4...)
- `pref_name`: название классификации
- `short_name`: краткое название

### Пагинация

ChEMBL использует стандартную пагинацию через параметры `limit` и `offset`:

```bash
# Первая страница (дефолт limit=20)
GET /target?limit=20&offset=0

# Вторая страница
GET /target?limit=20&offset=20
```

**page_meta в ответе:**
```json
{
  "page_meta": {
    "limit": 20,
    "offset": 0,
    "total_count": 12543
  },
  "targets": [...]
}
```

**Правила пагинации:**
- Дефолт: `limit=20`, `offset=0`
- Максимум: нет ограничения (но рекомендуется ≤100 для стабильности)
- Обход: постранично до `offset >= total_count`

### Фильтрация

ChEMBL поддерживает фильтрацию по полям ресурса:

**Синтаксис фильтров:**
- `field__exact`: точное совпадение
- `field__contains`: поиск подстроки (case-insensitive)
- `field__icontains`: case-insensitive contains
- `field__in`: список значений (comma-separated в URL)
- `field__gt`, `field__lt`: сравнения для числовых полей

**Примеры:**
```bash
# Поиск по названию
GET /target?pref_name__contains=cyclin

# Фильтрация по типу
GET /target?target_type__exact=SINGLE%20PROTEIN

# Множественный фильтр
GET /target?organism__icontains=human&target_type__exact=SINGLE%20PROTEIN
```

### POST с X-HTTP-Method-Override: GET

Для длинных запросов (>2000 символов URL) используется POST с заголовком:

```bash
POST https://www.ebi.ac.uk/chembl/api/data/target.json
Headers: X-HTTP-Method-Override: GET
Body:
{
  "target_chembl_id__in": "CHEMBL203,CHEMBL204,CHEMBL205,..."
}
```

**Когда использовать:**
- Большой список IDs (>100)
- Длинные фильтры
- Избежание проблем с URL encoding

---

## 2.2 Ресурсы и параметры

### TARGET_FIELDS спецификация

Всего 20 полей, извлекаемых из ChEMBL API:

```python
TARGET_FIELDS = [
    "pref_name",                 # Предпочтительное название
    "target_chembl_id",          # PRIMARY KEY
    "component_description",     # Описание компонента
    "component_id",              # ID компонента
    "relationship",              # Отношение (из target_type)
    "gene",                      # Gene symbols (pipe-delimited)
    "uniprot_id",                # UniProt ID из cross_references
    "mapping_uniprot_id",    # UniProt ID из mapping service
    "chembl_alternative_name",  # Альтернативные названия
    "ec_code",                   # EC numbers (pipe-delimited)
    "hgnc_name",                 # HGNC название
    "hgnc_id",                   # HGNC ID
    "target_type",               # Тип таргета
    "tax_id",                    # NCBI taxonomy ID
    "species_group_flag",        # Флаг группировки
    "target_components",         # JSON компонентов
    "protein_classifications",   # JSON классификаций
    "cross_references",          # JSON xrefs
    "reaction_ec_numbers",       # EC numbers из компонентов
]
```

**Типы данных:**
- **String** (nullable): большинство полей
- **Int64** (nullable): tax_id, hgnc_id, component_id
- **Boolean** (nullable): species_group_flag
- **JSON String**: target_components, protein_classifications, cross_references

**Nullable constraints:**
- NOT NULL: target_chembl_id (PRIMARY KEY)
- Nullable: все остальные поля

---

## 2.3 Batch Retrieval

### iter_target_batches функция

Основная функция для batch извлечения таргетов:

```python
def iter_target_batches(
    ids: Sequence[str],
    *,
    cfg: ApiCfg,
    client: ChemblClient,
    mapping_cfg: UniprotMappingCfg,
    chunk_size: int = 5,
    timeout: float | None = None,
    enable_split_fallback: bool = True,
) -> Iterator[tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame]]:
    """Yield payloads, raw and parsed target data frames for ids."""
```

**Параметры:**
- `ids`: список `target_chembl_id`
- `chunk_size`: размер батча (default=5 для стабильности)
- `timeout`: override таймаута
- `enable_split_fallback`: автоматическое разбиение при timeout

**Возвращает:** итератор триплетов:
1. `payloads`: raw JSON records
2. `raw_frame`: pandas DataFrame из JSON (pd.json_normalize)
3. `parsed_frame`: parsed и нормализованный DataFrame

### Chunk size стратегия

**Рекомендуемый размер:** 5 IDs на запрос

**Обоснование:**
- Устойчивость к таймаутам
- Размер URL < 2000 символов
- Batch-эффективность без риска overload

**Пример URL:**
```bash
/target.json?target_chembl_id__in=CHEMBL203,CHEMBL204,CHEMBL205,CHEMBL206,CHEMBL207&format=json&include=protein_classifications,cross_references
```

### URL construction

```python
base = f"{cfg.chembl_base.rstrip('/')}/target.json?format=json"
base += f"&include={TARGET_INCLUDE_PARAMS}"  # protein_classifications,cross_references

for chunk in _chunked(ids, chunk_size):
    ids_param = ','.join(chunk)
    url = f"{base}&target_chembl_id__in={ids_param}"
```

**Include параметры:**
- `protein_classifications`: полная иерархия
- `cross_references`: внешние ссылки (UniProt, Ensembl, etc.)

### Timeout handling и fallback splitting

```python
def _iter_target_chunk_with_fallback(
    chunk: Sequence[str],
    *,
    base_url: str,
    cfg: ApiCfg,
    client: ChemblClient,
    mapping_cfg: UniprotMappingCfg,
    timeout: float,
    enable_split_fallback: bool,
) -> Iterator[tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame]]:
    """Yield processed records for chunk with timeout-aware retries."""
    
    try:
        data = client.request_json(url, cfg=cfg, timeout=timeout)
    except requests.ReadTimeout as exc:
        if len(chunk) <= 1 or not enable_split_fallback:
            raise exc
        # Рекурсивное разбиение на одиночные IDs
        for identifier in chunk:
            yield from _iter_target_chunk_with_fallback(
                [identifier],
                base_url=base_url,
                cfg=cfg,
                client=client,
                mapping_cfg=mapping_cfg,
                timeout=timeout,
                enable_split_fallback=enable_split_fallback,
            )
```

**Логика:**
1. Timeout на chunk → разбить пополам
2. Повторный timeout → разбить до одиночных IDs
3. Timeout на одиночном ID → пропустить с warning

---

## 2.4 Retry и Adaptive Chunking

### iter_target_batches_with_retry

Функция с продвинутой retry логикой и adaptive chunk sizing:

```python
def iter_target_batches_with_retry(
    ids: Iterable[str],
    *,
    cfg: ApiCfg,
    client: ChemblClient,
    mapping_cfg: UniprotMappingCfg,
    chunk_size: int = 5,
    timeout: float | None = None,
    retry_cfg: TargetChemblBatchRetryCfg | None = None,
    log: Any | None = None,
    on_attempt: Callable[[], None] | None = None,
) -> Iterator[tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame]]:
```

### Конфигурация retry

```yaml
target_chembl_batch_retry:
  enable: true
  shrink_factor: 0.5      # Уменьшение в 2 раза при retry
  min_size: 1            # Минимум 1 ID
  single_timeout_retries: 3    # Retry count для одиночного ID
  single_timeout_delay: 2.0    # Задержка между retry
```

### Exponential backoff

При 5xx или timeout:
1. Уменьшить chunk_size: `new_size = int(chunk_size * 0.5)`
2. Если `new_size < 1`: установить `new_size = 1`
3. Повторить с новым размером

### Single retry logic

```python
def _should_retry_single(key: tuple[str, ...], exc: Exception) -> bool:
    if single_retry_limit <= 0:
        return False
    if not isinstance(exc, ReadTimeout):
        return False
    attempts = single_retry_counts.get(key, 0)
    if attempts >= single_retry_limit:
        return False
    single_retry_counts[key] = attempts + 1
    return True
```

**Поведение:**
- Только для ReadTimeout
- Максимум 3 retry на ID
- Задержка 2.0 сек между попытками

### Троттлинг

**Rate limiting:** 5 запросов / 15 секунд

```python
from library.common.rate_limiter import sleep

# Перед каждым запросом
sleep(15.0 / 5.0)  # ~3 секунды между запросами
```

**Соблюдение лимитов сервиса:**
- Нет DoS на ChEMBL API
- Предсказуемое время выполнения
- Graceful degradation при rate limiting

---

## 2.5 Парсинг компонентов

### _parse_target_record

Функция трансформации raw ChEMBL JSON в flat dictionary:

```python
def _parse_target_record(
    data: dict[str, Any], mapping_cfg: UniprotMappingCfg
) -> dict[str, Any]:
    """Transform a raw target record into a flat dictionary."""
    
    # Extract components
    components = _get_items(data.get("target_components"), "target_component")
    comp = components[0] if components else {}
    
    # Extract synonyms и xrefs
    synonyms = _get_items(
        comp.get("target_component_synonyms"), "target_component_synonym"
    )
    xrefs = _get_items(comp.get("target_component_xrefs"), "target")
    
    # Parse fields
    gene_syn = _parse_gene_synonyms(synonyms)
    ec_code = _parse_ec_codes(synonyms)
    alt_name = _parse_alt_names(synonyms)
    uniprot_id, mapping_uniprot_id = _parse_uniprot_id(xrefs, data.get("target_chembl_id"), mapping_cfg)
    hgnc_name, hgnc_id = _parse_hgnc(xrefs)
    
    # Collect reaction EC numbers
    reaction_ec_numbers = _collect_reaction_ec_numbers(components)
    
    # Build result
    res = dict(EMPTY_TARGET)
    res.update({
        "pref_name": data.get("pref_name", ""),
        "target_chembl_id": data.get("target_chembl_id", ""),
        "component_description": comp.get("component_description", ""),
        "component_id": str(comp.get("component_id", "")),
        "relationship": data.get("target_type", ""),
        "gene": gene_syn,
        "uniprot_id": uniprot_id,
        "mapping_uniprot_id": mapping_uniprot_id,
        "chembl_alternative_name": alt_name,
        "ec_code": ec_code,
        "hgnc_name": hgnc_name,
        "hgnc_id": hgnc_id,
        "target_type": _stringify(data.get("target_type")),
        "tax_id": _stringify(data.get("tax_id")),
        "species_group_flag": _stringify(data.get("species_group_flag")),
        "target_components": _serialize_structure(components),
        "protein_classifications": _serialize_structure(data.get("protein_classifications")),
        "cross_references": _serialize_structure(data.get("cross_references")),
        "reaction_ec_numbers": reaction_ec_numbers,
    })
    return res
```

### Парсинг gene synonyms

```python
def _parse_gene_synonyms(synonyms: list[dict[str, str]]) -> str:
    """Return a sorted, pipe separated list of gene synonyms."""
    names = {
        s["component_synonym"]
        for s in synonyms
        if s.get("syn_type") in {"GENE_SYMBOL", "GENE_SYMBOL_OTHER"}
    }
    return "|".join(sorted(names))
```

**Поведение:**
- Фильтрация по `syn_type`: только GENE_SYMBOL, GENE_SYMBOL_OTHER
- Сортировка алфавитно
- Pipe-delimited для множественных значений

### Парсинг HGNC

```python
def _parse_hgnc(xrefs: list[dict[str, str]]) -> tuple[str, str]:
    """Extract HGNC name and identifier from a list of cross references."""
    for x in xrefs:
        if x.get("xref_src_db") == "HGNC":
            name = x.get("xref_name", "")
            ident = x.get("xref_id", "")
            hgnc_id = ident.split(":")[-1] if ident else ""
            return name, hgnc_id
    return "", ""
```

**Формат HGNC ID:**
- ChEMBL format: "HGNC:123"
- Normalized: "123" (только число)
- Возврат: (name, id) tuple

---

## 2.6 EC Numbers нормализация

### normalize_reaction_ec_numbers

Функция очистки и нормализации EC numbers:

```python
def normalize_reaction_ec_numbers(values: Iterable[str | None]) -> str:
    """Return a pipe-delimited string of sanitized EC numbers from values."""
    numbers = _collect_normalized_ec_tokens(values)
    return "|".join(sorted(numbers))
```

### Regex patterns

**Полный валидный EC number:**
```python
_EC_FULL_PATTERN = re.compile(r"^\d+(?:\.(?:\d+|-)){3}$")
```

**Примеры валидных:**
- `1.2.3.4`
- `1.2.-.-`
- `123.45.67.8`

**Token splitting:**
```python
_EC_TOKEN_SPLIT = re.compile(r"[|;,/\\\s]+")
```

Разбиение по: `|`, `;`, `,`, `/`, `\`, whitespace

### Prefix cleaning

```python
def _normalise_ec_token(token: str) -> str:
    """Return a cleaned EC token stripped of prefixes and whitespace."""
    token = token.strip()
    if not token:
        return ""
    upper = token.upper()
    if upper.startswith("EC"):
        token = token[2:]
        token = token.lstrip(":._- ")
    return token.strip()
```

**Удаляемые префиксы:**
- "EC:"
- "EC."
- "EC "
- "EC-"

**Сортировка и pipe-delimited:**
- Дедупликация через set
- Алфавитная сортировка
- Объединение через `|`

---

## 2.7 UniProt Mapping через ChEMBL

### _parse_uniprot_id

Извлечение UniProt ID из ChEMBL cross_references и mapping service:

```python
def _parse_uniprot_id(
    xrefs: list[dict[str, str]], chembl_id: str, mapping_cfg: UniprotMappingCfg
) -> tuple[str, str]:
    """Return UniProt IDs from cross references and mapping."""
    uniprot_id = ""
    
    # 1. Извлечение из cross_references
    for x in xrefs:
        src = (x.get("xref_src_db") or "").upper()
        if src in {"UNIPROT", "UNIPROT ACCESSION", "UNIPROT ACC", "UNIPROTKB"}:
            ident = x.get("xref_id", "")
            if ident:
                uniprot_id = ident
                break
    
    # 2. Mapping через UniProt ID Mapping Service
    mapping_uniprot_id = ""
    try:
        mapping_uniprot_id = _map_to_uniprot(chembl_id, mapping_cfg) or ""
    except Exception as exc:
        logger.warning("uniprot_mapping_error", chembl_id=str(chembl_id), error=str(exc))
    
    return uniprot_id, mapping_uniprot_id
```

### Валидация формата UniProt Accession

```python
import re

UNIPROT_PATTERN = re.compile(
    r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$'
)

def validate_uniprot_accession(accession: str) -> bool:
    """Validate UniProt accession format."""
    return bool(UNIPROT_PATTERN.match(accession))
```

**Форматы:**
- **Swiss-Prot**: `P01234` или `A0A123B456`
- **TrEMBL**: `Q12ABC3` или `O12345`

### Fallback logic

1. **Primary**: ChEMBL cross_reference
2. **Secondary**: UniProt ID Mapping Service (fallback при отсутствии)
3. **Error handling**: логирование ошибок mapping, не бросаем exception

---

## 2.8 Код примеры

### Полный пример batch retrieval

```python
from library.clients import ChemblClient
from library.config import ApiCfg, UniprotMappingCfg
from library.pipelines.target.chembl_target import iter_target_batches_with_retry

# Конфигурация
cfg = ApiCfg(
    chembl_base="https://www.ebi.ac.uk/chembl/api/data",
    timeout_read=60.0,
)

mapping_cfg = UniprotMappingCfg(
    base_url="https://www.uniprot.org/idmapping",
    timeout_sec=30.0,
)

# Список таргетов
target_ids = ["CHEMBL203", "CHEMBL204", "CHEMBL205"]

# Batch retrieval
client = ChemblClient()
frames = []

for payloads, raw_frame, parsed_frame in iter_target_batches_with_retry(
    target_ids,
    cfg=cfg,
    client=client,
    mapping_cfg=mapping_cfg,
    chunk_size=5,
    timeout=60.0,
    retry_cfg=retry_config,
):
    frames.append(parsed_frame)

# Объединение
result_df = pd.concat(frames, ignore_index=True)
```

### Edge cases

**Пустой список IDs:**
```python
if not ids:
    return pd.DataFrame(columns=TARGET_FIELDS)
```

**Неправильный формат ID:**
```python
valid = [i for i in ids if i not in {"", "#N/A"}]
```

**Timeout на одиночном ID:**
- Логирование warning
- Пропуск с fallback к "-" значениям

---

## Ссылки

- [ChEMBL Documentation](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services)
- [ChEMBL Data Web Services](https://www.ebi.ac.uk/chembl/api/data/docs)
- Код: `e:\github\ChEMBL_data_acquisition6\library\pipelines\target\chembl_target.py`

