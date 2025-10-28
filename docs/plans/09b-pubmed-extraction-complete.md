# План: Извлечение метаданных из PubMed E-utilities для обогащения документов ChEMBL

## 0. Цель и артефакты

### Цель

Детерминированно извлекать метаданные публикаций по PMID из PubMed E-utilities API и обогащать document-измерение ChEMBL с полным audit trail и контролем качества.

### Артефакты на выходе

**Landing (сырые данные):**

- `data/landing/pubmed_raw_{yyyymmdd}.xml` — сырые XML батчи для audit trail

**Bronze (нормализованные):**

- `data/bronze/pubmed_records.parquet` — нормализованные записи, одна строка на PMID

**Silver (денормализованные справочники):**

- `data/silver/pubmed_mesh.parquet` — MeSH дескрипторы и квалификаторы

- `data/silver/pubmed_chemicals.parquet` — химические вещества

- `data/silver/pubmed_authors.parquet` — авторы с аффилиациями и ORCID

**Метаданные:**

- `meta/pubmed/meta.yaml` — run_id, pipeline_version, row_count, hash_business_key, входы/выходы

**QC отчёты:**

- `qc/pubmed/quality_report.csv` — пропуски по ключевым полям, дубликаты DOI, расхождения с Crossref

- `qc/pubmed/errors.csv` — все аварии и ошибки извлечения

**Логи:**

- `logs/pubmed/*.jsonl` — структурированные логи в JSON Lines формате

## 1. Соответствие политике NCBI и режимы доступа

### Обязательные параметры запросов

- `tool=<our_app>` — идентификация приложения (обязательно)

- `email=<contact>` — контактный email (обязательно)

- `api_key=<key>` — для увеличения лимита до 10 rps (опционально)

**Без этих параметров прилетит блокировка от NCBI!**

### History Server для массивных выборок

Использование: `ESearch/EPost → WebEnv + query_key → EFetch`

Преимущества:

- Снижает число запросов

- Позволяет выборку "несколько сотен записей за один EFetch"

- Пагинация через `retstart`/`retmax`

### HTTP POST для больших списков

NCBI рекомендует POST при ~200+ UID в одном вызове вместо GET с длинными query strings.

### Формат ответов

**Реальность**: EFetch для PubMed возвращает **только XML** (PubMed DTD).

ESearch/ESummary могут возвращать JSON, но полноценные записи — только XML.

## 2. Поддерживаемые E-utilities и шаблоны запросов

### ESearch - поиск по условиям

```

https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?
  db=pubmed
  &term=<query>
  &retmax=<n>
  &retstart=<k>
  &usehistory=y
  &tool=<app>
  &email=<contact>
  &api_key=<key>

```

Поддерживаемые параметры: `datetype`, `reldate`, `sort`, `field`

### EPost - загрузка UID в историю

```

POST https://eutils.ncbi.nlm.nih.gov/entrez/eutils/epost.fcgi?db=pubmed
Body: id=PMID1,PMID2,...

```

Возвращает: `WebEnv` + `query_key`

### EFetch - извлечение полных записей

**По истории (рекомендуется для больших списков):**

```

https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?
  db=pubmed
  &query_key=<key>
  &WebEnv=<env>
  &retmode=xml
  &rettype=abstract
  &retstart=0
  &retmax=200

```

**По явному списку ID (POST для >200):**

```

POST https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?
  db=pubmed
  &retmode=xml
  &rettype=abstract
Body: id=PMID1,PMID2,...

```

### ELink - связанные ресурсы (опционально)

**PMCID маппинг:**

```

dbfrom=pubmed&linkname=pubmed_pmc&id=<PMIDs>

```

**Список референсов:**

```

dbfrom=pubmed&linkname=pubmed_pubmed_refs&id=<PMIDs>

```

Использование: определение наличия PMC full-text

## 3. Рейт-лимитинг, параллелизм и батчинг

### Rate limits

- **Без API ключа**: 3 запроса/секунду

- **С API ключом**: 10 запросов/секунду

### Стратегия

- Глобальный token bucket limiter

- `workers=1` (NCBI не приветствует агрессивный параллелизм)

- Exponential backoff с jitter при 429 ошибках

### Batch size

- **Рекомендация**: 200 UID в EFetch

- **Допустимо**: несколько сотен за один EFetch

- **Обязательно POST**: при >200 UID

## 4. Парсинг: DTD-осознанная выборка полей

### XML структура (PubMed DTD)

Корень: `PubmedArticleSet` → `PubmedArticle` → `MedlineCitation` + `PubmedData`

### Минимальная карточка (XPath)

**Основные идентификаторы:**

- `PubMed.PMID` → `//MedlineCitation/PMID/text()`

**Метаданные статьи:**

- `PubMed.ArticleTitle` → `//Article/ArticleTitle`

- `PubMed.Abstract` → конкатенация `//Abstract/AbstractText` с учётом `@Label` и `@NlmCategory`

- `PubMed.Language[]` → `//Article/Language`

**Журнал:**

- `PubMed.JournalTitle` → `//Journal/Title`

- `PubMed.JournalISOAbbrev` → `//MedlineJournalInfo/MedlineTA`

- `PubMed.ISSN` → `//Journal/ISSN[@IssnType='Print' | @IssnType='Electronic']`

**Библиография:**

- `PubMed.Volume` → `//JournalIssue/Volume`

- `PubMed.Issue` → `//JournalIssue/Issue`

- `PubMed.StartPage/EndPage` → парсинг `//Pagination/MedlinePgn` (формат "123-145")

**Типы и классификация:**

- `PubMed.PublicationType[]` → `//PublicationTypeList/PublicationType`

**Даты публикации:**

- `PubMed.Year/Month/Day` → `//Article/Journal/JournalIssue/PubDate/*`

- Альтернативно: `//PubmedData/History/PubMedPubDate[@PubStatus='pubmed'|'medline']/*`

### DOI - приоритеты извлечения

**Два источника в XML (согласно DTD):**

1. **Приоритетный**: `//PubmedData/ArticleIdList/ArticleId[@IdType='doi']`

2. **Fallback**: `//Article/ELocationID[@EIdType='doi']`

Фиксируем источник в поле `doi_source` для трейсабилити.

### Авторы с расширенными данными

`//AuthorList/Author` извлекаем:

- `LastName`, `ForeName`, `Initials`

- `AffiliationInfo/Affiliation`

- `Identifier[@Source='ORCID']` (если есть)

### MeSH терминология

`//MeshHeadingList/MeshHeading`:

- `DescriptorName[@UI, @MajorTopicYN]`

- `QualifierName[@UI]`

### Химические вещества

`//ChemicalList/Chemical`:

- `NameOfSubstance[@UI]`

- `RegistryNumber` (CAS номер)

### Все идентификаторы статьи

`PubMed.ArticleIds[]` → все `//ArticleId` с `@IdType` in {doi, pii, pmcid, pubmed...}

### PMCID через ELink

Если нужен PMCID: отдельный запрос `linkname=pubmed_pmc`

## 5. Нормализация и детерминизм

### DOI нормализация

```python
def normalize_doi(doi_value, source="ArticleIdList"):
    """Нормализация с фиксацией источника"""
    if not doi_value:
        return None, None

    doi = doi_value.strip().lower()

    # Удалить префиксы

    for prefix in ["doi:", "https://doi.org/", "http://dx.doi.org/"]:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]

    return doi.strip(), source

```

**Политика при множественных значениях:**

- Приоритет: `ArticleIdList` > `ELocationID`

- Сохранить `doi_source` для audit trail

### Даты нормализация

```python
def normalize_pubmed_date(year, month, day):
    """ISO YYYY-MM-DD с паддингом"""
    year = str(year).zfill(4) if year else "0000"
    month = str(month).zfill(2) if month else "00"
    day = str(day).zfill(2) if day else "00"
    return f"{year}-{month}-{day}"

```

### Авторский список

- Сортировка по порядку в XML (стабильная)

- Для детерминизма: хешируем `authors_json_normalized`

- Сохраняем как отдельный столбец `authors_hash`

### Текстовые поля

- Trim + normalize whitespace

- MedlinePgn: парсинг формата "123-145" → `start_page=123`, `end_page=145`

### Сортировка выгрузки

1. По `PMID` ASC

2. По `updated_at` DESC (для версионности)

## 6. Обработка ошибок и graceful degradation

### 429 Too Many Requests

- Exponential backoff с jitter

- Уважение rps-лимитов

- Логирование в structured logs

### 404 Not Found для PMID

- Создать "tombstone" запись: `{pmid, error="NotFound", fetched_at}`

- Продолжить обработку остальных

### Timeout / 5xx ошибки

- Retries с капом (max 5 попыток)

- Логирование каждой попытки

- Финальный fallback в error записи

### Malformed XML

- Сохранить сырой батч в `landing/`

- Эмитировать `parse_error` с `xpath_context`

- Записать в `qc/pubmed/errors.csv`

### Централизованный error tracking

Все аварии → `qc/pubmed/errors.csv` со структурой:

```csv
pmid,error_type,error_message,xpath_context,timestamp,batch_id

```

## 7. Конфликты и приоритеты источников

### Политика разрешения коллизий

При конфликтах между источниками применяется следующий приоритет:

**1. PubMed** (структура и MeSH для биомеда) → **2. Crossref** (чистые DOI/журнальные метаданные) → **3. OpenAlex** (классификация и типы публикаций)

**Для DOI:**

- Выбираем PubMed-версию, если валидируется по регулярке `^10\.\d+/[^\s]+$`

- Иначе используем Crossref

- Основание: DOI может быть в `ArticleIdList` или `ELocationID` (согласно DTD)

Политика документируется в `docs/data-fusion.md`

## 8. Конфигурация (YAML)

```yaml
sources:
  pubmed:
    enabled: true
    http:
      base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
      timeout_sec: 45
      retries:
        total: 5
        backoff_multiplier: 2.0
        backoff_max: 180
      identify:
        tool: "bioactivity_etl"
        email: "owner@example.org"
        api_key_env: "PUBMED_API_KEY"
    batching:
      ids_per_fetch: 200
      transport_for_large_ids: "POST"   # по рекомендации E-utilities

    rate_limit:
      max_calls_per_sec_no_key: 3
      max_calls_per_sec_with_key: 10
      workers: 1
    history:
      use_history: true   # ESearch/EPost → WebEnv/query_key

    caching:
      driver: "redis"
      ttl_sec: 86400

```

**Обоснование параметров:**

- Лимиты соответствуют рекомендациям NCBI

- POST для больших списков ID (NCBI best practice)

- History server для массивных выборок

## 9. Клиент и контракты

### Интерфейс PubMedClient

```python
class PubMedClient:
    def epost(self, ids: list[str]) -> WebEnvContext:
        """Загрузить UID в History server"""

    def esearch(self, term: str, **opts) -> WebEnvContext:
        """Поиск по запросу с историей"""

    def efetch_by_history(
        self,
        ctx: WebEnvContext,
        retstart: int,
        retmax: int
    ) -> XmlBatch:
        """Извлечь по WebEnv/query_key"""

    def efetch_by_ids(self, ids: list[str]) -> XmlBatch:
        """Извлечь по ID (авто-POST при >200)"""

    def elink_pmc(self, pmids: list[str]) -> dict[str, str]:
        """Маппинг PMID → PMCID"""

```

### Гарантии клиента

1. Каждый метод принимает `tool`, `email`, опционально `api_key`

2. Все вызовы обёрнуты в rate-limiter и retry-политику

3. На каждый сырой ответ сохраняется бинарный снапшот в `landing/`

### Интеграция с Unified-компонентами

- **PubMedUnifiedClientAdapter** оборачивает `UnifiedAPIClient`, наследуя токен-бакет и retry контракты (Respect Retry-After, fail-fast на 4xx) без дублирования логики; используем штатную реализацию лимитера и backoff из [docs/requirements/03-data-extraction.md#rate-limiting-и-retry-after](../requirements/03-data-extraction.md#rate-limiting-и-retry-after) для выполнения AC-07 и AC-19.

- **UnifiedLogger** фиксирует обязательные поля (`run_id`, `stage`, `endpoint`, `attempt`, `retry_after`, `duration_ms`) при каждом HTTP-вызове, соблюдая инвариант G12 из [docs/requirements/01-logging-system.md#обязательные-поля-логов-инвариант-g12](../requirements/01-logging-system.md#обязательные-поля-логов-инвариант-g12) и поддерживая проверку AC-05 из [docs/acceptance-criteria.md#ac5](../acceptance-criteria.md#ac5).

- **UnifiedOutputWriter** отвечает за детерминированную запись Bronze/Silver/QC артефактов в соответствии с инвариантами атомарности и канонизации ([docs/requirements/02-io-system.md#инварианты](../requirements/02-io-system.md#инварианты)) и акцептанс-критериям AC-01/AC-02/AC-05 ([docs/requirements/02-io-system.md#ac-01-golden-compare-детерминизма](../requirements/02-io-system.md#ac-01-golden-compare-%D0%B4%D0%B5%D1%82%D0%B5%D1%80%D0%BC%D0%B8%D0%BD%D0%B8%D0%B7%D0%BC%D0%B0), [docs/requirements/02-io-system.md#ac-02-запрет-частичных-артефактов](../requirements/02-io-system.md#ac-02-%D0%B7%D0%B0%D0%BF%D1%80%D0%B5%D1%82-%D1%87%D0%B0%D1%81%D1%82%D0%B8%D1%87%D0%BD%D1%8B%D1%85-%D0%B0%D1%80%D1%82%D0%B5%D1%84%D0%B0%D0%BA%D1%82%D0%BE%D0%B2), [docs/requirements/02-io-system.md#ac-05-na-policy-в-сериализации](../requirements/02-io-system.md#ac-05-na-policy-%D0%B2-%D1%81%D0%B5%D1%80%D0%B8%D0%B0%D0%BB%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D0%B8)).

## 10. Схемы данных

### PubMedRecordSchema (Bronze)

```python
@dataclass
class PubMedRecord:
    pmid: int
    doi: Optional[str]
    doi_source: str  # ArticleIdList | ELocationID

    article_title: str
    abstract: Optional[str]
    journal_title: str
    journal_iso: Optional[str]
    issn_print: Optional[str]
    issn_electronic: Optional[str]
    volume: Optional[str]
    issue: Optional[str]
    start_page: Optional[str]
    end_page: Optional[str]
    pub_date_iso: str  # YYYY-MM-DD

    publication_types: list[str]
    languages: list[str]
    article_ids: dict[str, str]
    has_pmc: bool
    created_at: datetime
    updated_at: datetime
    hash_business_key: str  # sha1(pmid)

    hash_row: str  # sha1(all_normalized_columns)

```

### PubMedAuthorSchema (Silver)

```python
@dataclass
class PubMedAuthor:
    pmid: int
    position: int
    last_name: str
    fore_name: Optional[str]
    initials: Optional[str]
    affiliation: list[str]
    orcid: Optional[str]

```

### PubMedMeshSchema (Silver)

```python
@dataclass
class PubMedMesh:
    pmid: int
    descriptor_ui: str
    descriptor_name: str
    major_topic_yn: bool
    qualifier_ui: Optional[str]
    qualifier_name: Optional[str]

```

### PubMedChemicalSchema (Silver)

```python
@dataclass
class PubMedChemical:
    pmid: int
    substance_ui: str
    substance_name: str
    registry_number: Optional[str]  # CAS number

```

**Основание структур:** PubMed DTD specification

## 11. Тест-план

### Unit тесты

1. **DOI парсинг из двух источников:**
   - Тест с DOI в `ArticleIdList`
   - Тест с DOI в `ELocationID`
   - Тест с DOI в обоих местах (проверка приоритета)

2. **Abstract с атрибутами:**
   - Конкатенация `AbstractText` с `@Label`
   - Обработка структурированных абстрактов

3. **MeSH и Chemicals:**
   - Парсинг `MeshHeading` с `Qualifiers`
   - Извлечение `ChemicalList`

### Integration тесты

1. **EPost → EFetch цикл:**
   - На известном множестве PMID
   - Сверка хешей XML батчей

2. **Rate limiting:**
   - Throttling корректность
   - Exponential backoff при искусственно ускоренном лимите (эмуляция 429)

3. **Golden files:**
   - Стабильные Parquet + meta.yaml с контрольными суммами
   - Не меняем порядок колонок и сортировку (детерминизм)

## 16. Чек-лист тестирования

- `pytest` — прогнать модульные и интеграционные тесты (валидирует юнит-контракты клиентов и парсеров, покрывая AC-07/AC-19).

- `mypy src` — статический анализ типов для подтверждения контрактов Unified-компонентов.

- `python -m pipeline.pubmed run --config configs/config_pubmed.yaml --mode epost-efetch --golden data/golden/pubmed_records.parquet` — golden-run для проверки AC-01/AC-02/AC-05.

## 12. Документация

### Структура документов

**`docs/sources/pubmed-extraction.md`:**

- Спецификация с примерами URL

- Выдержки из правил NCBI об api_key, tool, email

- Допустимые объёмы запросов

**`docs/api/pubmed-client.md`:**

- Контракты клиента

- Исключения и их обработка

**`docs/examples/pubmed-usage.md`:**

- Сценарий a) по списку PMID

- Сценарий b) по поисковому запросу

- Сценарий c) обогащение PMCID через ELink

## 13. CLI интерфейс

### Команда запуска

```bash
python -m scripts/get_pubmed.py \
  --input data/input/pmids.csv \
  --config configs/config_pubmed.yaml \
  --mode epost-efetch \
  --limit 0 \
  --save-raw landing/pubmed_$(date +%Y%m%d).xml \
  --out parquet:data/bronze/pubmed_records.parquet

```

### Опции

- `--mode {ids, search, epost-efetch}` — режим извлечения

- `--rps {3, 10}` — requests per second

- `--api-key-env PUBMED_API_KEY` — переменная окружения с ключом

- `--use-history` — использовать History server

- `--retmax 200` — размер страницы

- `--retstart 0` — offset для пагинации

- `--elink-pmc` — обогатить PMCID через ELink

## 14. QC и соответствие лицензиям

### Quality checks

Проверяем:

- Пустые DOI при наличии `ArticleIdList`/`ELocationID`

- Несогласованные ISSN (Print vs Electronic)

- Дубликаты PMID

- Расхождения с Crossref по DOI

### Лицензирование

⚠️ **Важно:** PubMed abstracts могут подпадать под авторское право.

В `README.md` обязательно указываем:

- Ссылку на дисклеймер NCBI

- Условия использования E-utilities

- Ограничения на коммерческое использование

**Не строим из себя юристов — ссылаемся на официальные источники!**

## 15. Что именно улучшено

По сравнению с базовой реализацией добавлено:

1. ✅ Обязательные `tool`/`email` и честная политика rps + api_key (иначе бан)

2. ✅ Работа через History server (EPost/ESearch → WebEnv/query_key)

3. ✅ POST для больших списков ID (NCBI рекомендация)

4. ✅ DTD-осознанный парсинг с приоритетами извлечения DOI из ArticleIdList → ELocationID

5. ✅ Полная нормализация, детерминизм, артефакты, meta.yaml, QC

6. ✅ ELink для PMCID/refs (PMC full-text availability)

7. ✅ Graceful degradation и централизованный error tracking

8. ✅ Golden files для regression testing

