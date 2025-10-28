# 8. Извлечение данных Target из ChEMBL

## Executive Summary

### Обзор

Документ описывает спецификацию извлечения данных таргетов (targets) из ChEMBL API с последующим обогащением через UniProt и IUPHAR/GtoPdb. Данный pipeline обеспечивает полное покрытие метаданных о молекулярных таргетах для биоактивности, включая:

- **ChEMBL как базовый источник**: каркас таргета, компоненты, protein classifications, cross-references
- **UniProt как обогащение белковых данных**: имена белков, таксономия, изоформы, PTM, features, субклеточная локализация
- **IUPHAR/GtoPdb как фармакологическая классификация**: тип, класс, подкласс, иерархия семейств, natural ligands

### Что извлекаем

Pipeline производит нормализованную таблицу `targets` и денормализованные компоненты:

- **targets.parquet**: основная таблица таргетов с ключевыми полями
- **target_components.parquet**: денормализация компонентов таргета (для комплексов, мультимеров)
- **protein_class.parquet**: иерархия protein classifications
- **xref.parquet**: cross-references на внешние базы данных

### Ключевые метрики успеха

| Метрика | Target | Критичность |
|---------|--------|-------------|
| **ChEMBL coverage** | 100% идентификаторов | HIGH |
| **UniProt enrichment rate** | ≥80% для protein targets | HIGH |
| **IUPHAR coverage** | ≥60% для receptors | MEDIUM |
| **Accession resolution** | ≥90% согласованность | HIGH |
| **Component completeness** | ≥85% последовательностей | MEDIUM |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

### Границы ответственности источников

| Источник | Ответственность | Приоритет |
|----------|----------------|-----------|
| **ChEMBL** | target_chembl_id, pref_name, target_type, organism, taxonomy, components, protein_classifications, reaction_ec_numbers, HGNC | PRIMARY |
| **UniProt** | protein names, gene symbols, taxonomy (lineage), isoforms, PTM, features, subcellular location, cross-references, sequence length | ENRICHMENT |
| **IUPHAR/GtoPdb** | pharmacological classification (type/class/subclass), full ID/name paths, natural ligands, interactions | CLASSIFICATION |

---

## 1.2 Архитектура Pipeline

### Диаграмма потока данных

```text
Target ETL Pipeline
├── Stage 1: ChEMBL Extraction (Primary)
│   ├── /target → target_chembl_id, pref_name, target_type, organism
│   ├── /target_component → component_id, accession, sequence, GO-terms
│   ├── /protein_classification → hierarchy (l1, l2, l3, l4...)
│   ├── /target_relation → inter-target relationships
│   └── Materialization checkpoint: data/bronze/targets.parquet
├── Stage 2: UniProt Enrichment (Optional)
│   ├── ID Mapping: accession → UniProtKB Accession
│   ├── Stream: protein names, gene, taxonomy, isoforms, PTM, features
│   └── Materialization checkpoint: data/silver/targets_uniprot.parquet
├── Stage 3: IUPHAR Classification (Optional)
│   ├── /targets → target_id, type, class, subclass
│   ├── /targets/families → full ID/name paths
│   └── Materialization checkpoint: data/gold/targets_final.parquet
└── Stage 4: Post-processing
    ├── fetch_orthologs: приоритизация Homo sapiens → Mus musculus → Rattus norvegicus
    ├── Isoform expansion: canonical vs isoform accessions, ALTERNATIVE_PRODUCTS
    ├── Normalization: identifiers, names, EC numbers, secondary accessions
    ├── Fallback mapping: UniProt → ChEMBL accession, fuzzy gene symbol match
    ├── Validation: Pandera schemas, QC reports
    └── Export: targets, target_components, protein_class, xref
```

### Stage 4: Post-processing

Финальная стадия агрегирует результаты всех источников, применяет биологические эвристики и нормализует идентификаторы перед материализацией `gold`-слоя.

#### fetch_orthologs

- Функция `fetch_orthologs` вызывается после успешного разрешения UniProt accession на стадии 2.
- Вход: canonical accession и полный taxonomic lineage компонента.
- Выход: DataFrame с колонками `accession`, `ortholog_accession`, `organism`, `source_priority`, `evidence`.
- Источник данных: UniProt `/uniprotkb/search` c фильтрами `relationship_type:ortholog` и `reviewed:true`.
- Приоритет организмов фиксирован: **Homo sapiens > Mus musculus > Rattus norvegicus**. Остальные orthologs сохраняются, но помечаются низким приоритетом (`source_priority = "fallback"`).
- Используется в `finalise_targets` для заполнения пропущенных компонентов, когда ChEMBL содержит non-human accessions или отсутствуют UniProt ID для мыши/крысы.

#### ID Mapping сценарии

1. **Direct accession**: `target_component.accession` совпадает с UniProt canonical → прямое соединение, приоритет `source_priority = 0`.
2. **Secondary accession**: ChEMBL хранит устаревший accession → использует UniProt `secondaryAccessions` для маппинга, помечается как `source_priority = 1`.
3. **ID Mapping service**: если accession не найден, `idmapping/run` (UniProt → UniProtKB) выполняет перекодировку, возвращая canonical/isoform пары (`mapped_to`).
4. **Ortholog bridge**: если отсутствуют прямые accession, `fetch_orthologs` возвращает ближайший ortholog, который подставляется в `target_components` с флагом `is_ortholog = true`.
5. **Gene symbol merge**: финальный fallback через нормализованный `gene_symbol` (upper-case, `HGNC` aliases).

#### Стратегии объединения результатов

- Все стадии объединяются в `postprocess_targets` через последовательные `merge_asof`/`merge` операции по `target_chembl_id` и `accession`.
- Для конфликтующих полей используется `coalesce_by_priority(columns=["chembl", "uniprot", "iuphar"])`.
- Ortholog и isoform данные агрегируются в структуру `component_enrichment`, затем разворачиваются (`explode`) при построении `target_components`.
- Каждому компоненту назначается `data_origin` (`chembl`, `uniprot`, `ortholog`, `fallback`) и `merge_rank`, что обеспечивает детерминированную сортировку.

#### Извлечение изоформ

- UniProt entries содержат блок `ALTERNATIVE_PRODUCTS`. В нём canonical isoform (`isoformType="canonical"`) и альтернативные изоформы (`isoformType="displayed"/"alternative"`).
- Canonical accession = `entry.primaryAccession`. Isoform accession образуется конкатенацией canonical + суффикс (`-1`, `-2`, ...), например `P00533-2`.
- Если ChEMBL предоставляет только canonical accession, UniProt enrichment расширяет его до списка изоформ: `canonical_accession`, `isoform_accessions`, `secondary_accessions`.
- Вторичные accession (устаревшие) переносятся из `secondaryAccessions` и влияют на ID Mapping сценарий №2.
- `target_components` получает дополнительные строки на изоформы с колонками: `accession`, `is_canonical`, `isoform_variant`, `sequence_length_isoform` (если `sequence` предоставлена в UniProt).
- Canonical компоненты помечаются `is_canonical = true`, изоформы — `false`. При отсутствии isoform sequence поле остаётся NULL, но сохраняется ссылка на `isoform_id`.
- Данные изоформ влияют на downstream-агрегации: `targets.isoform_count`, `targets.has_alternative_products`, `targets.secondary_accessions`.

#### Управление отсутствующими маппингами

Все fallback-стратегии сосредоточены на стадии пост-обработки:

1. **UniProt accession отсутствует в ChEMBL** → запрос `idmapping/run` (ChEMBL accession → UniProtKB) и повторное соединение.
2. **ChEMBL accession отсутствует в UniProt** → поиск в `secondaryAccessions`; при успехе accession заменяется на canonical.
3. **Не найден canonical accession** → `fetch_orthologs` возвращает ortholog для приоритетных организмов, компонент помечается как `is_ortholog`.
4. **gene_symbol отсутствует** → извлекается из UniProt `gene.primary`, включая `synonyms`; используется fuzzy match (`rapidfuzz.fuzz.token_sort_ratio ≥ 90`).
5. **classification отсутствует в IUPHAR** → воспроизводится ChEMBL `protein_classification` с флагом `classification_source = "chembl"`.
6. **organism lineage отсутствует** → берётся из UniProt taxonomy (`lineage[].scientificName`).

Каждый fallback логируется с уровнем `WARNING`, добавляется в QC-отчёт (`qc_missing_mappings.csv`) и фиксируется в `meta.yaml` (`fallback_counts`).
### Stage 2: UniProt Enrichment — детализация

Stage 2 отвечает за нормализацию и обогащение белковых компонентов через UniProt REST API. Этот этап запускается для всех компонентов с валидным `accession` из ChEMBL и материализует данные в `data/silver/targets_uniprot.parquet`.

#### Режимы UniProt REST API

- **`/uniprotkb/{accession}` (`entry` mode)** — точечные запросы по одному accession. Используется для повторной проверки спорных записей и добора редких полей.
- **`/uniprotkb/search` (`search` mode)** — массовый выбор по фильтрам (`accession`, `gene`, `organism_id`) с поддержкой пагинации. Применяется для выборок небольших объёмов (<500 записей) и для пере-запроса после обновлений.
- **`/uniprotkb/stream` (`stream` mode)** — основной массовый канал, возвращающий NDJSON. Используется после ID Mapping для выгрузки всех атрибутов. Обязательно передавать параметр `fields` для ограничения набора колонок.

Рекомендуемое значение параметра `fields`:

```
fields=accession,gene_names,organism_name,organism_id,lineage,sequence_length,features,cc_ptm,protein_name,protein_existence
```

Обязательные для сохранения поля Stage 2:

1. `accession`
2. `gene_names`
3. `taxonomy` (минимум `organism_id`, `organism_name`, `lineage`)
4. `features` (включая тип, позицию и описание)
5. `cc_ptm`

#### Rate limiting и стратегия backoff

- Не превышать **3 запроса в секунду** (≈180 запросов в минуту) по каждому REST-эндпоинту.
- Обрабатывать HTTP 429/5xx через экспоненциальный backoff: базовая задержка 2 секунды, множитель 2.0, максимум 5 повторов, джиттер `uniform(0, 1)`.
- Для `stream` использовать один долгоживущий запрос; при сетевых сбоях повторять с возобновлением по последнему успешно обработанному accession.

#### Workflow UniProt ID Mapping

1. **`/idmapping/run`** — отправка списка исходных идентификаторов (`from=UniProtKB_AC`, `to=UniProtKB`). Батчим списки до 100 000 accession за вызов, разбивая входные данные из Stage 1.
2. **`/idmapping/status/{jobId}`** — polling статуса не чаще одного раза в 5 секунд. При `jobStatus=FINISHED` переходить к стриму; при `FAILED` логировать и повторять отправку джобов с уменьшенным размером пакета (50%).
3. **`/idmapping/stream/{jobId}`** — стрим результатов ID Mapping в NDJSON. Поля, возвращённые сервисом, передаются в Stage 2 `stream` запрос для дальнейшего обогащения.

#### Валидация и обновление accession

1. **Предвалидация**: проверка формата accession с помощью регэкспа UniProt перед отправкой в ID Mapping; дубликаты фильтруются.
2. **Синхронизация**: результаты ID Mapping мержатся с входными компонентами. Для записей с изменённым accession (primary vs secondary) фиксируем соответствие `old_accession → new_accession` в журнале.
3. **Пере-запрос `entry`**: если запись помечена как `isObsolete`, выполняем запрос `/uniprotkb/{new_accession}` для валидации и подтягивания обязательных полей.
4. **Контроль полноты**: Pandera-схема Stage 2 проверяет наличие обязательных полей (`accession`, `gene_names`, `taxonomy`, `features`, `cc_ptm`) и непротиворечивость таксономии (`organism_id` совпадает с `taxonomy.tax_id`).
5. **Обновления**: при обнаружении новых primary accession (из-за слияния записей) повторно инициируем workflow ID Mapping и стрим обновлённых данных, чтобы silver-слой оставался консистентным.

### Stage 1: ChEMBL REST ресурсы и контракты

#### Ключевые эндпоинты

| Ресурс | Назначение | Обязательные поля (тип, nullable) |
|--------|------------|------------------------------------|
| `GET /target` | Базовая карточка таргета с возможностью включать `protein_classifications`, `cross_references`. | `target_chembl_id` (string, NOT NULL); `pref_name` (string, nullable); `target_type` (string, nullable); `organism` (string, nullable); `tax_id` (int64, nullable); `species_group_flag` (boolean, nullable) |
| `GET /target_component` | Денормализованные компоненты и последовательности для комплексных таргетов. | `component_id` (int64, nullable); `component_type` (string, nullable); `accession` (string, nullable); `sequence` (string, nullable); `component_description` (string, nullable) |
| `GET /target_relation` | Связи между таргетами (гомология, эквивалентность). | `target_relation_id` (string, nullable); `relationship_type` (string, nullable); `related_target_chembl_id` (string, nullable) |
| `GET /protein_classification` | Иерархия protein families (уровни L1-L4). | `protein_class_id` (int64, nullable); `class_level` (int64, nullable); `pref_name` (string, nullable); `short_name` (string, nullable) |

> **Примечание:** `target_chembl_id` является единственным полем с ограничением NOT NULL. Остальные поля допускают пропуски и должны обрабатываться в стадии трансформации.

#### Правила пагинации и фильтрации Stage 1

- **Пагинация**: запросы используют `limit` и `offset` (по умолчанию `limit=20`, `offset=0`). Ответ содержит блок `page_meta.limit`, `page_meta.offset`, `page_meta.total_count`. Итерация продолжается, пока `offset >= total_count`.
- **Фильтры**: поддерживаются суффиксы `__exact`, `__contains`, `__icontains`, `__in`, `__gt`, `__lt` для любых полей ресурса. Фильтры можно комбинировать (`&`) для сложных критериев.
- **Длинные запросы**: при превышении лимита URL (>2000 символов) выполняется `POST` на соответствующий ресурс с заголовком `X-HTTP-Method-Override: GET`. Тело запроса передает параметры (например, `{"target_chembl_id__in": "CHEMBL203,CHEMBL204"}`) в формате JSON.

#### TARGET_FIELDS для материализации Stage 1

```python
TARGET_FIELDS = [
    "pref_name",                 # string, nullable
    "target_chembl_id",          # string, NOT NULL (PRIMARY KEY)
    "component_description",     # string, nullable
    "component_id",              # int64, nullable
    "relationship",              # string, nullable (derived from target_type)
    "gene",                      # string, nullable (pipe-delimited)
    "uniprot_id",                # string, nullable
    "mapping_uniprot_id",        # string, nullable
    "chembl_alternative_name",   # string, nullable
    "ec_code",                   # string, nullable (pipe-delimited)
    "hgnc_name",                 # string, nullable
    "hgnc_id",                   # int64, nullable
    "target_type",               # string, nullable
    "tax_id",                    # int64, nullable
    "species_group_flag",        # boolean, nullable
    "target_components",         # json string, nullable
    "protein_classifications",   # json string, nullable
    "cross_references",          # json string, nullable
    "reaction_ec_numbers",       # json/string, nullable
]
```

> **Инварианты Stage 1:** `target_chembl_id` должен быть уникальным и заполненным, остальные поля допускают `NULL`, но сохраняются в детерминированном формате (строки — UTF-8, JSON — сериализованный словарь с сортировкой ключей).

### Контрольные точки материализации

Pipeline сохраняет промежуточные результаты на каждой стадии для:

1. **Аудит**: проверка сырых данных API (landing/ directory)
2. **Перезапуск**: возможность resume с любой стадии
3. **Отладка**: изоляция проблем до конкретного источника
4. **Regression testing**: snapshot файлы для сравнения

**Материализация происходит в:**

- `data/bronze/targets.parquet`: сырые ChEMBL данные
- `data/silver/targets_uniprot.parquet`: после UniProt enrichment
- `data/gold/targets_final.parquet`: финальный merge + postprocessing
- `data/landing/chembl/target_{id}.json`: сырые API ответы

### Orchestration flow

Pipeline orchestration реализован через `run_pipeline` в `pipeline.py`:

```python
PipelineResult = dataclass(
    chembl: FrameLike,      # Обязательно: ChEMBL base data
    uniprot: FrameLike | None,    # Опционально: enrichment
    isoforms: FrameLike | None,   # Опционально: isoform data
    orthologs: FrameLike | None,  # Опционально: ortholog mappings
    iuphar: FrameLike | None,     # Опционально: classification
)
```

**Зависимости стадий:**

- UniProt требует материализованные ChEMBL данные
- Isoforms требует UniProt enrichment
- Orthologs требует UniProt accession resolution
- IUPHAR требует accession или gene_symbol

### Stage 3: IUPHAR Classification (Optional)

**Цель:** дополнить таргеты фармакологической классификацией и согласованными идентификаторами из GtoPdb/IUPHAR.

**Ключевые REST ресурсы:**

- `GET /targets`: базовый список таргетов c полями `targetId`, `type`, `familyId`, `annotationStatus`. Поддерживаемые фильтры включают `targetType` (например, `"GPCR"`, `"Ion channel"`), `familyId` для выборки по конкретному семейству и `annotationStatus` для исключения черновиков.
- `GET /targets/families`: дерево семейств с полями `familyId`, `parentFamilyId`, `level`, `name`, `type`. Фильтры `type` и `parentFamilyId` используются для построения поддеревьев.
- `GET /targets/{targetId}/synonyms`: дополнительные названия и алиасы, используемые для нормализации `pref_name` и `pref_symbol`.
- `GET /targets/{targetId}/geneProteinInformation`: HGNC/UniProt атрибуты (`hgncId`, `geneSymbol`, `uniprotIds`, `species`).

**Обработка и фильтрация:**

- Ограничиваем выдачу активными таргетами (`annotationStatus=CURATED`).
- Для белковых таргетов используем `targetType in {"GPCR", "Enzyme", "Ion channel", "Transporter"}` для снижения шума.
- Ветка `targets/families` собирается итеративно: сначала `type`, затем `parentFamilyId` по уровням до листьев.

**Структура данных:**

```python
@dataclass
class IUPHARData:
    target_df: pd.DataFrame  # записи /targets + geneProteinInformation + synonyms
    family_df: pd.DataFrame  # дерево /targets/families со всеми уровнями
```

`family_df` нормализует иерархию классификации: `type → class → subclass → chain → target`. Для каждой ветви рассчитываем два пути:

- `full_id_path`: `'typeId/classId/subclassId/chainId/targetId'`
- `full_name_path`: `'Type name > Class name > Subclass name > Chain name > Target name'`

**Согласование идентификаторов:**

- `hgncId` из `geneProteinInformation` маппится на `hgnc_id` ChEMBL: допускаются различия в формате (`HGNC:1234` → `1234`). При конфликте приоритет у значения из ChEMBL, а IUPHAR сохраняется как альтернатива в `xref`.
- `uniprotIds` приводятся к верхнему регистру и сравниваются с `accession` из UniProt стадии. Несоответствия логируются и попадают в QC-отчет, а консенсусное значение выбирается по правилу: если `uniprot_id_primary` из UniProt присутствует в списке IUPHAR, принимаем его, иначе добавляем IUPHAR ID в `uniprot_ids_all` без замены основного.
- `geneSymbol` сверяется с `gene_symbol` из UniProt; расхождения фиксируются, но не переписывают основное значение без ручной валидации.

Результат Stage 3 — DataFrame `iuphar_classification`, содержащий агрегированные поля `iuphar_target_id`, `iuphar_family_id`, `iuphar_type`, `full_id_path`, `full_name_path`, которые присоединяются к `targets` на этапе Stage 4.

### Метаданные pipeline

Каждый run фиксирует:
- `pipeline_version`: git SHA
- `chembl_release`: версия ChEMBL database
- `uniprot_release`: версия UniProt KB
- `extracted_at`: UTC timestamp
- `source_system`: ChEMBL/UniProt/IUPHAR
- `checksums`: SHA256 для воспроизводимости

---

## 1.3 Сравнительный анализ

### Таблица расхождений: bioactivity_data_acquisition5 vs ChEMBL_data_acquisition6

| Компонент | bioactivity_data_acquisition5 | ChEMBL_data_acquisition6 | Gap Analysis |
|-----------|-------------------------------|--------------------------|--------------|
| **Источники данных** | ChEMBL (базовая схема) | ChEMBL + UniProt + IUPHAR/GtoPdb | ❌ Отсутствует multi-source enrichment |
| **Схемы (полнота)** | `target_schema.py`, `target_schema_normalized.py` (~252 поля) | `target_schema.py`, `targets.py` (~80 полей, более чистая нормализация) | ⚠️ Схемы различаются структурой |
| **Ресурсы ChEMBL** | `/target` | `/target`, `/target_component`, `/protein_classification`, `/target_relation` | ❌ Отсутствуют components и classifications |
| **Batch retrieval** | Не реализовано | `iter_target_batches` с chunk_size=5, adaptive splitting | ❌ Отсутствует batch processing |
| **Retry/Trottling** | Базовая retry логика | Adaptive chunking, shrink factor, single retry для timeouts | ❌ Отсутствует advanced error recovery |
| **UniProt integration** | Не реализовано | ID Mapping (100k limit), stream API, full enrichment pipeline | ❌ Полностью отсутствует UniProt |
| **IUPHAR integration** | Не реализовано | GtoPdb API integration, hierarchical classification | ❌ Полностью отсутствует IUPHAR |
| **Кэширование** | Не документировано | TTL cache, release-scoped invalidation | ⚠️ Кэширование не задокументировано |
| **Post-processing** | Базовая нормализация | Multi-stage: postprocess_targets → finalise_targets → cellularity classification | ❌ Отсутствует sophisticated postprocessing |
| **Детерминизм** | Частичный | Полный: стабильная сортировка, хэши, meta.yaml | ⚠️ Требуется усиление детерминизма |
| **QC и метаданные** | meta.yaml, quality_report | meta.yaml, quality_report, correlation_report, validation errors | ⚠️ QC менее детализирован |

### Рекомендации по портированию

**Приоритет 1 (Critical):**
1. Добавить UniProt enrichment pipeline для protein targets
2. Реализовать batch retrieval с adaptive chunking для ChEMBL
3. Добавить IUPHAR/GtoPdb classification для receptors

**Приоритет 2 (High):**
4. Реализовать multi-source merge с priority handling
5. Добавить materialization checkpoints для resume capability
6. Усилить детерминизм через стабильную сортировку и хэши

**Приоритет 3 (Medium):**
7. Улучшить post-processing (genus derivation, cellularity classification)
8. Добавить correlation reports для validation
9. Реализовать landing zone для сырых API ответов

---

## 1.4 Интерфейсные контракты

### Входные данные

**Обязательные поля:**
- `target_chembl_id` (String, NOT NULL, regex: `^CHEMBL\d+$`)

**Опциональные фильтры:**
- `organism` (String, nullable): фильтрация по organism
- `target_type` (String, nullable): фильтрация по типу (SINGLE PROTEIN, PROTEIN COMPLEX, etc.)

**Формат:** CSV или DataFrame с минимальным набором колонок

**Пример входного файла:**
```csv
target_chembl_id,organism,target_type
CHEMBL203,Homo sapiens,SINGLE PROTEIN
CHEMBL204,Mus musculus,PROTEIN COMPLEX
```

### Выходные данные

Pipeline производит 4 таблицы:

#### 1. targets.parquet (нормализованная)

Основная таблица таргетов с ключевыми полями из всех источников.

**Ключевые поля:**
- `target_chembl_id`: PRIMARY KEY
- `pref_name`: предпочтительное название
- `organism`, `tax_id`, `genus`: таксономия
- `uniprot_id_primary`, `uniprot_ids_all`: UniProt identifiers
- `gene_symbol`, `hgnc_id`: gene information
- `protein_class_pred_L1/L2/L3`: predicted classification
- `cellularity`: derived organism type (eukaryote/prokaryote/virus)

**Размер:** ~5-10% от входных данных (дедупликация по target_chembl_id)

#### 2. target_components.parquet (денормализация)

Развернутые компоненты таргетов для комплексов и мультимеров.

**Ключевые поля:**
- `target_chembl_id`: FK к targets
- `component_id`: уникальный ID компонента
- `component_type`: тип компонента
- `accession`: UniProt accession
- `sequence`: amino acid sequence (если доступна)

**Размер:** ~1-5x от targets (зависит от количества компонентов)

#### 3. protein_class.parquet (иерархия)

Protein classification hierarchy для интеграции с внешними классификаторами.

**Ключевые поля:**
- `target_chembl_id`: FK к targets
- `class_level`: L1, L2, L3, L4...
- `class_name`: название уровня классификации
- `full_path`: полный путь иерархии

**Размер:** ~2-3x от targets

#### 4. xref.parquet (cross-references)

Внешние ссылки на другие базы данных.

**Ключевые поля:**
- `target_chembl_id`: FK к targets
- `xref_src_db`: источник (UniProt, Ensembl, PDB, AlphaFold, etc.)
- `xref_id`: identifier в внешней БД

**Размер:** ~5-10x от targets

### Форматы и типы данных

- **Целые числа**: `tax_id`, `hgnc_id` → `Int64` (nullable)
- **Строки**: большинство полей → `String` (nullable)
- **Булевы**: `species_group_flag`, PTM flags → `Boolean` (nullable)
- **Даты**: `extracted_at` → `Timestamp[ns]`
- **JSON**: `target_components`, `protein_classifications` → `String` (JSON serialized)

---

## 1.5 Конфигурация

- Базовые требования см. `docs/requirements/10-configuration.md`.
- Профильный файл: `configs/pipelines/target.yaml` (`extends: "../base.yaml"`).

| Секция | Ключ | Значение | Ограничение | Комментарий |
|--------|------|----------|-------------|-------------|
| HTTP | `http.global.timeout_sec` | `60.0` | `> 0` | Используется по умолчанию всеми клиентами. |
| Источники / ChEMBL | `sources.chembl.batch_size` | `5` | `≤ 5` | Стабильность batch-запросов к `/target.json`. |
| Источники / UniProt | `sources.uniprot.id_mapping_max_ids` | `100000` | `≤ 100000` | Лимит официального API. |
| Источники / IUPHAR | `sources.iuphar.retries` | `3` | `≤ 5` | Встроенный ограничитель API. |
| Cache | `cache.ttl.chembl` | `86400` | `≥ 0` | Кэш на сутки для ChEMBL. |
| Cache | `cache.ttl.uniprot` | `604800` | `≥ 0` | Кэш на неделю для UniProt. |
| Cache | `cache.ttl.iuphar` | `2592000` | `≥ 0` | Кэш на 30 дней для IUPHAR. |
| Determinism | `determinism.sort.by` | `['target_chembl_id', 'accession', 'component_id']` | — | Гарантия детерминированного порядка. |
| Determinism | `determinism.hash_columns` | `['target_chembl_id', 'pref_name', 'organism', 'uniprot_id_primary']` | Не пусто | Используется для хешей целостности. |
| Output | `output.format` | `parquet` | `{'parquet', 'csv'}` | Основной формат выгрузки. |

**Переопределения:**
- CLI: `--set sources.uniprot.batch_size=100` для стресс-тестов (не для продакшн);
- ENV: `BIOETL_SOURCES__UNIPROT__API_KEY` (если предоставлен приватный канал), `BIOETL_OUTPUT__DIRECTORY=/mnt/out/target`.
- `determinism.hash_columns` не допускает пустых значений — нарушение ведёт к ошибке загрузки конфигурации.

### HTTP настройки

- **Таймауты**: 60 сек для ChEMBL, 300 сек для UniProt stream
- **Ретраи**: экспоненциальный backoff с jitter
- **Rate limiting**: максимум 5 запросов в 15 секунд

### Источники

Включение/исключение источников:
- ChEMBL: всегда включен (primary source)
- UniProt: опционально для protein enrichment
- IUPHAR: опционально для pharmacological classification

### Кэширование

- **TTL**: ChEMBL 24ч, UniProt 7 дней, IUPHAR 30 дней
- **Release-scoped invalidation**: автоматическая инвалидация при смене release

---

## 1.6 Политика слияния

### Приоритеты атрибутов

При конфликтах данных между источниками применяется правило приоритета:

**ChEMBL > UniProt > IUPHAR**

**Примеры конфликтов:**

| Поле | ChEMBL | UniProt | IUPHAR | Выбранное значение |
|------|--------|---------|--------|--------------------|
| `gene_symbol` | "ADORA1" | "ADORA1" | "A1" | ChEMBL: "ADORA1" |
| `organism` | "Homo sapiens" | "Homo sapiens" | "Human" | ChEMBL: "Homo sapiens" |
| `type` | "SINGLE PROTEIN" | — | "GPCR" | ChEMBL: "SINGLE PROTEIN" |
| `classification` | — | — | "Class A Adenosine" | IUPHAR: "Class A Adenosine" |

### Ключи соединения

1. **Primary key**: `target_chembl_id` (всегда ChEMBL)
2. **UniProt join**: `accession` из ChEMBL `/target_component` → UniProt entry
3. **IUPHAR join**: `uniprot_id_primary` или `gene_symbol` (HGNC)

### Нормализация идентификаторов

- **UniProt Accession**: формат `[OPQ][0-9][A-Z0-9]{3}[0-9]` или `[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}`
- **HGNC ID**: формат `HGNC:\d+`
- **EC numbers**: формат `\d+(?:\.(?:\d+|-)){3}` (strict validation)

## 1.7 Детерминизм и воспроизводимость

### Стабильная сортировка

Все выходные таблицы сортируются по фиксированному порядку:

```python
sort_by = ["target_chembl_id", "accession", "component_id"]
ascending = [True, True, True]
```

Гарантирует бит-в-бит одинаковый вывод для одинакового входа.

### Хэширование

**hash_business_key:**
```python
hash_business_key = sha256(target_chembl_id).hexdigest()
```

**hash_row:**
```python
# Сортированные нормализованные колонки
normalized_values = [target_chembl_id, pref_name, organism, uniprot_id_primary, ...]
hash_row = sha256("|".join(normalized_values)).hexdigest()
```

Используются для:
- Regression testing
- Detecting data drift
- Incremental processing

### meta.yaml структура

Каждый выходной файл сопровождается `meta.yaml`:

```yaml
pipeline_version: "abc123def"  # Git SHA
chembl_release: "33"
uniprot_release: "2024_01"
extracted_at: "2024-01-15T10:30:00Z"
source_system: "chembl,uniprot,iuphar"
row_count: 12543
checksums:
  sha256_file: "a1b2c3..."
  sha256_hash_row_aggregate: "d4e5f6..."
  sha256_hash_business_key_aggregate: "g7h8i9..."
determinism:
  sorted_by: ["target_chembl_id", "accession", "component_id"]
  encoding: "utf-8"
  format: "parquet"
```

### Snapshot файлы

Для regression testing создаются snapshot файлы:
- `data/snapshots/targets_{date}_{version}.parquet`
- Сравнение через `hash_row_aggregate`

---

## 1.8 Метаданные и QC

### meta.yaml обязательные поля

- `pipeline_version`, `source_system`, `extracted_at`
- `row_count`, `checksums`
- `chembl_release`, `uniprot_release`

### quality_report.csv

Ключевые метрики по полям:

| Поле | Completeness | Uniqueness | Validity | Notes |
|------|--------------|------------|----------|-------|
| target_chembl_id | 100% | 100% | 100% | PRIMARY KEY |
| uniprot_id_primary | 85% | 98% | 100% | Не для всех targets |
| gene_symbol | 80% | 90% | 95% | Some conflicts |
| organism | 95% | — | 100% | Taxonomy validated |

### Отчеты корреляции

**Между источниками:**
- Accession agreement: ChEMBL vs UniProt (≥90%)
- Gene symbol agreement: ChEMBL vs UniProt vs IUPHAR
- Classification overlap: ChEMBL vs IUPHAR

### Несогласованности

1. **Accession несовпадения**: ChEMBL xref vs UniProt ID Mapping
2. **Targets без components**: пропуск, логирование
3. **Components без sequence**: нормализация к "-"
4. **Семейства несовпадают**: ChEMBL vs GtoPdb (разные классификации)
5. **Дубликаты ключей**: дедупликация по `keep="first"`

---

## 1.9 Примеры использования

### Минимальная команда CLI

```bash
# 1) ChEMBL targets + components
python -m scripts.get_target_data \
  --source chembl \
  --config configs/pipelines/target.yaml \
  --input data/input/target_ids.csv \
  --out data/bronze/targets.parquet
```

### Полный pipeline

```bash
# 1) ChEMBL extraction
python -m scripts.get_target_data \
  --source chembl \
  --config configs/pipelines/target.yaml \
  --input data/input/target_ids.csv \
  --out data/bronze/targets.parquet \
  --write-raw

# 2) UniProt enrichment
python -m scripts.get_target_data \
  --source uniprot \
  --config configs/pipelines/target.yaml \
  --input data/bronze/targets.parquet \
  --out data/silver/targets_uniprot.parquet

# 3) IUPHAR classification
python -m scripts.get_target_data \
  --source iuphar \
  --config configs/pipelines/target.yaml \
  --input data/silver/targets_uniprot.parquet \
  --out data/gold/targets_final.parquet

# 4) Post-processing
python -m scripts.postprocess_targets \
  --input data/gold/targets_final.parquet \
  --output data/output/target/targets.parquet \
  --quality-report data/output/target/targets_quality.csv
```

### Python API

```python
from library.pipelines.target import run_pipeline, TargetPipelineOptions

options = TargetPipelineOptions(
    input_csv=Path("data/input/target_ids.csv"),
    output_csv=Path("data/output/target/targets.csv"),
    command="all",  # ChEMBL + UniProt + IUPHAR
    batch_size=5,
)

result = run_pipeline(config, options)
print(f"Extracted {result.row_count} targets")
print(f"Errors: {result.errors}")
```

---

## Далее

- [08a-target-chembl-extraction.md](./08a-target-chembl-extraction.md) - ChEMBL источник (детали API, batch retrieval, парсинг)
- [08b-target-uniprot-extraction.md](./08b-target-uniprot-extraction.md) - UniProt обогащение (ID Mapping, stream API, enrichment)
- [08c-target-iuphar-extraction.md](./08c-target-iuphar-extraction.md) - IUPHAR/GtoPdb классификация
- [08d-target-orthologs-isoforms.md](./08d-target-orthologs-isoforms.md) - Ортологи и изоформы

