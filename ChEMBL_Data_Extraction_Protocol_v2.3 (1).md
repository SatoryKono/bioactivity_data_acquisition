# Требования к извлечению данных ChEMBL v2.3+

**Версия:** 2.3+ (2025-10-21)
**Репозиторий:** [https://github.com/SatoryKono/bioactivity_data_acquisition/tree/test_refactoring_04](https://github.com/SatoryKono/bioactivity_data_acquisition/tree/test_refactoring_04)

## Область применения

Выполнить извлечение, нормализацию и контроль качества данных о биоактивности из ChEMBL и вторичных источников; схема звезды; детерминированные артефакты (CSV, QA-отчёты, `meta.yaml`).
**Критерии приемки:** сформированы артефакты согласно §5.2 и §9.2; побайтная идентичность экспорта при повторном прогоне с неизменными входами.

## Статус

Одобрен для тестовой среды.
**Критерии приемки:** строки change control заполнены (§0).

---

## 0. Контроль изменений — CHEMBL-DM02

Вести таблицу изменений протокола с влиянием на данные.

| Этап        | Владелец                   | Дата       | Подпись     | Влияние на данные                                         |
| ----------- | -------------------------- | ---------- | ----------- | --------------------------------------------------------- |
| Подготовлен | Хранитель документации     | 2025-10-21 | CHEMBL-DM02 | Уточнены источники, схемы, постобработка, кэш, метрики QA |
| Проверен    | Руководитель QA, DataOps   | 2025-10-21 | CHEMBL-DM02 | Согласованность, резолюция конфликтов, устойчивость       |
| Одобрен     | Руководитель управления ДЗ | 2025-10-21 | CHEMBL-DM02 | Одобрено для тестовой среды                               |

**Критерии приемки:** все поля заполнены; указано влияние на данные.

---

## 1. Введение

### 1.1 Функциональная цель

Выполнить извлечение, нормализацию и контроль качества данных о биоактивности из ChEMBL с обогащением из PubMed, Semantic Scholar, Crossref, OpenAlex, UniProt, IUPHAR/Guide to Pharmacology, PubChem. Выход: CSV, QA-отчёты, `meta.yaml`.
**Критерии приемки:** перечисленные артефакты созданы по путям §5.2 и §9.2.

### 1.2 Модель данных

Схема звезды: факты `activities`; измерения `documents`, `targets`, `assays`, `test items`. Ссылочная целостность обязательна; факты без валидных FK запрещены к публикации.
**Критерии приемки:** 0 orphan-строк; все FK валидны.

### 1.3 Источники и контракты

Использовать официальные REST-эндпоинты; соблюдать пагинацию, лимиты, TTL кэша, политику ошибок (§3, §8).
**Критерии приемки:** отчёт по API (§3.2) содержит `rps_peak`, `rps_95p`, бюджеты и статусы ошибок.

### 1.4 Среда выполнения и CI

Зависимости в `pyproject.toml`, фактические версии в lock-файле. Обязательны джобы CI: `lint`, `unit`, `integration`, `schema-validate`, `export-determinism`, `thresholds-check`.
Артефакты CI:

* `reports/coverage.xml`
* `reports/lint.txt`
* `reports/test_results.xml`
* `reports/schema_validate.json`
* `reports/export_determinism.json`
* `reports/thresholds_check.json`
* `reports/ci_status.json`

**Критерии приемки:** все джобы PASSED; при нарушениях порогов §5.1, сортировки (§2.0.2), формата чисел (§5.4), guard-rails (§5.5) — fail CI.

#### 1.4.1 Покрытие тестами

Совокупное покрытие unit+integration ≥ 90%.
**Критерии приемки:** при покрытии ниже порога — fail CI (см. `reports/ci_status.json`).

---

## 2. Схемы данных

### 2.0.1 Порядок колонок и `hash_row`

Порядок колонок фиксируется спецификацией §2.1–§2.5; новые поля добавлять в конец. Порядок сериализации для `hash_row` совпадает с финальным CSV.
**Критерии приемки:** стабильная сортировка по PK; побайтная идентичность.

### 2.0.2 Первичные ключи и сортировка

PK: Documents=`document_chembl_id`, Targets=`target_chembl_id`, Assays=`assay_chembl_id`, Test Items=`molecule_chembl_id`, Activities=`activity_id`. Экспорт CSV сортировать строго по PK по возрастанию. Нарушения → `<stem>_failure_cases.csv` с `sort_violation`.
**Критерии приемки:** финальные CSV отсортированы по PK.

### 2.1 Documents

| Столбец            | Тип | Ограничения                | Описание                        |
| ------------------ | --- | -------------------------- | ------------------------------- |
| document_chembl_id | str | ≠ null, уникальный         | Ключ ссылочной целостности      |
| doi                | str | ∪ ∅, валидный DOI          | Нормализация и валидация (§3.4) |
| title              | str | ≠ null                     | Очистка пробелов и кодировки    |
| journal            | str | ∪ ∅                        | Нормализация                    |
| year               | int | ∪ ∅, 1800–(current_year+1) | Восстановление по §3.4          |
| document_pubmed_id | str | ∪ ∅, целое как строка      | PMID                            |

**Критерии приемки:** 100% валидных `document_chembl_id`; `year` из доверенного источника; `doi` валиден при наличии.

### 2.2 Targets

| Столбец          | Тип | Ограничения        | Описание                      |
| ---------------- | --- | ------------------ | ----------------------------- |
| target_chembl_id | str | ≠ null, уникальный | Связь с assays и activities   |
| pref_name        | str | ≠ null             | Синонимы унифицированы        |
| organism         | str | ∪ ∅                | Нормализация таксономии       |
| uniprot_id       | str | ∪ ∅, регэксп       | UniProt accession без изоформ |

**Регэксп `uniprot_id`:**

```
^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9])$
```

Запрещены суффиксы `-\d+` (удалить, исходник логировать в `outputs/qa/uniprot_normalization.log`).
**Критерии приемки:** ≥85% валидных `uniprot_id` по паттерну; 0 неоднозначностей (§3.3); 0 суффиксов.

### 2.3 Assays

| Столбец          | Тип       | Ограничения          | Описание                     |
| ---------------- | --------- | -------------------- | ---------------------------- |
| assay_chembl_id  | str       | ≠ null, уникальный   |                              |
| target_chembl_id | str       | ≠ null, FK → targets | Orphan запрещены             |
| assay_type       | str       | ≠ null               | Нормализация словарём ChEMBL |
| bao_endpoint     | str       | ∪ ∅                  | Мэппинг по словарю           |
| description      | str       | ∪ ∅                  | Очистка HTML                 |
| assay_parameters | str(JSON) | ∪ ∅, схема §2.3.2    | Биоконтекст                  |

**Критерии приемки:** `bao_coverage_pct ≥ 80%`; валидный JSON; invalid → failures.

#### 2.3.1 BAO coverage и словари

Справочник `config/dictionary/bao_mapping.csv`, контроль `bao_mapping.sha256`.
**Критерии приемки:** контроль целостности SHA256.

#### 2.3.2 JSON-схема `assay_parameters` v1.1

Draft-07, `additionalProperties=false`; ключи и домены фиксированы. Версия схемы и её SHA256 фиксируются в `outputs/qa/assay_parameters.schema.sha256`.
Минимальный набор свойств (выдержка, полный список в схеме):

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "assay_parameters v1.1",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "organism_strain": {"type":"string","minLength":1},
    "cell_line": {"type":"string"},
    "tissue": {"type":"string"},
    "target_isoform": {"type":"string","pattern":"^[A-Z0-9]+(?:-[0-9]+)?$"},
    "variant_mutation": {"type":"string","pattern":"^[A-Z][0-9]+[A-Z]$"},
    "temperature_C": {"type":"number","minimum":0,"maximum":60},
    "pH": {"type":"number","minimum":0,"maximum":14},
    "buffer": {"type":"string"},
    "detection_technique": {"type":"string"},
    "readout": {"type":"string"},
    "replicates_n": {"type":"integer","minimum":1,"maximum":1000},
    "concentration_range": {
      "type":"object","additionalProperties":false,
      "properties":{
        "lower":{"type":["number","null"],"minimum":0},
        "upper":{"type":["number","null"],"minimum":0},
        "units":{"type":"string","enum":["pM","nM","uM","mM","M"]}
      },
      "required":["units"]
    }
  }
}
```

**Критерии приемки:** 100% JSON валидны; invalid → `assay_params_invalid`; мягкий KPI полноты: ≥4 заполненных ключа, в отчёте QA.

### 2.4 Test Items

| Столбец                   | Тип   | Ограничения        | Описание                       |
| ------------------------- | ----- | ------------------ | ------------------------------ |
| molecule_chembl_id        | str   | ≠ null, уникальный |                                |
| canonical_smiles          | str   | ∪ ∅                | Синтакс. валидация/канонизация |
| molecule_type             | str   | ∪ ∅                |                                |
| full_mwt                  | float | ∪ ∅, ≥0            | Обновление из PubChem          |
| parent_molecule_chembl_id | str   | ∪ ∅                | Группировки                    |

**Критерии приемки:** 0 невалидных SMILES; `full_mwt ≥ 0`.

### 2.5 Activities

| Столбец            | Тип   | Ограничения                   | Описание              |
| ------------------ | ----- | ----------------------------- | --------------------- |
| activity_id        | str   | ≠ null, уникальный            |                       |
| assay_chembl_id    | str   | ≠ null, FK → assays           |                       |
| molecule_chembl_id | str   | ≠ null, FK → test items       |                       |
| target_chembl_id   | str   | ≠ null, FK → targets          |                       |
| document_chembl_id | str   | ≠ null, FK → documents        |                       |
| standard_value     | float | ∪ ∅, ≥0, в nM после конверсии |                       |
| standard_units     | str   | ∪ ∅, whitelist                | См. §2.5.1            |
| pchembl_value      | float | ∪ ∅                           | См. §4.6              |
| standard_relation  | str   | ∪ ∅, {"=","<",">","≤","≥"}    | Управляет интервалами |
| interval_lower     | float | ∪ ∅                           | Нижняя граница (nM)   |
| interval_upper     | float | ∪ ∅                           | Верхняя граница (nM)  |

#### 2.5.1 Единицы и конверсии к nM

Входной регэксп:

```
^(pM|nM|uM|µM|μM|mM|M)$
```

Нормализовать `µ/μ → u`; итоговый домен: `{"pM","nM","uM","mM","M"}`.
Конверсии: pM→0.001; nM→1; uM→1000; mM→1_000_000; M→1_000_000_000.
Запрещённые единицы → `unit_unknown`, запись исключить из финала.
Whitelist endpoints (регистрозависимо): `{"IC50","Ki","Kd","EC50","AC50","pIC50","pKi"}`; прочее → `endpoint_blacklisted`.
**Критерии приемки:** 100% единиц в домене; нет сырых `µ/μ` в финале; интервалы согласованы; `pchembl_value` только для `"="` и `"≤"`.

---

## 3. Источники данных и интеграции

### 3.1 Таблица источников

Структура без изменения семантики; поля источников, базовые URL, параметры, ключи аутентификации при необходимости.
**Критерии приемки:** таблица присутствует в документации и в отчёте постобработки.

### 3.2 SLA и лимиты

Тайм-ауты: connect=5s, read=20s, total=25s.
Ретраи: 5xx/сеть — до 3; 429 — до 5; backoff `t_k=min(2^k,60)s`, jitter ±10%.
Параллелизм: ≤8 на источник, ≤24 глобально.
RPS: ChEMBL≤5; PubMed≤3; UniProt≤3; Crossref≤3; OpenAlex≤3; PubChem≤5; IUPHAR≤2.
Бюджеты: ChEMBL≤100k; PubMed≤50k; UniProt≤30k; Crossref≤30k; OpenAlex≤30k; PubChem≤50k; IUPHAR≤10k.
Красные зоны: ≥90% warn; >100% fail. 4xx — immediate fail; 5xx/429 — после ретраев fail.
Отчёт: `outputs/qa/api_usage.report.json` со схемой:

```json
{ "source": "...", "calls_used": 0, "budget": 0, "rps_peak": 0.0, "rps_95p": 0.0,
  "retries_total": 0, "http_2xx": 0, "http_4xx": 0, "http_5xx": 0, "http_429": 0 }
```

RPS считать на окне 60 s, шаг 1 s.
**Критерии приемки:** бюджеты и RPS не превышены; отчёт валиден.

### 3.3 ChEMBL↔UniProt mapping

Приоритет reviewed > unreviewed; обязательный match по `organism`; при множественных reviewed с одинаковым `organism` — выбрать запись с максимальным `updated_date` (UniProt). Невозможность однозначного выбора → `uniprot_ambiguous`. `targets.uniprot_id` без `-\d+`; удалённый суффикс логировать в `outputs/qa/uniprot_normalization.log`.
**Критерии приемки:** ≥85% targets с валидным `uniprot_id`; 0 неоднозначностей.

### 3.4 Конфликт-резолюция документов

Приоритет: PubMed > Crossref > OpenAlex > Semantic Scholar.
Ключи сопоставления: DOI → PMID → title по Jaccard ≥ 0.60 (lower-case, без пунктуации).
Нормализация DOI: lower-case; удалить `doi:`, `https://doi.org/`, пробелы.
Валидация DOI:

```
(?i)^10\.\d{4,9}/[-._;()/:A-Z0-9]+$
```

Отчёт: `outputs/qa/document_conflicts.jsonl` (JSON Lines) с полями и `chosen_source`.
**Критерии приемки:** `conflict_rate ≤ 0.05` (до 4 знаков, half-even); при превышении — fail.

### 3.5 Кэширование

`.cache/{source}/`, TTL: ChEMBL=1d; PubMed/Crossref/OpenAlex=7d; UniProt/IUPHAR=14d; PubChem=7d.
Ключ кэша: `SHA256(base_url + "?" + normalized_query)`; для POST — хеш отсортированного JSON. Цель `cache_hit_rate ≥ 95%` при повторном запуске в пределах TTL.
**Критерии приемки:** достигнут hit rate; TTL соблюдён.

---

## 4. Потоки постобработки

### 4.1 Общая цепочка

`raw → normalized → final`: нормализация, обогащение, валидация, дедуп по `hash_row`, производные, логирование изменений.
**Критерии приемки:** слои и журналы присутствуют; отчёты собраны.

### 4.2 Documents

Тримминг, кодировка; `year` → int; проверка DOI; восстановление `year` (§3.4); фильтр предаторских журналов `config/dictionary/blacklist_journals.txt` (SHA256). Финал: `hash_row`, дедуп, отчёт.
**Критерии приемки:** отчёт отказов; статистика дедупликации; `blacklisted_count` и примеры.

### 4.3 Targets

Нормализация имён и таксономии; валидация формата UniProt; обогащение синонимами/классами; приоритет UniProt; 4xx логируются; финал: `hash_row`, отчёт покрытия.
**Критерии приемки:** пороги §5.1 соблюдены; изоформ-суффиксы отсутствуют.

### 4.4 Test Items

Нормализация, валидация FK; SMILES: синтаксическая валидация и канонизация; очистка соль/гидрат по словарю `config/dictionary/salts_hydrates.csv` (SHA256). Невалидность → `smiles_invalid`. Финал: `hash_row`, дедуп.
**Критерии приемки:** 0 `smiles_invalid`; протокол дельт присутствует.

### 4.5 Цензура и интервалы

Норма `v` в nM.

* `"="` → `[v, v]`
* `"<"` → `(NULL, v)`
* `">"` → `(v, NULL)`
* `"≤"` → `(NULL, v]`
* `"≥"` → `[v, NULL)`
  Бесконечности не записывать; вместо ±∞ использовать `NULL`.
  **Контроль предикатами:**

```sql
(interval_lower IS NULL OR interval_lower >= 0)
AND (interval_upper IS NULL OR interval_upper >= 0)
AND (interval_lower IS NULL OR interval_upper IS NULL OR interval_lower <= interval_upper)
```

Нарушения → `interval_inconsistent`.
**Критерии приемки:** 100% согласованность relation ↔ интервалы; 0 случаев `interval_lower > interval_upper`.

### 4.6 Расчёт pChEMBL и лог-метрики

Если вход в pIC50/pKi: `value_nM = 10^(9 − pValue)`.
`pchembl_value = −log10(value_nM × 1e−9)`; заполнять для `"="` и `"≤"` (для `"≤"` использовать `interval_upper`). Округление half-even до 2 знаков; диапазон [0;20]; вне диапазона → `pchembl_out_of_range`.
**Критерии приемки:** отсутствует `pchembl_value` при relation ∈ {"<",">","≥"}; диапазон соблюдён.

### 4.7 Детерминированный экспорт

Кодировка UTF-8; LF; разделитель `,`; кавычки `"`; NA → `""`. Числа рендерить как `"%.6g"`; запрет экспоненты; локаль "C"; до 6 значащих цифр. Сортировка по PK; фиксированный порядок колонок.
**Критерии приемки:** побайтная идентичность; отсутствие `[eE]` в числовых полях; `reports/export_determinism.json = PASSED`.

### 4.8 `hash_row`

Сериализация значений в порядке финального CSV; разделитель для хеша `|`; экранирование внутренних `|` как `\|`; `NULL → ""`; числа как в §4.7.
`hash = sha256(utf8(serialized_line))` в hex нижним регистром.
Коллизии → failures `hash_collision` с `serialized_line`.
**Критерии приемки:** 0 коллизий; протокол коллизий присутствует.

---

## 5. Обеспечение качества и валидация

### 5.1 Пороговые метрики (единый источник)

`configs/qa_thresholds.yaml` (приоритет над текстом).

| Метрика                       | Documents | Targets | Assays | Test Items | Activities |
| ----------------------------- | --------: | ------: | -----: | ---------: | ---------: |
| Валидные строки после дедупа  |        >0 |      >0 |     >0 |         >0 |         >0 |
| Пропуски в ключевых полях     |      <10% |    <15% |   <20% |        <5% |       <10% |
| Дубликаты (`hash_row`)        |        0% |      0% |     0% |         0% |         0% |
| Покрытие нормализаций/обогащ. |      >90% |    >85% |   >80% |       >95% |       >90% |
| BAO coverage (assays)         |         — |       — |   ≥80% |          — |          — |

**Критерии приемки:** `reports/thresholds_check.json = PASSED`; при нарушении — fail CI.

### 5.2 Отчётность и пути артефактов

* Финал: `outputs/final/{documents|targets|assays|test_items|activities}.csv`
* QA-таблицы: `outputs/qa/<stem>_quality_report_table.csv`
* Пост-отчёты: `outputs/qa/<stem>.postprocess.report.json`
* Failures: `outputs/failures/<stem>_failure_cases.csv`
* Метаданные: `outputs/meta/meta.yaml`, `outputs/meta/meta.sha256`
* API-отчёт: `outputs/qa/api_usage.report.json`
* Конфликты документов: `outputs/qa/document_conflicts.jsonl`
* SHA256 ресурсов словарей/схем — рядом с ресурсами
  **Критерии приемки:** все файлы существуют; JSON валиден; SHA256 совпадает.

### 5.3 Дедупликация

По `hash_row`; при необходимости — по бизнес-ключам. Коллизии — см. §4.8.
**Критерии приемки:** 0% дубликатов.

### 5.4 Формат чисел

Без экспоненты; ≤6 значащих цифр; локаль "C"; round-half-to-even.
**Критерии приемки:** отсутствуют `[eE]` в CSV.

### 5.5 Guard-rails объёма

Если `row_count` финальной таблицы снизился строго более чем на 10.0% относительно ближайшего предыдущего успешного релиза (`meta.yaml`) — fail CI; отчёт `reports/rowcount_regression.json`. Допустимы waiver в `configs/waivers.yaml` с полями `{id, reason, scope, expires_at, approver}`.
**Критерии приемки:** либо соблюдено, либо waiver валиден.

### 5.6 Таксономия failure codes

Словарь в `configs/failure_codes.yaml` с `severity ∈ {blocker, major, minor}` и `configs/failure_codes.sha256`. Минимальный набор:
`unit_blacklisted, unit_unknown, hash_collision, orphan_fk, assay_params_invalid, smiles_invalid, rate_limited, http_5xx_exhausted, http_4xx, uniprot_ambiguous, doi_invalid, bao_missing, endpoint_blacklisted, cache_key_conflict, sort_violation, pchembl_out_of_range, interval_inconsistent`.
**Критерии приемки:** 100% строк в failures имеют код из словаря; SHA256 совпадает.

---

## 6. Локализация и документация

`docs/en/PROTOCOL_EN.md`, `docs/ru/PROTOCOL_RU.md` — источники истины; генерация DOCX из Markdown; бинарники не коммитить. Русские артефакты — UTF-8.
**Критерии приемки:** Markdown синхронизированы; DOCX сгенерированы; UTF-8 валиден.

## 7. Журнал изменений

| Версия | Дата       | Автор                  | Ключевые обновления                                                                 |
| ------ | ---------- | ---------------------- | ----------------------------------------------------------------------------------- |
| 2.2    | 2025-10-01 | Хранитель документации | Усилены схемы и постобработка; локализация                                          |
| 2.3    | 2025-10-21 | Хранитель документации | Среда выполнения, TTL-кэш, валидаторы, raw/normalized, единицы/pChEMBL, BAO, QA, FK |

**Критерии приемки:** строки отражают изменения и влияние на данные; хронология монотонна.

## 8. Операционные параметры

Логирование: JSON Lines `{ts, run_id, stage, source, entity, key, level, message, http_status, retry_k, cache_hit, rps_bucket}`. Seed: 0. SLA/RPS/TTL см. §3.2 и §3.5.
**Критерии приемки:** бюджеты и RPS соблюдены; метрики в отчётах; `cache_hit_rate ≥ 95%`.

## 9. Метаданные релиза

### 9.1 Структура `meta.yaml`

Обязательные поля:
`pipeline_version (semver)`, `chembl_release (строка)`, `row_count`, `checksums`, `run_id (UUIDv4)`, `started_at`, `finished_at`, `current_year`, `chembl_release_source ∈ {cli,status}`.
Валидировать по JSON Schema v1.0 (преобразуя YAML в JSON), с правилом `finished_at ≥ started_at`.
**Критерии приемки:** `meta.yaml` валиден схеме.

### 9.2 Пути и контроль

`outputs/meta/meta.yaml`, `outputs/meta/meta.sha256` (hex от `meta.yaml`).
**Критерии приемки:** все поля заполнены; checksums соответствуют; схема зелёная.

---

## Приложение A. Справочные ссылки

* ChEMBL Data Web Services
* NCBI E-utilities
* Crossref REST Works
* OpenAlex Works

**Критерии приемки:** ссылки перечислены; не влияют на CI.
