Итоговые требования v2.3+
 

Название: Требования к извлечению данных ChEMBL v2.3+
Версия: 2.3+ (октябрь 2025)
Репозиторий: https://github.com/SatoryKono/bioactivity_data_acquisition/tree/test_refactoring_04

Область применения: извлечение, нормализация и контроль качества данных о биоактивности из ChEMBL и вторичных источников; схема звезды; детерминированные артефакты (CSV, QA-отчёты, meta.yaml).
Статус: одобрен для тестовой среды.

Контроль изменений — CHEMBL-DM02 (обязательно)
Вести таблицу изменений протокола с влиянием на данные.

Этап	Владелец	Дата	Подпись	Влияние на данные
Подготовлен	Хранитель документации	2025-10-21	CHEMBL-DM02	Уточнены источники, схемы, постобработка, кэш, метрики QA
Проверен	Руководитель QA, DataOps	2025-10-21	CHEMBL-DM02	Согласованность, резолюция конфликтов, устойчивость
Одобрен	Руководитель управления ДЗ	2025-10-21	CHEMBL-DM02	Одобрено для тестовой среды

Критерии приемки: каждая версия сопровождается строкой в таблице; каждое изменение имеет зафиксированное влияние.

1. Введение

1.1 Функциональная цель
Обязательно реализовать извлечение, нормализацию и контроль качества данных о биоактивности из ChEMBL с обогащением из PubMed, Semantic Scholar, Crossref, OpenAlex, UniProt, IUPHAR/Guide to Pharmacology, PubChem. Выходные артефакты: CSV, QA-отчёты, meta.yaml.

Критерии приемки: все артефакты сформированы по путям §5.2 и §9.2.

1.2 Модель данных
Схема звезды: facts activities; dimensions documents, targets, assays, test items. Ссылочная целостность обязательна; факты без валидных измерений запрещены к публикации.

Критерии приемки: 0 orphan-строк; все FK валидны.

1.3 Источники и контракты
Использовать официальные REST-эндпоинты, соблюдать пагинацию, лимиты, TTL кэша и политику ошибок согласно §3 и §8.

1.4 Среда выполнения и CI
Зависимости в pyproject.toml, фактические версии в lock-файле. Линтеры и тесты в CI обязательны. Валидация финальных таблиц декларативными схемами.

Критерии приемки: все CI-задачи зелёные; схемы проходят; нарушения в *_failure_cases.csv.

1.4.1 Покрытие тестами
Минимальное совокупное покрытие unit+integration ≥ 85%.

Критерии приемки: при покрытии ниже порога — fail CI.

2. Схемы данных

2.0.1 Порядок колонок и hash_row
Порядок колонок фиксируется спецификацией данного раздела; дополнительные поля допускаются только в конец и не меняют базовый порядок. Порядок сериализации для hash_row равен порядку финального CSV.

2.1 Documents

Столбец	Тип	Ограничения	Описание
document_chembl_id	str	≠ null, уникальный	Ключ ссылочной целостности
doi	str	∪ ∅, валидный DOI	Нормализация и валидация (§3.4.2)
title	str	≠ null	Очистка пробелов и кодировки
journal	str	∪ ∅	Нормализация
year	int	∪ ∅, 1800–2025	Восстановление по приоритету (§3.4.1)
document_pubmed_id	str	∪ ∅	Целое как строка

Критерии приемки: 100% валидных document_chembl_id; источник восстановления year указан; DOI валиден при наличии.

2.2 Targets

Столбец	Тип	Ограничения	Описание
target_chembl_id	str	≠ null, уникальный	Связь с assays и activities
pref_name	str	≠ null	Синонимы унифицированы по UniProt
organism	str	∪ ∅	Нормализация таксономии
uniprot_id	str	∪ ∅, формат UniProt, без isoform	Без суффиксов "-\d+"
protein_class	str	∪ ∅	По IUPHAR

Критерии приемки: ≥ 85% с валидным uniprot_id; 0 неоднозначных маппингов.

2.3 Assays

Столбец	Тип	Ограничения	Описание
assay_chembl_id	str	≠ null, уникальный	
target_chembl_id	str	≠ null, FK → targets	Orphan запрещены
assay_type	str	≠ null	Нормализация словарём ChEMBL
bao_endpoint	str	∪ ∅	Мэппинг по словарю config/dictionary/bao…
description	str	∪ ∅	Очистка HTML
assay_parameters	str	∪ ∅, JSON	Валидация JSON-схемой (§2.3.2)

Критерии приемки: bao_coverage_pct ≥ 80%; assay_parameters валиден; невалидные записи в failure.

2.3.1 BAO coverage и словари
Словарь BAO и мэппинг: config/dictionary/bao_mapping.csv, checksum в config/dictionary/bao_mapping.sha256.

2.3.2 JSON-схема assay_parameters
Допустимые ключи: temperature (float 0..100), pH (float 0..14), buffer (str ≤100), incubation_time_min (float ≥0), substrate (str ≤200), isoform (str ^[A-Z0-9]+(-\d+)?$), mutation (str ≤50), replicate_count (int ≥1), readout (str ≤100). Политика unknown_keys="reject".

Критерии приемки: 100% валидных JSON; причины реджекта протоколируются.

2.4 Test Items

Столбец	Тип	Ограничения	Описание
molecule_chembl_id	str	≠ null, уник	
canonical_smiles	str	∪ ∅	Синтаксическая валидация и канонизация (§4.4.1)
molecule_type	str	∪ ∅	
full_mwt	float	∪ ∅, ≥ 0	Обновление из PubChem
parent_molecule_chembl_id	str	∪ ∅	Группировки

Критерии приемки: 0 невалидных SMILES; full_mwt ≥ 0.

2.5 Activities

Столбец	Тип	Ограничения	Описание
activity_id	str	≠ null, уникальный	
assay_chembl_id	str	≠ null, FK → assays	
molecule_chembl_id	str	≠ null, FK → test items	
target_chembl_id	str	≠ null, FK → targets	
document_chembl_id	str	≠ null, FK → documents	
standard_value	float	∪ ∅, ≥ 0	В nM после конверсии
standard_units	str	∪ ∅, whitelist	См. §2.5.1
pchembl_value	float	∪ ∅	См. §4.6
standard_relation	str	∪ ∅, из {"=", "<", ">", "≤", "≥"}	Управляет интервалами (§4.5)
interval_lower	float	∪ ∅	Нижняя граница в nM
interval_upper	float	∪ ∅	Верхняя граница в nM

2.5.1 Единицы и конверсии к nM

unit	multiplier_to_nM
pM	0.001
nM	1
µM	1000
uM	1000
mM	1_000_000
M	1_000_000_000

Запрещённые единицы: "%", "ratio", "cells/ml" и любые безразмерные текстовые дескрипторы — реджект.

Критерии приемки (2.5): 100% единиц из whitelist; интервалы согласованы; pchembl_value только для "=" и "≤".

3. Источники данных и интеграции

3.1 Таблица источников

Источник	База эндпоинта	Минимум полей	Таблицы
ChEMBL REST	https://www.ebi.ac.uk/chembl/api/data
	document_chembl_id, doi, title, journal, year, document_pubmed_id; target_chembl_id, pref_name, organism; assay_chembl_id, assay_type, bao_endpoint, description; molecule_chembl_id, canonical_smiles, molecule_type, full_mwt, parent_molecule_chembl_id; activity_id, standard_value, standard_units, pchembl_value, standard_relation	Documents, Targets, Assays, Test Items, Activities
PubMed E-utilities	https://eutils.ncbi.nlm.nih.gov/entrez/eutils/
	document_pubmed_id, title, journal, year	Documents
Semantic Scholar Graph	https://api.semanticscholar.org/graph/v1/paper
	title, journal, year	Documents
Crossref REST	https://api.crossref.org/works
	doi, title, journal, year	Documents
OpenAlex	https://api.openalex.org/works
	doi, title, journal, year	Documents
UniProt REST	https://rest.uniprot.org/uniprotkb
	uniprot_id, organism, meta	Targets
IUPHAR/GtoP	https://www.guidetopharmacology.org/webServices
	protein_class	Targets
PubChem PUG-REST	https://pubchem.ncbi.nlm.nih.gov/rest/pug
	canonical_smiles, full_mwt, parent	Test Items

3.2 SLA и лимиты
Тайм-ауты: connect=5s, read=20s, total=25s. Ретраи: 3 для 5xx/сетевых. 429: до 5 ретраев. Backoff t_k=min(2^k,60)s, jitter ±10%. Параллелизм: ≤8 на источник, ≤24 глобально. RPS: ChEMBL≤5; PubMed≤3; UniProt≤3; Crossref≤3; OpenAlex≤3; PubChem≤5; IUPHAR≤2. Бюджеты: ChEMBL≤100k; PubMed≤50k; UniProt≤30k; Crossref≤30k; OpenAlex≤30k; PubChem≤50k; IUPHAR≤10k. Предупреждение на 90%, стоп на 100%. 4xx — сразу failure; 5xx/429 — по ретраям, затем failure.

Критерии приемки: не превышены RPS и бюджеты; отчёт содержит api_calls_used/budget по каждому источнику.

3.3 ChEMBL↔UniProt mapping
Reviewed > unreviewed; сопоставление по organism; при множественных reviewed выбирается запись с совпадающим organism, при равенстве — наиболее свежая по дате обновления. Невозможность однозначного выбора → failure "uniprot_ambiguous". targets.uniprot_id без суффиксов "-\d+".

Критерии приемки: ≥ 85% targets с валидным uniprot_id; 0 неоднозначностей в финале.

3.4 Конфликт-резолюция документов
Порядок приоритета: PubMed > Crossref > OpenAlex > Semantic Scholar. Ключи сопоставления: DOI → PMID → title по Jaccard ≥ 0.6. Нормализация DOI: нижний регистр, удалить "doi:", "https://doi.org/
", пробелы; валидация по regex ^10.\d{4,9}/[-._;()/:A-Z0-9]+$ регистронезависимо. Tie-break по полноте полей {title, journal, year, doi}, при равенстве — PubMed.

3.4.1 Токенизация для Jaccard
lower-case, удалить пунктуацию, нормализовать пробелы, токенизация по словам, удалить стоп-слова {a, an, the, of, and} и русские эквиваленты; метрика Jaccard на множествах токенов.

Критерии приемки: доля конфликтов < 5%; все конфликты помечены conflict=true в отчёте.

3.5 Кэширование
Диск .cache/{source}/; TTL: Crossref/OpenAlex/PubMed=7d; ChEMBL=1d; UniProt/IUPHAR=14d; PubChem=7d. Целевой cache_hit_rate ≥ 95% на повторном запуске в пределах TTL.

3.5.1 Ключ кэша
cache_key = SHA256(base_url + "?" + normalized_query); normalized_query — параметры, отсортированные по ключу; значения trimmed; ключи в нижнем регистре. Для POST JSON — SHA256 сериализованного тела с отсортированными ключами.

Критерии приемки: cache_hit_rate ≥ 95%; при истёкшем TTL выполняется re-fetch.

4. Потоки постобработки

4.1 Общая цепочка
raw → normalized → final. Шаги: нормализация, обогащение, валидация, дедуп по hash_row, вычисление производных, логирование изменений.

Критерии приемки: наличие слоёв и журналов.

4.2 Documents
Тримминг, кодировка, приведение year к int, проверка DOI, обогащение year по §3.4, hash_row, дедуп, фильтр предаторских журналов из config/dictionary/blacklist_journals.txt.

Критерии приемки: отчёт об отказах с причинами; статистика дедупликации.

4.3 Targets
Нормализация имён и таксономии; валидация UniProt; обогащение синонимами и классами; приоритет UniProt; 4xx логировать; hash_row; отчёт покрытия.

Критерии приемки: пороги §5.1 соблюдены.

4.4 Assays
Нормализация, валидация FK; очистка описаний; нормализация assay_type; извлечение assay_parameters и валидация JSON-схемой; hash_row; дедуп; фильтр по BAO coverage.

Критерии приемки: bao_coverage_pct ≥ 80%; некорректный JSON отсутствует в финале.

4.4.1 SMILES
Синтаксическая валидация, канонизация, очистка соль-гидратных меток по внутреннему словарю; при невалидности → failure "smiles_invalid".

Критерии приемки: 0 невалидных SMILES в финале; протоколирование дельт.

4.5 Цензура и интервалы
"=": [v, v]; "<": (0, v]; ">": [v, ∞); "≤": (−∞, v]; "≥": [v, ∞). В хранении −∞ и ∞ → NULL.

Критерии приемки: 100% согласованности relation↔interval.

4.6 Расчёт pChEMBL
pchembl_value = −log10(value_nM × 1e−9). Заполняется для "=" и "≤" (по interval_upper). Округление round-half-to-even до 2 знаков.

Критерии приемки: [0; 20]; наличие только при "=" и "≤".

4.7 Детерминированный экспорт
Фиксированный порядок колонок, сортировка по PK, UTF-8, LF, ".", ≤ 6 значащих цифр, локаль "C", без экспоненты, NA — пустая строка.

Критерии приемки: побайтная идентичность файлов; совпадение checksums.

4.8 hash_row
hash_row=SHA256(serialized_row), serialized_row — значения в порядке финального CSV, разделитель "|", экранирование "|", NULL → "". Коллизии → обе строки в failure "hash_collision".

Критерии приемки: 0 коллизий в финале; протокол коллизий присутствует.

5. Обеспечение качества и валидация

5.1 Пороговые метрики (источник истины)

Метрика	Documents	Targets	Assays	Test Items	Activities
Валидные строки после дедупа	>0	>0	>0	>0	>0
Пропуски в ключевых полях	<10%	<15%	<20%	<5%	<10%
Дубликаты (hash_row)	0%	0%	0%	0%	0%
Покрытие нормализаций/обогащ.	>90%	>85%	>80%	>95%	>90%
BAO coverage (assays)	—	—	≥80%	—	—

Конфигурация хранится в configs/qa_thresholds.yaml. Единственный источник истины.

Критерии приемки: пороги достигнуты; отклонения с планом устранения; при нарушении — fail CI.

5.2 Отчётность и пути артефактов

Финал: outputs/final/{documents|targets|assays|test_items|activities}.csv

QA-таблицы: outputs/qa/<stem>_quality_report_table.csv

Отчёты: outputs/qa/<stem>.postprocess.report.json

Failure: outputs/failures/<stem>_failure_cases.csv

Содержимое отчётов включает cache_hit_rate, api_calls_used/budget, доли цензуры, метрики ретраев, checksums словарей и схем.

Критерии приемки: файлы существуют; JSON валиден.

5.3 Дедупликация
По hash_row; где необходимо — по бизнес-ключам. Коллизии по §4.8.

5.4 Формат чисел
Без экспоненты; ≤ 6 значащих цифр; локаль "C"; round-half-to-even для вычисляемых полей.

Критерии приемки: в CSV нет "e"/"E".

5.5 Гвардрайлы объёма
Если row_count любой финальной таблицы снизился более чем на 10% относительно предыдущего релиза — fail CI с отчётом причин. Увеличение не лимитируется.

Критерии приемки: сравнение с meta.yaml предыдущего релиза; при нарушении — падение CI.

5.6 Таксономия failure codes
unit_blacklisted, unit_unknown, hash_collision, orphan_fk, assay_params_invalid, smiles_invalid, rate_limited, http_5xx_exhausted, http_4xx, uniprot_ambiguous, doi_invalid, bao_missing, cache_key_conflict.

Критерии приемки: 100% строк в *_failure_cases.csv имеют код из словаря.

6. Локализация и документация

docs/en/PROTOCOL_EN.md и docs/ru/PROTOCOL_RU.md — источники истины.
Генерация DOCX на релизе: docs/en/PROTOCOL_EN.docx, docs/ru/PROTOCOL_RU.docx из Markdown. Бинарники в разработке не коммитятся. Русские артефакты проверяются на UTF-8.

Критерии приемки: Markdown синхронизированы; DOCX сгенерированы; UTF-8 валиден.

7. Журнал изменений
Версия	Дата	Автор	Ключевые обновления
2.3	2025-10-21	Хранитель документации	Среда выполнения, TTL-кэш, расширенные схемы/валидаторы, raw/normalized, единицы/pChEMBL, assay_parameters, резолюция источников, QA-чек-лист, FK
2.2	2025-11-01	Хранитель документации	Усилены схемы и постобработка; локализация
2.1	2025-10-15	Совет по релизу DocsOps	Согласование метаданных релиза

Критерии приемки: каждая строка отражает изменения и влияние на данные.

8. Операционные параметры

8.1 Тайм-ауты: connect=5s, read=20s, total=25s.
8.2 Ретраи: retries=3 (для 429 — 5), backoff t_k=min(2^k,60)s, jitter ±10%.
8.3 Параллелизм: ≤ 8 на источник, ≤ 24 глобально.
8.4 Бюджеты: ChEMBL≤100k; PubMed≤50k; UniProt≤30k; Crossref≤30k; OpenAlex≤30k; PubChem≤50k; IUPHAR≤10k. Предупреждение на 90%, стоп на 100%.
8.5 RPS: ChEMBL≤5; PubMed≤3; UniProt≤3; Crossref≤3; OpenAlex≤3; PubChem≤5; IUPHAR≤2.
8.6 Логирование: JSON Lines с {ts, run_id, stage, source, entity, key, level, message, http_status, retry_k}.
8.7 Seed: 0.
8.8 Кэш: §3.5 и §3.5.1; целевой cache_hit_rate ≥ 95%.

Критерии приемки: бюджеты и RPS соблюдены; метрики в отчётах; cache_hit_rate выдержан.

9. Метаданные релиза

9.1 Структура meta.yaml

pipeline_version (semver)

chembl_release (строка)

row_count: {documents, targets, assays, test_items, activities}

checksums: {имя CSV → SHA256 hex}

run_id (UUIDv4)

started_at, finished_at (UTC ISO8601)

9.2 Пути и контроль
outputs/meta/meta.yaml и outputs/meta/meta.sha256. Валидация схемой.

Критерии приемки: все поля заполнены; checksums соответствуют; проверка схемы зелёная.

Приложение A. Справочные ссылки

ChEMBL Data Web Services; NCBI E-utilities; Crossref REST works; OpenAlex Works; UniProt REST UniProtKB; IUPHAR/BPS Guide to Pharmacology web services; PubChem PUG-REST; Pandera DataFrameSchema.
